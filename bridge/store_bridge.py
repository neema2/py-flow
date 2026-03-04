"""
StoreBridge — streams object store events into Deephaven ticking tables.

The bridge is a library, not a service. Embed it in:
- The store server process (always-on, central bridging)
- A user process (personal filters, pattern-3 direct push)

Usage:
    bridge = StoreBridge(host=host, port=port, dbname=dbname,
                         user="bridge_user", password="bridge_pw")
    bridge.register(Order)
    bridge.register(Trade, filter=Field("symbol") == Const("AAPL"))
    bridge.start()

    orders_raw  = bridge.table(Order)
    orders_live = orders_raw.last_by("EntityId")   # auto-locked!
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any

from store.base import Storable
from store.client import StoreClient
from store.subscriptions import ChangeEvent, EventBus, SubscriptionListener
from streaming import TickingTable

from bridge.type_mapping import extract_row, infer_schema

if TYPE_CHECKING:
    from reactive.expr import Expr


class _Registration:
    """Internal: tracks a registered Storable type and its TickingTable."""

    __slots__ = (
        "column_names",
        "filter_expr",
        "read_cls",
        "storable_cls",
        "ticking",
        "type_name",
    )

    def __init__(self, storable_cls: type[Storable], ticking: TickingTable, column_names: list[str], filter_expr: Expr | None) -> None:
        self.storable_cls = storable_cls
        self.type_name = storable_cls.type_name()
        self.ticking = ticking
        self.column_names = column_names
        self.filter_expr = filter_expr
        self.read_cls = storable_cls


class StoreBridge:
    """Streams object store ChangeEvents into Deephaven ticking tables.

    One DynamicTableWriter per registered Storable type. Events arrive
    via SubscriptionListener (PG LISTEN/NOTIFY) and are dispatched to
    the appropriate writer after optional Expr filtering.

    Usage::

        # Via alias (registered by StoreServer)
        bridge = StoreBridge("demo", user="alice", password="pw")

        # Explicit params (backward compat)
        bridge = StoreBridge(host="/tmp/pg", port=5432, dbname="postgres",
                             user="alice", password="pw")
    """

    def __init__(self, alias_or_host: str | None = None, port: int | None = None, dbname: str | None = None,
                 user: str | None = None, password: str | None = None, *,
                 host: str | None = None,
                 subscriber_id: str = "deephaven_bridge") -> None:
        # Resolve alias vs explicit params
        if alias_or_host is not None and port is None and host is None:
            # Looks like an alias — try to resolve
            from store.connection import _resolve_alias
            resolved = _resolve_alias(alias_or_host)
            if resolved is not None:
                self._conn_params = dict(
                    host=resolved["host"], port=resolved["port"],
                    dbname=resolved.get("dbname", "postgres"),
                    user=user, password=password,
                )
            else:
                # Treat as explicit host (backward compat)
                self._conn_params = dict(
                    host=alias_or_host, port=port or 5432,
                    dbname=dbname or "postgres",
                    user=user, password=password,
                )
        else:
            self._conn_params = dict(
                host=host or alias_or_host, port=port or 5432,
                dbname=dbname or "postgres",
                user=user, password=password,
            )
        self._subscriber_id = subscriber_id
        self._registrations: dict[str, _Registration] = {}  # type_name → reg
        self._bus = EventBus()
        self._listener: SubscriptionListener | None = None
        self._client: StoreClient | None = None
        self._started = False

    def _require_client(self) -> StoreClient:
        assert self._client is not None, "StoreBridge not started"
        return self._client

    # ── Registration ─────────────────────────────────────────────────

    def register(self, storable_cls: type[Storable], *, filter: Expr | None = None, columns: dict | None = None) -> None:
        """Register a Storable type to be bridged to a ticking table.

        Args:
            storable_cls: @dataclass Storable subclass (e.g. Order, Trade).
            filter: Optional Expr predicate. Only events where
                    filter.eval(obj_data_dict) is truthy are shipped.
            columns: Optional dict override for column schema
                     (Python types). If None, auto-generated from
                     dataclass fields.
        """
        if self._started:
            raise RuntimeError("Cannot register types after start()")

        schema = columns if columns is not None else infer_schema(storable_cls)
        column_names = list(schema.keys())
        tt = TickingTable(schema)

        reg = _Registration(
            storable_cls=storable_cls,
            ticking=tt,
            column_names=column_names,
            filter_expr=filter,
        )
        self._registrations[reg.type_name] = reg

    def table(self, storable_cls: type[Storable]) -> TickingTable:
        """Return the TickingTable for a registered type.

        The returned TickingTable supports auto-locked derivations::

            raw = bridge.table(Order)
            live = raw.last_by("EntityId")   # auto shared_lock
        """
        type_name = storable_cls.type_name()
        reg = self._registrations.get(type_name)
        if reg is None:
            raise KeyError(f"{storable_cls.__name__} is not registered")
        return reg.ticking

    # ── Lifecycle ────────────────────────────────────────────────────

    def start(self) -> None:
        """Start listening for store events and bridging to Deephaven."""
        if self._started:
            return

        # StoreClient for read-back (uses same PG connection params)
        self._client = StoreClient(**self._conn_params)

        # Wire EventBus → _dispatch
        self._bus.on_all(self._dispatch)

        # Start SubscriptionListener (PG LISTEN/NOTIFY + durable catch-up)
        self._listener = SubscriptionListener(
            event_bus=self._bus,
            subscriber_id=self._subscriber_id,
            **self._conn_params,
        )
        self._listener.start()
        self._started = True

    def stop(self) -> None:
        """Stop listening and clean up."""
        if self._listener:
            self._listener.stop()
            self._listener = None
        if self._client:
            self._client.close()
            self._client = None
        self._started = False

    # ── Internal dispatch ────────────────────────────────────────────

    def _dispatch(self, event: ChangeEvent) -> None:
        """Called for every ChangeEvent. Route to the correct writer."""
        reg = self._registrations.get(event.type_name)
        if reg is None:
            return  # Not a registered type — ignore

        try:
            # Read back the full object from the store
            obj = self._require_client().read(reg.read_cls, event.entity_id)
        except Exception:
            return  # Object not readable (deleted, permission, etc.)

        # Apply Expr filter if configured
        if reg.filter_expr is not None:
            try:
                obj_data: dict[str, Any] = dataclasses.asdict(obj) if dataclasses.is_dataclass(obj) else {}  # type: ignore[arg-type]
                if not reg.filter_expr.eval(obj_data):
                    return
            except Exception:
                return  # Filter evaluation error — skip

        # Extract row values and write to the ticking table.
        # write_row() is thread-safe per DH docs.
        row = extract_row(obj, reg.column_names)
        reg.ticking.write_row(*row)
