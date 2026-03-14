"""
StoreBridge — streams object store events into Deephaven ticking tables
and any registered EventSinks.

The bridge is a library, not a service. Embed it in:
- The store server process (always-on, central bridging)
- A user process (personal filters, pattern-3 direct push)

Usage:
    bridge = StoreBridge(host=host, port=port, dbname=dbname,
                         user="bridge_user", password="bridge_pw")
    bridge.register(Order)
    bridge.register(Trade, filter=lambda d: d.get("symbol") == "AAPL")

    # Optional: add extra sinks
    bridge.add_sink(LakehouseSink(catalog))

    bridge.start()

    orders_raw  = bridge.table(Order)
    orders_live = orders_raw.last_by("EntityId")   # auto-locked!
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any

from store.admin import EventBus, SubscriptionListener
from streaming import TickingTable

from bridge.sinks import EventSink
from bridge.type_mapping import extract_row, infer_schema
from store import ChangeEvent, Storable, connect


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

    def __init__(self, storable_cls: type[Storable], ticking: TickingTable, column_names: list[str], filter_expr: Callable[[dict], bool] | None) -> None:
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
                 subscriber_id: str | None = "deephaven_bridge") -> None:
        # Store raw connection params — resolved via connect() in start()
        self._alias_or_host = alias_or_host
        self._host = host
        self._port = port or 5432
        self._dbname = dbname or "postgres"
        self._user = user
        self._password = password
        self._subscriber_id = subscriber_id
        self._registrations: dict[str, _Registration] = {}  # type_name → reg
        self._sinks: list[EventSink] = []  # pluggable extra sinks
        self._bus = EventBus()
        self._listener: SubscriptionListener | None = None
        self._conn: Any = None  # set in start()
        self._started = False

    def _require_connection(self) -> Any:
        assert self._conn is not None, "StoreBridge not started"
        return self._conn

    # ── Sinks ─────────────────────────────────────────────────────

    def add_sink(self, sink: EventSink) -> None:
        """Register an EventSink to receive all ChangeEvents.

        Must be called before start(). Sinks receive raw ChangeEvents
        in addition to the Deephaven ticking-table dispatch.

        Args:
            sink: An EventSink (e.g. LakehouseSink).
        """
        if self._started:
            raise RuntimeError("Cannot add sinks after start()")
        self._sinks.append(sink)

    # ── Registration ─────────────────────────────────────────────────

    def register(self, storable_cls: type[Storable], *, filter: Callable[[dict], bool] | None = None, columns: dict | None = None) -> None:
        """Register a Storable type to be bridged to a ticking table.

        Args:
            storable_cls: @dataclass Storable subclass (e.g. Order, Trade).
            filter: Optional predicate ``(dict) -> bool``. Only events where
                    ``filter(obj_data_dict)`` is truthy are shipped.
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

        # Connect via public API — connect() handles alias resolution
        assert self._user is not None, "StoreBridge requires user"
        assert self._password is not None, "StoreBridge requires password"
        self._conn = connect(
            self._alias_or_host,
            host=self._host,
            port=self._port,
            dbname=self._dbname,
            user=self._user,
            password=self._password,
        )

        # Wire EventBus → _dispatch
        self._bus.on_all(self._dispatch)

        # Start SubscriptionListener (PG LISTEN/NOTIFY + durable catch-up)
        # SubscriptionListener needs raw PG params — use resolved conn params
        conn_params = self._conn._conn_params  # resolved by connect()
        self._listener = SubscriptionListener(
            event_bus=self._bus,
            subscriber_id=self._subscriber_id,
            **conn_params,
        )
        self._listener.start()
        self._started = True

    def stop(self) -> None:
        """Stop listening and clean up."""
        if self._listener:
            self._listener.stop()
            self._listener = None
        # Flush all sinks before closing
        for sink in self._sinks:
            try:
                sink.close()
            except Exception:
                pass
        for reg in self._registrations.values():
            try:
                reg.ticking.close()
            except Exception:
                pass
        if self._conn:
            self._conn.close()
            self._conn = None
        self._started = False

    # ── Internal dispatch ────────────────────────────────────────────

    def _dispatch(self, event: ChangeEvent) -> None:
        """Called for every ChangeEvent. Route to sinks and ticking tables."""
        # Dispatch to all registered EventSinks (LakehouseSink, etc.)
        for sink in self._sinks:
            try:
                sink.on_event(event)
            except Exception:
                pass

        # Dispatch to Deephaven ticking tables (existing behavior)
        reg = self._registrations.get(event.type_name)
        if reg is None:
            return  # Not a registered type — ignore

        try:
            # Activate bridge connection in this thread (may be listener's bg thread)
            self._conn.activate()
            obj = reg.read_cls.find(event.entity_id)
        except Exception:
            return  # Object not readable (deleted, permission, etc.)

        # Apply filter predicate if configured
        if reg.filter_expr is not None:
            try:
                obj_data: dict[str, Any] = dataclasses.asdict(obj) if dataclasses.is_dataclass(obj) else {}  # type: ignore[arg-type]
                if not reg.filter_expr(obj_data):
                    return
            except Exception:
                return  # Filter evaluation error — skip

        # Extract row values and write to the ticking table.
        # write_row() is thread-safe per DH docs.
        row = extract_row(obj, reg.column_names)
        reg.ticking.write_row(*row)
