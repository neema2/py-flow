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
    orders_live = orders_raw.last_by("EntityId")
"""

import json
import dataclasses
import threading
from typing import Optional, Dict, Type, Any

from deephaven import DynamicTableWriter

from store.base import Storable
from store.client import StoreClient
from store.subscriptions import EventBus, ChangeEvent, SubscriptionListener
from bridge.type_mapping import infer_dh_schema, extract_row


class _Registration:
    """Internal: tracks a registered Storable type and its writer."""

    __slots__ = ("storable_cls", "type_name", "writer", "table",
                 "column_names", "filter_expr", "read_cls")

    def __init__(self, storable_cls, writer, table, column_names, filter_expr):
        self.storable_cls = storable_cls
        self.type_name = storable_cls.type_name()
        self.writer = writer
        self.table = table
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

    def __init__(self, alias_or_host=None, port=None, dbname=None,
                 user=None, password=None, *,
                 host=None,
                 subscriber_id="deephaven_bridge"):
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
        self._registrations: Dict[str, _Registration] = {}  # type_name → reg
        self._bus = EventBus()
        self._listener: Optional[SubscriptionListener] = None
        self._client: Optional[StoreClient] = None
        self._started = False

    # ── Registration ─────────────────────────────────────────────────

    def register(self, storable_cls, *, filter=None, columns=None, writer=None):
        """Register a Storable type to be bridged to Deephaven.

        Args:
            storable_cls: @dataclass Storable subclass (e.g. Order, Trade).
            filter: Optional Expr predicate. Only events where
                    filter.eval(obj_data_dict) is truthy are shipped.
            columns: Optional dict override for DH column schema.
                     If None, auto-generated from dataclass fields.
            writer: Optional pre-created DynamicTableWriter. If None,
                    one is created from the inferred/provided schema.
        """
        if self._started:
            raise RuntimeError("Cannot register types after start()")

        schema = columns if columns is not None else infer_dh_schema(storable_cls)
        column_names = list(schema.keys())

        if writer is None:
            writer = DynamicTableWriter(schema)

        reg = _Registration(
            storable_cls=storable_cls,
            writer=writer,
            table=writer.table,
            column_names=column_names,
            filter_expr=filter,
        )
        self._registrations[reg.type_name] = reg

    def table(self, storable_cls):
        """Return the raw (append-only) Deephaven table for a registered type."""
        type_name = storable_cls.type_name()
        reg = self._registrations.get(type_name)
        if reg is None:
            raise KeyError(f"{storable_cls.__name__} is not registered")
        return reg.table

    # ── Lifecycle ────────────────────────────────────────────────────

    def start(self):
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

    def stop(self):
        """Stop listening and clean up."""
        if self._listener:
            self._listener.stop()
            self._listener = None
        if self._client:
            self._client.close()
            self._client = None
        self._started = False

    # ── Internal dispatch ────────────────────────────────────────────

    def _dispatch(self, event: ChangeEvent):
        """Called for every ChangeEvent. Route to the correct writer."""
        reg = self._registrations.get(event.type_name)
        if reg is None:
            return  # Not a registered type — ignore

        try:
            # Read back the full object from the store
            obj = self._client.read(reg.read_cls, event.entity_id)
        except Exception:
            return  # Object not readable (deleted, permission, etc.)

        # Apply Expr filter if configured
        if reg.filter_expr is not None:
            try:
                obj_data = dataclasses.asdict(obj) if dataclasses.is_dataclass(obj) else {}
                if not reg.filter_expr.eval(obj_data):
                    return
            except Exception:
                return  # Filter evaluation error — skip

        # Extract row values and write to DH
        row = extract_row(obj, reg.column_names)
        reg.writer.write_row(*row)
