"""
Event subscription system for the object store.

Public API:
    EventListener  — unified event listener (in-process or durable PG LISTEN).
    ChangeEvent    — notification payload dataclass.

Internal:
    EventBus              — in-process dispatch (used by EventListener + StoreBridge).
    SubscriptionListener  — PG LISTEN/NOTIFY thread (used by EventListener + StoreBridge).
"""

from __future__ import annotations

import json
import logging
import select
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extensions

logger = logging.getLogger(__name__)

NOTIFY_CHANNEL = "object_events"


@dataclass
class ChangeEvent:
    """Notification payload for an entity change."""
    entity_id: str
    version: int
    event_type: str          # CREATED / UPDATED / DELETED / STATE_CHANGE / CORRECTED
    type_name: str
    updated_by: str
    state: str | None
    tx_time: datetime


class EventBus:
    """
    In-process pub/sub for entity change events.

    Subscribe by type, by entity_id, or catch-all.
    Thread-safe for concurrent emit/subscribe.
    """

    def __init__(self) -> None:
        self._type_listeners: dict[str, list[Callable]] = {}       # type_name → [callback]
        self._entity_listeners: dict[str, list[Callable]] = {}     # entity_id → [callback]
        self._all_listeners: list[Callable] = []        # [callback]
        self._lock = threading.Lock()

    def on(self, type_name: str, callback: Callable) -> None:
        """Subscribe to all changes for a given type_name."""
        with self._lock:
            self._type_listeners.setdefault(type_name, []).append(callback)

    def on_entity(self, entity_id: str, callback: Callable) -> None:
        """Subscribe to changes for a specific entity."""
        with self._lock:
            self._entity_listeners.setdefault(entity_id, []).append(callback)

    def on_all(self, callback: Callable) -> None:
        """Subscribe to all changes regardless of type or entity."""
        with self._lock:
            self._all_listeners.append(callback)

    def off(self, type_name: str, callback: Callable) -> None:
        """Unsubscribe a type listener."""
        with self._lock:
            listeners = self._type_listeners.get(type_name, [])
            if callback in listeners:
                listeners.remove(callback)

    def off_entity(self, entity_id: str, callback: Callable) -> None:
        """Unsubscribe an entity listener."""
        with self._lock:
            listeners = self._entity_listeners.get(entity_id, [])
            if callback in listeners:
                listeners.remove(callback)

    def off_all(self, callback: Callable) -> None:
        """Unsubscribe a catch-all listener."""
        with self._lock:
            if callback in self._all_listeners:
                self._all_listeners.remove(callback)

    def emit(self, event: ChangeEvent) -> None:
        """Dispatch a ChangeEvent to all matching listeners."""
        with self._lock:
            listeners = list(self._all_listeners)
            listeners += list(self._type_listeners.get(event.type_name, []))
            listeners += list(self._entity_listeners.get(event.entity_id, []))

        for cb in listeners:
            try:
                cb(event)
            except Exception:
                pass  # Don't let a bad callback break the chain


class SubscriptionListener:
    """
    Background listener for PostgreSQL LISTEN/NOTIFY with durable catch-up.

    Runs a daemon thread that:
    1. On start, catches up from the last checkpoint (or start time)
    2. Runs LISTEN on the object_events channel
    3. Dispatches notifications to the EventBus
    4. Optionally persists checkpoint to DB for crash recovery

    Args:
        event_bus: EventBus to dispatch events to
        host, port, dbname, user, password: PG connection params
        subscriber_id: Optional. If set, checkpoint is persisted to DB.
    """

    def __init__(self, event_bus: EventBus, host: str, port: int, dbname: str, user: str, password: str,
                 subscriber_id: str | None = None) -> None:
        self.event_bus = event_bus
        self._conn_params = dict(host=host, port=port, dbname=dbname,
                                 user=user, password=password)
        self.subscriber_id = subscriber_id
        self._conn: psycopg2.extensions.connection | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._last_tx_time: datetime | None = None

    def _require_conn(self) -> psycopg2.extensions.connection:
        assert self._conn is not None, "SubscriptionListener not started"
        return self._conn

    def start(self) -> None:
        """Start the listener background thread."""
        self._stop_event.clear()
        self._conn = psycopg2.connect(**self._conn_params)
        self._conn.autocommit = True

        # Load checkpoint
        self._last_tx_time = self._load_checkpoint()

        # Catch up on missed events
        self._catch_up()

        # Start LISTEN
        with self._conn.cursor() as cur:
            cur.execute(f"LISTEN {NOTIFY_CHANNEL};")

        self._thread = threading.Thread(target=self._listen_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the listener and close the connection."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)
            self._thread = None
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def _listen_loop(self) -> None:
        """Background loop: poll for NOTIFY, dispatch to bus."""
        while not self._stop_event.is_set():
            if self._conn is None or self._conn.closed:
                break
            try:
                # Use select to wait for notifications with timeout
                if select.select([self._conn], [], [], 0.5) != ([], [], []):
                    self._conn.poll()
                    while self._conn.notifies:
                        notify = self._conn.notifies.pop(0)
                        self._handle_notify(notify)
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                # Server closed connection or connection is in bad state
                if not self._stop_event.is_set():
                    logger.warning("SubscriptionListener lost connection to PostgreSQL: %s", e)
                break
            except Exception as e:
                logger.error("Unexpected error in SubscriptionListener loop: %s", e)
                if self._stop_event.is_set():
                    break

    def _parse_iso(self, val: Any) -> datetime:
        """Robustly parse ISO 8601 strings from PG NOTIFY payloads.
        Handles variable fractional second precision (3.10 fromisoformat is strict)."""
        if not isinstance(val, str):
            return val
        # Replace Z with UTC offset if present
        s = val.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            # Fallback for non-standard fractional precision (e.g. 5 digits)
            import re
            # If there's a dot, normalize the fractional part to 6 digits
            if "." in s:
                # Split into (prefix, fractional+offset)
                prefix, tail = s.split(".", 1)
                # Split tail into (fraction, offset)
                # Matches the start of + or - for the timezone
                match = re.search(r"[+-]", tail)
                if match:
                    offset_idx = match.start()
                    frac = tail[:offset_idx]
                    offset = tail[offset_idx:]
                    # Pad fraction to 6 digits
                    frac = frac.ljust(6, "0")[:6]
                    s = f"{prefix}.{frac}{offset}"
            return datetime.fromisoformat(s)

    def _handle_notify(self, notify: psycopg2.extensions.Notify) -> None:
        """Parse a PG notification and emit to EventBus."""
        try:
            payload = json.loads(notify.payload)
            event = ChangeEvent(
                entity_id=str(payload["entity_id"]),
                version=payload["version"],
                event_type=payload["event_type"],
                type_name=payload["type_name"],
                updated_by=payload["updated_by"],
                state=payload.get("state"),
                tx_time=self._parse_iso(payload["tx_time"]),
            )
            self.event_bus.emit(event)
            self._last_tx_time = event.tx_time
            self._save_checkpoint()
        except (json.JSONDecodeError, KeyError):
            pass  # Malformed notification — skip

    def _catch_up(self) -> None:
        """Replay missed events from the event log since last checkpoint."""
        if self._last_tx_time is None:
            self._last_tx_time = datetime.now(timezone.utc)
            self._save_checkpoint()
            return

        with self._require_conn().cursor() as cur:
            cur.execute(
                """
                SELECT entity_id, version, event_type, type_name,
                       updated_by, state, tx_time
                FROM object_events
                WHERE tx_time > %s
                ORDER BY tx_time ASC
                """,
                (self._last_tx_time,),
            )
            for row in cur.fetchall():
                event = ChangeEvent(
                    entity_id=str(row[0]),
                    version=row[1],
                    event_type=row[2],
                    type_name=row[3],
                    updated_by=row[4],
                    state=row[5],
                    tx_time=row[6],
                )
                self.event_bus.emit(event)
                self._last_tx_time = event.tx_time

        self._save_checkpoint()

    def _load_checkpoint(self) -> datetime | None:
        """Load the last checkpoint from DB (if subscriber_id is set)."""
        if not self.subscriber_id:
            return None

        with self._require_conn().cursor() as cur:
            cur.execute(
                "SELECT last_tx_time FROM subscription_checkpoints WHERE subscriber_id = %s",
                (self.subscriber_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def _save_checkpoint(self) -> None:
        """Persist the current checkpoint to DB (if subscriber_id is set)."""
        if not self.subscriber_id or self._last_tx_time is None:
            return
        if self._conn is None or self._conn.closed:
            return

        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO subscription_checkpoints (subscriber_id, last_tx_time)
                VALUES (%s, %s)
                ON CONFLICT (subscriber_id) DO UPDATE
                    SET last_tx_time = EXCLUDED.last_tx_time,
                        updated_at = now()
                """,
                (self.subscriber_id, self._last_tx_time),
            )


class EventListener:
    """Unified event listener for store changes.

    Mode is decided by a single parameter:

        EventListener()                          → in-process only
        EventListener(subscriber_id="my-svc")    → durable PG LISTEN/NOTIFY

    In-process mode dispatches events synchronously via callbacks.
    Durable mode additionally listens for cross-process PG notifications
    using the active connection (from ``store.connect()``).  The PG listener
    thread starts lazily on the first ``.on()`` / ``.on_entity()`` /
    ``.on_all()`` call.

    Use as a context manager for clean shutdown::

        with EventListener(subscriber_id="x") as listener:
            listener.on("Order", handle)
    """

    def __init__(self, subscriber_id: str | None = None) -> None:
        self._bus = EventBus()
        self._subscriber_id = subscriber_id
        self._listener: SubscriptionListener | None = None
        self._started = False

    def _ensure_listener(self) -> None:
        """Lazy-start the PG LISTEN thread if subscriber_id is set."""
        if self._started or self._subscriber_id is None:
            return
        from store.connection import active_connection
        conn = active_connection()
        params: dict[str, Any] = dict(conn._conn_params)
        self._listener = SubscriptionListener(
            event_bus=self._bus,
            subscriber_id=self._subscriber_id,
            **params,
        )
        self._listener.start()
        self._started = True

    # ── Subscribe ─────────────────────────────────────────────────────

    def on(self, type_name: str, callback: Callable) -> None:
        """Subscribe to all changes for a given type_name."""
        self._bus.on(type_name, callback)
        self._ensure_listener()

    def on_entity(self, entity_id: str, callback: Callable) -> None:
        """Subscribe to changes for a specific entity."""
        self._bus.on_entity(entity_id, callback)
        self._ensure_listener()

    def on_all(self, callback: Callable) -> None:
        """Subscribe to all changes regardless of type or entity."""
        self._bus.on_all(callback)
        self._ensure_listener()

    # ── Unsubscribe ───────────────────────────────────────────────────

    def off(self, type_name: str, callback: Callable) -> None:
        """Unsubscribe a type listener."""
        self._bus.off(type_name, callback)

    def off_entity(self, entity_id: str, callback: Callable) -> None:
        """Unsubscribe an entity listener."""
        self._bus.off_entity(entity_id, callback)

    def off_all(self, callback: Callable) -> None:
        """Unsubscribe a catch-all listener."""
        self._bus.off_all(callback)

    # ── Emit (used internally by ActiveRecordMixin) ─────────────────────────

    def emit(self, event: ChangeEvent) -> None:
        """Dispatch a ChangeEvent to all matching listeners."""
        self._bus.emit(event)

    # ── Lifecycle ─────────────────────────────────────────────────────

    def _stop(self) -> None:
        """Stop the PG listener thread (if running)."""
        if self._listener is not None:
            self._listener.stop()
            self._listener = None
        self._started = False

    def __enter__(self) -> EventListener:
        return self

    def __exit__(self, *args: Any) -> None:
        self._stop()

    def __del__(self) -> None:
        try:
            self._stop()
        except Exception:
            pass
