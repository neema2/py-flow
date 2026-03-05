"""
UserConnection — high-level connection API that hides StoreClient.

Users call ``connect()`` with an alias (or explicit params) plus their
credentials.  The returned ``UserConnection`` becomes the *active*
connection used by ``Storable.save()``, ``Position.find()``, etc.

    from store import connect

    db = connect("trading", user="alice", password="alice_pw")
    pos.save()           # uses the active connection
    db.close()           # or use ``with connect(...) as db:``
"""

from __future__ import annotations

import threading
from datetime import datetime
from typing import TYPE_CHECKING, Any, TypeVar

from store._client import QueryResult, StoreClient
from store.base import Storable

if TYPE_CHECKING:
    import psycopg2.extensions

    from store.subscriptions import EventBus

_S = TypeVar("_S", bound=Storable)

# ── Alias registry ────────────────────────────────────────────────────

_aliases: dict[str, dict] = {}   # name → {"host": …, "port": …, "dbname": …}
_lock = threading.Lock()


def register_alias(name: str, host: str, port: int, dbname: str = "postgres") -> None:
    """Register a connection alias so users can ``connect("name", …)``."""
    with _lock:
        _aliases[name] = {"host": host, "port": port, "dbname": dbname}


def _resolve_alias(name: str) -> dict | None:
    with _lock:
        return _aliases.get(name)



class _ConnectionLocal(threading.local):
    """Typed thread-local storage for the active UserConnection."""
    connection: UserConnection | None

    def __init__(self) -> None:
        super().__init__()
        self.connection = None


_active = _ConnectionLocal()


def active_connection() -> UserConnection:
    """Return the current thread's active ``UserConnection``, or raise if none is set.

    A connection becomes *active* when created via :func:`connect` or
    when :meth:`UserConnection.activate` is called explicitly.
    """
    conn = _active.connection
    if conn is None:
        raise RuntimeError(
            "No active connection. Call store.connect() first."
        )
    return conn


def _set_active(conn: UserConnection | None) -> None:
    _active.connection = conn


# ── UserConnection ────────────────────────────────────────────────────

class UserConnection:
    """Wraps a ``StoreClient`` and acts as the active connection context.

    Normally created via :func:`connect` — not instantiated directly.
    """

    def __init__(self, *, user: str, password: str,
                 host: str, port: int, dbname: str,
                 alias: str | None = None,
                 event_bus: EventBus | None = None) -> None:
        self.user = user
        self.alias = alias
        self._conn_params = dict(host=host, port=port, dbname=dbname,
                                 user=user, password=password)
        self._client = StoreClient(
            user=user, password=password,
            host=host, port=port, dbname=dbname,
            event_bus=event_bus,
        )

    # Expose the raw psycopg2 connection for permissions helpers
    @property
    def conn(self) -> psycopg2.extensions.connection:
        return self._client.conn

    def activate(self) -> None:
        """Make this the active connection for the current thread."""
        _set_active(self)

    def deactivate(self) -> None:
        """Remove this connection from the active slot (if it is active)."""
        if _active.connection is self:
            _set_active(None)

    # ── Store operations (typed proxies to internal StoreClient) ─────

    def write(self, obj: Storable, valid_from: datetime | None = None) -> str:
        """Create a new entity. Returns entity_id."""
        return self._client.write(obj, valid_from=valid_from)

    def update(self, obj: Storable, valid_from: datetime | None = None) -> None:
        """Update an existing entity (new version)."""
        self._client.update(obj, valid_from=valid_from)

    def delete(self, obj: Storable) -> bool:
        """Soft-delete an entity."""
        return self._client.delete(obj)

    def read(self, cls: type[_S], entity_id: str) -> _S | None:
        """Read the latest version of an entity by ID."""
        return self._client.read(cls, entity_id)

    def query(self, cls: type[_S], filters: dict | None = None,
              limit: int = 100, cursor: Any = None) -> QueryResult[_S]:
        """Query entities of a type with optional filters."""
        return self._client.query(cls, filters=filters, limit=limit, cursor=cursor)

    def history(self, cls: type[_S], entity_id: str) -> list[_S]:
        """Return all versions of an entity."""
        return self._client.history(cls, entity_id)

    def as_of(self, cls: type[_S], entity_id: str,
              tx_time: datetime | None = None,
              valid_time: datetime | None = None) -> _S | None:
        """Bi-temporal point-in-time query."""
        return self._client.as_of(cls, entity_id, tx_time=tx_time, valid_time=valid_time)

    def transition(self, obj: Storable, new_state: str,
                   valid_from: datetime | None = None) -> None:
        """Transition an entity to a new lifecycle state."""
        self._client.transition(obj, new_state, valid_from=valid_from)

    def write_many(self, objects: list[Storable],
                   valid_from: datetime | None = None) -> list[str]:
        """Write multiple entities in a single transaction."""
        return self._client.write_many(objects, valid_from=valid_from)

    def update_many(self, objects: list[Storable],
                    valid_from: datetime | None = None) -> None:
        """Update multiple entities in a single transaction."""
        self._client.update_many(objects, valid_from=valid_from)

    # ── Lifecycle ──────────────────────────────────────────────────

    def close(self) -> None:
        """Close the underlying connection and deactivate."""
        self.deactivate()
        self._client.close()

    # Context-manager support
    def __enter__(self) -> UserConnection:
        self.activate()
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        alias_str = f" alias={self.alias!r}" if self.alias else ""
        return f"<UserConnection user={self.user!r}{alias_str}>"


# ── connect() — the public entry point ────────────────────────────────

def connect(alias_or_host: str | None = None, *,
            user: str, password: str,
            host: str | None = None,
            port: int = 5432,
            dbname: str = "postgres",
            event_bus: EventBus | None = None) -> UserConnection:
    """Open a connection and make it *active*.

    Usage::

        # With an alias (platform registers aliases):
        db = connect("trading", user="alice", password="alice_pw")

        # Explicit params (local dev / testing):
        db = connect(host="/tmp/pg", port=5432, user="alice", password="alice_pw")
    """
    resolved = None
    alias = None

    if alias_or_host is not None:
        resolved = _resolve_alias(alias_or_host)
        if resolved is not None:
            alias = alias_or_host

    if resolved is not None:
        conn = UserConnection(
            user=user, password=password,
            host=resolved["host"],
            port=resolved["port"],
            dbname=resolved["dbname"],
            alias=alias,
            event_bus=event_bus,
        )
    else:
        # Treat alias_or_host as an explicit host if not a known alias
        actual_host = host or alias_or_host or "localhost"
        conn = UserConnection(
            user=user, password=password,
            host=actual_host, port=port, dbname=dbname,
            event_bus=event_bus,
        )

    conn.activate()
    return conn
