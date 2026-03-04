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
from typing import TYPE_CHECKING, Any

from store.client import StoreClient

if TYPE_CHECKING:
    import psycopg2.extensions

    from store.subscriptions import EventBus

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


def get_connection() -> UserConnection:
    """Return the active ``UserConnection`` or raise."""
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

    def close(self) -> None:
        """Close the underlying StoreClient and deactivate."""
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
