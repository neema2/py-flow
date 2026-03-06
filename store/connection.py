"""
UserConnection — pure connection wrapper for the object store.

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
from typing import Any

import psycopg2
import psycopg2.extensions
import psycopg2.extras

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
    """Pure connection wrapper — manages the psycopg2 connection and thread-local context.

    All SQL persistence logic lives in the ActiveRecordMixin (store/_active_record.py).
    This class only provides the raw connection, user identity, and event bus.
    """

    def __init__(self, *, user: str, password: str,
                 host: str, port: int, dbname: str,
                 alias: str | None = None,
                 event_bus: EventBus | None = None) -> None:
        self.user = user
        self.alias = alias
        self.event_bus = event_bus
        self._conn_params = {
            "host": host, "port": port, "dbname": dbname,
            "user": user, "password": password,
        }
        self.conn: psycopg2.extensions.connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )
        self.conn.autocommit = True
        psycopg2.extras.register_uuid()
        self.activate()

    def activate(self) -> None:
        """Make this the active connection for the current thread."""
        _set_active(self)

    def deactivate(self) -> None:
        """Remove this connection from the active slot (if it is active)."""
        if _active.connection is self:
            _set_active(None)

    # ── Lifecycle ──────────────────────────────────────────────────

    def close(self) -> None:
        """Close the underlying connection and deactivate."""
        self.deactivate()
        if self.conn and not self.conn.closed:
            self.conn.close()

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


    return conn
