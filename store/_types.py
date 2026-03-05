"""
store._types — Platform-owned DB connection types.

Defines PEP 249 (DB-API 2.0) Protocols so that code outside ``store/``
never imports driver-specific modules like ``psycopg2``.

Usage::

    from store._types import Connection

    def bootstrap_my_schema(admin_conn: Connection) -> None:
        with admin_conn.cursor() as cur:
            cur.execute("CREATE TABLE ...")
        admin_conn.commit()
"""

from __future__ import annotations

from typing import Any, Protocol, Sequence


class Cursor(Protocol):
    """PEP 249 DB-API 2.0 cursor interface."""

    @property
    def rowcount(self) -> int: ...                                      # noqa: E704
    @property
    def description(self) -> Any: ...                                    # noqa: E704

    def execute(self, sql: str, params: Sequence[Any] | dict[str, Any] | None = None) -> Any: ...
    def fetchone(self) -> tuple[Any, ...] | None: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...
    def fetchmany(self, size: int = ...) -> list[tuple[Any, ...]]: ...
    def close(self) -> None: ...
    def __enter__(self) -> Cursor: ...
    def __exit__(self, *args: Any) -> None: ...


class Connection(Protocol):
    """PEP 249 DB-API 2.0 connection interface.

    Matches psycopg2, mysql-connector, sqlite3, and most Python DB adapters.
    """

    autocommit: bool

    def cursor(self, **kwargs: Any) -> Cursor: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


def connect(dsn: str = "", **kwargs: Any) -> Connection:
    """Create a database connection.

    Wraps the underlying DB driver so callers never import it directly.
    Accepts a DSN string and/or keyword arguments.
    """
    import psycopg2
    if dsn:
        return psycopg2.connect(dsn, **kwargs)  # type: ignore[no-any-return]
    return psycopg2.connect(**kwargs)  # type: ignore[no-any-return]
