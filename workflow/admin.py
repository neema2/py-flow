"""
workflow.admin — Platform API for Workflow Infrastructure
==========================================================
Start/stop embedded PostgreSQL for workflow state, register aliases.

Platform usage::

    from workflow.admin import WorkflowServer

    server = WorkflowServer(data_dir="data/workflow")
    server.start()
    server.register_alias("demo")

User code uses ``WorkflowEngine("demo")`` or
``workflow.factory.create_engine("demo")``.
"""

from __future__ import annotations

import os
import urllib.parse
from typing import Any

from db import Connection
from db import connect as _db_connect
from store.pg_compat import PostgresServer, get_server, PGSERVER_FILE, ensure_uuid_ossp_shim

from workflow._registry import register_alias as _register_alias

# Reuse the same role constants as store for DBOS compatibility
ADMIN_ROLE = "app_admin"
DEFAULT_ADMIN_PASSWORD = "admin_secret"


class WorkflowServer:
    """Manages an embedded PostgreSQL instance for workflow state.

    Fully decoupled from StoreServer — runs its own PG process.
    In production, configure to point to an external PG instead.
    """

    def __init__(self, data_dir: str = "data/workflow", admin_password: str | None = None) -> None:
        self.data_dir = os.path.abspath(data_dir)
        self.admin_password = admin_password or DEFAULT_ADMIN_PASSWORD
        self._pg: PostgresServer | None = None
        self._superuser: str | None = None

    def start(self) -> WorkflowServer:
        """Start the embedded PostgreSQL server and bootstrap for workflow use."""
        os.makedirs(self.data_dir, exist_ok=True)
        self._pg = get_server(self.data_dir)
        ensure_uuid_ossp_shim(PGSERVER_FILE)
        self._detect_superuser()
        self._bootstrap()
        return self

    # ── Internal ─────────────────────────────────────────────────────

    def _require_pg(self) -> PostgresServer:
        assert self._pg is not None, "PostgreSQL server not started. Call .start() first."
        return self._pg

    def _detect_superuser(self) -> None:
        """Detect the superuser name from the pgserver URI."""
        uri = self._require_pg().get_uri()
        parsed = urllib.parse.urlparse(uri)
        self._superuser = parsed.username or os.getenv("USER", "postgres")

    def _superuser_conn(self) -> Connection:
        """Get a superuser connection (local socket, trust auth)."""
        return _db_connect(self._require_pg().get_uri())

    def _bootstrap(self) -> None:
        """Create admin role and grant CREATE on database. Idempotent.
        DBOS needs CREATE privilege to make its 'dbos' schema."""
        conn = self._superuser_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            # Admin role
            cur.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s", (ADMIN_ROLE,)
            )
            if cur.fetchone() is None:
                cur.execute(
                    f"CREATE ROLE {ADMIN_ROLE} LOGIN PASSWORD %s "
                    f"NOSUPERUSER NOCREATEDB CREATEROLE NOBYPASSRLS",
                    (self.admin_password,),
                )
            else:
                cur.execute(
                    f"ALTER ROLE {ADMIN_ROLE} PASSWORD %s", (self.admin_password,),
                )

            # Grant CREATE on database (DBOS creates 'dbos' schema)
            info = self.conn_info()
            cur.execute(
                f"GRANT CREATE ON DATABASE {info['dbname']} TO {ADMIN_ROLE};"
            )

            # Install uuid-ossp extension
            cur.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
        conn.close()

    # ── Public API ───────────────────────────────────────────────────

    def conn_info(self) -> dict:
        """Return connection parameters for this server."""
        uri = self._require_pg().get_uri()
        parsed = urllib.parse.urlparse(uri)
        params = urllib.parse.parse_qs(parsed.query)

        dbname = parsed.path.lstrip("/") or "postgres"
        host = params.get("host", ["/tmp"])[0]
        port = parsed.port or 5432

        return {
            "host": host,
            "port": port,
            "dbname": dbname,
        }

    def pg_url(self) -> str:
        """Return a generic postgres:// connection URL for this server."""
        info = self.conn_info()
        host_encoded = urllib.parse.quote(info["host"], safe="")
        return (
            f"postgresql://{ADMIN_ROLE}:{self.admin_password}@"
            f"localhost:{info['port']}/{info['dbname']}?host={host_encoded}"
        )

    def register_alias(self, name: str) -> None:
        """Register this server's PG URL under an alias name."""
        _register_alias(name, pg_url=self.pg_url())

    def stop(self) -> None:
        """Stop the embedded PostgreSQL server."""
        if self._pg:
            self._pg.cleanup()
            self._pg = None

    def __enter__(self) -> WorkflowServer:
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()


__all__ = ["WorkflowServer"]
