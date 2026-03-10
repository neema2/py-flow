"""
Embedded PostgreSQL server with zero-trust configuration.
Uses pgserver for pip-installable PostgreSQL binaries.
Configures pg_hba.conf for scram-sha-256 authentication.
"""

from __future__ import annotations

import os
import urllib.parse
import logging
from typing import Any

import psycopg2
from store.pg_compat import PostgresServer, get_server, PGSERVER_FILE, ensure_uuid_ossp_shim

DEFAULT_DATA_DIR = os.path.join(
    os.path.dirname(__file__), "..", ".pgdata", "objectstore"
)

ADMIN_ROLE = "app_admin"
ADMIN_PASSWORD = "admin_secret"  # In production, use env var / secrets manager
GROUP_ROLE = "app_user"

# pg_hba.conf: superuser keeps trust on local socket (secured by OS file
# permissions on the private temp-dir socket), every other role must
# authenticate with scram-sha-256.
_PG_HBA = """\
# Zero-trust pg_hba.conf
# TYPE  DATABASE  USER           ADDRESS        METHOD
# Superuser on local socket only (bootstrap/provisioning)
local   all       {superuser}                   trust
# Everything else: password required
local   all       all                           scram-sha-256
host    all       all            127.0.0.1/32   scram-sha-256
host    all       all            ::1/128        scram-sha-256
"""


class StoreServer:
    """Manages an embedded PostgreSQL instance with zero-trust RLS config."""

    def __init__(self, data_dir: str | None = None, admin_password: str | None = None) -> None:
        self.data_dir = os.path.abspath(data_dir or DEFAULT_DATA_DIR)
        self.admin_password = admin_password or ADMIN_PASSWORD
        self._pg: PostgresServer | None = None
        self._superuser: str | None = None  # detected from pgserver URI

    def start(self) -> StoreServer:
        """Start the embedded PostgreSQL server and bootstrap if needed."""
        os.makedirs(self.data_dir, exist_ok=True)
        self._pg = get_server(self.data_dir)
        ensure_uuid_ossp_shim(PGSERVER_FILE)
        self._detect_superuser()
        self._bootstrap()
        self._harden_auth()
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

    def _superuser_conn(self) -> psycopg2.extensions.connection:
        """Get a superuser connection (local socket, trust auth)."""
        return psycopg2.connect(self._require_pg().get_uri())

    def _bootstrap(self) -> None:
        """Create admin role, group role, and schema. Idempotent."""
        conn = self._superuser_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            # Admin role (provisions users, owns the objects table)
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

            # Group role (all app users inherit from this)
            cur.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s", (GROUP_ROLE,)
            )
            if cur.fetchone() is None:
                cur.execute(
                    f"CREATE ROLE {GROUP_ROLE} NOLOGIN NOSUPERUSER "
                    f"NOCREATEDB NOCREATEROLE NOBYPASSRLS"
                )

            # Grant schema usage so admin can create tables
            cur.execute(f"GRANT CREATE, USAGE ON SCHEMA public TO {ADMIN_ROLE};")
            cur.execute(f"GRANT USAGE ON SCHEMA public TO {GROUP_ROLE};")

            # Admin needs ADMIN option on group role to grant it to new users
            cur.execute(f"GRANT {GROUP_ROLE} TO {ADMIN_ROLE} WITH ADMIN OPTION;")

            # Allow admin to create schemas (needed by workflow engine)
            cur.execute(
                f"GRANT CREATE ON DATABASE {self.conn_info()['dbname']} "
                f"TO {ADMIN_ROLE};"
            )

            # Install uuid-ossp (shim created by _ensure_uuid_ossp_shim if needed)
            cur.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')

            # Install pgvector for vector similarity search
            cur.execute('CREATE EXTENSION IF NOT EXISTS vector;')
        conn.close()

        # Bootstrap schema as admin
        from store.schema import bootstrap_schema
        admin_conn = self.admin_conn()
        bootstrap_schema(admin_conn)
        admin_conn.close()

    def _harden_auth(self) -> None:
        """Rewrite pg_hba.conf so all non-superuser connections require
        scram-sha-256 password authentication, then reload."""
        pg_hba_path = os.path.join(self.data_dir, "pg_hba.conf")
        desired = _PG_HBA.format(superuser=self._superuser)

        current = ""
        if os.path.exists(pg_hba_path):
            with open(pg_hba_path) as f:
                current = f.read()

        if current.strip() != desired.strip():
            with open(pg_hba_path, "w") as f:
                f.write(desired)
            # Reload via superuser (still trust on local socket)
            conn = self._superuser_conn()
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT pg_reload_conf();")
            conn.close()

    # ── Public API ───────────────────────────────────────────────────

    def admin_conn(self) -> psycopg2.extensions.connection:
        """Get a connection as app_admin (password auth)."""
        info = self.conn_info()
        return psycopg2.connect(
            host=info["host"],
            port=info["port"],
            dbname=info["dbname"],
            user=ADMIN_ROLE,
            password=self.admin_password,
        )

    def register_alias(self, name: str) -> None:
        """Register this server's connection params under an alias name."""
        from store.connection import register_alias
        info = self.conn_info()
        register_alias(name, host=info["host"], port=info["port"],
                       dbname=info["dbname"])

    def conn_info(self) -> dict[str, Any]:
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
        host_encoded = urllib.parse.quote(str(info["host"]), safe="")
        return (
            f"postgresql://{ADMIN_ROLE}:{self.admin_password}@"
            f"localhost:{info['port']}/{info['dbname']}?host={host_encoded}"
        )

    def provision_user(self, username: str, password: str) -> None:
        """Create a user with RLS access. Idempotent."""
        from store.schema import _provision_user as _provision
        admin_conn = self.admin_conn()
        _provision(admin_conn, username, password)
        admin_conn.close()

    def stop(self) -> None:
        """Stop the embedded PostgreSQL server."""
        if self._pg:
            self._pg.cleanup()
            self._pg = None

    def __enter__(self) -> StoreServer:
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()

