"""
Embedded PostgreSQL server with zero-trust configuration.
Uses pgserver for pip-installable PostgreSQL binaries.
Configures pg_hba.conf for scram-sha-256 authentication.
"""

import os
import urllib.parse
import psycopg2

import pgserver


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

    def __init__(self, data_dir=None, admin_password=None):
        self.data_dir = os.path.abspath(data_dir or DEFAULT_DATA_DIR)
        self.admin_password = admin_password or ADMIN_PASSWORD
        self._pg = None
        self._superuser = None  # detected from pgserver URI

    def start(self):
        """Start the embedded PostgreSQL server and bootstrap if needed."""
        os.makedirs(self.data_dir, exist_ok=True)
        self._pg = pgserver.get_server(self.data_dir)
        self._ensure_uuid_ossp_shim()
        self._detect_superuser()
        self._bootstrap()
        self._harden_auth()
        return self

    # ── Internal ─────────────────────────────────────────────────────

    def _ensure_uuid_ossp_shim(self):
        """Create a pure-SQL uuid-ossp shim if the extension files are missing.
        pgserver on Linux doesn't bundle the C-based uuid-ossp extension,
        but PG 13+ has gen_random_uuid() built-in. This shim satisfies
        CREATE EXTENSION "uuid-ossp" (used by DBOS migrations) without
        needing the native library."""
        ext_dir = os.path.join(
            os.path.dirname(pgserver.__file__),
            "pginstall", "share", "postgresql", "extension",
        )
        control = os.path.join(ext_dir, "uuid-ossp.control")
        sql = os.path.join(ext_dir, "uuid-ossp--1.1.sql")
        if os.path.exists(control) and os.path.exists(sql):
            return
        os.makedirs(ext_dir, exist_ok=True)
        if not os.path.exists(control):
            with open(control, "w") as f:
                f.write(
                    "# uuid-ossp shim — gen_random_uuid() is built-in since PG 13\n"
                    "comment = 'generate universally unique identifiers (UUIDs)'\n"
                    "default_version = '1.1'\n"
                    "relocatable = true\n"
                )
        if not os.path.exists(sql):
            with open(sql, "w") as f:
                f.write(
                    "-- uuid-ossp shim for pgserver (no C library available)\n"
                    "-- gen_random_uuid() is built-in since PG 13\n"
                    "-- Provide uuid_generate_v4 as an alias for compatibility\n"
                    "CREATE OR REPLACE FUNCTION uuid_generate_v4() RETURNS uuid\n"
                    "AS $$ SELECT gen_random_uuid() $$ LANGUAGE SQL;\n"
                )

    def _detect_superuser(self):
        """Detect the superuser name from the pgserver URI."""
        uri = self._pg.get_uri()
        parsed = urllib.parse.urlparse(uri)
        self._superuser = parsed.username or os.getenv("USER", "postgres")

    def _superuser_conn(self):
        """Get a superuser connection (local socket, trust auth)."""
        return psycopg2.connect(self._pg.get_uri())

    def _bootstrap(self):
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

    def _harden_auth(self):
        """Rewrite pg_hba.conf so all non-superuser connections require
        scram-sha-256 password authentication, then reload."""
        pg_hba_path = os.path.join(self.data_dir, "pg_hba.conf")
        desired = _PG_HBA.format(superuser=self._superuser)

        current = ""
        if os.path.exists(pg_hba_path):
            with open(pg_hba_path, "r") as f:
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

    def admin_conn(self):
        """Get a connection as app_admin (password auth)."""
        info = self.conn_info()
        return psycopg2.connect(
            host=info["host"],
            port=info["port"],
            dbname=info["dbname"],
            user=ADMIN_ROLE,
            password=self.admin_password,
        )

    def register_alias(self, name: str):
        """Register this server's connection params under an alias name."""
        from store.connection import register_alias
        info = self.conn_info()
        register_alias(name, host=info["host"], port=info["port"],
                       dbname=info["dbname"])

    def conn_info(self):
        """Return connection parameters for this server."""
        uri = self._pg.get_uri()
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

    def pg_url(self):
        """Return a generic postgres:// connection URL for this server."""
        info = self.conn_info()
        host_encoded = urllib.parse.quote(info["host"], safe="")
        return (
            f"postgresql://{ADMIN_ROLE}:{self.admin_password}@"
            f"localhost:{info['port']}/{info['dbname']}?host={host_encoded}"
        )

    def provision_user(self, username: str, password: str):
        """Create a user with RLS access. Idempotent."""
        from store.schema import provision_user as _provision
        admin_conn = self.admin_conn()
        _provision(admin_conn, username, password)
        admin_conn.close()

    def stop(self):
        """Stop the embedded PostgreSQL server."""
        if self._pg:
            self._pg.cleanup()
            self._pg = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()


# Deprecated alias — use StoreServer instead
ObjectStoreServer = StoreServer
