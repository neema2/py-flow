"""
lakehouse.admin — Platform API for the Lakehouse
==================================================
Start/stop the lakehouse stack, run sync services, manage catalogs and tables.

Platform usage::

    from lakehouse.admin import LakehouseServer

    server = LakehouseServer(data_dir="data/lakehouse")
    await server.start()
    server.register_alias("demo")

    # User code:  Lakehouse("demo")

With RLS::

    from lakehouse.admin import LakehouseServer, RLSPolicy

    server = LakehouseServer(
        data_dir="data/lakehouse",
        rls_policies=[RLSPolicy("sales_data", "sales_acl", "row_id", "user_token")],
        rls_users={"alice-token": "alice", "bob-token": "bob"},
    )
    await server.start()            # starts lakehouse stack + RLS Flight server
    server.register_alias("demo")   # alias includes Flight endpoint automatically

    # User code:
    from lakehouse import Lakehouse
    lh = Lakehouse("demo", token="alice-token")
    lh.query("SELECT * FROM lakehouse.default.sales_data")  # → RLS-filtered

"""

from __future__ import annotations

import logging
import threading
from typing import Any

import duckdb

from lakehouse._registry import register_alias as _register_alias
from lakehouse.catalog import create_catalog
from lakehouse.models import SyncState
from lakehouse.rls_server import RLSFlightServer, RLSPolicy
from lakehouse.services import LakehouseStack as _LakehouseStack
from lakehouse.services import start_lakehouse as _start_lakehouse
from lakehouse.services import stop_lakehouse as _stop_lakehouse
from lakehouse.sync import SyncEngine
from lakehouse.tables import ensure_tables

logger = logging.getLogger(__name__)


def _find_free_port() -> int:
    """Find a free TCP port."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return int(s.getsockname()[1])


class LakehouseServer:
    """Manages the lakehouse infrastructure stack.

    Wraps PG (zonkyio) + Lakekeeper + object store + optional RLS Flight server
    into a single class with consistent ``start()`` / ``stop()`` / ``register_alias()`` API.
    """

    def __init__(
        self,
        data_dir: str = "data/lakehouse",
        pg_port: int = 5488,
        lakekeeper_port: int = 8181,
        s3_api_port: int = 9002,
        s3_console_port: int = 9003,
        warehouse: str = "lakehouse",
        bucket: str = "lakehouse",
        # RLS configuration (optional)
        rls_policies: list[RLSPolicy] | None = None,
        rls_users: dict[str, str] | None = None,
        rls_port: int | None = None,
    ) -> None:
        self._data_dir = data_dir
        self._pg_port = pg_port
        self._lakekeeper_port = lakekeeper_port
        self._s3_api_port = s3_api_port
        self._s3_console_port = s3_console_port
        self._warehouse = warehouse
        self._bucket = bucket
        self._stack: _LakehouseStack | None = None

        # RLS
        self._rls_policies = rls_policies
        self._rls_users = rls_users or {}
        self._rls_port = rls_port
        self._rls_server: RLSFlightServer | None = None
        self._rls_conn: duckdb.DuckDBPyConnection | None = None
        self._rls_thread: threading.Thread | None = None

    async def start(self) -> LakehouseServer:
        """Start the full lakehouse stack (PG + Lakekeeper + object store + optional RLS)."""
        self._stack = await _start_lakehouse(
            data_dir=self._data_dir,
            pg_port=self._pg_port,
            lakekeeper_port=self._lakekeeper_port,
            s3_api_port=self._s3_api_port,
            s3_console_port=self._s3_console_port,
            warehouse=self._warehouse,
            bucket=self._bucket,
        )

        # Start RLS Flight server if policies are configured
        if self._rls_policies:
            self._start_rls_server()

        return self

    async def stop(self) -> None:
        """Stop all lakehouse services (including RLS if running)."""
        self._stop_rls_server()
        if self._stack:
            await _stop_lakehouse(self._stack)
            self._stack = None

    # ── RLS server lifecycle ──────────────────────────────────────────────

    def _start_rls_server(self) -> None:
        """Start the RLS Flight SQL server with a DuckDB connection to the catalog."""
        port = self._rls_port or _find_free_port()

        # Create a server-side DuckDB connected to the Iceberg catalog
        conn = duckdb.connect()
        s3_ep = self.s3_endpoint.replace("http://", "")
        conn.execute("INSTALL iceberg; LOAD iceberg;")
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute(f"SET s3_endpoint='{s3_ep}';")
        conn.execute("SET s3_access_key_id='minioadmin';")
        conn.execute("SET s3_secret_access_key='minioadmin';")
        conn.execute("SET s3_region='us-east-1';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")
        conn.execute(f"""
            ATTACH '{self._warehouse}' AS lakehouse (
                TYPE ICEBERG,
                ENDPOINT '{self.catalog_url}',
                AUTHORIZATION_TYPE 'none'
            );
        """)

        self._rls_conn = conn
        self._rls_server = RLSFlightServer(
            duckdb_conn=conn,
            policies=self._rls_policies,
            users=self._rls_users,
            host="localhost",
            port=port,
        )
        self._rls_port = port

        self._rls_thread = threading.Thread(target=self._rls_server.serve, daemon=True)
        self._rls_thread.start()

        import time
        time.sleep(0.3)
        logger.info(
            "RLS Flight server started on port %d with %d policies",
            port, len(self._rls_policies or []),
        )

    def _stop_rls_server(self) -> None:
        """Stop the RLS Flight SQL server."""
        if self._rls_server:
            self._rls_server.shutdown()
            self._rls_server = None
        if self._rls_conn:
            self._rls_conn.close()
            self._rls_conn = None

    # ── Properties ────────────────────────────────────────────────────────

    @property
    def catalog_url(self) -> str:
        if not self._stack:
            raise RuntimeError("LakehouseServer not started")
        return self._stack.catalog_url

    @property
    def s3_endpoint(self) -> str:
        if not self._stack:
            raise RuntimeError("LakehouseServer not started")
        return self._stack.s3_endpoint

    @property
    def pg_url(self) -> str:
        if not self._stack:
            raise RuntimeError("LakehouseServer not started")
        return self._stack.pg_url

    @property
    def flight_port(self) -> int | None:
        """RLS Flight SQL server port (None if RLS not configured)."""
        return self._rls_port if self._rls_server else None

    # ── Alias management ──────────────────────────────────────────────────

    def register_alias(self, name: str) -> None:
        """Register this server's connection info under an alias name.

        Automatically includes Flight endpoint if RLS is configured.
        """
        flight_host = "localhost" if self._rls_server else None
        flight_port = self._rls_port if self._rls_server else None
        _register_alias(
            name,
            catalog_url=self.catalog_url,
            s3_endpoint=self.s3_endpoint,
            flight_host=flight_host,
            flight_port=flight_port,
        )

    # ── Context manager ───────────────────────────────────────────────────

    async def __aenter__(self) -> LakehouseServer:
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()


__all__ = [
    "LakehouseServer",
    "SyncEngine",
    "SyncState",
    "create_catalog",
    "ensure_tables",
    "RLSFlightServer",
    "RLSPolicy",
]
