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

Legacy functions ``start_lakehouse()`` / ``stop_lakehouse()`` still work.
"""

from __future__ import annotations

import logging

from lakehouse.services import start_lakehouse, stop_lakehouse, LakehouseStack
from lakehouse.sync import SyncEngine
from lakehouse.models import SyncState
from lakehouse.catalog import create_catalog
from lakehouse.tables import ensure_tables
from lakehouse._registry import register_alias as _register_alias

logger = logging.getLogger(__name__)


class LakehouseServer:
    """Manages the lakehouse infrastructure stack.

    Wraps PG (zonkyio) + Lakekeeper + MinIO into a single class
    with consistent ``start()`` / ``stop()`` / ``register_alias()`` API.
    """

    def __init__(
        self,
        data_dir: str = "data/lakehouse",
        pg_port: int = 5488,
        lakekeeper_port: int = 8181,
        minio_api_port: int = 9002,
        minio_console_port: int = 9003,
        warehouse: str = "lakehouse",
        bucket: str = "lakehouse",
    ):
        self._data_dir = data_dir
        self._pg_port = pg_port
        self._lakekeeper_port = lakekeeper_port
        self._minio_api_port = minio_api_port
        self._minio_console_port = minio_console_port
        self._warehouse = warehouse
        self._bucket = bucket
        self._stack: LakehouseStack | None = None

    async def start(self) -> "LakehouseServer":
        """Start the full lakehouse stack (PG + Lakekeeper + MinIO)."""
        self._stack = await start_lakehouse(
            data_dir=self._data_dir,
            pg_port=self._pg_port,
            lakekeeper_port=self._lakekeeper_port,
            minio_api_port=self._minio_api_port,
            minio_console_port=self._minio_console_port,
            warehouse=self._warehouse,
            bucket=self._bucket,
        )
        return self

    async def stop(self) -> None:
        """Stop all lakehouse services."""
        if self._stack:
            await stop_lakehouse(self._stack)
            self._stack = None

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

    def register_alias(self, name: str):
        """Register this server's connection info under an alias name."""
        _register_alias(
            name,
            catalog_url=self.catalog_url,
            s3_endpoint=self.s3_endpoint,
        )

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.stop()


__all__ = [
    # New consistent API
    "LakehouseServer",
    # Legacy functions (backward compat)
    "start_lakehouse",
    "stop_lakehouse",
    "LakehouseStack",
    # Sync service
    "SyncEngine",
    "SyncState",
    # Bootstrap
    "create_catalog",
    "ensure_tables",
]
