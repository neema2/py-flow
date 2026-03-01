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

"""

from __future__ import annotations

import logging

from lakehouse.services import start_lakehouse as _start_lakehouse, stop_lakehouse as _stop_lakehouse, LakehouseStack as _LakehouseStack
from lakehouse.sync import SyncEngine
from lakehouse.models import SyncState
from lakehouse.catalog import create_catalog
from lakehouse.tables import ensure_tables
from lakehouse._registry import register_alias as _register_alias

logger = logging.getLogger(__name__)


class LakehouseServer:
    """Manages the lakehouse infrastructure stack.

    Wraps PG (zonkyio) + Lakekeeper + object store into a single class
    with consistent ``start()`` / ``stop()`` / ``register_alias()`` API.
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
    ):
        self._data_dir = data_dir
        self._pg_port = pg_port
        self._lakekeeper_port = lakekeeper_port
        self._s3_api_port = s3_api_port
        self._s3_console_port = s3_console_port
        self._warehouse = warehouse
        self._bucket = bucket
        self._stack: _LakehouseStack | None = None

    async def start(self) -> "LakehouseServer":
        """Start the full lakehouse stack (PG + Lakekeeper + object store)."""
        self._stack = await _start_lakehouse(
            data_dir=self._data_dir,
            pg_port=self._pg_port,
            lakekeeper_port=self._lakekeeper_port,
            s3_api_port=self._s3_api_port,
            s3_console_port=self._s3_console_port,
            warehouse=self._warehouse,
            bucket=self._bucket,
        )
        return self

    async def stop(self) -> None:
        """Stop all lakehouse services."""
        if self._stack:
            await _stop_lakehouse(self._stack)
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
    "LakehouseServer",
    "SyncEngine",
    "SyncState",
    "create_catalog",
    "ensure_tables",
]
