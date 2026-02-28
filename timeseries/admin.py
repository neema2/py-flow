"""
timeseries.admin — Platform API for Time-Series Database
=========================================================
Start/stop TSDB infrastructure, register aliases.

Platform usage::

    from timeseries.admin import TsdbServer

    server = TsdbServer(data_dir="data/tsdb")
    await server.start()
    server.register_alias("demo")

User code uses ``Timeseries("demo")`` to connect.
"""

from __future__ import annotations

import logging

from timeseries._registry import register_alias as _register_alias

logger = logging.getLogger(__name__)


class TsdbServer:
    """Manages time-series database infrastructure.

    Hides the underlying QuestDB implementation.
    """

    def __init__(
        self,
        data_dir: str = "data/tsdb",
        host: str = "localhost",
        http_port: int = 9000,
        ilp_port: int = 9009,
        pg_port: int = 8812,
    ):
        self._data_dir = data_dir
        self._host = host
        self._http_port = http_port
        self._ilp_port = ilp_port
        self._pg_port = pg_port
        self._manager = None

    async def start(self) -> "TsdbServer":
        """Start the TSDB server."""
        from timeseries.backends.questdb.manager import QuestDBManager
        self._manager = QuestDBManager(
            data_dir=self._data_dir,
            host=self._host,
            http_port=self._http_port,
            ilp_port=self._ilp_port,
            pg_port=self._pg_port,
        )
        await self._manager.start()
        return self

    async def stop(self) -> None:
        """Stop the TSDB server."""
        if self._manager:
            await self._manager.stop()
            self._manager = None

    async def health(self) -> bool:
        """Check if the TSDB server is healthy."""
        if self._manager:
            return await self._manager.health()
        return False

    @property
    def host(self) -> str:
        return self._host

    @property
    def http_port(self) -> int:
        return self._http_port

    @property
    def ilp_port(self) -> int:
        return self._ilp_port

    @property
    def pg_port(self) -> int:
        return self._pg_port

    def register_alias(self, name: str):
        """Register this server's connection info under an alias name."""
        _register_alias(
            name,
            data_dir=self._data_dir,
            host=self._host,
            http_port=self._http_port,
            ilp_port=self._ilp_port,
            pg_port=self._pg_port,
        )

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.stop()


__all__ = ["TsdbServer"]
