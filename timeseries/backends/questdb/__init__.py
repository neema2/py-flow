"""
QuestDB Backend
===============
Concrete TSDBBackend implementation backed by QuestDB.

Composes:
- QuestDBManager: binary lifecycle (start/stop/health)
- QuestDBWriter: ILP ingestion (fire-and-forget, high throughput)
- QuestDBReader: PGWire queries (standard SQL via psycopg2)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from marketdata.models import Tick, FXTick, CurveTick
from timeseries.base import TSDBBackend
from timeseries.models import Bar

from timeseries.backends.questdb.manager import QuestDBManager
from timeseries.backends.questdb.schema import create_tables
from timeseries.backends.questdb.writer import QuestDBWriter
from timeseries.backends.questdb.reader import QuestDBReader

logger = logging.getLogger(__name__)


class QuestDBBackend(TSDBBackend):
    """Concrete TSDBBackend backed by QuestDB.

    Manages the full lifecycle: binary start → DDL → ILP writes → SQL reads → stop.
    """

    def __init__(
        self,
        data_dir: str | None = None,
        host: str = "localhost",
        http_port: int | None = None,
        ilp_port: int | None = None,
        pg_port: int | None = None,
        ttl_days: int = 90,
        auto_start: bool = True,
    ):
        import os
        data_dir = data_dir or os.environ.get("QUESTDB_DATA_DIR", "data/questdb")
        http_port = http_port or int(os.environ.get("QUESTDB_HTTP_PORT", "9000"))
        ilp_port = ilp_port or int(os.environ.get("QUESTDB_ILP_PORT", "9009"))
        pg_port = pg_port or int(os.environ.get("QUESTDB_PG_PORT", "8812"))
        self._manager = QuestDBManager(
            data_dir=data_dir,
            host=host,
            http_port=http_port,
            ilp_port=ilp_port,
            pg_port=pg_port,
        )
        self._writer = QuestDBWriter(host=host, ilp_port=ilp_port)
        self._reader = QuestDBReader(host=host, pg_port=pg_port)
        self._ttl_days = ttl_days
        self._auto_start = auto_start

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start QuestDB, create tables, open writer connection."""
        if self._auto_start:
            await self._manager.start()
        self._reader.connect()
        create_tables(self._reader.connection, ttl_days=self._ttl_days)
        self._writer.connect()
        logger.info("QuestDBBackend started")

    async def stop(self) -> None:
        """Flush writer, close connections, stop QuestDB."""
        await self._writer.flush()
        self._writer.close()
        self._reader.close()
        if self._auto_start:
            await self._manager.stop()
        logger.info("QuestDBBackend stopped")

    # ── Write ──────────────────────────────────────────────────────────────────

    async def write_tick(self, msg: Tick | FXTick | CurveTick) -> None:
        await self._writer.write_tick(msg)

    async def flush(self) -> None:
        await self._writer.flush()

    # ── Read ───────────────────────────────────────────────────────────────────

    def get_all_ticks(
        self,
        msg_type: str,
        since: Optional[datetime] = None,
    ) -> list[dict]:
        return self._reader.get_all_ticks(msg_type, since)

    def get_ticks(
        self,
        msg_type: str,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> list[dict]:
        return self._reader.get_ticks(msg_type, symbol, start, end, limit)

    def get_bars(
        self,
        msg_type: str,
        symbol: str,
        interval: str = "1m",
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> list[Bar]:
        return self._reader.get_bars(msg_type, symbol, interval, start, end)

    def get_latest(
        self,
        msg_type: str,
        symbol: Optional[str] = None,
    ) -> list[dict]:
        return self._reader.get_latest(msg_type, symbol)
