"""
TSDBBackend — Abstract Base Class
==================================
Backend-agnostic interface for time-series storage.
Covers lifecycle (start/stop), write (tick ingestion), and read (history/bars).

Concrete implementations live in timeseries/backends/<name>/.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from marketdata.models import Tick, FXTick, CurveTick
from timeseries.models import Bar


class TSDBBackend(ABC):
    """Backend-agnostic time-series database interface."""

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    @abstractmethod
    async def start(self) -> None:
        """Start the backend (create tables, open connections)."""

    @abstractmethod
    async def stop(self) -> None:
        """Flush buffers and close connections."""

    # ── Write ──────────────────────────────────────────────────────────────────

    @abstractmethod
    async def write_tick(self, msg: Tick | FXTick | CurveTick) -> None:
        """Persist a single tick. Implementations should buffer internally."""

    @abstractmethod
    async def flush(self) -> None:
        """Force-flush any buffered writes."""

    # ── Read ───────────────────────────────────────────────────────────────────

    @abstractmethod
    def get_ticks(
        self,
        msg_type: str,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> list[dict]:
        """Raw tick history for a symbol within a time range."""

    @abstractmethod
    def get_bars(
        self,
        msg_type: str,
        symbol: str,
        interval: str = "1m",
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> list[Bar]:
        """OHLCV bars for a symbol at the given interval."""

    @abstractmethod
    def get_latest(
        self,
        msg_type: str,
        symbol: Optional[str] = None,
    ) -> list[dict]:
        """Latest tick(s) per symbol for a given message type."""
