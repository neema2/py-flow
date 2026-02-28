"""
Timeseries — User-facing time-series client
=============================================
Connects to a running TsdbServer via alias, or auto-starts one.
Wraps a TSDBBackend internally — user never sees QuestDB.

Usage::

    from timeseries import Timeseries

    ts = Timeseries("demo")          # connect via alias
    await ts.start()
    bars = ts.get_bars("equity", "AAPL", interval="1m")
    await ts.stop()
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

from timeseries.base import TSDBBackend
from timeseries.models import Bar
from marketdata.models import Tick, FXTick, CurveTick


class Timeseries:
    """User-facing time-series client.

    Connects to a TSDB via alias or auto-starts a local instance.
    Delegates to a TSDBBackend internally.
    """

    def __init__(
        self,
        alias_or_backend: str | None = None,
        *,
        data_dir: str | None = None,
        backend: str | None = None,
        # Pass-through kwargs for advanced/testing use
        **kwargs,
    ):
        self._backend: TSDBBackend | None = None
        self._auto_server = None
        self._alias = alias_or_backend
        self._data_dir = data_dir
        self._backend_name = backend
        self._kwargs = kwargs

    async def start(self) -> "Timeseries":
        """Start the timeseries client (and auto-start server if needed)."""
        self._backend = self._create_backend()
        await self._backend.start()
        return self

    async def stop(self) -> None:
        """Stop the client and any auto-started server."""
        if self._backend:
            await self._backend.stop()
            self._backend = None
        if self._auto_server:
            await self._auto_server.stop()
            self._auto_server = None

    def _create_backend(self) -> TSDBBackend:
        """Resolve alias/config and create a backend."""
        backend_name = self._backend_name or os.environ.get("TSDB_BACKEND", "questdb")

        # 1. Try alias
        if self._alias is not None:
            from timeseries._registry import resolve_alias
            resolved = resolve_alias(self._alias)
            if resolved is not None:
                return self._backend_from_name(backend_name, **resolved, **self._kwargs)
            # Not an alias — could be a backend name for backward compat
            return self._backend_from_name(self._alias, **self._kwargs)

        # 2. Auto-start from data_dir
        if self._data_dir is not None:
            return self._backend_from_name(
                backend_name, data_dir=self._data_dir, auto_start=True, **self._kwargs
            )

        # 3. Default — auto-start
        return self._backend_from_name(backend_name, auto_start=True, **self._kwargs)

    @staticmethod
    def _backend_from_name(name: str, **kwargs) -> TSDBBackend:
        """Create a backend by name."""
        if name == "questdb":
            from timeseries.backends.questdb import QuestDBBackend
            return QuestDBBackend(**kwargs)
        if name == "memory":
            from timeseries.backends.memory import MemoryBackend
            return MemoryBackend(**kwargs)
        raise ValueError(f"Unknown TSDB backend: {name!r}. Available: 'questdb', 'memory'")

    # ── Delegate to backend ──────────────────────────────────────────────

    async def write_tick(self, msg: Tick | FXTick | CurveTick) -> None:
        return await self._backend.write_tick(msg)

    async def flush(self) -> None:
        return await self._backend.flush()

    def get_all_ticks(self, msg_type: str, since: Optional[datetime] = None) -> list[dict]:
        return self._backend.get_all_ticks(msg_type, since)

    def get_ticks(self, msg_type: str, symbol: str,
                  start: datetime, end: datetime, limit: int = 1000) -> list[dict]:
        return self._backend.get_ticks(msg_type, symbol, start, end, limit)

    def get_bars(self, msg_type: str, symbol: str, interval: str = "1m",
                 start: Optional[datetime] = None, end: Optional[datetime] = None) -> list[Bar]:
        return self._backend.get_bars(msg_type, symbol, interval, start, end)

    def get_latest(self, msg_type: str, symbol: str) -> Optional[dict]:
        return self._backend.get_latest(msg_type, symbol)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.stop()
