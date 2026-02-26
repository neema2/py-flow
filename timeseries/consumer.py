"""
TSDBConsumer — TickBus → TSDBBackend
====================================
Subscribes to the TickBus and routes every tick to a TSDBBackend.
Same pattern as marketdata.consumers.ws_publisher.WebSocketPublisher.

This module only imports the TSDBBackend ABC — never a concrete backend.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from marketdata.bus import TickBus
from timeseries.base import TSDBBackend

logger = logging.getLogger(__name__)


class TSDBConsumer:
    """TickBus consumer that routes ticks to a TSDBBackend."""

    def __init__(self, bus: TickBus, backend: TSDBBackend):
        self._bus = bus
        self._backend = backend
        self._sub_id: Optional[str] = None
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Subscribe to all ticks on the bus and start the write loop."""
        self._sub_id, tick_iter = await self._bus.subscribe()
        self._task = asyncio.create_task(self._write_loop(tick_iter))
        logger.info("TSDBConsumer started (sub_id=%s)", self._sub_id)

    async def stop(self) -> None:
        """Unsubscribe, cancel the write loop, and flush remaining buffer."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._sub_id:
            await self._bus.unsubscribe(self._sub_id)
        await self._backend.flush()
        logger.info("TSDBConsumer stopped")

    async def _write_loop(self, msg_iter) -> None:
        """Main loop: read ticks from bus, write to backend."""
        try:
            async for msg in msg_iter:
                try:
                    await self._backend.write_tick(msg)
                except Exception:
                    logger.exception("TSDBConsumer write error")
        except asyncio.CancelledError:
            pass
