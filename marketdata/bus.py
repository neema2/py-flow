"""
TickBus — Central Async Pub/Sub for Market Data
================================================
Feeds publish ticks; consumers subscribe with optional symbol filtering.
Each subscriber gets its own asyncio.Queue with backpressure (drop-oldest).
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass

from marketdata.models import CurveTick, FXTick, Tick, SwapTick, JacobianTick, get_symbol_key

logger = logging.getLogger(__name__)


@dataclass
class _Subscription:
    """Internal subscription state."""
    sub_id: str
    types: set[str] | None    # None = all types
    symbols: set[str] | None  # None = all symbols
    queue: asyncio.Queue
    active: bool = True


class TickBus:
    """Central async message bus for market data messages.

    - ``publish(msg)`` dispatches to all matching subscriber queues.
    - ``subscribe(types, symbols)`` returns an async iterator of messages.
    - ``latest`` dict provides snapshot cache keyed by ``(type, symbol_key)``.
    """

    def __init__(self, maxsize: int = 1000) -> None:
        self._maxsize = maxsize
        self._subscriptions: dict[str, _Subscription] = {}
        self._lock = asyncio.Lock()
        self.latest: dict[tuple[str, str], Tick | FXTick | CurveTick | SwapTick | JacobianTick] = {}

    async def publish(self, msg: Tick | FXTick | CurveTick | SwapTick | JacobianTick) -> None:
        """Publish a market data message to all matching subscribers.

        Updates the latest snapshot cache and dispatches to queues.
        If a subscriber's queue is full, the oldest item is dropped.
        """
        key = get_symbol_key(msg)
        self.latest[(msg.type, key)] = msg

        async with self._lock:
            subs = list(self._subscriptions.values())

        for sub in subs:
            if not sub.active:
                continue
            if sub.types is not None and msg.type not in sub.types:
                continue
            if sub.symbols is not None and key not in sub.symbols:
                continue
            try:
                if sub.queue.full():
                    # Drop oldest to avoid blocking the publisher
                    try:
                        sub.queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                sub.queue.put_nowait(msg)
            except Exception:
                logger.warning("Failed to enqueue msg for sub %s", sub.sub_id)

    def publish_sync(
        self, msg: Tick | FXTick | CurveTick | SwapTick | JacobianTick, loop: asyncio.AbstractEventLoop
    ) -> None:
        """Thread-safe publish — call from non-async code (e.g. feed threads).

        Schedules ``publish(msg)`` on the given event loop.
        """
        asyncio.run_coroutine_threadsafe(self.publish(msg), loop)

    async def subscribe(
        self,
        types: set[str] | None = None,
        symbols: set[str] | None = None,
    ) -> tuple[str, AsyncIterator[Tick | FXTick | CurveTick | SwapTick | JacobianTick]]:
        """Subscribe to messages, optionally filtered by type and symbol.

        Args:
            types: Set of message types to receive ("equity","fx","curve"),
                   or None for all.
            symbols: Set of symbols/pairs/labels to receive, or None for all.

        Returns:
            (subscription_id, async_iterator) — iterate to receive messages.
        """
        sub_id = str(uuid.uuid4())
        queue: asyncio.Queue = asyncio.Queue(maxsize=self._maxsize)
        sub = _Subscription(
            sub_id=sub_id, types=types, symbols=symbols, queue=queue,
        )

        async with self._lock:
            self._subscriptions[sub_id] = sub

        async def _iter() -> AsyncIterator[Tick | FXTick | CurveTick | SwapTick | JacobianTick]:
            try:
                while sub.active:
                    try:
                        msg = await asyncio.wait_for(queue.get(), timeout=1.0)
                        yield msg
                    except asyncio.TimeoutError:
                        continue
            finally:
                await self.unsubscribe(sub_id)

        return sub_id, _iter()

    async def unsubscribe(self, sub_id: str) -> None:
        """Remove a subscription and clean up its queue."""
        async with self._lock:
            sub = self._subscriptions.pop(sub_id, None)
            if sub:
                sub.active = False

    @property
    def subscriber_count(self) -> int:
        """Number of active subscriptions."""
        return len(self._subscriptions)
