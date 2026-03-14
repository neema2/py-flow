"""
MemoryBackend — In-Memory TSDBBackend
======================================
Fully functional TSDBBackend that stores ticks in memory and supports
real bar aggregation. Useful for tests, development, and demos without
requiring QuestDB or any external process.

Implements the same read semantics (get_ticks, get_bars, get_latest)
so integration tests prove the real pipeline works.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from marketdata.models import CurveTick, FXTick, Tick, SwapTick, JacobianTick

from timeseries.base import TSDBBackend
from timeseries.models import Bar

logger = logging.getLogger(__name__)

# Same mappings as QuestDB backend — proves the abstraction works
_SYMBOL_COL = {"equity": "symbol", "fx": "pair", "curve": "label", "swap": "symbol", "jacobian": "symbol"}
_PRICE_COL = {"equity": "price", "fx": "mid", "curve": "rate", "swap": "rate", "jacobian": "value"}

# Interval string → timedelta
_INTERVAL_TD = {
    "1s": timedelta(seconds=1),
    "5s": timedelta(seconds=5),
    "15s": timedelta(seconds=15),
    "30s": timedelta(seconds=30),
    "1m": timedelta(minutes=1),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "30m": timedelta(minutes=30),
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "1d": timedelta(days=1),
}


class MemoryBackend(TSDBBackend):
    """In-memory TSDB backend with real bar aggregation."""

    def __init__(self) -> None:
        # Storage: {msg_type: [(timestamp, dict), ...]}
        self._ticks: dict[str, list[tuple[datetime, dict]]] = defaultdict(list)
        self._started = False

    @property
    def tick_count(self) -> int:
        """Total number of ticks stored across all types."""
        return sum(len(v) for v in self._ticks.values())

    async def start(self) -> None:
        self._started = True
        logger.info("MemoryBackend started")

    async def stop(self) -> None:
        self._started = False
        logger.info("MemoryBackend stopped (held %d ticks)", self.tick_count)

    async def write_tick(self, msg: Tick | FXTick | CurveTick | SwapTick | JacobianTick) -> None:
        row = msg.model_dump()
        ts = msg.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        row["timestamp"] = ts
        self._ticks[msg.type].append((ts, row))

    async def flush(self) -> None:
        pass  # No buffering needed — writes are immediate

    def get_all_ticks(
        self,
        msg_type: str,
        since: datetime | None = None,
    ) -> list[dict]:
        rows = self._ticks.get(msg_type, [])
        if since:
            if since.tzinfo is None:
                since = since.replace(tzinfo=timezone.utc)
            rows = [(ts, row) for ts, row in rows if ts > since]
        rows.sort(key=lambda x: x[0])
        return [row for _ts, row in rows]

    def get_ticks(
        self,
        msg_type: str,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> list[dict]:
        sym_col = _SYMBOL_COL.get(msg_type)
        if sym_col is None:
            raise ValueError(f"Unknown message type: {msg_type!r}")

        # Normalize to aware UTC for comparison
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)

        rows = self._ticks.get(msg_type, [])
        result = []
        for ts, row in rows:
            if row.get(sym_col) != symbol:
                continue
            if ts < start or ts > end:
                continue
            result.append(row)
            if len(result) >= limit:
                break
        return result

    def get_bars(
        self,
        msg_type: str,
        symbol: str,
        interval: str = "1m",
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Bar]:
        td = _INTERVAL_TD.get(interval)
        if td is None:
            raise ValueError(f"Invalid interval: {interval!r}")

        sym_col = _SYMBOL_COL.get(msg_type)
        price_col = _PRICE_COL.get(msg_type)
        if sym_col is None or price_col is None:
            raise ValueError(f"Unknown message type: {msg_type!r}")

        # Collect matching ticks
        rows = self._ticks.get(msg_type, [])
        matching = []
        for ts, row in rows:
            if row.get(sym_col) != symbol:
                continue
            if start and ts < start:
                continue
            if end and ts > end:
                continue
            matching.append((ts, row))

        if not matching:
            return []

        # Bucket into intervals
        matching.sort(key=lambda x: x[0])
        first_ts = matching[0][0]

        buckets: dict[datetime, list[dict]] = defaultdict(list)
        for ts, row in matching:
            # Floor timestamp to interval boundary
            delta = ts - first_ts
            bucket_idx = int(delta.total_seconds() // td.total_seconds())
            bucket_ts = first_ts + td * bucket_idx
            buckets[bucket_ts].append(row)

        # Aggregate into bars
        bars = []
        for bucket_ts in sorted(buckets.keys()):
            bucket = buckets[bucket_ts]
            prices = [r[price_col] for r in bucket]
            volume = sum(r.get("volume", 0) or 0 for r in bucket) or None
            bars.append(Bar(
                symbol=symbol,
                interval=interval,
                open=prices[0],
                high=max(prices),
                low=min(prices),
                close=prices[-1],
                volume=volume,
                trade_count=len(bucket),
                timestamp=bucket_ts,
            ))

        return bars

    def get_latest(
        self,
        msg_type: str,
        symbol: str | None = None,
    ) -> list[dict]:
        sym_col = _SYMBOL_COL.get(msg_type)
        if sym_col is None:
            raise ValueError(f"Unknown message type: {msg_type!r}")

        rows = self._ticks.get(msg_type, [])
        if not rows:
            return []

        # Group by symbol, keep last
        latest: dict[str, dict] = {}
        for _ts, row in rows:
            key = row.get(sym_col, "")
            latest[key] = row

        if symbol:
            match = latest.get(symbol)
            return [match] if match else []

        return list(latest.values())
