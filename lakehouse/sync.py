"""
Lakehouse Sync Engine — Incremental ETL
=========================================
Syncs data from QuestDB (ticks/bars) into the Lakehouse via ``Lakehouse.ingest()``.
Watermark-based incremental sync — no full table scans after initial load.

Store events are synced via EventBridge + LakehouseSink (see bridge.sinks.lakehouse).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from lakehouse.models import SyncState

if TYPE_CHECKING:
    from lakehouse.query import Lakehouse
    from timeseries import TSDBBackend

logger = logging.getLogger(__name__)

# Watermark persistence file
DEFAULT_STATE_PATH = "data/lakehouse/sync_state.json"


class SyncEngine:
    """
    Incremental ETL engine: QuestDB → Lakehouse.

    Tracks watermarks (last synced timestamps) to avoid re-syncing old data.
    Watermarks are persisted to a local JSON file between runs.

    All writes go through ``Lakehouse.ingest(mode="append")``.
    Store events are handled separately by EventBridge + LakehouseSink.
    """

    def __init__(
        self,
        lakehouse: Lakehouse,
        state_path: str = DEFAULT_STATE_PATH,
    ) -> None:
        self._lakehouse = lakehouse
        self._state_path = Path(state_path).resolve()
        self._state = self._load_state()

    @property
    def state(self) -> SyncState:
        return self._state

    # ── Sync: QuestDB ticks → Lakehouse ticks table ────────────────────────

    def sync_ticks(self, backend: TSDBBackend) -> int:
        """
        Incremental sync from a TSDBBackend to Lakehouse ticks table.

        Args:
            backend: Any TSDBBackend instance (QuestDB, Memory, etc.).

        Returns:
            Number of ticks synced.
        """
        initial_watermark = self._state.ticks_watermark
        new_watermark = initial_watermark
        total = 0

        for tick_type in ("equity", "fx", "curve"):
            rows = backend.get_all_ticks(tick_type, since=initial_watermark)
            if not rows:
                continue

            arrow_table = _tick_rows_to_arrow(tick_type, rows)
            self._lakehouse.ingest("ticks", arrow_table, mode="append")
            total += len(rows)

            # Track the latest timestamp across all tick types
            for row in rows:
                ts = row.get("timestamp")
                if ts and (new_watermark is None or ts > new_watermark):
                    new_watermark = ts

        if total > 0:
            self._state.ticks_watermark = new_watermark
            self._state.ticks_synced += total
            self._state.last_sync_time = datetime.now(timezone.utc)
            self._save_state()
            logger.info("Synced %d ticks (watermark → %s)", total, new_watermark)

        return total

    # ── Sync: QuestDB bars → Lakehouse bars_daily table ────────────────────

    def sync_bars(self, backend: TSDBBackend, interval: str = "1d") -> int:
        """
        Sync OHLCV bars from a TSDBBackend to Lakehouse bars_daily table.

        This does a full refresh of bars (not incremental) since bar
        aggregation may update existing buckets as new ticks arrive.

        Args:
            backend: Any TSDBBackend instance (QuestDB, Memory, etc.).
            interval: Bar interval (default "1d").

        Returns:
            Number of bars synced.
        """
        total = 0

        for tick_type in ("equity", "fx", "curve"):
            try:
                bars = backend.get_bars(tick_type, "", interval=interval)
            except Exception as e:
                logger.warning("Failed to query bars for %s: %s", tick_type, e)
                continue
            if not bars:
                continue

            arrow_table = _bars_to_arrow(bars, tick_type, interval)
            self._lakehouse.ingest("bars_daily", arrow_table, mode="append")
            total += len(bars)

        if total > 0:
            self._state.bars_synced += total
            self._state.last_sync_time = datetime.now(timezone.utc)
            self._save_state()
            logger.info("Synced %d bars", total)

        return total

    # ── Full sync (convenience) ─────────────────────────────────────────────

    def sync_all(self, backend: TSDBBackend | None = None) -> dict:
        """
        Run all available sync operations.

        Returns dict with counts: {ticks, bars}.

        Note: Store events are synced separately via EventBridge + LakehouseSink.
        """
        result = {"ticks": 0, "bars": 0}

        if backend:
            result["ticks"] = self.sync_ticks(backend)
            result["bars"] = self.sync_bars(backend)

        return result

    def _load_state(self) -> SyncState:
        """Load sync state from disk."""
        if self._state_path.exists():
            try:
                data = json.loads(self._state_path.read_text())
                return SyncState(**data)
            except Exception as e:
                logger.warning("Failed to load sync state: %s", e)
        return SyncState()

    def _save_state(self) -> None:
        """Persist sync state to disk."""
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        self._state_path.write_text(self._state.model_dump_json(indent=2))


# ── Arrow conversion helpers ────────────────────────────────────────────────


def _tick_rows_to_arrow(tick_type: str, rows: list[dict]) -> pa.Table:
    """Convert QuestDB tick rows to unified Arrow table."""
    n = len(rows)

    arrow_schema = pa.schema([
        pa.field("tick_type", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("price", pa.float64(), nullable=True),
        pa.field("bid", pa.float64(), nullable=True),
        pa.field("ask", pa.float64(), nullable=True),
        pa.field("mid", pa.float64(), nullable=True),
        pa.field("volume", pa.int64(), nullable=True),
        pa.field("change", pa.float64(), nullable=True),
        pa.field("change_pct", pa.float64(), nullable=True),
        pa.field("spread_pips", pa.float64(), nullable=True),
        pa.field("rate", pa.float64(), nullable=True),
        pa.field("tenor_years", pa.float64(), nullable=True),
        pa.field("discount_factor", pa.float64(), nullable=True),
        pa.field("currency", pa.string(), nullable=True),
        pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
    ])

    result = {
        "tick_type": [tick_type] * n,
        "symbol": [r.get("symbol") or r.get("pair") or r.get("label") for r in rows],
        "price": [r.get("price") for r in rows],
        "bid": [r.get("bid") for r in rows],
        "ask": [r.get("ask") for r in rows],
        "mid": [r.get("mid") for r in rows],
        "volume": [int(r["volume"]) if r.get("volume") is not None else None for r in rows],
        "change": [r.get("change") for r in rows],
        "change_pct": [r.get("change_pct") for r in rows],
        "spread_pips": [r.get("spread_pips") for r in rows],
        "rate": [r.get("rate") for r in rows],
        "tenor_years": [r.get("tenor_years") for r in rows],
        "discount_factor": [r.get("discount_factor") for r in rows],
        "currency": [r.get("currency") for r in rows],
        "timestamp": [_ensure_tz(r.get("timestamp")) for r in rows],
    }
    return pa.table(result, schema=arrow_schema)


def _bars_to_arrow(bars: list, tick_type: str, interval: str) -> pa.Table:
    """Convert bar objects/dicts to Arrow table."""
    n = len(bars)

    def _get(bar: Any, key: str) -> Any:
        if isinstance(bar, dict):
            return bar.get(key)
        return getattr(bar, key, None)

    return pa.table({
        "symbol": pa.array([_get(b, "symbol") for b in bars], type=pa.string()),
        "tick_type": pa.array([tick_type] * n, type=pa.string()),
        "interval": pa.array([interval] * n, type=pa.string()),
        "open": pa.array([float(_get(b, "open") or 0) for b in bars], type=pa.float64()),
        "high": pa.array([float(_get(b, "high") or 0) for b in bars], type=pa.float64()),
        "low": pa.array([float(_get(b, "low") or 0) for b in bars], type=pa.float64()),
        "close": pa.array([float(_get(b, "close") or 0) for b in bars], type=pa.float64()),
        "volume": pa.array(
            [int(_get(b, "volume")) if _get(b, "volume") is not None else None for b in bars],
            type=pa.int64(),
        ),
        "trade_count": pa.array(
            [int(_get(b, "trade_count")) if _get(b, "trade_count") is not None else None for b in bars],
            type=pa.int32(),
        ),
        "timestamp": pa.array(
            [_ensure_tz(_get(b, "timestamp")) for b in bars],
            type=pa.timestamp("us", tz="UTC"),
        ),
    })


def _ensure_tz(dt: datetime | None) -> datetime | None:
    """Ensure a datetime is timezone-aware (UTC). Returns None if input is None."""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    return None  # type: ignore[unreachable]
