"""
Lakehouse Sync Engine — Incremental ETL
=========================================
Syncs data from PG (object_events) and QuestDB (ticks/bars) into Iceberg
tables via PyArrow. Watermark-based incremental sync — no full table scans
after initial load.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from pyiceberg.catalog import Catalog

from lakehouse.models import SyncState

if TYPE_CHECKING:
    from store._types import Connection
    from timeseries.base import TSDBBackend

logger = logging.getLogger(__name__)

# Watermark persistence file
DEFAULT_STATE_PATH = "data/lakehouse/sync_state.json"


class SyncEngine:
    """
    Incremental ETL engine: PG + QuestDB → Iceberg.

    Tracks watermarks (last synced timestamps) to avoid re-syncing old data.
    Watermarks are persisted to a local JSON file between runs.
    """

    def __init__(
        self,
        catalog: Catalog,
        state_path: str = DEFAULT_STATE_PATH,
        namespace: str = "default",
    ) -> None:
        self._catalog = catalog
        self._state_path = Path(state_path).resolve()
        self._namespace = namespace
        self._state = self._load_state()

    @property
    def state(self) -> SyncState:
        return self._state

    # ── Sync: PG object_events → Iceberg events table ──────────────────────

    def sync_events(self, pg_conn: Connection) -> int:
        """
        Incremental sync from PG object_events to Iceberg events table.

        Args:
            pg_conn: Database connection to the source store.

        Returns:
            Number of events synced.
        """
        watermark = self._state.events_watermark
        table = self._catalog.load_table((self._namespace, "events"))

        with pg_conn.cursor() as cur:
            if watermark:
                cur.execute(
                    """
                    SELECT event_id, entity_id, version, type_name, owner,
                           updated_by, data, state, event_type, event_meta,
                           tx_time, valid_from, valid_to
                    FROM object_events
                    WHERE tx_time > %s
                    ORDER BY tx_time ASC
                    """,
                    (watermark,),
                )
            else:
                cur.execute(
                    """
                    SELECT event_id, entity_id, version, type_name, owner,
                           updated_by, data, state, event_type, event_meta,
                           tx_time, valid_from, valid_to
                    FROM object_events
                    ORDER BY tx_time ASC
                    """
                )

            rows = cur.fetchall()

        if not rows:
            logger.info("No new events to sync")
            return 0

        # Convert to Arrow table
        arrow_table = _pg_rows_to_events_arrow(rows)
        table.append(arrow_table)

        # Update watermark to the latest tx_time
        last_tx_time = rows[-1][10]  # tx_time column
        self._state.events_watermark = last_tx_time
        self._state.events_synced += len(rows)
        self._state.last_sync_time = datetime.now(timezone.utc)
        self._save_state()

        logger.info("Synced %d events (watermark → %s)", len(rows), last_tx_time)
        return len(rows)

    # ── Sync: QuestDB ticks → Iceberg ticks table ──────────────────────────

    def sync_ticks(self, backend: TSDBBackend) -> int:
        """
        Incremental sync from a TSDBBackend to Iceberg ticks table.

        Args:
            backend: Any TSDBBackend instance (QuestDB, Memory, etc.).

        Returns:
            Number of ticks synced.
        """
        initial_watermark = self._state.ticks_watermark
        new_watermark = initial_watermark
        table = self._catalog.load_table((self._namespace, "ticks"))
        total = 0

        for tick_type in ("equity", "fx", "curve"):
            rows = backend.get_all_ticks(tick_type, since=initial_watermark)
            if not rows:
                continue

            arrow_table = _tick_rows_to_arrow(tick_type, rows)
            table.append(arrow_table)
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

    # ── Sync: QuestDB bars → Iceberg bars_daily table ──────────────────────

    def sync_bars(self, backend: TSDBBackend, interval: str = "1d") -> int:
        """
        Sync OHLCV bars from a TSDBBackend to Iceberg bars_daily table.

        This does a full refresh of bars (not incremental) since bar
        aggregation may update existing buckets as new ticks arrive.

        Args:
            backend: Any TSDBBackend instance (QuestDB, Memory, etc.).
            interval: Bar interval (default "1d").

        Returns:
            Number of bars synced.
        """
        table = self._catalog.load_table((self._namespace, "bars_daily"))
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
            table.append(arrow_table)
            total += len(bars)

        if total > 0:
            self._state.bars_synced += total
            self._state.last_sync_time = datetime.now(timezone.utc)
            self._save_state()
            logger.info("Synced %d bars", total)

        return total

    # ── Full sync (convenience) ─────────────────────────────────────────────

    def sync_all(self, pg_conn: Connection | None = None, backend: TSDBBackend | None = None) -> dict:
        """
        Run all available sync operations.

        Returns dict with counts: {events, ticks, bars}.
        """
        result = {"events": 0, "ticks": 0, "bars": 0}

        if pg_conn:
            result["events"] = self.sync_events(pg_conn)

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


def _pg_rows_to_events_arrow(rows: list[tuple]) -> pa.Table:
    """Convert PG object_events rows to Arrow table matching EVENTS_SCHEMA."""
    event_ids = []
    entity_ids = []
    versions = []
    type_names = []
    owners = []
    updated_bys = []
    datas = []
    states = []
    event_types = []
    event_metas = []
    tx_times = []
    valid_froms = []
    valid_tos = []

    for row in rows:
        (event_id, entity_id, version, type_name, owner,
         updated_by, data, state, event_type, event_meta,
         tx_time, valid_from, valid_to) = row

        event_ids.append(str(event_id) if event_id else None)
        entity_ids.append(str(entity_id) if entity_id else None)
        versions.append(version)
        type_names.append(type_name)
        owners.append(owner)
        updated_bys.append(updated_by)
        datas.append(json.dumps(data) if isinstance(data, dict) else str(data) if data else None)
        states.append(state)
        event_types.append(event_type)
        event_metas.append(
            json.dumps(event_meta) if isinstance(event_meta, dict) else str(event_meta) if event_meta else None
        )
        tx_times.append(_ensure_tz(tx_time))
        valid_froms.append(_ensure_tz(valid_from))
        valid_tos.append(_ensure_tz(valid_to))

    # Build schema matching EVENTS_SCHEMA required/optional fields
    arrow_schema = pa.schema([
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("entity_id", pa.string(), nullable=False),
        pa.field("version", pa.int32(), nullable=False),
        pa.field("type_name", pa.string(), nullable=False),
        pa.field("owner", pa.string(), nullable=True),
        pa.field("updated_by", pa.string(), nullable=True),
        pa.field("data", pa.string(), nullable=True),
        pa.field("state", pa.string(), nullable=True),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("event_meta", pa.string(), nullable=True),
        pa.field("tx_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("valid_from", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("valid_to", pa.timestamp("us", tz="UTC"), nullable=True),
    ])

    return pa.table({
        "event_id": event_ids,
        "entity_id": entity_ids,
        "version": versions,
        "type_name": type_names,
        "owner": owners,
        "updated_by": updated_bys,
        "data": datas,
        "state": states,
        "event_type": event_types,
        "event_meta": event_metas,
        "tx_time": tx_times,
        "valid_from": valid_froms,
        "valid_to": valid_tos,
    }, schema=arrow_schema)


def _tick_rows_to_arrow(tick_type: str, rows: list[dict]) -> pa.Table:
    """Convert QuestDB tick rows to unified Arrow table matching TICKS_SCHEMA."""
    n = len(rows)

    # Build schema matching TICKS_SCHEMA required/optional fields
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
    """Convert bar objects/dicts to Arrow table matching BARS_SCHEMA."""
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
