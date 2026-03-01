"""
Lakehouse Tests
================
Unit tests for the lakehouse package: models, table schemas, Arrow conversion,
sync watermarks. Integration tests require Lakekeeper + object store (marked separately).
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from lakehouse.models import SyncState, TableInfo
from lakehouse.tables import (
    BARS_PARTITION,
    BARS_SCHEMA,
    EVENTS_PARTITION,
    EVENTS_SCHEMA,
    NAMESPACE,
    POSITIONS_PARTITION,
    POSITIONS_SCHEMA,
    TABLE_DEFS,
    TICKS_PARTITION,
    TICKS_SCHEMA,
)
from lakehouse.sync import (
    _bars_to_arrow,
    _ensure_tz,
    _pg_rows_to_events_arrow,
    _tick_rows_to_arrow,
)


# ── Model Tests ─────────────────────────────────────────────────────────────


class TestSyncState:
    """Tests for the SyncState Pydantic model."""

    def test_defaults(self):
        state = SyncState()
        assert state.events_watermark is None
        assert state.ticks_watermark is None
        assert state.bars_watermark is None
        assert state.last_sync_time is None
        assert state.events_synced == 0
        assert state.ticks_synced == 0
        assert state.bars_synced == 0

    def test_round_trip_json(self):
        now = datetime.now(timezone.utc)
        state = SyncState(
            events_watermark=now,
            events_synced=42,
            last_sync_time=now,
        )
        dumped = state.model_dump_json()
        restored = SyncState(**json.loads(dumped))
        assert restored.events_synced == 42
        assert restored.events_watermark is not None


class TestTableInfo:
    """Tests for the TableInfo Pydantic model."""

    def test_defaults(self):
        info = TableInfo(name="events")
        assert info.name == "events"
        assert info.namespace == "default"
        assert info.snapshot_count == 0
        assert info.schema_fields == []
        assert info.partition_fields == []

    def test_full(self):
        info = TableInfo(
            name="ticks",
            namespace="prod",
            current_snapshot_id=12345,
            snapshot_count=3,
            schema_fields=["symbol", "price", "timestamp"],
            partition_fields=["tick_type", "timestamp_day"],
        )
        assert info.current_snapshot_id == 12345
        assert len(info.schema_fields) == 3


# ── Schema Tests ────────────────────────────────────────────────────────────


class TestIcebergSchemas:
    """Tests for Iceberg table schema definitions."""

    def test_events_schema_fields(self):
        field_names = [f.name for f in EVENTS_SCHEMA.fields]
        assert "event_id" in field_names
        assert "entity_id" in field_names
        assert "type_name" in field_names
        assert "tx_time" in field_names
        assert "valid_from" in field_names
        assert "data" in field_names
        assert "event_type" in field_names

    def test_events_partition_spec(self):
        assert len(EVENTS_PARTITION.fields) == 2
        names = [f.name for f in EVENTS_PARTITION.fields]
        assert "type_name_identity" in names
        assert "tx_time_day" in names

    def test_ticks_schema_fields(self):
        field_names = [f.name for f in TICKS_SCHEMA.fields]
        assert "tick_type" in field_names
        assert "symbol" in field_names
        assert "price" in field_names
        assert "bid" in field_names
        assert "mid" in field_names
        assert "rate" in field_names
        assert "timestamp" in field_names

    def test_ticks_partition_spec(self):
        assert len(TICKS_PARTITION.fields) == 2
        names = [f.name for f in TICKS_PARTITION.fields]
        assert "tick_type_identity" in names
        assert "timestamp_day" in names

    def test_bars_schema_fields(self):
        field_names = [f.name for f in BARS_SCHEMA.fields]
        for col in ["symbol", "tick_type", "interval", "open", "high", "low", "close", "timestamp"]:
            assert col in field_names

    def test_bars_partition_spec(self):
        assert len(BARS_PARTITION.fields) == 2

    def test_positions_schema_fields(self):
        field_names = [f.name for f in POSITIONS_SCHEMA.fields]
        assert "entity_id" in field_names
        assert "symbol" in field_names
        assert "quantity" in field_names
        assert "valid_from" in field_names

    def test_positions_partition_spec(self):
        assert len(POSITIONS_PARTITION.fields) == 1

    def test_table_defs_registry(self):
        assert set(TABLE_DEFS.keys()) == {"events", "ticks", "bars_daily", "positions"}
        for name, (schema, partition) in TABLE_DEFS.items():
            assert len(schema.fields) > 0
            assert len(partition.fields) > 0


# ── Arrow Conversion Tests ──────────────────────────────────────────────────


class TestArrowConversion:
    """Tests for PG/QuestDB row → Arrow table conversion helpers."""

    def test_pg_rows_to_events_arrow(self):
        now = datetime.now(timezone.utc)
        rows = [
            (
                "evt-1", "ent-1", 1, "Trade", "alice",
                "alice", {"symbol": "AAPL", "price": 150.0}, "ACTIVE", "CREATED", None,
                now, now, None,
            ),
            (
                "evt-2", "ent-2", 1, "Order", "bob",
                "bob", {"symbol": "MSFT"}, None, "CREATED", {"guard": "check"},
                now, now, None,
            ),
        ]
        table = _pg_rows_to_events_arrow(rows)
        assert isinstance(table, pa.Table)
        assert table.num_rows == 2
        assert "event_id" in table.column_names
        assert "entity_id" in table.column_names
        assert "tx_time" in table.column_names
        assert table.column("event_id")[0].as_py() == "evt-1"
        assert table.column("type_name")[1].as_py() == "Order"

    def test_pg_rows_handles_none_data(self):
        now = datetime.now(timezone.utc)
        rows = [
            ("evt-1", "ent-1", 1, "Trade", None, None, None, None, "CREATED", None, now, now, None),
        ]
        table = _pg_rows_to_events_arrow(rows)
        assert table.num_rows == 1

    def test_tick_rows_to_arrow_equity(self):
        now = datetime.now(timezone.utc)
        rows = [
            {"symbol": "AAPL", "price": 150.0, "bid": 149.9, "ask": 150.1,
             "volume": 1000, "change": 1.5, "change_pct": 0.01, "timestamp": now},
            {"symbol": "MSFT", "price": 300.0, "bid": 299.9, "ask": 300.1,
             "volume": 2000, "change": -0.5, "change_pct": -0.002, "timestamp": now},
        ]
        table = _tick_rows_to_arrow("equity", rows)
        assert table.num_rows == 2
        assert table.column("tick_type")[0].as_py() == "equity"
        assert table.column("symbol")[0].as_py() == "AAPL"
        assert table.column("price")[0].as_py() == 150.0

    def test_tick_rows_to_arrow_fx(self):
        now = datetime.now(timezone.utc)
        rows = [
            {"pair": "EUR/USD", "bid": 1.0850, "ask": 1.0852, "mid": 1.0851,
             "spread_pips": 0.2, "currency": "USD", "timestamp": now},
        ]
        table = _tick_rows_to_arrow("fx", rows)
        assert table.num_rows == 1
        assert table.column("symbol")[0].as_py() == "EUR/USD"
        assert table.column("mid")[0].as_py() == 1.0851

    def test_tick_rows_to_arrow_curve(self):
        now = datetime.now(timezone.utc)
        rows = [
            {"label": "USD_5Y", "rate": 0.045, "tenor_years": 5.0,
             "discount_factor": 0.82, "currency": "USD", "timestamp": now},
        ]
        table = _tick_rows_to_arrow("curve", rows)
        assert table.num_rows == 1
        assert table.column("symbol")[0].as_py() == "USD_5Y"
        assert table.column("rate")[0].as_py() == 0.045

    def test_bars_to_arrow(self):
        now = datetime.now(timezone.utc)
        bars = [
            {"symbol": "AAPL", "open": 150.0, "high": 155.0, "low": 148.0,
             "close": 153.0, "volume": 5000, "trade_count": 100, "timestamp": now},
        ]
        table = _bars_to_arrow(bars, "equity", "1d")
        assert table.num_rows == 1
        assert table.column("tick_type")[0].as_py() == "equity"
        assert table.column("interval")[0].as_py() == "1d"
        assert table.column("high")[0].as_py() == 155.0

    def test_bars_to_arrow_with_bar_objects(self):
        """Test conversion with objects that have attributes instead of dict keys."""
        now = datetime.now(timezone.utc)

        class MockBar:
            def __init__(self):
                self.symbol = "MSFT"
                self.open = 300.0
                self.high = 310.0
                self.low = 295.0
                self.close = 305.0
                self.volume = 3000
                self.trade_count = 50
                self.timestamp = now

        bars = [MockBar()]
        table = _bars_to_arrow(bars, "equity", "1d")
        assert table.num_rows == 1
        assert table.column("symbol")[0].as_py() == "MSFT"


# ── Timezone Helper Tests ───────────────────────────────────────────────────


class TestEnsureTz:
    """Tests for the _ensure_tz helper."""

    def test_none(self):
        assert _ensure_tz(None) is None

    def test_naive_datetime(self):
        dt = datetime(2025, 1, 1, 12, 0, 0)
        result = _ensure_tz(dt)
        assert result.tzinfo == timezone.utc

    def test_aware_datetime_passthrough(self):
        dt = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = _ensure_tz(dt)
        assert result is dt

    def test_non_datetime(self):
        assert _ensure_tz("not a datetime") is None
        assert _ensure_tz(12345) is None


# ── Catalog Tests ───────────────────────────────────────────────────────────


class TestCatalogConfig:
    """Tests for catalog configuration logic."""

    def test_default_env_vars(self):
        from lakehouse.catalog import (
            DEFAULT_CATALOG_URI,
            DEFAULT_S3_ACCESS_KEY,
            DEFAULT_S3_ENDPOINT,
            DEFAULT_WAREHOUSE,
        )
        assert "8181" in DEFAULT_CATALOG_URI
        assert "9002" in DEFAULT_S3_ENDPOINT
        assert DEFAULT_WAREHOUSE == "lakehouse"
        assert DEFAULT_S3_ACCESS_KEY == "minioadmin"


# ── Service Manager Tests ──────────────────────────────────────────────────


class TestLakekeeperManager:
    """Tests for LakekeeperManager configuration."""

    def test_defaults(self):
        from lakehouse.services import LakekeeperManager
        mgr = LakekeeperManager()
        assert mgr._port == 8181
        assert mgr.catalog_url == "http://localhost:8181/catalog"
        assert not mgr.is_running

    def test_custom_port(self):
        from lakehouse.services import LakekeeperManager
        mgr = LakekeeperManager(port=9999)
        assert mgr._port == 9999
        assert mgr.catalog_url == "http://localhost:9999/catalog"


class TestObjectStoreBackend:
    """Tests for objectstore backend configuration."""

    def test_defaults(self):
        from objectstore._minio import _MinIOBackend
        mgr = _MinIOBackend()
        assert mgr._api_port == 9002
        assert mgr._console_port == 9003
        assert mgr.endpoint == "http://localhost:9002"
        assert not mgr.is_running

    def test_custom_ports(self):
        from objectstore._minio import _MinIOBackend
        mgr = _MinIOBackend(api_port=9010, console_port=9011)
        assert mgr.endpoint == "http://localhost:9010"


# ── Sync Engine Tests ──────────────────────────────────────────────────────


class TestSyncEngine:
    """Tests for SyncEngine watermark and state logic."""

    def test_state_persistence(self, tmp_path):
        """Test that sync state round-trips through JSON."""
        state_file = tmp_path / "sync_state.json"

        mock_catalog = MagicMock()
        from lakehouse.sync import SyncEngine
        engine = SyncEngine(catalog=mock_catalog, state_path=str(state_file))

        assert engine.state.events_synced == 0

        # Manually update state
        engine._state.events_synced = 100
        engine._state.events_watermark = datetime(2025, 6, 1, tzinfo=timezone.utc)
        engine._save_state()

        # Create new engine reading same file
        engine2 = SyncEngine(catalog=mock_catalog, state_path=str(state_file))
        assert engine2.state.events_synced == 100
        assert engine2.state.events_watermark is not None

    def test_sync_events_empty(self, tmp_path):
        """Test sync_events with no new rows."""
        state_file = tmp_path / "sync_state.json"
        mock_catalog = MagicMock()

        from lakehouse.sync import SyncEngine
        engine = SyncEngine(catalog=mock_catalog, state_path=str(state_file))

        # Mock PG connection returning no rows
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        count = engine.sync_events(mock_conn)
        assert count == 0

    def test_sync_all_with_no_sources(self, tmp_path):
        """Test sync_all with no sources provided."""
        state_file = tmp_path / "sync_state.json"
        mock_catalog = MagicMock()

        from lakehouse.sync import SyncEngine
        engine = SyncEngine(catalog=mock_catalog, state_path=str(state_file))

        result = engine.sync_all()
        assert result == {"events": 0, "ticks": 0, "bars": 0}


# ── Platform Detection Tests ───────────────────────────────────────────────


class TestPlatformDetection:
    """Tests for binary download URL detection."""

    def test_lakekeeper_archive_name(self):
        from lakehouse.services import _lakekeeper_archive_name
        name = _lakekeeper_archive_name()
        assert name.endswith(".tar.gz")
        assert "lakekeeper" in name

    def test_objectstore_download_url(self):
        from objectstore._minio import _minio_download_url
        url = _minio_download_url()
        assert "dl.min.io" in url
