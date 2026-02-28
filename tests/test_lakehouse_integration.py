"""
Lakehouse Integration Tests — Full Round-Trip
===============================================
End-to-end tests using the real public API:

  ObjectStoreServer  →  connect()  →  Storable.save()
       ↓ (object_events)
  SyncEngine  →  Iceberg (via Lakekeeper + MinIO)
       ↓
  Lakehouse (DuckDB SQL)

Two PostgreSQL instances:
  - pgserver (ObjectStoreServer): Storable object store with RLS
  - zonkyio (EmbeddedPG via start_lakehouse): Lakekeeper catalog backend

No Docker needed.

Run:
    python3 -m pytest tests/test_lakehouse_integration.py -v -s
"""

import asyncio
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

import psycopg2
import pytest

from store.base import Storable
from store.connection import connect
from store.server import ObjectStoreServer
from store.schema import provision_user
from marketdata.models import Tick, FXTick, CurveTick

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Storable types for testing ─────────────────────────────────────────────


@dataclass
class Trade(Storable):
    symbol: str = ""
    quantity: int = 0
    price: float = 0.0
    side: str = ""


@dataclass
class Order(Storable):
    symbol: str = ""
    quantity: int = 0
    price: float = 0.0
    side: str = ""


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def server():
    """Start the Storable object store (pgserver with RLS)."""
    tmp_dir = tempfile.mkdtemp(prefix="test_lakehouse_store_")
    srv = ObjectStoreServer(data_dir=tmp_dir, admin_password="test_admin_pw")
    srv.start()
    # Provision a test user
    admin_conn = srv.admin_conn()
    provision_user(admin_conn, "alice", "alice_pw")
    admin_conn.close()
    yield srv
    srv.stop()


@pytest.fixture(scope="module")
def stack():
    """Start the lakehouse stack (Lakekeeper PG + Lakekeeper + MinIO)."""
    from lakehouse.admin import start_lakehouse, stop_lakehouse
    # Use /tmp to keep Unix socket path < 103 bytes (macOS limit)
    tmp_dir = tempfile.mkdtemp(prefix="tst_lh_", dir="/tmp")
    s = asyncio.run(start_lakehouse(data_dir=tmp_dir))
    yield s
    asyncio.run(stop_lakehouse(s))


@pytest.fixture(scope="module")
def db(server):
    """Connect as alice via the public connect() API."""
    info = server.conn_info()
    conn = connect(
        host=info["host"], port=info["port"], dbname=info["dbname"],
        user="alice", password="alice_pw",
    )
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def seeded(db):
    """Write real Storable objects via the public API."""
    symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"]
    trades = []
    orders = []

    for i in range(6):
        t = Trade(
            symbol=symbols[i % len(symbols)],
            quantity=(i + 1) * 10,
            price=100.0 + i * 5.0,
            side="BUY" if i % 2 == 0 else "SELL",
        )
        t.save()
        trades.append(t)

    for i in range(4):
        o = Order(
            symbol=symbols[i % len(symbols)],
            quantity=(i + 1) * 25,
            price=200.0 + i * 10.0,
            side="BUY" if i % 2 == 0 else "SELL",
        )
        o.save()
        orders.append(o)

    yield {"trades": trades, "orders": orders, "total": 10}


@pytest.fixture(scope="module")
def pg_conn(server):
    """Admin psycopg2 connection to the Storable PG (for SyncEngine)."""
    conn = server.admin_conn()
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def tsdb():
    """Start a TSDBBackend via the public factory (real QuestDB)."""
    from timeseries import create_backend
    tmp_dir = tempfile.mkdtemp(prefix="test_lakehouse_questdb_")
    backend = create_backend(
        "questdb",
        data_dir=tmp_dir,
        http_port=19000,
        ilp_port=19009,
        pg_port=18812,
    )
    asyncio.run(backend.start())
    yield backend
    asyncio.run(backend.stop())


@pytest.fixture(scope="module")
def seeded_ticks(tsdb):
    """Write real Tick/FXTick/CurveTick objects via the public TSDBBackend API."""
    now = datetime.now(timezone.utc)

    # Equity ticks
    equity_ticks = []
    for i, sym in enumerate(["AAPL", "MSFT", "GOOG"]):
        for j in range(3):
            t = Tick(
                symbol=sym,
                price=150.0 + i * 10 + j * 0.5,
                bid=149.5 + i * 10 + j * 0.5,
                ask=150.5 + i * 10 + j * 0.5,
                volume=1000 + j * 100,
                change=j * 0.5,
                change_pct=j * 0.1,
                timestamp=now - timedelta(minutes=10 - j),
            )
            asyncio.run(tsdb.write_tick(t))
            equity_ticks.append(t)

    # FX ticks
    fx_ticks = []
    for i, pair in enumerate(["EUR/USD", "GBP/USD"]):
        for j in range(3):
            ft = FXTick(
                pair=pair,
                bid=1.08 + i * 0.2 + j * 0.001,
                ask=1.0805 + i * 0.2 + j * 0.001,
                mid=1.08025 + i * 0.2 + j * 0.001,
                spread_pips=0.5 + j * 0.1,
                currency="USD",
                timestamp=now - timedelta(minutes=10 - j),
            )
            asyncio.run(tsdb.write_tick(ft))
            fx_ticks.append(ft)

    # Curve ticks
    curve_ticks = []
    for i, label in enumerate(["USD_2Y", "USD_5Y"]):
        for j in range(2):
            ct = CurveTick(
                label=label,
                tenor_years=2.0 + i * 3,
                rate=0.04 + i * 0.005 + j * 0.001,
                discount_factor=0.92 - i * 0.02 - j * 0.001,
                currency="USD",
                timestamp=now - timedelta(minutes=10 - j),
            )
            asyncio.run(tsdb.write_tick(ct))
            curve_ticks.append(ct)

    asyncio.run(tsdb.flush())
    # QuestDB WAL needs time to commit ILP writes across all tables
    import time; time.sleep(4)

    total = len(equity_ticks) + len(fx_ticks) + len(curve_ticks)
    yield {
        "equity": equity_ticks,
        "fx": fx_ticks,
        "curve": curve_ticks,
        "total": total,
    }


@pytest.fixture(scope="module")
def catalog(stack):
    """PyIceberg REST catalog pointing at Lakekeeper + MinIO."""
    from lakehouse.admin import create_catalog
    return create_catalog(
        uri=stack.catalog_url,
        s3_endpoint=stack.s3_endpoint,
    )


@pytest.fixture(scope="module")
def iceberg_tables(catalog):
    """Ensure all Iceberg tables exist."""
    from lakehouse.admin import ensure_tables
    return ensure_tables(catalog)


@pytest.fixture(scope="module")
def sync_state_path():
    """Fresh sync state file per test run — prevents stale watermarks."""
    return os.path.join(tempfile.mkdtemp(prefix="test_lh_sync_"), "state.json")


# ── Test Classes ────────────────────────────────────────────────────────────


class TestLakehouseRoundTrip:
    """End-to-end: Storable.save() → sync → Iceberg → DuckDB query."""

    def test_storable_objects_created(self, seeded):
        """Storable objects have entity_ids after .save()."""
        for t in seeded["trades"]:
            assert t._store_entity_id is not None
            assert t._store_version == 1
            assert t._store_event_type == "CREATED"
        for o in seeded["orders"]:
            assert o._store_entity_id is not None

    def test_tables_created(self, iceberg_tables):
        """All 4 Iceberg tables should exist."""
        assert set(iceberg_tables.keys()) == {"events", "ticks", "bars_daily", "positions"}

    def test_sync_events_from_pg(self, catalog, pg_conn, iceberg_tables, seeded, sync_state_path):
        """Sync PG object_events → Iceberg events table."""
        from lakehouse.admin import SyncEngine
        sync = SyncEngine(
            catalog=catalog,
            state_path=sync_state_path,
        )

        count = sync.sync_events(pg_conn)

        assert count == seeded["total"]
        assert sync.state.events_synced == seeded["total"]
        assert sync.state.events_watermark is not None

    def test_query_events_via_duckdb(self, stack, seeded):
        """Query synced events via DuckDB SQL — see real Storable type_names."""
        from lakehouse import Lakehouse

        lq = Lakehouse(
            catalog_uri=stack.catalog_url,
            s3_endpoint=stack.s3_endpoint,
        )
        try:
            # Count all events
            results = lq.sql("SELECT count(*) as cnt FROM lakehouse.default.events")
            assert results[0]["cnt"] == seeded["total"]

            # Group by type_name — should see real qualified Storable type_names
            results = lq.sql("""
                SELECT type_name, count(*) as cnt
                FROM lakehouse.default.events
                GROUP BY type_name
                ORDER BY type_name
            """)
            type_names = {r["type_name"] for r in results}
            assert Trade.type_name() in type_names
            assert Order.type_name() in type_names

            # Verify data column has real Storable JSON
            results = lq.sql("""
                SELECT entity_id, type_name, data
                FROM lakehouse.default.events
                LIMIT 1
            """)
            assert results[0]["entity_id"] is not None
            assert "symbol" in results[0]["data"]

        finally:
            lq.close()

    def test_query_arrow_output(self, stack):
        """Query returning PyArrow Table."""
        import pyarrow as pa
        from lakehouse import Lakehouse

        lq = Lakehouse(
            catalog_uri=stack.catalog_url,
            s3_endpoint=stack.s3_endpoint,
        )
        try:
            table = lq.sql_arrow("SELECT * FROM lakehouse.default.events LIMIT 5")
            assert isinstance(table, pa.Table)
            assert table.num_rows <= 5
            assert "entity_id" in table.column_names
            assert "tx_time" in table.column_names
        finally:
            lq.close()

    def test_query_dataframe_output(self, stack):
        """Query returning Pandas DataFrame."""
        from lakehouse import Lakehouse

        lq = Lakehouse(
            catalog_uri=stack.catalog_url,
            s3_endpoint=stack.s3_endpoint,
        )
        try:
            df = lq.sql_df("SELECT type_name, count(*) as cnt FROM lakehouse.default.events GROUP BY type_name")
            assert len(df) > 0
            assert "type_name" in df.columns
            assert "cnt" in df.columns
        finally:
            lq.close()

    def test_incremental_sync(self, db, catalog, pg_conn, iceberg_tables, seeded, sync_state_path):
        """Second sync picks up only newly saved Storable objects."""
        from lakehouse.admin import SyncEngine
        sync = SyncEngine(
            catalog=catalog,
            state_path=sync_state_path,
        )

        # First: no new events since last sync
        count = sync.sync_events(pg_conn)
        assert count == 0

        # Save 3 more Trade objects via public API
        for i in range(3):
            Trade(symbol="NVDA", quantity=100 + i, price=800.0, side="BUY").save()

        # Sync again — should pick up only the 3 new events
        count = sync.sync_events(pg_conn)
        assert count == 3
        assert sync.state.events_synced == seeded["total"] + 3

    def test_list_tables(self, stack):
        """Lakehouse.tables() lists available tables."""
        from lakehouse import Lakehouse

        lq = Lakehouse(
            catalog_uri=stack.catalog_url,
            s3_endpoint=stack.s3_endpoint,
        )
        try:
            tables = lq.tables()
            assert "events" in tables
            assert "ticks" in tables
            assert "bars_daily" in tables
            assert "positions" in tables
        finally:
            lq.close()

    def test_row_count(self, stack, seeded):
        """Lakehouse.row_count() returns correct count after syncs."""
        from lakehouse import Lakehouse

        lq = Lakehouse(
            catalog_uri=stack.catalog_url,
            s3_endpoint=stack.s3_endpoint,
        )
        try:
            count = lq.row_count("events")
            # 10 original + 3 from incremental = 13
            assert count == seeded["total"] + 3
        finally:
            lq.close()

    def test_table_info(self, stack):
        """Lakehouse.table_info() returns column metadata."""
        from lakehouse import Lakehouse

        lq = Lakehouse(
            catalog_uri=stack.catalog_url,
            s3_endpoint=stack.s3_endpoint,
        )
        try:
            info = lq.table_info("events")
            col_names = [c["column_name"] for c in info]
            assert "entity_id" in col_names
            assert "type_name" in col_names
            assert "tx_time" in col_names
        finally:
            lq.close()

    def test_sync_ticks(self, catalog, tsdb, iceberg_tables, seeded_ticks, sync_state_path):
        """Sync TSDBBackend ticks → Iceberg ticks table."""
        from lakehouse.admin import SyncEngine
        sync = SyncEngine(
            catalog=catalog,
            state_path=sync_state_path,
        )

        count = sync.sync_ticks(tsdb)

        assert count == seeded_ticks["total"]
        assert sync.state.ticks_synced == seeded_ticks["total"]
        assert sync.state.ticks_watermark is not None

    def test_query_ticks_via_duckdb(self, stack, seeded_ticks):
        """Query synced ticks via DuckDB — see real tick types and symbols."""
        from lakehouse import Lakehouse

        lq = Lakehouse(
            catalog_uri=stack.catalog_url,
            s3_endpoint=stack.s3_endpoint,
        )
        try:
            # Total tick count
            results = lq.sql("SELECT count(*) as cnt FROM lakehouse.default.ticks")
            assert results[0]["cnt"] == seeded_ticks["total"]

            # By tick_type
            results = lq.sql("""
                SELECT tick_type, count(*) as cnt
                FROM lakehouse.default.ticks
                GROUP BY tick_type
                ORDER BY tick_type
            """)
            type_counts = {r["tick_type"]: r["cnt"] for r in results}
            assert type_counts["equity"] == len(seeded_ticks["equity"])
            assert type_counts["fx"] == len(seeded_ticks["fx"])
            assert type_counts["curve"] == len(seeded_ticks["curve"])

            # Verify equity tick has price and symbol
            results = lq.sql("""
                SELECT symbol, price, volume
                FROM lakehouse.default.ticks
                WHERE tick_type = 'equity'
                LIMIT 1
            """)
            assert results[0]["symbol"] in ("AAPL", "MSFT", "GOOG")
            assert results[0]["price"] > 0
            assert results[0]["volume"] > 0

            # Verify FX tick has mid and pair
            results = lq.sql("""
                SELECT symbol, mid, spread_pips
                FROM lakehouse.default.ticks
                WHERE tick_type = 'fx'
                LIMIT 1
            """)
            assert results[0]["symbol"] in ("EUR/USD", "GBP/USD")
            assert results[0]["mid"] > 0

        finally:
            lq.close()

    def test_ticks_row_count(self, stack, seeded_ticks):
        """Lakehouse.row_count('ticks') matches synced tick count."""
        from lakehouse import Lakehouse

        lq = Lakehouse(
            catalog_uri=stack.catalog_url,
            s3_endpoint=stack.s3_endpoint,
        )
        try:
            count = lq.row_count("ticks")
            assert count == seeded_ticks["total"]
        finally:
            lq.close()

    # ── Ingest + Transform tests ──────────────────────────────────────────

    def test_ingest_append(self, stack):
        """Ingest mode=append: raw append with _batch_id and _batch_ts."""
        import uuid as _uuid
        from lakehouse import Lakehouse

        tbl = f"test_append_{_uuid.uuid4().hex[:8]}"
        lh = Lakehouse(catalog_uri=stack.catalog_url, s3_endpoint=stack.s3_endpoint)
        try:
            data = [
                {"symbol": "AAPL", "score": 0.95, "signal": "BUY"},
                {"symbol": "MSFT", "score": 0.80, "signal": "HOLD"},
                {"symbol": "GOOG", "score": 0.60, "signal": "SELL"},
            ]
            count = lh.ingest(tbl, data, mode="append")
            assert count == 3

            # Append more
            count2 = lh.ingest(tbl, [{"symbol": "TSLA", "score": 0.70, "signal": "BUY"}], mode="append")
            assert count2 == 1

            # Query back — 4 total, has _batch_id/_batch_ts, no _is_current
            results = lh.query(f"SELECT * FROM lakehouse.default.{tbl}")
            assert len(results) == 4
            assert "_batch_id" in results[0]
            assert "_batch_ts" in results[0]
            assert "_is_current" not in results[0]

            # Two different batch IDs (one per ingest call)
            batch_ids = set(r["_batch_id"] for r in results)
            assert len(batch_ids) == 2
        finally:
            lh.close()

    def test_ingest_snapshot(self, stack):
        """Ingest mode=snapshot: batch versioning with _is_current."""
        import uuid as _uuid
        from lakehouse import Lakehouse

        tbl = f"test_snap_{_uuid.uuid4().hex[:8]}"
        lh = Lakehouse(catalog_uri=stack.catalog_url, s3_endpoint=stack.s3_endpoint)
        try:
            # First batch
            batch1 = [
                {"symbol": "AAPL", "score": 0.90},
                {"symbol": "MSFT", "score": 0.85},
            ]
            count = lh.ingest(tbl, batch1, mode="snapshot")
            assert count == 2

            # Query current — 2 rows, all _is_current = true
            results = lh.query(f"SELECT * FROM lakehouse.default.{tbl} WHERE _is_current = true")
            assert len(results) == 2
            batch_id_1 = results[0]["_batch_id"]
            assert results[0]["_batch_ts"] is not None

            # Second batch (replaces first)
            batch2 = [
                {"symbol": "AAPL", "score": 0.95},
                {"symbol": "GOOG", "score": 0.70},
                {"symbol": "TSLA", "score": 0.60},
            ]
            count2 = lh.ingest(tbl, batch2, mode="snapshot")
            assert count2 == 3

            # Current batch: 3 rows
            current = lh.query(f"SELECT * FROM lakehouse.default.{tbl} WHERE _is_current = true")
            assert len(current) == 3
            batch_id_2 = current[0]["_batch_id"]
            assert batch_id_2 != batch_id_1

            # Old batch still queryable
            old = lh.query(f"SELECT * FROM lakehouse.default.{tbl} WHERE _batch_id = '{batch_id_1}'")
            assert len(old) == 2
            assert all(not r["_is_current"] for r in old)

            # Total rows = 5 (2 expired + 3 current)
            total = lh.query(f"SELECT count(*) as cnt FROM lakehouse.default.{tbl}")
            assert total[0]["cnt"] == 5
        finally:
            lh.close()

    def test_ingest_incremental(self, stack):
        """Ingest mode=incremental: upsert by PK, soft delete, full audit trail."""
        import uuid as _uuid
        from lakehouse import Lakehouse

        tbl = f"test_incr_{_uuid.uuid4().hex[:8]}"
        lh = Lakehouse(catalog_uri=stack.catalog_url, s3_endpoint=stack.s3_endpoint)
        try:
            # Initial insert
            data1 = [
                {"trade_id": "T1", "symbol": "AAPL", "pnl": 100.0},
                {"trade_id": "T2", "symbol": "MSFT", "pnl": 200.0},
                {"trade_id": "T3", "symbol": "GOOG", "pnl": -50.0},
            ]
            count = lh.ingest(tbl, data1, mode="incremental", primary_key="trade_id")
            assert count == 3

            # All current
            current = lh.query(f"SELECT * FROM lakehouse.default.{tbl} WHERE _is_current = true ORDER BY trade_id")
            assert len(current) == 3
            assert current[0]["pnl"] == 100.0
            assert current[0]["_batch_id"] is not None

            # Update T1 and T2, leave T3 alone
            data2 = [
                {"trade_id": "T1", "symbol": "AAPL", "pnl": 150.0},
                {"trade_id": "T2", "symbol": "MSFT", "pnl": 250.0},
            ]
            count2 = lh.ingest(tbl, data2, mode="incremental", primary_key="trade_id")
            assert count2 == 2

            # Current rows: T1=150, T2=250, T3=-50 (unchanged)
            current2 = lh.query(f"SELECT * FROM lakehouse.default.{tbl} WHERE _is_current = true ORDER BY trade_id")
            assert len(current2) == 3
            assert current2[0]["pnl"] == 150.0  # T1 updated
            assert current2[1]["pnl"] == 250.0  # T2 updated
            assert current2[2]["pnl"] == -50.0   # T3 unchanged

            # Historical: old T1 and T2 versions (UPDATE set _is_current=false in place)
            history = lh.query(f"SELECT * FROM lakehouse.default.{tbl} WHERE _is_current = false")
            assert len(history) == 2
            old_pnls = sorted([r["pnl"] for r in history])
            assert old_pnls == [100.0, 200.0]

            # Total rows = 5 (3 original with T1,T2 expired + 2 new current)
            total = lh.query(f"SELECT count(*) as cnt FROM lakehouse.default.{tbl}")
            assert total[0]["cnt"] == 5
        finally:
            lh.close()

    def test_ingest_bitemporal(self, stack):
        """Ingest mode=bitemporal: system time + business time."""
        import uuid as _uuid
        from lakehouse import Lakehouse

        tbl = f"test_bitemp_{_uuid.uuid4().hex[:8]}"
        lh = Lakehouse(catalog_uri=stack.catalog_url, s3_endpoint=stack.s3_endpoint)
        try:
            data1 = [
                {"entity_id": "E1", "value": 100.0},
                {"entity_id": "E2", "value": 200.0},
            ]
            count = lh.ingest(tbl, data1, mode="bitemporal", primary_key="entity_id")
            assert count == 2

            # Current: both have _is_current=True, _tx_time set, _valid_from set, _valid_to null
            current = lh.query(f"SELECT * FROM lakehouse.default.{tbl} WHERE _is_current = true ORDER BY entity_id")
            assert len(current) == 2
            assert current[0]["_tx_time"] is not None
            assert current[0]["_valid_from"] is not None
            assert current[0]["_valid_to"] is None
            assert current[0]["_batch_id"] is not None

            # Update E1
            data2 = [{"entity_id": "E1", "value": 150.0}]
            count2 = lh.ingest(tbl, data2, mode="bitemporal", primary_key="entity_id")
            assert count2 == 1

            # Current: E1=150, E2=200
            current2 = lh.query(f"SELECT * FROM lakehouse.default.{tbl} WHERE _is_current = true ORDER BY entity_id")
            assert len(current2) == 2
            assert current2[0]["value"] == 150.0

            # Old E1 version: _is_current=False, _valid_to set
            old = lh.query(f"SELECT * FROM lakehouse.default.{tbl} WHERE _is_current = false")
            assert len(old) == 1
            assert old[0]["entity_id"] == "E1"
            assert old[0]["value"] == 100.0
            assert old[0]["_valid_to"] is not None
        finally:
            lh.close()

    def test_transform_append(self, stack, seeded):
        """Transform mode=append: SQL result written to new table."""
        import uuid as _uuid
        from lakehouse import Lakehouse

        tbl = f"test_xform_append_{_uuid.uuid4().hex[:8]}"
        lh = Lakehouse(catalog_uri=stack.catalog_url, s3_endpoint=stack.s3_endpoint)
        try:
            count = lh.transform(
                tbl,
                "SELECT type_name, count(*) as cnt FROM lakehouse.default.events GROUP BY type_name",
                mode="append",
            )
            assert count > 0

            # Query back
            results = lh.query(f"SELECT * FROM lakehouse.default.{tbl} ORDER BY cnt DESC")
            assert len(results) > 0
            assert "type_name" in results[0]
            assert "cnt" in results[0]
            assert "_batch_id" in results[0]
        finally:
            lh.close()

    def test_transform_snapshot(self, stack, seeded):
        """Transform mode=snapshot: SQL materialized view with batch history."""
        import uuid as _uuid
        from lakehouse import Lakehouse

        tbl = f"test_xform_snap_{_uuid.uuid4().hex[:8]}"
        lh = Lakehouse(catalog_uri=stack.catalog_url, s3_endpoint=stack.s3_endpoint)
        try:
            # First run
            count1 = lh.transform(
                tbl,
                "SELECT type_name, count(*) as cnt FROM lakehouse.default.events GROUP BY type_name",
                mode="snapshot",
            )
            assert count1 > 0

            current1 = lh.query(f"SELECT * FROM lakehouse.default.{tbl} WHERE _is_current = true")
            batch1 = current1[0]["_batch_id"]

            # Second run (same query, new batch)
            count2 = lh.transform(
                tbl,
                "SELECT type_name, count(*) as cnt FROM lakehouse.default.events GROUP BY type_name",
                mode="snapshot",
            )
            assert count2 > 0

            current2 = lh.query(f"SELECT * FROM lakehouse.default.{tbl} WHERE _is_current = true")
            batch2 = current2[0]["_batch_id"]
            assert batch2 != batch1

            # Old batch still there
            old = lh.query(f"SELECT * FROM lakehouse.default.{tbl} WHERE _batch_id = '{batch1}'")
            assert len(old) == count1
        finally:
            lh.close()
