"""
Datacube Lakehouse Integration Tests
======================================
End-to-end tests using a real Lakehouse stack (Lakekeeper + S3 object store).

Data flows:  ingest → Iceberg (Parquet on S3) → DuckDB Iceberg extension → Datacube

No mocks.  Requires the full lakehouse infrastructure to start.

Run:
    python3 -m pytest tests/test_datacube_integration.py -v -s
"""

import asyncio
import logging
import tempfile

import pyarrow as pa
import pandas as pd
import pytest

from datacube.config import PIVOT_COLUMN_NAME_SEPARATOR
from datacube.engine import Datacube, _is_lakehouse, _columns_from_lakehouse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def stack():
    """Start the lakehouse stack (PG + Lakekeeper + S3 object store)."""
    from lakehouse.admin import LakehouseServer
    tmp_dir = tempfile.mkdtemp(prefix="tst_dc_", dir="/tmp")
    srv = LakehouseServer(data_dir=tmp_dir)
    asyncio.run(srv.start())
    yield srv
    asyncio.run(srv.stop())


@pytest.fixture(scope="module")
def lh(stack):
    """Create a real Lakehouse instance connected to the test stack."""
    from lakehouse import Lakehouse
    lakehouse = Lakehouse(
        catalog_uri=stack.catalog_url,
        s3_endpoint=stack.s3_endpoint,
    )
    yield lakehouse
    lakehouse.close()


@pytest.fixture(scope="module")
def seeded(lh):
    """Ingest test data into Iceberg tables via Lakehouse.ingest()."""
    # ── trades table ──
    trades = pa.table({
        "sector": ["Tech", "Tech", "Tech", "Tech", "Finance", "Finance", "Finance", "Finance", "Energy", "Energy"],
        "symbol": ["AAPL", "AAPL", "GOOGL", "GOOGL", "JPM", "JPM", "GS", "GS", "XOM", "XOM"],
        "side":   ["BUY", "SELL", "BUY", "SELL", "BUY", "SELL", "BUY", "SELL", "BUY", "SELL"],
        "quantity": [100, 50, 200, 100, 300, 150, 250, 100, 400, 200],
        "price":  [150.0, 155.0, 2800.0, 2850.0, 160.0, 162.0, 380.0, 385.0, 95.0, 97.0],
        "pnl":    [500.0, -200.0, 1000.0, -500.0, 800.0, -300.0, 600.0, -100.0, 200.0, -50.0],
    })
    n_trades = lh.ingest("trades", trades, mode="append")

    # ── positions table ──
    positions = pa.table({
        "symbol": ["AAPL", "GOOGL", "JPM", "GS", "XOM"],
        "market_value": [50000.0, 200000.0, 80000.0, 95000.0, 38000.0],
    })
    n_positions = lh.ingest("positions", positions, mode="append")

    yield {"n_trades": n_trades, "n_positions": n_positions}


# ── Lakehouse Detection ─────────────────────────────────────────────────────


class TestLakehouseDetection:

    def test_is_lakehouse(self, lh):
        assert _is_lakehouse(lh) is True

    def test_not_lakehouse(self):
        assert _is_lakehouse("not a lakehouse") is False
        assert _is_lakehouse(42) is False


# ── Schema Discovery via table_info() ───────────────────────────────────────


class TestLakehouseSchema:

    def test_table_info_discovers_columns(self, lh, seeded):
        cols = _columns_from_lakehouse(lh, "trades")
        col_names = [c.name for c in cols]
        assert "sector" in col_names
        assert "symbol" in col_names
        assert "side" in col_names
        assert "quantity" in col_names
        assert "price" in col_names
        assert "pnl" in col_names

    def test_internal_columns_filtered(self, lh, seeded):
        cols = _columns_from_lakehouse(lh, "trades")
        col_names = [c.name for c in cols]
        assert "_batch_id" not in col_names
        assert "_batch_ts" not in col_names

    def test_column_types_inferred(self, lh, seeded):
        cols = _columns_from_lakehouse(lh, "trades")
        col_map = {c.name: c for c in cols}
        assert col_map["sector"].kind == "dimension"
        assert col_map["quantity"].kind == "measure"
        assert col_map["price"].kind == "measure"
        assert col_map["price"].aggregate_operator == "sum"


# ── Datacube Convenience Method ─────────────────────────────────────────────


class TestLakehouseDatacubeMethod:

    def test_convenience_method_creates_datacube(self, lh, seeded):
        dc = lh.datacube("trades")
        assert dc._lakehouse is lh
        col_names = [c.name for c in dc.snapshot.columns]
        assert "sector" in col_names
        assert "_batch_id" not in col_names

    def test_source_is_fqn(self, lh, seeded):
        dc = lh.datacube("trades")
        assert dc.snapshot.source == "lakehouse.default.trades"


# ── Flat Query (S3 → DuckDB → result, no Python data transit) ──────────────


class TestLakehouseFlatQuery:

    def test_flat_query_returns_all_rows(self, lh, seeded):
        dc = lh.datacube("trades")
        df = dc.query_df()
        assert len(df) == 10

    def test_flat_query_excludes_internal_cols(self, lh, seeded):
        dc = lh.datacube("trades")
        df = dc.query_df()
        assert "_batch_id" not in df.columns
        assert "_batch_ts" not in df.columns

    def test_query_arrow_output(self, lh, seeded):
        dc = lh.datacube("trades")
        table = dc.query()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 10

    def test_query_dicts_output(self, lh, seeded):
        dc = lh.datacube("trades")
        rows = dc.query_dicts()
        assert len(rows) == 10
        assert "sector" in rows[0]


# ── VPivot (GROUP BY) ──────────────────────────────────────────────────────


class TestLakehouseVPivot:

    def test_group_by_sector(self, lh, seeded):
        dc = lh.datacube("trades").set_group_by("sector")
        df = dc.query_df()
        assert len(df) == 3  # Tech, Finance, Energy
        assert "sector" in df.columns

    def test_group_by_multi(self, lh, seeded):
        dc = lh.datacube("trades").set_group_by("sector", "symbol")
        df = dc.query_df()
        assert len(df) == 5  # 5 unique (sector, symbol) combos

    def test_aggregation_sum(self, lh, seeded):
        dc = lh.datacube("trades").set_group_by("sector")
        df = dc.query_df()
        tech = df[df["sector"] == "Tech"]
        # Tech: 100+50+200+100 = 450
        assert tech["quantity"].values[0] == 450

    def test_aggregation_avg(self, lh, seeded):
        dc = lh.datacube("trades").set_group_by("sector")
        dc = dc.set_column("price", aggregate_operator="avg")
        df = dc.query_df()
        assert "price" in df.columns


# ── HPivot (conditional aggregation) ───────────────────────────────────────


class TestLakehouseHPivot:

    def test_pivot_by_side(self, lh, seeded):
        dc = lh.datacube("trades").set_group_by("sector").set_pivot_by("side")
        df = dc.query_df()
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        assert f"BUY{sep}quantity" in df.columns
        assert f"SELL{sep}quantity" in df.columns
        assert len(df) == 3

    def test_pivot_values_correct(self, lh, seeded):
        dc = lh.datacube("trades").set_group_by("sector").set_pivot_by("side")
        df = dc.query_df()
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        tech = df[df["sector"] == "Tech"]
        # Tech BUY quantity: 100 (AAPL) + 200 (GOOGL) = 300
        assert tech[f"BUY{sep}quantity"].values[0] == 300
        # Tech SELL quantity: 50 (AAPL) + 100 (GOOGL) = 150
        assert tech[f"SELL{sep}quantity"].values[0] == 150

    def test_vpivot_and_hpivot_simultaneously(self, lh, seeded):
        dc = (
            lh.datacube("trades")
            .set_group_by("sector", "symbol")
            .set_pivot_by("side")
        )
        df = dc.query_df()
        assert len(df) == 5
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        assert f"BUY{sep}quantity" in df.columns
        assert f"SELL{sep}quantity" in df.columns

    def test_pivot_total_column(self, lh, seeded):
        dc = (
            lh.datacube("trades")
            .set_group_by("sector")
            .set_pivot_by("side")
            .set_pivot_statistic("Total")
        )
        df = dc.query_df()
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        assert f"Total{sep}quantity" in df.columns

    def test_pivot_statistic_disabled(self, lh, seeded):
        dc = (
            lh.datacube("trades")
            .set_group_by("sector")
            .set_pivot_by("side")
            .set_pivot_statistic(None)
        )
        df = dc.query_df()
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        total_cols = [c for c in df.columns if c.startswith(f"Total{sep}")]
        assert len(total_cols) == 0


# ── Filters ────────────────────────────────────────────────────────────────


class TestLakehouseFilters:

    def test_filter_eq(self, lh, seeded):
        dc = lh.datacube("trades").add_filter("sector", "eq", "Tech")
        df = dc.query_df()
        assert len(df) == 4
        assert all(row == "Tech" for row in df["sector"])

    def test_filter_ne(self, lh, seeded):
        dc = lh.datacube("trades").add_filter("sector", "ne", "Energy")
        df = dc.query_df()
        assert len(df) == 8  # Tech(4) + Finance(4)

    def test_filter_with_group_by(self, lh, seeded):
        dc = (
            lh.datacube("trades")
            .add_filter("sector", "ne", "Energy")
            .set_group_by("sector")
        )
        df = dc.query_df()
        assert len(df) == 2  # Tech, Finance

    def test_clear_filters(self, lh, seeded):
        dc = lh.datacube("trades").add_filter("sector", "eq", "Tech")
        dc = dc.clear_filters()
        df = dc.query_df()
        assert len(df) == 10


# ── Sort + Limit ───────────────────────────────────────────────────────────


class TestLakehouseSortLimit:

    def test_sort(self, lh, seeded):
        from datacube.config import Sort
        dc = lh.datacube("trades").set_sort(Sort(field="price", descending=True))
        df = dc.query_df()
        prices = df["price"].tolist()
        assert prices == sorted(prices, reverse=True)

    def test_limit(self, lh, seeded):
        dc = lh.datacube("trades").set_limit(3)
        df = dc.query_df()
        assert len(df) == 3


# ── Drilldown ──────────────────────────────────────────────────────────────


class TestLakehouseDrilldown:

    def test_drill_into_sector(self, lh, seeded):
        dc = lh.datacube("trades").set_group_by("sector", "symbol")
        dc2 = dc.drill_down(sector="Tech")
        df = dc2.query_df()
        assert len(df) == 2  # AAPL, GOOGL

    def test_drill_to_leaf(self, lh, seeded):
        dc = lh.datacube("trades").set_group_by("sector", "symbol")
        dc2 = dc.drill_down(sector="Tech").drill_down(symbol="AAPL")
        df = dc2.query_df()
        assert len(df) == 2  # BUY + SELL for Tech/AAPL

    def test_drill_up(self, lh, seeded):
        dc = lh.datacube("trades").set_group_by("sector", "symbol")
        dc2 = dc.drill_down(sector="Tech")
        dc3 = dc2.drill_up()
        assert dc3.snapshot.drill_path == ()


# ── Extended Columns ───────────────────────────────────────────────────────


class TestLakehouseExtendedColumns:

    def test_leaf_extend(self, lh, seeded):
        dc = lh.datacube("trades").add_leaf_extend("notional", "price * quantity")
        df = dc.query_df()
        assert "notional" in df.columns
        row0 = df.iloc[0]
        assert abs(row0["notional"] - (row0["price"] * row0["quantity"])) < 0.01

    def test_group_extend(self, lh, seeded):
        dc = (
            lh.datacube("trades")
            .set_group_by("sector")
            .set_column("price", aggregate_operator="avg")
            .add_group_extend("price_x2", '"price" * 2')
        )
        df = dc.query_df()
        assert "price_x2" in df.columns


# ── Cross-Table Joins ──────────────────────────────────────────────────────


class TestLakehouseJoins:

    def test_join_with_positions(self, lh, seeded):
        dc = lh.datacube("trades")
        dc = dc.add_join(
            "lakehouse.default.positions",
            on={"symbol": "symbol"},
        )
        df = dc.query_df()
        assert len(df) > 0

    def test_auto_fqn_join(self, lh, seeded):
        dc = lh.datacube("trades")
        # Short name — should auto-resolve to lakehouse.default.positions
        dc = dc.add_join("positions", on={"symbol": "symbol"})
        join_source = dc.snapshot.joins[0].source
        assert join_source == "lakehouse.default.positions"


# ── Snapshot Immutability ──────────────────────────────────────────────────


class TestLakehouseImmutability:

    def test_mutations_return_new_datacube(self, lh, seeded):
        dc1 = lh.datacube("trades")
        dc2 = dc1.set_group_by("sector")
        assert dc1.snapshot.group_by == ()
        assert dc2.snapshot.group_by == ("sector",)

    def test_lakehouse_ref_preserved(self, lh, seeded):
        dc = lh.datacube("trades")
        dc2 = dc.set_group_by("sector")
        dc3 = dc2.set_pivot_by("side")
        dc4 = dc3.set_column("price", aggregate_operator="avg")
        dc5 = dc4.add_filter("sector", "eq", "Tech")
        assert dc5._lakehouse is lh


# ── Full Chaining ──────────────────────────────────────────────────────────


class TestLakehouseChaining:

    def test_full_pipeline(self, lh, seeded):
        df = (
            lh.datacube("trades")
            .set_group_by("sector")
            .set_pivot_by("side")
            .add_filter("sector", "ne", "Energy")
            .set_sort(("sector", False))
            .set_limit(10)
            .query_df()
        )
        assert len(df) == 2  # Finance, Tech
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        assert f"BUY{sep}quantity" in df.columns


# ── Cross-Source Joins (DataFrame + Lakehouse) ─────────────────────────────


class TestCrossSourceJoin:

    def test_dataframe_on_lakehouse_conn(self, lh, seeded):
        df = pd.DataFrame({
            "symbol": ["AAPL", "GOOGL", "JPM"],
            "signal": [1.0, -0.5, 0.8],
        })
        dc = Datacube(df, lakehouse=lh)
        result = dc.query_df()
        assert len(result) == 3

    def test_dataframe_join_with_lakehouse_table(self, lh, seeded):
        df = pd.DataFrame({
            "symbol": ["AAPL", "GOOGL", "JPM"],
            "signal": [1.0, -0.5, 0.8],
        })
        dc = Datacube(df, lakehouse=lh)
        dc = dc.add_join(
            "positions",
            on={"symbol": "symbol"},
        )
        result = dc.query_df()
        assert "market_value" in result.columns
        assert len(result) == 3

    def test_arrow_on_lakehouse_conn(self, lh, seeded):
        table = pa.table({
            "symbol": ["AAPL", "GOOGL"],
            "rating": [5, 4],
        })
        dc = Datacube(table, lakehouse=lh)
        result = dc.query_df()
        assert len(result) == 2


# ── Serialization Round-Trip ───────────────────────────────────────────────


class TestLakehouseSerialization:

    def test_json_round_trip(self, lh, seeded):
        from datacube.config import DatacubeSnapshot
        dc = (
            lh.datacube("trades")
            .set_group_by("sector")
            .set_pivot_by("side")
            .add_filter("sector", "ne", "Energy")
        )
        json_str = dc.to_json()
        restored = DatacubeSnapshot.from_json(json_str)
        assert restored.group_by == ("sector",)
        assert restored.pivot_by == ("side",)
        assert len(restored.filters) == 1
