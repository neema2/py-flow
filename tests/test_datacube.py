"""
Tests for the datacube package — config, compiler, and engine.

These tests are organized into:
1. Config model tests (frozen dataclasses, serialization)
2. Compiler tests (SQL string assertions — pure Python, no DuckDB)
3. Engine integration tests (real DuckDB with in-memory data)
"""

import duckdb
import pytest
from datacube.compiler import compile, discover_pivot_values
from datacube.config import (
    PIVOT_COLUMN_NAME_SEPARATOR,
    DatacubeColumnConfig,
    DatacubeSnapshot,
    ExtendedColumn,
    Filter,
    JoinSpec,
    Sort,
)
from datacube.engine import Datacube

# ═══════════════════════════════════════════════════════════════════════
# 1. Config Model Tests
# ═══════════════════════════════════════════════════════════════════════

class TestConfigModels:

    def test_column_config_dimension_and_measure(self):
        dim = DatacubeColumnConfig(name="sector", type="str", kind="dimension")
        assert dim.kind == "dimension" and dim.excluded_from_pivot is True and dim.is_selected is True
        meas = DatacubeColumnConfig(name="quantity", type="int", kind="measure",
                                     aggregate_operator="sum", excluded_from_pivot=False)
        assert meas.kind == "measure" and meas.aggregate_operator == "sum"

    def test_column_config_frozen_and_replace(self):
        col = DatacubeColumnConfig(name="x", type="str")
        with pytest.raises(AttributeError):
            col.name = "y"  # type: ignore[assignment]
        col2 = DatacubeColumnConfig(name="price", type="float", kind="measure", aggregate_operator="sum")
        col3 = col2.replace(aggregate_operator="avg")
        assert col2.aggregate_operator == "sum" and col3.aggregate_operator == "avg"

    def test_from_type_inference(self):
        assert DatacubeColumnConfig.from_type("price", float).kind == "measure"
        assert DatacubeColumnConfig.from_type("name", str).kind == "dimension"

    def test_extended_column_and_small_configs(self):
        e = ExtendedColumn(name="notional", expression="price * quantity")
        assert e.name == "notional" and e.type == "float"
        with pytest.raises(AttributeError):
            e.name = "y"  # type: ignore[misc]
        f = Filter(field="sector", op="eq", value="Tech")
        assert f.field == "sector" and f.op == "eq"
        s = Sort(field="price", descending=True)
        assert s.descending is True
        j = JoinSpec(source="positions", on=(("symbol", "symbol"),), join_type="LEFT")
        assert j.source == "positions"


class TestDatacubeSnapshot:

    def test_create_frozen_hashable_replace(self):
        snap = DatacubeSnapshot(source="trades")
        assert snap.source == "trades" and snap.columns == () and snap.group_by == ()
        with pytest.raises(AttributeError):
            snap.source = "x"  # type: ignore[misc]
        snap2 = DatacubeSnapshot(source="t", group_by=("a",))
        snap3 = DatacubeSnapshot(source="t", group_by=("a",))
        assert hash(snap2) == hash(snap3)
        snap4 = snap.replace(group_by=("sector",))
        assert snap.group_by == () and snap4.group_by == ("sector",)

    def test_get_column(self):
        cols = (
            DatacubeColumnConfig(name="a", type="str"),
            DatacubeColumnConfig(name="b", type="int"),
        )
        snap = DatacubeSnapshot(source="t", columns=cols)
        assert snap.get_column("a") is not None
        assert snap.get_column("a").name == "a"
        assert snap.get_column("z") is None

    def test_set_column(self):
        cols = (
            DatacubeColumnConfig(name="a", type="str", kind="dimension"),
            DatacubeColumnConfig(name="b", type="int", kind="measure", aggregate_operator="sum"),
        )
        snap = DatacubeSnapshot(source="t", columns=cols)
        snap2 = snap.set_column("b", aggregate_operator="avg")
        assert snap.get_column("b").aggregate_operator == "sum"
        assert snap2.get_column("b").aggregate_operator == "avg"

    def test_selected_columns(self):
        cols = (
            DatacubeColumnConfig(name="a", type="str", is_selected=True),
            DatacubeColumnConfig(name="b", type="str", is_selected=False),
        )
        snap = DatacubeSnapshot(source="t", columns=cols)
        selected = snap.selected_columns()
        assert len(selected) == 1
        assert selected[0].name == "a"

    def test_dimension_measure_columns(self):
        cols = (
            DatacubeColumnConfig(name="sector", type="str", kind="dimension"),
            DatacubeColumnConfig(name="qty", type="int", kind="measure"),
        )
        snap = DatacubeSnapshot(source="t", columns=cols)
        assert len(snap.dimension_columns()) == 1
        assert len(snap.measure_columns()) == 1

    def test_serialization_round_trip(self):
        cols = (
            DatacubeColumnConfig(name="sector", type="str", kind="dimension"),
            DatacubeColumnConfig(name="qty", type="int", kind="measure",
                                aggregate_operator="sum", excluded_from_pivot=False),
        )
        snap = DatacubeSnapshot(
            source="trades",
            columns=cols,
            group_by=("sector",),
            pivot_by=("side",),
            pivot_values=("BUY", "SELL"),
            filters=(Filter(field="sector", op="ne", value="Utilities"),),
            sort=(Sort(field="qty", descending=True),),
            limit=100,
        )
        json_str = snap.to_json()
        restored = DatacubeSnapshot.from_json(json_str)
        assert restored.source == "trades"
        assert len(restored.columns) == 2
        assert restored.group_by == ("sector",)
        assert restored.pivot_by == ("side",)
        assert restored.pivot_values == ("BUY", "SELL")
        assert len(restored.filters) == 1
        assert restored.filters[0].op == "ne"
        assert len(restored.sort) == 1
        assert restored.limit == 100


# ═══════════════════════════════════════════════════════════════════════
# 2. Compiler Tests (SQL string assertions)
# ═══════════════════════════════════════════════════════════════════════

def _make_cols(*specs):
    """Helper to create column configs from (name, type, kind, agg) tuples."""
    cols = []
    for spec in specs:
        name, typ = spec[0], spec[1]
        kind = spec[2] if len(spec) > 2 else ("measure" if typ in ("int", "float") else "dimension")
        agg = spec[3] if len(spec) > 3 else ("sum" if kind == "measure" else "")
        excluded = kind == "dimension"
        cols.append(DatacubeColumnConfig(
            name=name, type=typ, kind=kind,
            aggregate_operator=agg, excluded_from_pivot=excluded,
        ))
    return tuple(cols)


class TestCompilerFlatQuery:

    def test_select_all(self):
        snap = DatacubeSnapshot(
            source="trades",
            columns=_make_cols(("sector", "str"), ("qty", "int")),
        )
        sql = compile(snap)
        assert "SELECT" in sql
        assert "trades" in sql
        # No GROUP BY
        assert "GROUP BY" not in sql

    def test_select_with_filter(self):
        snap = DatacubeSnapshot(
            source="trades",
            columns=_make_cols(("sector", "str"), ("qty", "int")),
            filters=(Filter(field="sector", op="eq", value="Tech"),),
        )
        sql = compile(snap)
        assert "WHERE" in sql
        assert "'Tech'" in sql

    def test_filter_operators(self):
        ops = {
            "eq": "=", "ne": "!=", "gt": ">", "lt": "<",
            "ge": ">=", "le": "<=",
            "is_null": "IS NULL", "is_not_null": "IS NOT NULL",
        }
        for op_name, sql_op in ops.items():
            f = Filter(field="x", op=op_name, value=42)
            snap = DatacubeSnapshot(
                source="t",
                columns=_make_cols(("x", "int")),
                filters=(f,),
            )
            sql = compile(snap)
            assert sql_op in sql, f"Expected {sql_op!r} in SQL for op={op_name!r}"

    def test_filter_in(self):
        f = Filter(field="sector", op="in", value=["Tech", "Finance"])
        snap = DatacubeSnapshot(source="t", columns=_make_cols(("sector", "str")), filters=(f,))
        sql = compile(snap)
        assert "IN" in sql
        assert "'Tech'" in sql

    def test_filter_between(self):
        f = Filter(field="price", op="between", value=(10, 100))
        snap = DatacubeSnapshot(source="t", columns=_make_cols(("price", "float")), filters=(f,))
        sql = compile(snap)
        assert "BETWEEN" in sql
        assert "10" in sql
        assert "100" in sql

    def test_filter_contains(self):
        f = Filter(field="name", op="contains", value="abc")
        snap = DatacubeSnapshot(source="t", columns=_make_cols(("name", "str")), filters=(f,))
        sql = compile(snap)
        assert "'%abc%'" in sql

    def test_order_by(self):
        snap = DatacubeSnapshot(
            source="t",
            columns=_make_cols(("x", "int")),
            sort=(Sort(field="x", descending=True),),
        )
        sql = compile(snap)
        assert "ORDER BY" in sql
        assert "DESC" in sql

    def test_limit_offset(self):
        snap = DatacubeSnapshot(
            source="t",
            columns=_make_cols(("x", "int")),
            limit=50, offset=10,
        )
        sql = compile(snap)
        assert "LIMIT 50" in sql
        assert "OFFSET 10" in sql


class TestCompilerLeafExtend:

    def test_leaf_extend_adds_cte(self):
        snap = DatacubeSnapshot(
            source="trades",
            columns=_make_cols(("price", "float"), ("quantity", "int")),
            leaf_extended_columns=(
                ExtendedColumn(name="notional", expression="price * quantity"),
            ),
        )
        sql = compile(snap)
        assert "notional" in sql
        assert "price * quantity" in sql


class TestCompilerVPivot:

    def test_group_by_with_sum(self):
        snap = DatacubeSnapshot(
            source="trades",
            columns=_make_cols(
                ("sector", "str", "dimension"),
                ("quantity", "int", "measure", "sum"),
            ),
            group_by=("sector",),
        )
        sql = compile(snap)
        assert "GROUP BY" in sql
        assert '"sector"' in sql
        assert "SUM" in sql
        assert '"quantity"' in sql

    def test_group_by_with_avg(self):
        snap = DatacubeSnapshot(
            source="trades",
            columns=_make_cols(
                ("sector", "str", "dimension"),
                ("price", "float", "measure", "avg"),
            ),
            group_by=("sector",),
        )
        sql = compile(snap)
        assert "AVG" in sql

    def test_multiple_group_by(self):
        snap = DatacubeSnapshot(
            source="trades",
            columns=_make_cols(
                ("sector", "str", "dimension"),
                ("symbol", "str", "dimension"),
                ("qty", "int", "measure", "sum"),
            ),
            group_by=("sector", "symbol"),
        )
        sql = compile(snap)
        assert '"sector"' in sql
        assert '"symbol"' in sql


class TestCompilerHPivot:

    def test_single_column_pivot(self):
        snap = DatacubeSnapshot(
            source="trades",
            columns=_make_cols(
                ("sector", "str", "dimension"),
                ("quantity", "int", "measure", "sum"),
            ),
            group_by=("sector",),
            pivot_by=("side",),
            pivot_values=("BUY", "SELL"),
        )
        sql = compile(snap)
        assert "CASE WHEN" in sql
        assert "'BUY'" in sql
        assert "'SELL'" in sql
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        assert f"BUY{sep}quantity" in sql
        assert f"SELL{sep}quantity" in sql

    def test_pivot_statistic_total_column(self):
        snap = DatacubeSnapshot(
            source="trades",
            columns=_make_cols(
                ("sector", "str", "dimension"),
                ("quantity", "int", "measure", "sum"),
            ),
            group_by=("sector",),
            pivot_by=("side",),
            pivot_values=("BUY", "SELL"),
            pivot_statistic_column="Total",
        )
        sql = compile(snap)
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        assert f"Total{sep}quantity" in sql

    def test_pivot_with_excluded_measure(self):
        cols = (
            DatacubeColumnConfig(name="sector", type="str", kind="dimension",
                                excluded_from_pivot=True),
            DatacubeColumnConfig(name="quantity", type="int", kind="measure",
                                aggregate_operator="sum", excluded_from_pivot=False),
            DatacubeColumnConfig(name="pnl", type="float", kind="measure",
                                aggregate_operator="sum", excluded_from_pivot=True),
        )
        snap = DatacubeSnapshot(
            source="trades", columns=cols,
            group_by=("sector",),
            pivot_by=("side",),
            pivot_values=("BUY", "SELL"),
        )
        sql = compile(snap)
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        # quantity should be pivoted
        assert f"BUY{sep}quantity" in sql
        # pnl should NOT be pivoted (excluded)
        assert f"BUY{sep}pnl" not in sql
        # pnl should appear as plain aggregate
        assert 'SUM("pnl")' in sql


class TestCompilerVPivotHPivot:

    def test_both_simultaneously(self):
        cols = _make_cols(
            ("sector", "str", "dimension"),
            ("symbol", "str", "dimension"),
            ("quantity", "int", "measure", "sum"),
            ("price", "float", "measure", "avg"),
        )
        snap = DatacubeSnapshot(
            source="trades", columns=cols,
            group_by=("sector", "symbol"),
            pivot_by=("side",),
            pivot_values=("BUY", "SELL"),
        )
        sql = compile(snap)
        # Should have GROUP BY with both dimensions
        assert "GROUP BY" in sql
        assert '"sector"' in sql
        assert '"symbol"' in sql
        # Should have pivoted measures
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        assert f"BUY{sep}quantity" in sql
        assert f"SELL{sep}price" in sql
        # Should have CASE WHEN
        assert "CASE WHEN" in sql


class TestCompilerGroupExtend:

    def test_group_extend_wraps_query(self):
        cols = _make_cols(
            ("sector", "str", "dimension"),
            ("credits", "float", "measure", "sum"),
            ("volume", "float", "measure", "sum"),
        )
        snap = DatacubeSnapshot(
            source="trades", columns=cols,
            group_by=("sector",),
            group_extended_columns=(
                ExtendedColumn(name="yield", expression='"credits" / NULLIF("volume", 0)'),
            ),
        )
        sql = compile(snap)
        assert "yield" in sql
        assert "NULLIF" in sql
        # The group extend should wrap the aggregated query
        assert "_dc_agg" in sql


class TestCompilerDrilldown:

    def test_drilldown_adds_filter(self):
        cols = _make_cols(
            ("sector", "str", "dimension"),
            ("symbol", "str", "dimension"),
            ("qty", "int", "measure", "sum"),
        )
        snap = DatacubeSnapshot(
            source="trades", columns=cols,
            group_by=("sector", "symbol"),
            drill_path=({"sector": "Tech"},),
        )
        sql = compile(snap)
        # Should filter by sector = 'Tech'
        assert "'Tech'" in sql
        assert "WHERE" in sql
        # GROUP BY should be adjusted to just "symbol" (next level)
        assert "GROUP BY" in sql

    def test_drilldown_max_depth_no_group_by(self):
        cols = _make_cols(
            ("sector", "str", "dimension"),
            ("qty", "int", "measure", "sum"),
        )
        snap = DatacubeSnapshot(
            source="trades", columns=cols,
            group_by=("sector",),
            drill_path=({"sector": "Tech"},),
        )
        sql = compile(snap)
        # At max depth, GROUP BY should be removed
        assert "GROUP BY" not in sql


class TestCompilerJoins:

    def test_join_clause(self):
        snap = DatacubeSnapshot(
            source="trades",
            columns=_make_cols(("sector", "str"), ("qty", "int")),
            joins=(JoinSpec(
                source="positions",
                on=(("symbol", "symbol"),),
                join_type="LEFT",
                alias="p",
            ),),
        )
        sql = compile(snap)
        assert "LEFT JOIN" in sql
        assert "positions" in sql


# ═══════════════════════════════════════════════════════════════════════
# 3. Engine Integration Tests (real DuckDB)
# ═══════════════════════════════════════════════════════════════════════

@pytest.fixture
def trades_conn():
    """Create a DuckDB connection with a sample trades table."""
    conn = duckdb.connect()
    conn.execute("""
        CREATE TABLE trades (
            sector VARCHAR,
            symbol VARCHAR,
            side VARCHAR,
            order_type VARCHAR,
            quantity INTEGER,
            price DOUBLE,
            pnl DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO trades VALUES
        ('Tech', 'AAPL', 'BUY',  'LIMIT',  100, 150.0,  500.0),
        ('Tech', 'AAPL', 'SELL', 'MARKET', 50,  155.0, -200.0),
        ('Tech', 'GOOGL', 'BUY', 'LIMIT',  200, 2800.0, 1000.0),
        ('Tech', 'GOOGL', 'SELL', 'LIMIT',  100, 2850.0, -500.0),
        ('Finance', 'JPM', 'BUY', 'MARKET', 300, 160.0,  800.0),
        ('Finance', 'JPM', 'SELL', 'LIMIT',  150, 162.0, -300.0),
        ('Finance', 'GS', 'BUY',  'LIMIT',  250, 380.0,  600.0),
        ('Finance', 'GS', 'SELL', 'MARKET', 100, 385.0, -100.0),
        ('Energy', 'XOM', 'BUY',  'MARKET', 400, 95.0,   200.0),
        ('Energy', 'XOM', 'SELL', 'LIMIT',  200, 97.0,  -50.0)
    """)
    return conn


class TestEngineFromDuckDB:

    def test_flat_query(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        df = dc.query_df()
        assert len(df) == 10

    def test_vpivot_group_by(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.set_group_by("sector")
        df = dc.query_df()
        assert len(df) == 3  # Tech, Finance, Energy
        assert "sector" in df.columns

    def test_hpivot(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.set_group_by("sector")
        dc = dc.set_pivot_by("side")
        df = dc.query_df()
        assert len(df) == 3
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        # Should have pivot columns for measures
        pivot_cols = [c for c in df.columns if sep in c]
        assert len(pivot_cols) > 0

    def test_vpivot_and_hpivot(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.set_group_by("sector", "symbol")
        dc = dc.set_pivot_by("side")
        df = dc.query_df()
        # 5 unique (sector, symbol) combos
        assert len(df) == 5
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        buy_qty_col = f"BUY{sep}quantity"
        sell_qty_col = f"SELL{sep}quantity"
        assert buy_qty_col in df.columns
        assert sell_qty_col in df.columns

    def test_multi_column_pivot(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.set_group_by("sector")
        dc = dc.set_pivot_by("side", "order_type")
        df = dc.query_df()
        assert len(df) == 3
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        # Should have columns like BUY__|__LIMIT__|__quantity
        pivot_cols = [c for c in df.columns if sep in c]
        assert len(pivot_cols) > 0

    def test_filter(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.add_filter("sector", "eq", "Tech")
        df = dc.query_df()
        assert len(df) == 4  # 4 Tech trades
        assert all(row == "Tech" for row in df["sector"])

    def test_sort(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.set_sort(Sort(field="price", descending=True))
        df = dc.query_df()
        prices = df["price"].tolist()
        assert prices == sorted(prices, reverse=True)

    def test_limit(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.set_limit(3)
        df = dc.query_df()
        assert len(df) == 3

    def test_set_column_aggregation(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.set_group_by("sector")
        dc = dc.set_column("price", aggregate_operator="avg")
        df = dc.query_df()
        assert "price" in df.columns

    def test_leaf_extend(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.add_leaf_extend("notional", "price * quantity")
        df = dc.query_df()
        assert "notional" in df.columns
        # Check first row: 150 * 100 = 15000
        row0 = df.iloc[0]
        assert abs(row0["notional"] - (row0["price"] * row0["quantity"])) < 0.01

    def test_group_extend(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.set_group_by("sector")
        dc = dc.set_column("price", aggregate_operator="avg")
        dc = dc.add_group_extend("price_x2", '"price" * 2')
        df = dc.query_df()
        assert "price_x2" in df.columns

    def test_drilldown(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.set_group_by("sector", "symbol")

        # Level 0: group by sector
        df0 = dc.query_df()
        assert len(df0) == 5  # sector x symbol

        # Drill into Tech
        dc2 = dc.drill_down(sector="Tech")
        df1 = dc2.query_df()
        # Should show symbol-level for Tech only
        assert len(df1) == 2  # AAPL, GOOGL

        # Drill to leaf
        dc3 = dc2.drill_down(symbol="AAPL")
        df2 = dc3.query_df()
        # Max depth → leaf rows, no GROUP BY
        assert len(df2) == 2  # BUY + SELL for Tech/AAPL

    def test_drill_up(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.set_group_by("sector", "symbol")
        dc2 = dc.drill_down(sector="Tech")
        dc3 = dc2.drill_up()
        assert dc3.snapshot.drill_path == ()

    def test_snapshot_immutability(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc2 = dc.set_group_by("sector")
        # Original unchanged
        assert dc.snapshot.group_by == ()
        assert dc2.snapshot.group_by == ("sector",)

    def test_chaining(self, trades_conn):
        dc = (
            Datacube(trades_conn, source_name="trades")
            .set_group_by("sector")
            .set_pivot_by("side")
            .add_filter("sector", "ne", "Energy")
            .set_sort(Sort(field="sector", descending=False))
            .set_limit(10)
        )
        df = dc.query_df()
        assert len(df) == 2  # Tech, Finance (Energy filtered out)

    def test_clear_filters(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.add_filter("sector", "eq", "Tech")
        assert len(dc.snapshot.filters) == 1
        dc = dc.clear_filters()
        assert len(dc.snapshot.filters) == 0

    def test_result_columns(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.set_group_by("sector")
        dc = dc.set_pivot_by("side")
        cols = dc.result_columns()
        assert "sector" in cols
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        pivot_cols = [c for c in cols if sep in c]
        assert len(pivot_cols) > 0

    def test_sql_output(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.set_group_by("sector")
        sql = dc.sql()
        assert "GROUP BY" in sql
        assert "trades" in sql

    def test_pivot_statistic_disabled(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.set_group_by("sector").set_pivot_by("side")
        dc = dc.set_pivot_statistic(None)
        df = dc.query_df()
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        total_cols = [c for c in df.columns if c.startswith(f"Total{sep}")]
        assert len(total_cols) == 0


class TestEngineFromDataFrame:

    def test_from_pandas(self):
        import pandas as pd
        df = pd.DataFrame({
            "region": ["US", "US", "EU", "EU"],
            "product": ["A", "B", "A", "B"],
            "revenue": [100.0, 200.0, 150.0, 250.0],
        })
        dc = Datacube(df)
        dc = dc.set_group_by("region")
        result = dc.query_df()
        assert len(result) == 2


class TestEngineFromArrow:

    def test_from_arrow(self):
        import pyarrow as pa
        table = pa.table({
            "city": ["NYC", "NYC", "LA", "LA"],
            "sales": [10, 20, 30, 40],
        })
        dc = Datacube(table)
        dc = dc.set_group_by("city")
        result = dc.query_df()
        assert len(result) == 2
        # NYC: 10+20=30, LA: 30+40=70
        nyc_row = result[result["city"] == "NYC"]
        assert nyc_row["sales"].values[0] == 30


class TestDiscoverPivotValues:

    def test_discover_single_column(self, trades_conn):
        snap = DatacubeSnapshot(source="trades")
        snap = snap.replace(pivot_by=("side",))
        values = discover_pivot_values(trades_conn, snap)
        assert "BUY" in values
        assert "SELL" in values

    def test_discover_multi_column(self, trades_conn):
        snap = DatacubeSnapshot(source="trades")
        snap = snap.replace(pivot_by=("side", "order_type"))
        values = discover_pivot_values(trades_conn, snap)
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        # Should have Cartesian product: BUY+LIMIT, BUY+MARKET, SELL+LIMIT, SELL+MARKET
        assert len(values) == 4
        assert f"BUY{sep}LIMIT" in values
        assert f"SELL{sep}MARKET" in values


class TestLeafExtendWithAggregation:
    """Leaf extensions should be usable as aggregated measures."""

    def test_leaf_extend_registers_column(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.add_leaf_extend("notional", "price * quantity")
        col_names = [c.name for c in dc.snapshot.columns]
        assert "notional" in col_names

    def test_leaf_extend_column_defaults(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.add_leaf_extend("notional", "price * quantity")
        col = dc.snapshot.get_column("notional")
        assert col.kind == "measure"
        assert col.aggregate_operator == "sum"
        assert col.type == "float"

    def test_leaf_extend_set_column_avg(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.add_leaf_extend("notional", "price * quantity")
        dc = dc.set_column("notional", aggregate_operator="avg")
        col = dc.snapshot.get_column("notional")
        assert col.aggregate_operator == "avg"

    def test_leaf_extend_group_by(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.add_leaf_extend("notional", "price * quantity")
        dc = dc.set_group_by("sector")
        df = dc.query_df()
        assert "notional" in df.columns
        assert len(df) == 3  # Tech, Finance, Energy

    def test_leaf_extend_group_by_with_avg(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.add_leaf_extend("notional", "price * quantity")
        dc = dc.set_column("notional", aggregate_operator="avg")
        dc = dc.set_group_by("sector")
        df = dc.query_df()
        assert "notional" in df.columns
        assert len(df) == 3

    def test_leaf_extend_pivot(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.add_leaf_extend("notional", "price * quantity")
        dc = dc.set_group_by("sector").set_pivot_by("side")
        df = dc.query_df()
        sep = PIVOT_COLUMN_NAME_SEPARATOR
        assert f"BUY{sep}notional" in df.columns
        assert f"SELL{sep}notional" in df.columns

    def test_leaf_extend_no_duplicate(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.add_leaf_extend("price", "price * 1.0")
        # price already exists — should not add duplicate
        count = sum(1 for c in dc.snapshot.columns if c.name == "price")
        assert count == 1

    def test_leaf_extend_string_type(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.add_leaf_extend("label", "sector || '-' || symbol", type="str")
        col = dc.snapshot.get_column("label")
        assert col.kind == "dimension"
        assert col.aggregate_operator == ""


class TestSerializationIntegration:

    def test_json_round_trip(self, trades_conn):
        dc = Datacube(trades_conn, source_name="trades")
        dc = dc.set_group_by("sector").set_pivot_by("side").add_filter("sector", "ne", "Energy")
        json_str = dc.to_json()
        restored = DatacubeSnapshot.from_json(json_str)
        assert restored.group_by == ("sector",)
        assert restored.pivot_by == ("side",)
        assert len(restored.filters) == 1
