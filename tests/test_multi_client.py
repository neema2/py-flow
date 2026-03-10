"""
Integration tests for multi-client / cross-session table sharing.
Auto-starts StreamingServer via conftest, publishes tables via public client API.

Run with: pytest tests/test_multi_client.py -v
"""

import pytest
from streaming import StreamingClient
from streaming._utils import _is_remote


def _publish_tables():
    """Publish prices_live + portfolio_summary to DH query scope."""
    if not _is_remote():
        # EXACT ORIGINAL CODE for x86/macOS
        from deephaven import agg, new_table
        from deephaven.column import double_col, long_col, string_col
        from deephaven.execution_context import get_exec_ctx

        prices_live = new_table([
            string_col("Symbol", ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "NFLX"]),
            double_col("Price", [228.0, 192.0, 415.0, 225.0, 355.0, 138.0, 580.0, 920.0]),
            double_col("Bid", [227.9, 191.9, 414.9, 224.9, 354.9, 137.9, 579.9, 919.9]),
            double_col("Ask", [228.1, 192.1, 415.1, 225.1, 355.1, 138.1, 580.1, 920.1]),
            long_col("Volume", [1000000, 800000, 900000, 700000, 1200000, 1500000, 600000, 400000]),
            double_col("Change", [1.5, -0.8, 2.1, -1.2, 3.5, -0.5, 1.8, -2.0]),
            double_col("ChangePct", [0.66, -0.42, 0.51, -0.53, 0.99, -0.36, 0.31, -0.22]),
        ])

        portfolio_summary = new_table([
            string_col("Symbol", ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "NFLX"]),
            double_col("Delta", [0.65, 0.55, 0.70, 0.50, 0.80, 0.60, 0.45, 0.35]),
        ]).agg_by([
            agg.sum_(["TotalDelta=Delta"]),
            agg.count_("Count"),
        ])

        scope = get_exec_ctx().j_exec_ctx.getQueryScope()
        scope.putParam("prices_live", prices_live.j_table)
        scope.putParam("portfolio_summary", portfolio_summary.j_table)
    else:
        # ARM/WSL path: Run the same setup script on the remote server
        with StreamingClient() as client:
            client.run_script("""
from deephaven import agg, new_table
from deephaven.column import double_col, long_col, string_col
from deephaven.execution_context import get_exec_ctx

prices_live = new_table([
    string_col("Symbol", ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "NFLX"]),
    double_col("Price", [228.0, 192.0, 415.0, 225.0, 355.0, 138.0, 580.0, 920.0]),
    double_col("Bid", [227.9, 191.9, 414.9, 224.9, 354.9, 137.9, 579.9, 919.9]),
    double_col("Ask", [228.1, 192.1, 415.1, 225.1, 355.1, 138.1, 580.1, 920.1]),
    long_col("Volume", [1000000, 800000, 900000, 700000, 1200000, 1500000, 600000, 400000]),
    double_col("Change", [1.5, -0.8, 2.1, -1.2, 3.5, -0.5, 1.8, -2.0]),
    double_col("ChangePct", [0.66, -0.42, 0.51, -0.53, 0.99, -0.36, 0.31, -0.22]),
])

portfolio_summary = new_table([
    string_col("Symbol", ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "NFLX"]),
    double_col("Delta", [0.65, 0.55, 0.70, 0.50, 0.80, 0.60, 0.45, 0.35]),
]).agg_by([
    agg.sum_(["TotalDelta=Delta"]),
    agg.count_("Count"),
])

scope = get_exec_ctx().j_exec_ctx.getQueryScope()
scope.putParam("prices_live", prices_live.j_table)
scope.putParam("portfolio_summary", portfolio_summary.j_table)
""")


@pytest.fixture(scope="module", autouse=True)
def _setup_tables(streaming_server):
    """Publish tables once for this module."""
    _publish_tables()


def _connect():
    """Helper to create a client."""
    return StreamingClient()


# ── Cross-session table visibility ───────────────────────────────────────────

class TestCrossSessionVisibility:
    def test_client_a_publishes_client_b_sees(self, streaming_server):
        a = _connect()
        b = _connect()
        try:
            a.run_script('cross_test_ab = prices_live.where(["Symbol = `AAPL`"])')
            tables_b = b.list_tables()
            assert "cross_test_ab" in tables_b, \
                "Client B cannot see table published by Client A"
        finally:
            a.close()
            b.close()

    def test_client_b_reads_data_from_client_a_table(self, streaming_server):
        a = _connect()
        b = _connect()
        try:
            a.run_script('cross_test_data = prices_live.where(["Symbol = `TSLA`"])')
            df = b.open_table("cross_test_data").to_arrow().to_pandas()
            assert len(df) == 1
            assert df["Symbol"].iloc[0] == "TSLA"
        finally:
            a.close()
            b.close()

    def test_table_persists_after_creator_disconnects(self, streaming_server):
        a = _connect()
        a.run_script('cross_test_persist = prices_live.where(["Symbol = `NVDA`"])')
        a.close()

        b = _connect()
        try:
            assert "cross_test_persist" in b.list_tables()
            df = b.open_table("cross_test_persist").to_arrow().to_pandas()
            assert len(df) == 1
            assert df["Symbol"].iloc[0] == "NVDA"
        finally:
            b.close()


# ── Multiple concurrent sessions ─────────────────────────────────────────────

class TestConcurrentSessions:
    def test_three_clients_connect_simultaneously(self, streaming_server):
        clients = [_connect() for _ in range(3)]
        try:
            for c in clients:
                # session.is_alive depends on client type
                if hasattr(c._session, 'is_alive'):
                    assert c._session.is_alive
                tables = c.list_tables()
                assert "prices_live" in tables
        finally:
            for c in clients:
                c.close()

    def test_all_clients_see_same_symbols(self, streaming_server):
        clients = [_connect() for _ in range(3)]
        try:
            symbol_sets = []
            for c in clients:
                df = c.open_table("prices_live").to_arrow().to_pandas()
                symbol_sets.append(set(df["Symbol"].tolist()))
            # All clients should see the same 8 symbols
            assert all(s == symbol_sets[0] for s in symbol_sets)
            assert len(symbol_sets[0]) == 8
        finally:
            for c in clients:
                c.close()

    def test_each_client_publishes_independently(self, streaming_server):
        a = _connect()
        b = _connect()
        c = _connect()
        try:
            a.run_script('multi_a = prices_live.where(["Symbol = `AAPL`"])')
            b.run_script('multi_b = prices_live.where(["Symbol = `MSFT`"])')
            c.run_script('multi_c = prices_live.where(["Symbol = `AMZN`"])')

            tables = c.list_tables()
            assert "multi_a" in tables
            assert "multi_b" in tables
            assert "multi_c" in tables

            df_a = c.open_table("multi_a").to_arrow().to_pandas()
            df_b = c.open_table("multi_b").to_arrow().to_pandas()
            df_c = c.open_table("multi_c").to_arrow().to_pandas()
            assert df_a["Symbol"].iloc[0] == "AAPL"
            assert df_b["Symbol"].iloc[0] == "MSFT"
            assert df_c["Symbol"].iloc[0] == "AMZN"
        finally:
            a.close()
            b.close()
            c.close()


# ── Script isolation ─────────────────────────────────────────────────────────

class TestScriptIsolation:
    def test_bad_script_does_not_break_other_sessions(self, streaming_server):
        a = _connect()
        b = _connect()
        try:
            # Client A runs a bad script
            with pytest.raises(Exception):
                a.run_script("this_will_fail = nonexistent_table.where(['x'])")

            # Client B should still work fine
            tables = b.list_tables()
            assert "prices_live" in tables
            df = b.open_table("prices_live").to_arrow().to_pandas()
            assert len(df) == 8
        finally:
            a.close()
            b.close()

    def test_overwrite_table_visible_to_others(self, streaming_server):
        a = _connect()
        b = _connect()
        try:
            # Create then overwrite
            a.run_script('overwrite_test = prices_live.where(["Symbol = `AAPL`"])')
            df1 = b.open_table("overwrite_test").to_arrow().to_pandas()
            assert df1["Symbol"].iloc[0] == "AAPL"

            a.run_script('overwrite_test = prices_live.where(["Symbol = `GOOGL`"])')
            df2 = b.open_table("overwrite_test").to_arrow().to_pandas()
            assert df2["Symbol"].iloc[0] == "GOOGL"
        finally:
            a.close()
            b.close()


# ── Data consistency ─────────────────────────────────────────────────────────

class TestDataConsistency:
    def test_portfolio_summary_consistent_across_clients(self, streaming_server):
        """Two clients reading portfolio_summary should get same structure."""
        a = _connect()
        b = _connect()
        try:
            df_a = a.open_table("portfolio_summary").to_arrow().to_pandas()
            df_b = b.open_table("portfolio_summary").to_arrow().to_pandas()
            assert list(df_a.columns) == list(df_b.columns)
            assert len(df_a) == len(df_b) == 1
        finally:
            a.close()
            b.close()

    def test_derived_table_data_visible_to_other_client(self, streaming_server):
        """A derived table created by one client has correct data for another."""
        a = _connect()
        b = _connect()
        try:
            a.run_script('derived_test = prices_live.where(["Symbol = `META`"])')
            df = b.open_table("derived_test").to_arrow().to_pandas()
            assert len(df) == 1
            assert df["Symbol"].iloc[0] == "META"
            assert df["Price"].iloc[0] > 0
        finally:
            a.close()
            b.close()
