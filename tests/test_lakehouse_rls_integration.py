"""
RLS Integration Test — Full Lakehouse Round-Trip with Row-Level Security
=========================================================================
End-to-end test using ONLY public APIs:

  from lakehouse.admin import LakehouseServer, RLSPolicy
  from lakehouse import Lakehouse

  LakehouseServer(rls_policies=...) → start() → register_alias()
  → Lakehouse(alias, token=) → hybrid routing → row isolation
"""

from __future__ import annotations

import asyncio
import logging

import pytest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Sample Data ──────────────────────────────────────────────────────────────

TRADES = [
    {"trade_id": 1, "symbol": "AAPL", "quantity": 100, "price": 185.50, "side": "BUY"},
    {"trade_id": 2, "symbol": "GOOGL", "quantity": 50, "price": 140.25, "side": "BUY"},
    {"trade_id": 3, "symbol": "MSFT", "quantity": 75, "price": 420.00, "side": "SELL"},
]

SALES_DATA = [
    {"row_id": 1, "product": "Enterprise License", "region": "US", "amount": 50000.0},
    {"row_id": 2, "product": "Cloud Subscription", "region": "EU", "amount": 35000.0},
    {"row_id": 3, "product": "Support Package", "region": "US", "amount": 12000.0},
    {"row_id": 4, "product": "Enterprise License", "region": "APAC", "amount": 45000.0},
    {"row_id": 5, "product": "Cloud Subscription", "region": "US", "amount": 28000.0},
]

SALES_ACL = [
    {"row_id": 1, "user_token": "alice-token"},
    {"row_id": 2, "user_token": "alice-token"},
    {"row_id": 3, "user_token": "bob-token"},
    {"row_id": 4, "user_token": "bob-token"},
    {"row_id": 5, "user_token": "bob-token"},
]


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def lakehouse_stack():
    """Start the full lakehouse stack with RLS configured."""
    import shutil
    import tempfile
    from lakehouse.admin import LakehouseServer, RLSPolicy

    data_dir = tempfile.mkdtemp(prefix="lh_rls_", dir="/tmp")

    server = LakehouseServer(
        data_dir=data_dir,
        rls_policies=[
            RLSPolicy(
                table_name="sales_data",
                acl_table="sales_acl",
                join_column="row_id",
                user_column="user_token",
            ),
        ],
        rls_users={"alice-token": "alice", "bob-token": "bob"},
    )
    asyncio.run(server.start())
    server.register_alias("rls-test")

    yield server

    asyncio.run(server.stop())
    shutil.rmtree(data_dir, ignore_errors=True)


@pytest.fixture(scope="module")
def ingested_data(lakehouse_stack):
    """Ingest sample data via the public Lakehouse API."""
    from lakehouse import Lakehouse

    lh = Lakehouse("rls-test")
    lh.ingest("trades", TRADES)
    lh.ingest("sales_data", SALES_DATA)
    lh.ingest("sales_acl", SALES_ACL)
    lh.close()
    return True


# ── Test Class ──────────────────────────────────────────────────────────────


class TestRLSIntegration:
    """End-to-end RLS tests against the real lakehouse — public API only."""

    def test_open_table_direct(self, lakehouse_stack, ingested_data) -> None:
        """Open table queries go direct to DuckDB and return all rows."""
        from lakehouse import Lakehouse

        lh = Lakehouse("rls-test", token="alice-token")
        rows = lh.query("SELECT * FROM lakehouse.default.trades")
        lh.close()

        assert len(rows) == 3
        symbols = {r["symbol"] for r in rows}
        assert symbols == {"AAPL", "GOOGL", "MSFT"}

    def test_rls_alice_sees_own_rows(self, lakehouse_stack, ingested_data) -> None:
        """Alice should only see rows 1, 2 from sales_data."""
        from lakehouse import Lakehouse

        lh = Lakehouse("rls-test", token="alice-token")
        rows = lh.query("SELECT * FROM lakehouse.default.sales_data")
        lh.close()

        assert len(rows) == 2
        row_ids = sorted(r["row_id"] for r in rows)
        assert row_ids == [1, 2]

    def test_rls_bob_sees_own_rows(self, lakehouse_stack, ingested_data) -> None:
        """Bob should only see rows 3, 4, 5 from sales_data."""
        from lakehouse import Lakehouse

        lh = Lakehouse("rls-test", token="bob-token")
        rows = lh.query("SELECT * FROM lakehouse.default.sales_data")
        lh.close()

        assert len(rows) == 3
        row_ids = sorted(r["row_id"] for r in rows)
        assert row_ids == [3, 4, 5]

    def test_rls_with_where(self, lakehouse_stack, ingested_data) -> None:
        """RLS + user WHERE clause should combine correctly."""
        from lakehouse import Lakehouse

        lh = Lakehouse("rls-test", token="alice-token")
        rows = lh.query(
            "SELECT * FROM lakehouse.default.sales_data WHERE region = 'US'"
        )
        lh.close()

        assert len(rows) == 1
        assert rows[0]["row_id"] == 1
        assert rows[0]["region"] == "US"

    def test_rls_query_arrow(self, lakehouse_stack, ingested_data) -> None:
        """query_arrow should also respect RLS."""
        from lakehouse import Lakehouse

        lh = Lakehouse("rls-test", token="alice-token")
        table = lh.query_arrow("SELECT * FROM lakehouse.default.sales_data")
        lh.close()

        assert table.num_rows == 2

    def test_no_token_queries_open_only(self, lakehouse_stack, ingested_data) -> None:
        """Without token, all queries go direct (no RLS enforcement)."""
        from lakehouse import Lakehouse

        lh = Lakehouse("rls-test")
        rows = lh.query("SELECT * FROM lakehouse.default.trades")
        assert len(rows) == 3
        lh.close()
