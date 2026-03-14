"""
Mirror test for demo_lakehouse_ingest.py
==========================================
Verifies the full demo flow — all 4 ingest modes + transforms:

  1. APPEND — raw append with batch tracking
  2. SNAPSHOT — full batch replacement with history
  3. INCREMENTAL — row-level upsert with audit trail
  4. BITEMPORAL — system time + business time versioning
  5. TRANSFORM — SQL → Iceberg table
"""

import pytest

from lakehouse import Lakehouse


# ── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def lh(lakehouse_server):
    """Lakehouse client connected to the shared lakehouse stack."""
    lakehouse_server.register_alias("ingest-demo")
    inst = Lakehouse("ingest-demo")
    yield inst
    inst.close()


# ── Tests ────────────────────────────────────────────────────────────────

class TestDemoLakehouseIngest:
    """Mirrors demo_lakehouse_ingest.py — all 4 modes + transform."""

    def test_all_ingest_modes(self, lh) -> None:
        """Append: two batches get distinct _batch_ids."""
        batch1 = [
            {"symbol": "AAPL", "score": 0.95, "signal": "BUY"},
            {"symbol": "MSFT", "score": 0.80, "signal": "HOLD"},
            {"symbol": "GOOG", "score": 0.60, "signal": "SELL"},
        ]
        n1 = lh.ingest("ing_signals", batch1, mode="append")
        assert n1 == 3

        batch2 = [
            {"symbol": "TSLA", "score": 0.70, "signal": "BUY"},
            {"symbol": "NVDA", "score": 0.88, "signal": "BUY"},
        ]
        n2 = lh.ingest("ing_signals", batch2, mode="append")
        assert n2 == 2

        # Both batches present
        rows = lh.query("SELECT * FROM lakehouse.default.ing_signals")
        assert len(rows) == 5

        # Two distinct batch IDs
        batch_ids = lh.query("SELECT DISTINCT _batch_id FROM lakehouse.default.ing_signals")
        assert len(batch_ids) == 2

        eod_v1 = [
            {"symbol": "AAPL", "position": 1000, "pnl": 5200.0},
            {"symbol": "MSFT", "position": 500, "pnl": -1200.0},
        ]
        lh.ingest("ing_eod", eod_v1, mode="snapshot")

        eod_v2 = [
            {"symbol": "AAPL", "position": 1200, "pnl": 6100.0},
            {"symbol": "MSFT", "position": 400, "pnl": -800.0},
            {"symbol": "TSLA", "position": 200, "pnl": 350.0},
        ]
        lh.ingest("ing_eod", eod_v2, mode="snapshot")

        # Current rows = v2 only
        current = lh.query(
            "SELECT * FROM lakehouse.default.ing_eod WHERE _is_current = true"
        )
        assert len(current) == 3

        # Total rows = v1 + v2
        total = lh.query("SELECT count(*) as n FROM lakehouse.default.ing_eod")
        assert total[0]["n"] == 5

        trades_v1 = [
            {"trade_id": "T001", "symbol": "AAPL", "price": 185.50, "status": "FILLED"},
            {"trade_id": "T002", "symbol": "MSFT", "price": 420.00, "status": "FILLED"},
            {"trade_id": "T003", "symbol": "GOOG", "price": 175.25, "status": "PENDING"},
        ]
        lh.ingest("ing_trades", trades_v1, mode="incremental", primary_key="trade_id")

        # Update T001 price, T003 status
        trades_v2 = [
            {"trade_id": "T001", "symbol": "AAPL", "price": 186.00, "status": "FILLED"},
            {"trade_id": "T003", "symbol": "GOOG", "price": 175.25, "status": "FILLED"},
        ]
        lh.ingest("ing_trades", trades_v2, mode="incremental", primary_key="trade_id")

        # Current state: 3 rows (latest version of each)
        current = lh.query(
            "SELECT * FROM lakehouse.default.ing_trades WHERE _is_current = true ORDER BY trade_id"
        )
        assert len(current) == 3
        assert current[0]["price"] == 186.00  # T001 updated
        assert current[2]["status"] == "FILLED"  # T003 updated

        # Full audit trail: 5 rows (3 original + 2 updates)
        total = lh.query("SELECT count(*) as n FROM lakehouse.default.ing_trades")
        assert total[0]["n"] == 5

        positions_v1 = [
            {"entity_id": "POS_AAPL", "symbol": "AAPL", "notional": 185000.0},
            {"entity_id": "POS_MSFT", "symbol": "MSFT", "notional": 84000.0},
        ]
        lh.ingest("ing_positions", positions_v1, mode="bitemporal", primary_key="entity_id")

        # Update AAPL
        positions_v2 = [
            {"entity_id": "POS_AAPL", "symbol": "AAPL", "notional": 210000.0},
        ]
        lh.ingest("ing_positions", positions_v2, mode="bitemporal", primary_key="entity_id")

        # Current: AAPL updated, MSFT unchanged
        current = lh.query(
            "SELECT * FROM lakehouse.default.ing_positions WHERE _is_current = true ORDER BY entity_id"
        )
        assert len(current) == 2
        assert current[0]["notional"] == 210000.0  # AAPL updated
        assert current[1]["notional"] == 84000.0  # MSFT unchanged

        # Has _valid_from and _valid_to columns
        all_rows = lh.query("SELECT * FROM lakehouse.default.ing_positions ORDER BY entity_id")
        assert "_valid_from" in all_rows[0]
        assert "_valid_to" in all_rows[0]

        n = lh.transform(
            "ing_signal_summary",
            """
            SELECT signal, count(*) as cnt, round(avg(score), 3) as avg_score
            FROM lakehouse.default.ing_signals
            GROUP BY signal
            """,
            mode="snapshot",
        )
        assert n > 0

        rows = lh.query(
            "SELECT * FROM lakehouse.default.ing_signal_summary WHERE _is_current = true ORDER BY cnt DESC"
        )
        assert len(rows) > 0
        assert "signal" in rows[0]
        assert "cnt" in rows[0]
        assert "avg_score" in rows[0]

        tables = lh.tables()
        for expected in ["ing_signals", "ing_eod", "ing_trades", "ing_positions", "ing_signal_summary"]:
            assert expected in tables, f"Missing table: {expected}"
            assert lh.row_count(expected) > 0
