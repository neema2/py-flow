#!/usr/bin/env python3
"""
Lakehouse Ingest & Transform Demo
===================================
Demonstrates all 4 write modes and transform capabilities of the Lakehouse class.

  1. Start lakehouse stack (Lakekeeper + MinIO)
  2. Ingest data in all 4 modes: append, snapshot, incremental, bitemporal
  3. Run transforms (SQL → Iceberg)
  4. Query and display results

Usage:
  python3 demo_lakehouse_ingest.py
"""

import asyncio
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("demo_ingest")


def section(title: str):
    print()
    print(f"{'─' * 70}")
    print(f"  {title}")
    print(f"{'─' * 70}")


def show(lh, sql: str, label: str = ""):
    """Pretty-print a query result."""
    rows = lh.query(sql)
    if label:
        print(f"\n  {label} ({len(rows)} rows):")
    if not rows:
        print("    (empty)")
        return rows
    cols = list(rows[0].keys())
    # Column widths
    widths = {c: max(len(c), max(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    header = "    " + "  ".join(f"{c:<{widths[c]}}" for c in cols)
    print(header)
    print("    " + "  ".join("─" * widths[c] for c in cols))
    for r in rows:
        print("    " + "  ".join(f"{str(r.get(c, '')):<{widths[c]}}" for c in cols))
    return rows


async def run_demo():
    print("=" * 70)
    print("  Lakehouse Ingest & Transform Demo")
    print("=" * 70)

    # ── Platform setup ─────────────────────────────────────────────────────
    section("Starting lakehouse stack")
    from lakehouse.admin import LakehouseServer

    server = LakehouseServer(data_dir="data/demo_ingest")
    await server.start()
    server.register_alias("demo")
    print(f"  Catalog: {server.catalog_url}")
    print(f"  S3:      {server.s3_endpoint}")

    # ── User code ──────────────────────────────────────────────────────────
    from lakehouse import Lakehouse
    lh = Lakehouse("demo")

    try:
        # ── 1. Append Mode ───────────────────────────────────────────────
        section("1. APPEND MODE — Raw append with batch tracking")

        signals_batch1 = [
            {"symbol": "AAPL", "score": 0.95, "signal": "BUY"},
            {"symbol": "MSFT", "score": 0.80, "signal": "HOLD"},
            {"symbol": "GOOG", "score": 0.60, "signal": "SELL"},
        ]
        n = lh.ingest("signals", signals_batch1, mode="append")
        print(f"  Ingested batch 1: {n} rows")

        signals_batch2 = [
            {"symbol": "TSLA", "score": 0.70, "signal": "BUY"},
            {"symbol": "NVDA", "score": 0.88, "signal": "BUY"},
        ]
        n = lh.ingest("signals", signals_batch2, mode="append")
        print(f"  Ingested batch 2: {n} rows")

        show(lh, "SELECT symbol, score, signal, _batch_id, _batch_ts FROM lakehouse.default.signals ORDER BY symbol",
             "All signals (two batches, two _batch_ids)")

        batch_ids = lh.query("SELECT DISTINCT _batch_id FROM lakehouse.default.signals")
        print(f"\n  Unique batch IDs: {len(batch_ids)}")

        # ── 2. Snapshot Mode ─────────────────────────────────────────────
        section("2. SNAPSHOT MODE — Full batch replacement with history")

        # First snapshot: end-of-day positions
        eod_v1 = [
            {"symbol": "AAPL", "position": 1000, "pnl": 5200.0},
            {"symbol": "MSFT", "position": 500, "pnl": -1200.0},
            {"symbol": "GOOG", "position": 300, "pnl": 800.0},
        ]
        n = lh.ingest("eod_positions", eod_v1, mode="snapshot")
        print(f"  Snapshot v1: {n} rows")

        # Second snapshot: next day
        eod_v2 = [
            {"symbol": "AAPL", "position": 1200, "pnl": 6100.0},
            {"symbol": "MSFT", "position": 400, "pnl": -800.0},
            {"symbol": "GOOG", "position": 300, "pnl": 1500.0},
            {"symbol": "TSLA", "position": 200, "pnl": 350.0},
        ]
        n = lh.ingest("eod_positions", eod_v2, mode="snapshot")
        print(f"  Snapshot v2: {n} rows")

        show(lh, "SELECT symbol, position, pnl, _is_current, _batch_id FROM lakehouse.default.eod_positions ORDER BY _is_current DESC, symbol",
             "All snapshots (v1 expired, v2 current)")

        show(lh, "SELECT symbol, position, pnl FROM lakehouse.default.eod_positions WHERE _is_current = true ORDER BY pnl DESC",
             "Current snapshot only")

        total = lh.query("SELECT count(*) as total FROM lakehouse.default.eod_positions")
        current = lh.query("SELECT count(*) as current FROM lakehouse.default.eod_positions WHERE _is_current = true")
        print(f"\n  Total rows: {total[0]['total']} (current: {current[0]['current']}, historical: {total[0]['total'] - current[0]['current']})")

        # ── 3. Incremental Mode ──────────────────────────────────────────
        section("3. INCREMENTAL MODE — Row-level upsert with full audit trail")

        # Initial trades
        trades_v1 = [
            {"trade_id": "T001", "symbol": "AAPL", "qty": 100, "price": 185.50, "status": "FILLED"},
            {"trade_id": "T002", "symbol": "MSFT", "qty": 200, "price": 420.00, "status": "FILLED"},
            {"trade_id": "T003", "symbol": "GOOG", "qty": 50, "price": 175.25, "status": "PENDING"},
        ]
        n = lh.ingest("trades", trades_v1, mode="incremental", primary_key="trade_id")
        print(f"  Initial trades: {n} rows")

        show(lh, "SELECT trade_id, symbol, qty, price, status, _is_current FROM lakehouse.default.trades ORDER BY trade_id",
             "After initial insert")

        # Update: T001 price corrected, T003 filled
        trades_v2 = [
            {"trade_id": "T001", "symbol": "AAPL", "qty": 100, "price": 186.00, "status": "FILLED"},
            {"trade_id": "T003", "symbol": "GOOG", "qty": 50, "price": 175.25, "status": "FILLED"},
        ]
        n = lh.ingest("trades", trades_v2, mode="incremental", primary_key="trade_id")
        print(f"\n  Updated trades: {n} rows")

        show(lh, "SELECT trade_id, symbol, qty, price, status, _is_current, _batch_id FROM lakehouse.default.trades ORDER BY trade_id, _is_current DESC",
             "Full audit trail (current + expired)")

        show(lh, "SELECT trade_id, symbol, price, status FROM lakehouse.default.trades WHERE _is_current = true ORDER BY trade_id",
             "Current state only")

        show(lh, "SELECT trade_id, price, status, _is_current FROM lakehouse.default.trades WHERE trade_id = 'T001' ORDER BY _updated_at",
             "T001 version history")

        # ── 4. Bitemporal Mode ───────────────────────────────────────────
        section("4. BITEMPORAL MODE — System time + business time versioning")

        # Initial positions
        positions_v1 = [
            {"entity_id": "POS_AAPL", "symbol": "AAPL", "notional": 185000.0},
            {"entity_id": "POS_MSFT", "symbol": "MSFT", "notional": 84000.0},
        ]
        n = lh.ingest("risk_positions", positions_v1, mode="bitemporal", primary_key="entity_id")
        print(f"  Initial positions: {n} rows")

        # Update AAPL position
        positions_v2 = [
            {"entity_id": "POS_AAPL", "symbol": "AAPL", "notional": 210000.0},
        ]
        n = lh.ingest("risk_positions", positions_v2, mode="bitemporal", primary_key="entity_id")
        print(f"  Updated POS_AAPL: {n} row")

        show(lh, "SELECT entity_id, symbol, notional, _is_current, _valid_from, _valid_to FROM lakehouse.default.risk_positions ORDER BY entity_id, _is_current DESC",
             "Bitemporal audit trail")

        show(lh, "SELECT entity_id, symbol, notional FROM lakehouse.default.risk_positions WHERE _is_current = true ORDER BY entity_id",
             "Current positions")

        # ── 5. Transform ─────────────────────────────────────────────────
        section("5. TRANSFORM — SQL → Iceberg table")

        # First: make sure we have some signal data to aggregate
        print("  Creating aggregated signal summary from signals table...")
        n = lh.transform(
            "signal_summary",
            """
            SELECT signal, count(*) as cnt, round(avg(score), 3) as avg_score
            FROM lakehouse.default.signals
            GROUP BY signal
            """,
            mode="snapshot",
        )
        print(f"  Transform v1: {n} rows")

        show(lh, "SELECT signal, cnt, avg_score, _is_current, _batch_id FROM lakehouse.default.signal_summary ORDER BY cnt DESC",
             "Signal summary (snapshot v1)")

        # Ingest more signals, then re-run transform
        lh.ingest("signals", [
            {"symbol": "AMD", "score": 0.92, "signal": "BUY"},
            {"symbol": "INTC", "score": 0.45, "signal": "SELL"},
        ], mode="append")

        n = lh.transform(
            "signal_summary",
            """
            SELECT signal, count(*) as cnt, round(avg(score), 3) as avg_score
            FROM lakehouse.default.signals
            GROUP BY signal
            """,
            mode="snapshot",
        )
        print(f"\n  Transform v2 (after adding 2 signals): {n} rows")

        show(lh, "SELECT signal, cnt, avg_score, _is_current, _batch_id FROM lakehouse.default.signal_summary ORDER BY _is_current DESC, cnt DESC",
             "Signal summary (v1 expired, v2 current)")

        # ── Summary ──────────────────────────────────────────────────────
        section("Summary — All tables")

        for t in sorted(lh.tables()):
            count = lh.row_count(t)
            print(f"  lakehouse.default.{t:20s}  {count:5d} rows")

        # ── Interactive ──────────────────────────────────────────────────
        print()
        print("=" * 70)
        print("  Stack is running. Try:")
        print()
        print("    from lakehouse import Lakehouse")
        print(f"    lh = Lakehouse(catalog_uri='{stack.catalog_url}', s3_endpoint='{stack.s3_endpoint}')")
        print("    lh.query('SELECT * FROM lakehouse.default.trades WHERE _is_current = true')")
        print()
        print("  Press Ctrl+C to stop.")
        print("=" * 70)

        try:
            while True:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass

    finally:
        print("\n  Shutting down...")
        lh.close()
        await server.stop()
        print("  Done.")


if __name__ == "__main__":
    try:
        asyncio.run(run_demo())
    except KeyboardInterrupt:
        print("\nDemo stopped.")
