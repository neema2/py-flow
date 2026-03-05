#!/usr/bin/env python3
"""
Lakehouse Demo — Iceberg Analytical Store
===========================================
Runs the full lakehouse integration test suite, then starts the stack
for interactive exploration via LakehouseQuery.

  1. Run integration tests (Storable → Iceberg + QuestDB ticks → Iceberg)
  2. Start a fresh lakehouse stack with seeded data
  3. Print example queries and keep running until Ctrl+C

Prerequisites:
  pip install -e ".[lakehouse]"

Usage:
  python3 demo_lakehouse.py              # run tests + interactive
  python3 demo_lakehouse.py --skip-tests # just start the stack
"""

import argparse
import asyncio
import logging
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("demo_lakehouse")


# ── Storable types ────────────────────────────────────────────────────────

from store import Storable


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
    limit_price: float = 0.0
    order_type: str = "LIMIT"


# ── Seed data ─────────────────────────────────────────────────────────────

def seed_storable_objects(server_info: dict) -> int:
    """Save real Storable objects via the public API."""
    from store import connect

    conn = connect(
        host=server_info["host"],
        port=server_info["port"],
        dbname=server_info["dbname"],
        user="demo_user",
        password=server_info["password"],
    )

    count = 0
    symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "META", "NVDA", "JPM"]
    for i, sym in enumerate(symbols):
        Trade(symbol=sym, quantity=(i + 1) * 100, price=150.0 + i * 10, side="BUY").save()
        Order(symbol=sym, quantity=(i + 1) * 50, limit_price=148.0 + i * 10).save()
        count += 2

    conn.close()
    return count


def seed_ticks(backend) -> dict:
    """Write real ticks via the public TSDBBackend API."""
    from marketdata.models import CurveTick, FXTick, Tick

    now = datetime.now(timezone.utc)
    counts = {"equity": 0, "fx": 0, "curve": 0}

    for i, sym in enumerate(["AAPL", "MSFT", "GOOG"]):
        for j in range(5):
            asyncio.run(backend.write_tick(Tick(
                symbol=sym, price=150.0 + i * 10 + j * 0.5,
                bid=149.5 + i * 10 + j * 0.5, ask=150.5 + i * 10 + j * 0.5,
                volume=1000 + j * 100, change=j * 0.5, change_pct=j * 0.1,
                timestamp=now - timedelta(minutes=10 - j),
            )))
            counts["equity"] += 1

    for i, pair in enumerate(["EUR/USD", "GBP/USD", "USD/JPY"]):
        for j in range(3):
            asyncio.run(backend.write_tick(FXTick(
                pair=pair, bid=1.08 + i * 0.2 + j * 0.001,
                ask=1.0805 + i * 0.2 + j * 0.001,
                mid=1.08025 + i * 0.2 + j * 0.001,
                spread_pips=0.5 + j * 0.1, currency="USD",
                timestamp=now - timedelta(minutes=10 - j),
            )))
            counts["fx"] += 1

    for i, label in enumerate(["USD_2Y", "USD_5Y", "USD_10Y"]):
        for j in range(2):
            asyncio.run(backend.write_tick(CurveTick(
                label=label, tenor_years=2.0 + i * 3,
                rate=0.04 + i * 0.005 + j * 0.001,
                discount_factor=0.92 - i * 0.02 - j * 0.001,
                currency="USD",
                timestamp=now - timedelta(minutes=10 - j),
            )))
            counts["curve"] += 1

    asyncio.run(backend.flush())
    return counts


# ── Main demo ─────────────────────────────────────────────────────────────

async def run_demo(args):
    """Run the full lakehouse demo."""

    print("=" * 70)
    print("  Lakehouse Demo — Iceberg Analytical Store")
    print("=" * 70)
    print()

    # ── Step 0: Run integration tests ─────────────────────────────────
    if not args.skip_tests:
        print("Step 1: Running integration test suite...")
        print("-" * 70)
        result = subprocess.run(
            [sys.executable, "-m", "pytest",
             "tests/test_lakehouse_integration.py", "-v", "-s",
             "--tb=short"],
            cwd=".",
        )
        print("-" * 70)
        if result.returncode != 0:
            print("  FAILED — fix integration tests before running the demo.")
            return
        print("  All tests passed.")
        print()

    # ── Step 1: Start the lakehouse stack ──────────────────────────────
    print("Step 2: Starting lakehouse stack...")
    from lakehouse.admin import LakehouseServer, SyncEngine
    from store.server import StoreServer
    from timeseries.factory import create_backend

    # Object store (Storable PG)
    server = StoreServer(data_dir=tempfile.mkdtemp(prefix="dm_lh_st_", dir="/tmp"))
    server.start()
    server.provision_user("demo_user", "demo_pw")

    # Lakehouse infra (PG + Lakekeeper + object store)
    stack = LakehouseServer(data_dir=tempfile.mkdtemp(prefix="dm_lh_", dir="/tmp"))
    await stack.start()

    # TSDB (QuestDB)
    backend = create_backend(
        "questdb",
        data_dir=tempfile.mkdtemp(prefix="dm_lh_q_", dir="/tmp"),
        http_port=19200,
        ilp_port=19209,
        pg_port=18992,
    )
    await backend.start()

    print(f"  Catalog:  {stack.catalog_url}")
    print(f"  S3:       {stack.s3_endpoint}")
    print()

    # ── Step 2: Seed data ──────────────────────────────────────────────
    print("Step 3: Seeding data...")
    info = server.conn_info()
    info["password"] = "demo_pw"
    num_objects = seed_storable_objects(info)
    print(f"  {num_objects} Storable objects saved")

    tick_counts = seed_ticks(backend)
    total_ticks = sum(tick_counts.values())
    print(f"  {total_ticks} ticks written ({tick_counts})")

    import time
    time.sleep(4)  # QuestDB WAL commit
    print()

    # ── Step 3: Sync to Iceberg ────────────────────────────────────────
    print("Step 4: Syncing to Iceberg...")
    from lakehouse import Lakehouse

    lh = Lakehouse(catalog_uri=stack.catalog_url, s3_endpoint=stack.s3_endpoint)

    sync = SyncEngine(lakehouse=lh, state_path="data/demo_lakehouse_sync.json")
    ticks_synced = sync.sync_ticks(backend)
    print(f"  {ticks_synced} ticks synced")
    print(f"  (Store events sync via EventBridge + LakehouseSink)")
    print()

    # ── Step 4: Query via DuckDB ───────────────────────────────────────
    print("Step 5: Querying Iceberg via DuckDB...")
    print()

    print("  Tables:")
    for t in lh.tables():
        count = lh.row_count(t)
        print(f"    lakehouse.default.{t:15s}  {count:6d} rows")
    print()

    print("  Ticks by asset class:")
    for row in lh.sql("SELECT tick_type, count(*) as cnt, count(DISTINCT symbol) as symbols FROM lakehouse.default.ticks GROUP BY tick_type"):
        print(f"    {row['tick_type']:10s}  {row['cnt']:4d} ticks  {row['symbols']:2d} symbols")
    print()

    lh.close()

    # ── Interactive ────────────────────────────────────────────────────
    print("=" * 70)
    print("  Stack is running. Try:")
    print()
    print("    from lakehouse import Lakehouse")
    print(f"    lh = Lakehouse(catalog_uri='{stack.catalog_url}', s3_endpoint='{stack.s3_endpoint}')")
    print("    lh.query('SELECT * FROM lakehouse.default.ticks LIMIT 5')")
    print("    lh.ingest('my_signals', [{'symbol': 'AAPL', 'score': 0.95}], mode='snapshot')")
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
        await backend.stop()
        await stack.stop()
        server.stop()
        print("  Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lakehouse Demo — Iceberg Analytical Store")
    parser.add_argument("--skip-tests", action="store_true",
                        help="Skip running integration tests first")
    args = parser.parse_args()

    try:
        asyncio.run(run_demo(args))
    except KeyboardInterrupt:
        print("\nDemo stopped.")
