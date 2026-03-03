#!/usr/bin/env python3
"""
Demo: Built-in PlatformAgents — 8-Agent Data Engineering Team
===============================================================

Uses the BUILT-IN ``PlatformAgents`` class (one import, one constructor).
Spins up ALL platform services and exercises every specialist agent:

  1. OLTP        — create a trades dataset, insert records, query back
  2. Feed        — list live symbols, check feed health
  3. Timeseries  — get OHLCV bars, compute realized vol
  4. Lakehouse   — create analytical table, run SQL
  5. Quant       — statistics, anomaly detection, regression
  6. Document    — upload a research note, search it
  7. Dashboard   — create a ticking table, setup store bridge
  8. Query       — cross-store discovery, natural language queries

Services started:
  - StoreServer     (embedded PG)
  - MarketDataServer (FastAPI + SimulatorFeed + QuestDB)
  - LakehouseServer  (Lakekeeper + Iceberg + MinIO + PG)
  - MediaServer      (MinIO S3)
  - StreamingServer   (Deephaven JVM — from conftest or standalone)

Usage::

    export GEMINI_API_KEY="your-key"
    python3 demo_platform_agents.py
"""

import asyncio
import json
import os
import sys
import tempfile
import textwrap
import time

import httpx


def section(title: str):
    print(f"\n{'─' * 70}")
    print(f"  {title}")
    print(f"{'─' * 70}")


def step(msg: str):
    print(f"  → {msg}")


def result(msg: str):
    print(f"    ✓ {msg}")


def show_response(resp):
    """Pretty-print an agent response."""
    text = str(resp) if not hasattr(resp, "content") else resp.content
    # Truncate long responses
    lines = text.strip().split("\n")
    if len(lines) > 25:
        for line in lines[:25]:
            print(f"    {line}")
        print(f"    ... ({len(lines) - 25} more lines)")
    else:
        for line in lines:
            print(f"    {line}")


def main():
    print("=" * 70)
    print("  PLATFORM AGENTS — 8-Agent Data Engineering Team Demo")
    print("  Real services, real data, real AI routing")
    print("=" * 70)

    if not os.environ.get("GEMINI_API_KEY"):
        print("\n  ERROR: Set GEMINI_API_KEY environment variable.")
        print("    export GEMINI_API_KEY='your-key'")
        sys.exit(1)

    # ── Start Infrastructure ─────────────────────────────────────────
    section("Starting Platform Infrastructure")

    # 1. StoreServer
    from store.server import StoreServer
    store_dir = tempfile.mkdtemp(prefix="demo_pa_store_")
    store = StoreServer(data_dir=store_dir, admin_password="demo_admin")
    store.start()
    store.provision_user("demo_user", "demo_pw")
    store.register_alias("demo")
    result("StoreServer (embedded PG)")

    # Bootstrap media schemas
    from media.models import bootstrap_search_schema
    admin_conn = store.admin_conn()
    bootstrap_search_schema(admin_conn)
    admin_conn.close()
    result("Media search schema bootstrapped")

    # 2. MediaServer (MinIO S3)
    from media.admin import MediaServer
    media_dir = tempfile.mkdtemp(prefix="demo_pa_s3_")
    media_srv = MediaServer(data_dir=media_dir, api_port=9062, console_port=9063)
    asyncio.run(media_srv.start())
    media_srv.register_alias("demo")
    result("MediaServer (MinIO on port 9062)")

    # 3. MarketDataServer (+ embedded QuestDB)
    from timeseries.admin import TsdbServer
    tsdb_dir = tempfile.mkdtemp(prefix="demo_pa_tsdb_")
    tsdb = TsdbServer(data_dir=tsdb_dir)
    asyncio.run(tsdb.start())
    tsdb.register_alias("demo")
    result(f"TsdbServer (QuestDB: HTTP={tsdb.http_port}, ILP={tsdb.ilp_port})")

    from marketdata.admin import MarketDataServer
    md = MarketDataServer(port=8765, host="127.0.0.1")
    asyncio.run(md.start())
    md.register_alias("demo")
    result(f"MarketDataServer on port {md.port}")

    # Wait for simulator to produce some ticks
    step("Waiting for simulator feed to accumulate ticks...")
    for attempt in range(30):
        time.sleep(1)
        try:
            resp = httpx.get(f"{md.url}/md/latest/equity", timeout=2)
            if resp.status_code == 200 and len(resp.json()) >= 6:
                result(f"Simulator producing data ({len(resp.json())} symbols)")
                break
        except Exception:
            continue
    else:
        print("  ⚠ Simulator slow — continuing anyway")

    # 4. LakehouseServer
    from lakehouse.admin import LakehouseServer
    lh_dir = tempfile.mkdtemp(prefix="lh_demo_", dir="/tmp")
    lh_srv = LakehouseServer(data_dir=lh_dir, s3_api_port=9092, s3_console_port=9093)
    asyncio.run(lh_srv.start())
    lh_srv.register_alias("demo")
    result("LakehouseServer (Lakekeeper + Iceberg + MinIO)")

    # 5. StreamingServer (Deephaven JVM)
    from streaming.admin import StreamingServer
    streaming = StreamingServer(port=10000, max_heap="512m")
    streaming.start()
    result("StreamingServer (Deephaven JVM on port 10000)")

    # ── Build the Team ───────────────────────────────────────────────
    section("Building PlatformAgents Team (8 agents)")

    from agents import PlatformAgents

    team = PlatformAgents(
        alias="demo",
        user="demo_user",
        password="demo_pw",
    )
    result(f"PlatformAgents initialized: {len(team)} agents")
    for name, agent in team:
        print(f"    • {name}: {len(agent.tools)} tools")

    # ── Exercise Each Agent ──────────────────────────────────────────

    # 1. OLTP Agent
    section("1. OLTP Agent — Create & Query Dataset")
    step("Creating trades dataset...")
    resp = team.oltp.run("Create a dataset called DemoTrade with fields: symbol (str), price (float), quantity (int), side (str)")
    show_response(resp)

    step("Inserting records...")
    resp = team.oltp.run(
        "Insert these trades into DemoTrade: "
        "AAPL at 228.50 qty 100 buy, NVDA at 875.00 qty 50 sell, MSFT at 415.00 qty 200 buy"
    )
    show_response(resp)

    step("Querying back...")
    resp = team.oltp.run("Query all DemoTrade records")
    show_response(resp)

    # 2. Feed Agent
    section("2. Feed Agent — Live Market Data")
    step("Listing symbols...")
    resp = team.feed.run("What symbols are available on the market data feed?")
    show_response(resp)

    step("Checking feed health...")
    resp = team.feed.run("Is the market data feed healthy?")
    show_response(resp)

    # 3. Timeseries Agent
    section("3. Timeseries Agent — Historical Data")
    step("Getting AAPL bars...")
    resp = team.timeseries.run("Show me the latest 1-minute OHLCV bars for AAPL")
    show_response(resp)

    step("Computing realized vol...")
    resp = team.timeseries.run("What is the realized volatility for NVDA?")
    show_response(resp)

    # 4. Lakehouse Agent
    section("4. Lakehouse Agent — Analytical Tables")
    step("Creating analytical table...")
    resp = team.lakehouse.run(
        "Create a lakehouse table called demo_positions from this SQL: "
        "SELECT 'AAPL' as symbol, 228.5 as price, 500 as qty, 'Technology' as sector "
        "UNION ALL SELECT 'NVDA', 875.0, 200, 'Technology' "
        "UNION ALL SELECT 'TSLA', 242.0, 150, 'Consumer'"
    )
    show_response(resp)

    step("Querying lakehouse...")
    resp = team.lakehouse.run("What tables are in the lakehouse? Query demo_positions to show all rows.")
    show_response(resp)

    # 5. Quant Agent
    section("5. Quant Agent — Analytics & Statistics")
    step("Running statistical analysis...")
    resp = team.quant.run(
        "Run descriptive statistics on SELECT * FROM lakehouse.default.demo_positions, "
        "focusing on the price and qty columns"
    )
    show_response(resp)

    step("Detecting anomalies...")
    resp = team.quant.run(
        "Check for anomalies in the price column of "
        "SELECT * FROM lakehouse.default.demo_positions using zscore method"
    )
    show_response(resp)

    # 6. Document Agent
    section("6. Document Agent — Research Documents")
    # Write a temp doc
    doc_path = os.path.join(tempfile.mkdtemp(), "research_note.txt")
    with open(doc_path, "w") as f:
        f.write(textwrap.dedent("""\
            NVDA Q4 Earnings Analysis — March 2024

            NVIDIA reported record revenue of $22.1B, up 265% YoY. Data center
            revenue reached $18.4B, driven by Hopper GPU demand for AI training.

            Key risks: export controls on China sales, AMD MI300X competition,
            and potential customer vertical integration (Google TPU, Amazon Trainium).

            Recommendation: Hold with position size cap at 20% of portfolio.
        """))
    step("Uploading research note...")
    resp = team.document.run(f"Upload the document at {doc_path} with title 'NVDA Q4 Analysis' and tags 'nvda,earnings,ai'")
    show_response(resp)

    step("Searching documents...")
    resp = team.document.run("Search for documents about NVIDIA earnings risks")
    show_response(resp)

    # 7. Dashboard Agent
    section("7. Dashboard Agent — Real-time Dashboards")
    step("Listing ticking tables...")
    resp = team.dashboard.run("What ticking tables are currently available?")
    show_response(resp)

    step("Creating ticking table...")
    resp = team.dashboard.run(
        "Create a ticking table called demo_prices with columns: symbol (str), price (float), volume (int)"
    )
    show_response(resp)

    # 8. Query Agent
    section("8. Query Agent — Cross-Store Discovery")
    step("Discovering all datasets...")
    resp = team.query.run("What datasets are available across all platform stores?")
    show_response(resp)

    step("Cross-store query...")
    resp = team.query.run("Get a market data snapshot for equity symbols")
    show_response(resp)

    # ── Multi-Agent Routing Demo ─────────────────────────────────────
    section("Multi-Agent Routing — Let the Router Decide")

    prompts = [
        "What symbols do we have in the OLTP store and how many records?",
        "Show me AAPL's latest OHLCV bars and compute its realized volatility",
        "Run a regression on lakehouse.default.demo_positions with price as target and qty as feature",
    ]

    for prompt in prompts:
        step(f"Prompt: {prompt}")
        resp = team.run(prompt)
        show_response(resp)
        print()

    # ── Summary ──────────────────────────────────────────────────────
    section("Demo Complete")
    print("""
  Services used:
    • StoreServer     — embedded PostgreSQL (OLTP, documents, dashboard)
    • MarketDataServer — live equity + FX feeds (SimulatorFeed + QuestDB)
    • LakehouseServer  — Iceberg analytics (Lakekeeper + MinIO + DuckDB)
    • MediaServer      — S3 document storage (MinIO)
    • StreamingServer   — Deephaven JVM (ticking tables)

  Agents exercised: all 8
    • oltp, feed, timeseries, lakehouse, quant, document, dashboard, query

  All running against REAL services — no mocks.
""")

    # Cleanup
    step("Shutting down services...")
    asyncio.run(md.stop())
    asyncio.run(tsdb.stop())
    asyncio.run(lh_srv.stop())
    asyncio.run(media_srv.stop())
    store.stop()
    result("All services stopped")


if __name__ == "__main__":
    main()
