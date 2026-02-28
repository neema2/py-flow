"""
Demo: Trading Server
====================
Publishes ticking price + risk tables to Deephaven, fed by the Market Data Server.

Platform tier:
    StreamingServer   — Deephaven JVM
    MarketDataServer  — REST + WS + QuestDB

Published tables (available to all DH clients):
    prices_raw        — append-only equity ticks
    prices_live       — latest price per symbol
    risk_raw          — per-tick risk metrics
    risk_live         — latest risk per symbol
    portfolio_summary — aggregate portfolio view
    top_movers        — sorted by % change
    volume_leaders    — sorted by volume

Standalone:
    python3 demo_trading.py

Testable:
    from demo_trading import publish_tables, stop_feed
    publish_tables("ws://localhost:8000/md/subscribe")
"""

import asyncio
import json
import logging
import threading
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
_log = logging.getLogger("demo_trading")

# ── Module state (populated by publish_tables) ────────────────────────────────
_feed_thread: Optional[threading.Thread] = None
_feed_stop = threading.Event()


def publish_tables(md_ws_url: str = "ws://localhost:8000/md/subscribe"):
    """Create DH writers, derive all tables, start WS consumer.

    Must be called AFTER the Deephaven JVM is running (StreamingServer.start()
    or deephaven_server.Server.start()).

    Returns dict of table name → DH table for inspection.
    """
    global _feed_thread, _feed_stop

    # ── DH imports (available only after JVM is running) ──────────────────
    from deephaven import DynamicTableWriter, agg
    import deephaven.dtypes as dht

    # ── Price writer + raw table ──────────────────────────────────────────
    price_writer = DynamicTableWriter({
        "Symbol":    dht.string,
        "Price":     dht.double,
        "Bid":       dht.double,
        "Ask":       dht.double,
        "Volume":    dht.int64,
        "Change":    dht.double,
        "ChangePct": dht.double,
    })
    prices_raw = price_writer.table

    # ── Risk writer + raw table ───────────────────────────────────────────
    risk_writer = DynamicTableWriter({
        "Symbol":         dht.string,
        "Price":          dht.double,
        "Position":       dht.int64,
        "MarketValue":    dht.double,
        "UnrealizedPnL":  dht.double,
        "Delta":          dht.double,
        "Gamma":          dht.double,
        "Theta":          dht.double,
        "Vega":           dht.double,
    })
    risk_raw = risk_writer.table

    # ── Derived tables ────────────────────────────────────────────────────
    prices_live = prices_raw.last_by("Symbol")
    risk_live = risk_raw.last_by("Symbol")

    portfolio_summary = risk_live.agg_by([
        agg.sum_(["TotalMV=MarketValue", "TotalPnL=UnrealizedPnL", "TotalDelta=Delta"]),
        agg.avg_(["AvgGamma=Gamma", "AvgTheta=Theta", "AvgVega=Vega"]),
        agg.count_("NumPositions"),
    ])

    top_movers = prices_live.sort_descending("ChangePct")
    volume_leaders = prices_live.sort_descending("Volume")

    # ── Publish to DH global scope (visible to all clients) ──────────────
    from deephaven import execution_context
    import jpy

    _globals = jpy.get_type("io.deephaven.engine.util.ScriptSession").DATA_VARIABLE_NAME
    ctx = execution_context.get_exec_ctx()
    query_scope = ctx.j_exec_ctx.getQueryScope()
    for name, table in [
        ("prices_raw", prices_raw), ("prices_live", prices_live),
        ("risk_raw", risk_raw), ("risk_live", risk_live),
        ("portfolio_summary", portfolio_summary),
        ("top_movers", top_movers), ("volume_leaders", volume_leaders),
    ]:
        query_scope.putParam(name, table)

    # ── WS consumer: feed ticks from MarketDataServer → writers ──────────
    import random

    # Simulated position sizes per symbol
    _positions = {}

    def _on_tick(tick: dict):
        sym = tick["symbol"]
        price_writer.write_row(
            sym, tick["price"], tick["bid"], tick["ask"],
            tick["volume"], tick["change"], tick["change_pct"],
        )
        # Derive risk from each tick
        if sym not in _positions:
            _positions[sym] = random.randint(100, 1000)
        pos = _positions[sym]
        risk_writer.write_row(
            sym, tick["price"], pos,
            tick["price"] * pos,              # MarketValue
            tick["change"] * pos,             # UnrealizedPnL
            0.5 + random.random() * 0.3,     # Delta
            0.02 + random.random() * 0.04,   # Gamma
            -0.1 - random.random() * 0.15,   # Theta
            0.2 + random.random() * 0.2,     # Vega
        )

    async def _consume(url: str):
        import websockets

        while not _feed_stop.is_set():
            try:
                _log.info("Connecting to Market Data Server at %s ...", url)
                async with websockets.connect(url) as ws:
                    await ws.send(json.dumps({"types": ["equity"]}))
                    _log.info("Connected — streaming equity ticks")
                    async for msg in ws:
                        if _feed_stop.is_set():
                            return
                        tick = json.loads(msg)
                        if tick.get("type") == "equity":
                            _on_tick(tick)
            except Exception as e:
                if _feed_stop.is_set():
                    return
                _log.warning("Market Data connection lost (%s). Retrying in 2s...", e)
                await asyncio.sleep(2)

    _feed_stop.clear()
    _feed_thread = threading.Thread(
        target=lambda: asyncio.run(_consume(md_ws_url)),
        daemon=True, name="md-consumer",
    )
    _feed_thread.start()

    _log.info("Trading tables published — 7 tables available to all clients")

    return {
        "prices_raw": prices_raw, "prices_live": prices_live,
        "risk_raw": risk_raw, "risk_live": risk_live,
        "portfolio_summary": portfolio_summary,
        "top_movers": top_movers, "volume_leaders": volume_leaders,
    }


def stop_feed():
    """Stop the WS consumer thread."""
    global _feed_thread
    _feed_stop.set()
    if _feed_thread and _feed_thread.is_alive():
        _feed_thread.join(timeout=5)
    _feed_thread = None


# ── Standalone mode ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import time

    print("\n── Platform: starting infrastructure ──")

    # 1. Start streaming (Deephaven JVM)
    from streaming.admin import StreamingServer

    streaming = StreamingServer(port=10000)
    streaming.start()
    print(f"  Streaming server started on port {streaming.port}")
    print(f"  Web IDE: http://localhost:{streaming.port}")

    # 2. Start market data server (with QuestDB + simulator)
    from marketdata.admin import MarketDataServer

    md = MarketDataServer(port=8000)
    asyncio.run(md.start())
    print(f"  Market data server started on port {md.port}")

    # 3. Publish tables + start WS feed
    tables = publish_tables(f"ws://localhost:{md.port}/md/subscribe")

    print()
    print("=" * 60)
    print("  Trading Server is RUNNING")
    print()
    print("  Published tables:")
    for name in tables:
        print(f"    • {name}")
    print()
    print("  Tables populate as Market Data Server feeds ticks.")
    print("=" * 60)
    print()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        stop_feed()
        asyncio.run(md.stop())
        streaming.stop()
