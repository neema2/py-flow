#!/usr/bin/env python3
"""
Interactive Demo: Deephaven ↔ Store Bridge (Market Data Edition)
================================================================

This script acts as "Process 4" — a user process that:
1. Starts an embedded PostgreSQL (store server)
2. Starts an in-process Deephaven server (web UI at http://localhost:10000)
3. Wires a StoreBridge to stream Order/Trade events into DH ticking tables
4. Consumes live equity prices from the Market Data Server (port 8000)
5. Uses live prices for Order/Trade generation + reactive Position graph

Prerequisites:
  1. Start Market Data Server:  python -m marketdata.server
  2. Run this demo:             python3 demo_bridge.py
  3. Open http://localhost:10000 in your browser

Usage:  python3 demo_bridge.py
"""

import os
import sys
import time
import json
import asyncio
import logging
import random
import tempfile
import threading

sys.path.insert(0, os.path.dirname(__file__))

# ── 1. Start streaming server (must happen before DH imports) ──────────
print("=" * 64)
print("  Starting streaming server...")
from streaming.admin import StreamingServer

streaming = StreamingServer(port=10000)
streaming.start()
streaming.register_alias("demo")
print(f"  Streaming server started on {streaming.url}")

# Now safe to import DH modules
from deephaven.execution_context import get_exec_ctx

# ── 2. Start store server ───────────────────────────────────────────
print("  Starting store server...")
from store.admin import StoreServer
from store import connect, Storable
from dataclasses import dataclass

tmp_dir = tempfile.mkdtemp(prefix="demo_bridge_")
store = StoreServer(data_dir=tmp_dir, admin_password="demo_pw")
store.start()
store.register_alias("demo")
store.provision_user("demo_user", "demo_pw")
conn_info = store.conn_info()
print(f"  Store server started at {conn_info['host']}:{conn_info['port']}")

# ── 3. Define domain models ──────────────────────────────────────────────

@dataclass
class Order(Storable):
    symbol: str = ""
    quantity: int = 0
    price: float = 0.0
    side: str = ""

@dataclass
class Trade(Storable):
    symbol: str = ""
    quantity: int = 0
    price: float = 0.0
    side: str = ""
    pnl: float = 0.0

# ── 4. Wire the StoreBridge ──────────────────────────────────────────────
print("  Wiring StoreBridge...")
from bridge import StoreBridge

bridge = StoreBridge(
    "demo", user="demo_user", password="demo_pw",
    subscriber_id="demo_bridge",
)
bridge.register(Order)
bridge.register(Trade)
bridge.start()

# Get the raw + live DH tables
orders_raw  = bridge.table(Order)
trades_raw  = bridge.table(Trade)
orders_live = orders_raw.last_by("EntityId")
trades_live = trades_raw.last_by("EntityId")

# Create derived DH tables (these tick automatically!)
from deephaven import agg
portfolio = trades_live.agg_by(
    [
        agg.sum_(["TotalPnL=pnl", "TotalQty=quantity"]),
        agg.count_("NumTrades"),
    ]
)

# ── 5. In-memory @computed positions → DH direct push (NO persistence) ───
print("  Wiring @computed positions → DH (in-memory, no store)...")
from reactive.computed import computed, effect as reactive_effect
from store.dh import dh_table, get_dh_tables

@dh_table
@dataclass
class Position(Storable):
    __key__ = "symbol"

    symbol: str = ""
    price: float = 0.0
    quantity: int = 0

    @computed
    def market_value(self):
        return self.price * self.quantity

    @computed
    def risk_score(self):
        return self.price * self.quantity * 0.02

    @reactive_effect("market_value")
    def on_market_value(self, value):
        """Push computed values directly to DH writer on recomputation."""
        self.dh_write()

risk_totals = Position._dh_live.agg_by(
    [
        agg.sum_(["TotalMV=market_value", "TotalRisk=risk_score"]),
        agg.count_("NumPositions"),
    ]
)

# Track positions by symbol — plain dict, no graph needed
tracked_positions = {}  # symbol → Position

def ensure_tracked(symbol, price, quantity):
    """Create or update a position. No store writes."""
    if symbol not in tracked_positions:
        pos = Position(symbol=symbol, price=price, quantity=quantity)
        tracked_positions[symbol] = pos
    else:
        pos = tracked_positions[symbol]
        pos.batch_update(price=price, quantity=quantity)

# Publish all tables to DH global scope (visible in web UI)
pos_tables = get_dh_tables()
for name, tbl in {
    "orders_raw": orders_raw,
    "orders_live": orders_live,
    "trades_raw": trades_raw,
    "trades_live": trades_live,
    "portfolio": portfolio,
    "risk_totals": risk_totals,
    **pos_tables,
}.items():
    globals()[name] = tbl

print()
print("=" * 64)
print("  DEMO READY!")
print("  Web UI:  http://localhost:10000")
print()
print("  Published tables (open in DH web IDE):")
print("    orders_raw    — every order event (append-only)")
print("    orders_live   — latest state per order (ticking)")
print("    trades_raw    — every trade event (append-only)")
print("    trades_live   — latest state per trade (ticking)")
print("    portfolio     — aggregated P&L + quantity (ticking)")
print()
print("  In-memory @computed → DH (Pattern 3, NO persistence):")
print("    position_raw  — every risk calc pushed by @effect")
print("    position_live — latest risk per symbol (ticking)")
print("    risk_totals   — total market value + risk (ticking)")
print()
print("  Writing random orders + trades every 2 seconds...")
print("  Press Ctrl+C to stop.")
print("=" * 64)
print()

# ── 5. Market Data Server consumer ────────────────────────────────────────────
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA"]

# Live prices from market data server (updated by WS consumer thread)
_latest_prices: dict[str, float] = {
    "AAPL": 228.0, "GOOGL": 192.0, "MSFT": 415.0,
    "AMZN": 225.0, "TSLA": 355.0, "NVDA": 138.0,
}

MD_SERVER_URL = "ws://localhost:8000/md/subscribe"
RECONNECT_DELAY = 2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
_log = logging.getLogger("bridge-md-consumer")


async def _consume_equity_prices():
    """Connect to market data server, consume equity ticks, update _latest_prices."""
    import websockets

    while True:
        try:
            _log.info("Connecting to Market Data Server at %s ...", MD_SERVER_URL)
            async with websockets.connect(MD_SERVER_URL) as ws:
                # Subscribe to equity ticks only, for our symbols
                await ws.send(json.dumps({"types": ["equity"], "symbols": SYMBOLS}))
                _log.info("Connected — streaming live equity prices")

                async for msg_str in ws:
                    tick = json.loads(msg_str)
                    if tick.get("type") == "equity" and tick["symbol"] in _latest_prices:
                        _latest_prices[tick["symbol"]] = tick["price"]

        except Exception as e:
            _log.warning(
                "Market Data connection lost (%s). Retrying in %ds...",
                e, RECONNECT_DELAY,
            )
            await asyncio.sleep(RECONNECT_DELAY)


def _start_md_consumer():
    asyncio.run(_consume_equity_prices())


_md_thread = threading.Thread(
    target=_start_md_consumer, daemon=True, name="bridge-md-consumer",
)
_md_thread.start()

# ── 6. Write objects in a loop using live prices ─────────────────────────────

db = connect("demo", user="demo_user", password="demo_pw")

try:
    tick = 0
    while True:
        tick += 1
        sym = random.choice(SYMBOLS)
        qty = random.randint(10, 500)
        price = _latest_prices[sym]  # live price from market data server
        side = random.choice(["BUY", "SELL"])

        # Write an Order to the STORE (goes through bridge → DH)
        order = Order(symbol=sym, quantity=qty, price=round(price, 2), side=side)
        order.save()

        # Write a corresponding Trade to the STORE
        pnl = round(random.gauss(0, qty * 0.5), 2)
        trade = Trade(symbol=sym, quantity=qty, price=round(price, 2), side=side, pnl=pnl)
        trade.save()

        # Update the IN-MEMORY reactive graph (NO store write!)
        # This triggers: graph recomputes market_value + risk → effect pushes to DH
        ensure_tracked(sym, round(price, 2), qty)

        # Sometimes update an existing order (simulates state change)
        if tick % 3 == 0:
            order.price = round(price * 1.001, 2)
            order.save()

        print(f"  [{tick}] {side} {qty} {sym} @ ${price:.2f}  (pnl: ${pnl:+.2f})  [graph: mv={qty*price:.0f}]")

        # Flush DH update graph so tables tick
        get_exec_ctx().update_graph.j_update_graph.requestRefresh()

        time.sleep(2)

except KeyboardInterrupt:
    print("\n  Shutting down...")
    bridge.stop()
    db.close()
    store.stop()
    print("  Done!")
