#!/usr/bin/env python3
"""
Interactive Demo: Deephaven ↔ Store Bridge
==========================================

This script acts as "Process 4" — a user process that:
1. Starts an embedded PostgreSQL (store server)
2. Starts an in-process Deephaven server (web UI at http://localhost:10000)
3. Wires a StoreBridge to stream Order/Trade events into DH ticking tables
4. Writes objects to the store in a loop — watch them appear in the DH web UI

Open http://localhost:10000 in your browser to see the ticking tables!

Usage:  python3 demo_bridge.py
"""

import os
import sys
import time
import random
import tempfile

sys.path.insert(0, os.path.dirname(__file__))

# ── 1. Start Deephaven server (must happen before DH imports) ─────────────
print("=" * 64)
print("  Starting Deephaven server...")
from deephaven_server import Server

dh = Server(
    port=10000,
    jvm_args=[
        "-Xmx1g",
        "-Dprocess.info.system-info.enabled=false",
        "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
    ],
    default_jvm_args=[
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=100",
        "-XX:+UseStringDeduplication",
    ],
)
dh.start()
print("  Deephaven server started on http://localhost:10000")

# Now safe to import DH modules
from deephaven.execution_context import get_exec_ctx

# ── 2. Start embedded PostgreSQL (store server) ──────────────────────────
print("  Starting embedded PostgreSQL...")
from store.server import ObjectStoreServer
from store.schema import provision_user
from store.connection import connect
from store.base import Storable
from dataclasses import dataclass

tmp_dir = tempfile.mkdtemp(prefix="demo_bridge_")
store = ObjectStoreServer(data_dir=tmp_dir, admin_password="demo_pw")
store.start()
conn_info = store.conn_info()
print(f"  PostgreSQL started at {conn_info['host']}:{conn_info['port']}")

# Provision a demo user
admin = store.admin_conn()
provision_user(admin, "demo_user", "demo_pw")
admin.close()

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
    host=conn_info["host"], port=conn_info["port"], dbname=conn_info["dbname"],
    user="demo_user", password="demo_pw",
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
import deephaven.dtypes as dht
from deephaven import DynamicTableWriter

# DynamicTableWriter for computed risk values — NOT from the store
risk_writer = DynamicTableWriter({
    "symbol": dht.string,
    "price": dht.double,
    "quantity": dht.int64,
    "market_value": dht.double,
    "risk_score": dht.double,
})
risk_calcs = risk_writer.table
risk_live = risk_calcs.last_by("symbol")
risk_totals = risk_live.agg_by(
    [
        agg.sum_(["TotalMV=market_value", "TotalRisk=risk_score"]),
        agg.count_("NumPositions"),
    ]
)

@dataclass
class Position(Storable):
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
        risk_writer.write_row(
            self.symbol, self.price, self.quantity,
            self.market_value, self.risk_score,
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
for name, tbl in {
    "orders_raw": orders_raw,
    "orders_live": orders_live,
    "trades_raw": trades_raw,
    "trades_live": trades_live,
    "portfolio": portfolio,
    "risk_calcs": risk_calcs,
    "risk_live": risk_live,
    "risk_totals": risk_totals,
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
print("    risk_calcs    — every risk calc pushed by @effect")
print("    risk_live     — latest risk per symbol (ticking)")
print("    risk_totals   — total market value + risk (ticking)")
print()
print("  Writing random orders + trades every 2 seconds...")
print("  Press Ctrl+C to stop.")
print("=" * 64)
print()

# ── 5. Write objects in a loop ────────────────────────────────────────────
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA"]
BASE_PRICES = {"AAPL": 228, "GOOGL": 192, "MSFT": 415, "AMZN": 225, "TSLA": 355, "NVDA": 138}

db = connect(
    host=conn_info["host"], port=conn_info["port"], dbname=conn_info["dbname"],
    user="demo_user", password="demo_pw",
)

try:
    tick = 0
    while True:
        tick += 1
        sym = random.choice(SYMBOLS)
        qty = random.randint(10, 500)
        price = BASE_PRICES[sym] * (1 + random.gauss(0, 0.02))
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
