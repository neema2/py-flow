#!/usr/bin/env python3
"""
Demo: Interest Rate Swap — Fully Reactive Grid via Market Data Hub
===================================================================

Consumes FX ticks from the Market Data Server (port 8000).  Every
computation AND every Deephaven push is expressed as @computed or
@effect — the WS consumer loop is a single batch_update() call.

The reactive chain (triggered by each FX tick):
  FXSpot.batch_update(bid, ask)
    → @computed mid
    → @effect on_mid          → fx_writer.write_row(...)
    → @computed YieldCurvePoint.rate   (cross-entity: reads fx_ref.mid)
      → @computed discount_factor
      → @effect on_rate        → curve_writer.write_row(...) + queue CurveTick
    → @computed InterestRateSwap.float_rate  (cross-entity: reads curve_ref.rate)
      → @computed npv, dv01, pnl_status
      → @effect on_npv         → swap_writer.write_row(...)
    → @computed SwapPortfolio.total_npv      (cross-entity: reads swaps[].npv)
      → @effect on_total_npv   → portfolio_writer.write_row(...)

Prerequisites:
  1. Start Market Data Server:  python -m marketdata.server
  2. Run this demo:             python3 demo_irs.py
  3. Open http://localhost:10000 in your browser

Usage:  python3 demo_irs.py
"""

import os
import sys
import json
import asyncio
import logging
import threading
from collections import deque
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))

# ── 1. Start streaming server ──────────────────────────────────────────────
print("=" * 70)
print("  Interest Rate Swap — Reactive Ticking Demo")
print("=" * 70)
print()
print("  Starting streaming server...")

from streaming.admin import StreamingServer

streaming = StreamingServer(port=10000)
streaming.start()
streaming.register_alias("demo")
print(f"  Streaming server started on {streaming.url}")

from deephaven.execution_context import get_exec_ctx
from deephaven import agg

# ── 2. Domain models — fully reactive (@computed + @effect) ──────────────
from dataclasses import dataclass, field
from store.base import Storable
from store.dh import dh_table, get_dh_tables
from reactive.computed import computed, effect

# Publish queue: @effect on YieldCurvePoint.rate enqueues CurveTicks here;
# the WS consumer drains them and sends back to the market data hub.
_curve_publish_queue: deque = deque()


@dh_table
@dataclass
class FXSpot(Storable):
    """Live FX spot rate.  @effect pushes every mid change to DH."""
    __key__ = "pair"

    pair: str = ""
    bid: float = 0.0
    ask: float = 0.0
    currency: str = ""

    @computed
    def mid(self):
        return (self.bid + self.ask) / 2

    @computed
    def spread_pips(self):
        return (self.ask - self.bid) * 10000

    @effect("mid")
    def on_mid(self, value):
        self.dh_write()


@dh_table(exclude={"base_rate", "sensitivity", "fx_base_mid"})
@dataclass
class YieldCurvePoint(Storable):
    """Single curve point.  rate is @computed from fx_ref.mid (cross-entity)."""
    __key__ = "label"

    label: str = ""
    tenor_years: float = 0.0
    base_rate: float = 0.0
    sensitivity: float = 0.5
    currency: str = "USD"
    fx_ref: object = None
    fx_base_mid: float = 0.0

    @computed
    def rate(self):
        if self.fx_ref is None:
            return self.base_rate
        fx_base = self.fx_base_mid
        if fx_base == 0.0:
            return self.base_rate
        pct_move = (self.fx_ref.mid - fx_base) / fx_base
        return max(0.0001, self.base_rate + self.sensitivity * pct_move)

    @computed
    def discount_factor(self):
        return 1.0 / (1.0 + self.rate) ** self.tenor_years

    @effect("rate")
    def on_rate(self, value):
        self.dh_write()
        _curve_publish_queue.append({
            "type": "curve",
            "label": self.label,
            "tenor_years": self.tenor_years,
            "rate": self.rate,
            "discount_factor": self.discount_factor,
            "currency": self.currency,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })


@dh_table
@dataclass
class InterestRateSwap(Storable):
    """IRS pricing.  float_rate is @computed from curve_ref.rate (cross-entity)."""
    __key__ = "symbol"

    symbol: str = ""
    notional: float = 0.0
    fixed_rate: float = 0.0
    tenor_years: float = 0.0
    currency: str = "USD"
    curve_ref: object = None

    @computed
    def float_rate(self):
        if self.curve_ref is None:
            return 0.0
        return self.curve_ref.rate

    @computed
    def fixed_leg_pv(self):
        df = 1.0 / (1.0 + self.fixed_rate) ** self.tenor_years
        return self.notional * self.fixed_rate * self.tenor_years * df

    @computed
    def float_leg_pv(self):
        df = 1.0 / (1.0 + self.float_rate) ** self.tenor_years
        return self.notional * self.float_rate * self.tenor_years * df

    @computed
    def npv(self):
        float_df = 1.0 / (1.0 + self.float_rate) ** self.tenor_years
        fixed_df = 1.0 / (1.0 + self.fixed_rate) ** self.tenor_years
        float_pv = self.notional * self.float_rate * self.tenor_years * float_df
        fixed_pv = self.notional * self.fixed_rate * self.tenor_years * fixed_df
        return float_pv - fixed_pv

    @computed
    def dv01(self):
        return self.notional * self.tenor_years * 0.0001

    @computed
    def pnl_status(self) -> str:
        float_df = 1.0 / (1.0 + self.float_rate) ** self.tenor_years
        fixed_df = 1.0 / (1.0 + self.fixed_rate) ** self.tenor_years
        float_pv = self.notional * self.float_rate * self.tenor_years * float_df
        fixed_pv = self.notional * self.fixed_rate * self.tenor_years * fixed_df
        npv = float_pv - fixed_pv
        if npv > 0:
            return "PROFIT"
        if npv < 0:
            return "LOSS"
        return "FLAT"

    @effect("npv")
    def on_npv(self, value):
        self.dh_write()


@dh_table
@dataclass
class SwapPortfolio(Storable):
    """Aggregate portfolio.  @computed reads child swap NPVs (cross-entity)."""
    __key__ = "name"

    name: str = ""
    swaps: list = field(default_factory=list)

    @computed
    def total_npv(self):
        return sum(s.npv for s in self.swaps) if self.swaps else 0.0

    @computed
    def total_dv01(self):
        return sum(s.dv01 for s in self.swaps) if self.swaps else 0.0

    @computed
    def max_npv(self):
        return max(s.npv for s in self.swaps) if self.swaps else 0.0

    @computed
    def min_npv(self):
        return min(s.npv for s in self.swaps) if self.swaps else 0.0

    @computed
    def swap_count(self) -> int:
        return len(self.swaps) if self.swaps else 0

    @effect("total_npv")
    def on_total_npv(self, value):
        self.dh_write()


# ── 3. Publish @dh_table tables to DH global scope ───────────────────────
tables = get_dh_tables()

# Aggregates
swap_summary = InterestRateSwap._dh_live.agg_by([
    agg.sum_(["TotalNPV=npv", "TotalDV01=dv01"]),
    agg.count_("NumSwaps"),
    agg.avg(["AvgNPV=npv"]),
])
tables["swap_summary"] = swap_summary

for name, tbl in tables.items():
    globals()[name] = tbl


# ── 4. Build reactive objects — cross-entity refs wired at construction ───
print("  Building reactive objects...")

# FX spots (initial values match market data server's FX_BASE)
fx_spots = {
    "USD/JPY": FXSpot(pair="USD/JPY", bid=149.50, ask=149.60, currency="JPY"),
    "EUR/USD": FXSpot(pair="EUR/USD", bid=1.0850, ask=1.0855, currency="USD"),
    "GBP/USD": FXSpot(pair="GBP/USD", bid=1.2700, ask=1.2710, currency="USD"),
}

# Benchmark FX refs: which FX pair drives each currency's curve
_usd_fx = fx_spots["EUR/USD"]
_jpy_fx = fx_spots["USD/JPY"]

# Yield curve points (USD) — rate is @computed from fx_ref.mid
usd_curve_data = [
    ("USD_3M",  0.25, 0.0525),
    ("USD_1Y",  1.0,  0.0490),
    ("USD_2Y",  2.0,  0.0445),
    ("USD_5Y",  5.0,  0.0410),
    ("USD_10Y", 10.0, 0.0395),
    ("USD_30Y", 30.0, 0.0420),
]
usd_curve_points = {}
for label, tenor, base_rate in usd_curve_data:
    usd_curve_points[label] = YieldCurvePoint(
        label=label, tenor_years=tenor, base_rate=base_rate,
        sensitivity=0.5, currency="USD",
        fx_ref=_usd_fx, fx_base_mid=_usd_fx.mid,
    )

# JPY curve points — driven by USD/JPY
jpy_curve_data = [
    ("JPY_1Y",  1.0,  0.001),
    ("JPY_5Y",  5.0,  0.005),
    ("JPY_10Y", 10.0, 0.008),
]
jpy_curve_points = {}
for label, tenor, base_rate in jpy_curve_data:
    jpy_curve_points[label] = YieldCurvePoint(
        label=label, tenor_years=tenor, base_rate=base_rate,
        sensitivity=0.5, currency="JPY",
        fx_ref=_jpy_fx, fx_base_mid=_jpy_fx.mid,
    )

all_curve_points = {**usd_curve_points, **jpy_curve_points}

# Interest rate swaps — float_rate is @computed from curve_ref.rate
# Each swap references its tenor-matched curve point
_swap_curve_map = {
    "USD-5Y-A": "USD_5Y",
    "USD-5Y-B": "USD_5Y",
    "USD-10Y":  "USD_10Y",
    "USD-2Y":   "USD_2Y",
    "JPY-5Y":   "JPY_5Y",
    "JPY-10Y":  "JPY_10Y",
}

swap_configs = [
    ("USD-5Y-A", 50_000_000,  0.0400, 5.0,  "USD"),
    ("USD-5Y-B", 25_000_000,  0.0380, 5.0,  "USD"),
    ("USD-10Y",  100_000_000, 0.0395, 10.0, "USD"),
    ("USD-2Y",   75_000_000,  0.0450, 2.0,  "USD"),
    ("JPY-5Y",   5_000_000_000, 0.005, 5.0,  "JPY"),
    ("JPY-10Y",  10_000_000_000, 0.008, 10.0, "JPY"),
]

swaps = {}
for sym, notl, fixed, tenor, ccy in swap_configs:
    swaps[sym] = InterestRateSwap(
        symbol=sym, notional=notl, fixed_rate=fixed,
        tenor_years=tenor, currency=ccy,
        curve_ref=all_curve_points[_swap_curve_map[sym]],
    )

# Swap portfolios — @computed aggregates react to child swap changes
usd_swaps = [s for s in swaps.values() if s.currency == "USD"]
jpy_swaps = [s for s in swaps.values() if s.currency == "JPY"]

portfolios = {
    "ALL": SwapPortfolio(name="ALL", swaps=list(swaps.values())),
    "USD": SwapPortfolio(name="USD", swaps=usd_swaps),
    "JPY": SwapPortfolio(name="JPY", swaps=jpy_swaps),
}

# All initial DH pushes happened automatically via @effects during construction.
# Just flush the DH update graph once.
get_exec_ctx().update_graph.j_update_graph.requestRefresh()

# Drain initial CurveTick publishes (they'll be sent on first WS connect)
_initial_curve_ticks = list(_curve_publish_queue)
_curve_publish_queue.clear()

print(f"  Built: {len(fx_spots)} FX spots, {len(all_curve_points)} curve points, "
      f"{len(swaps)} swaps, {len(portfolios)} portfolios")
print("  All initial state pushed to DH via @effect (no manual push needed)")


# ── 5. Market Data Server WebSocket consumer ─────────────────────────────

MD_SERVER_URL = "ws://localhost:8000/md/subscribe"
RECONNECT_DELAY = 2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
_log = logging.getLogger("irs-md-consumer")


async def _consume_and_publish():
    """Connect to the Market Data Server, consume FX ticks, publish curves back.

    Each FX tick triggers the FULL reactive cascade via a single batch_update():
      batch_update(bid, ask)
        → @computed mid          → @effect → fx_writer
        → @computed rate         → @effect → curve_writer + CurveTick queue
        → @computed float_rate   → @computed npv → @effect → swap_writer
        → @computed total_npv    → @effect → portfolio_writer

    No imperative push functions — every DH write is an @effect side-effect.
    """
    import websockets

    tick_count = 0

    while True:
        try:
            _log.info("Connecting to Market Data Server at %s ...", MD_SERVER_URL)
            async with websockets.connect(MD_SERVER_URL) as ws:
                # Subscribe to FX ticks only
                await ws.send(json.dumps({"types": ["fx"]}))
                _log.info("Connected — consuming FX ticks, full reactive cascade active")

                # Publish any initial CurveTicks
                for ct in _initial_curve_ticks:
                    await ws.send(json.dumps(ct))

                async for msg_str in ws:
                    tick = json.loads(msg_str)
                    if tick.get("type") != "fx":
                        continue

                    pair = tick["pair"]
                    fx = fx_spots.get(pair)
                    if fx is None:
                        continue

                    # ── THE ONLY IMPERATIVE CALL ──────────────────────
                    # Everything else is reactive: @computed recalcs +
                    # @effect DH pushes all fire inside batch_update().
                    fx.batch_update(bid=tick["bid"], ask=tick["ask"])

                    # Flush DH update graph (all writes already queued by @effects)
                    get_exec_ctx().update_graph.j_update_graph.requestRefresh()

                    tick_count += 1

                    # Publish derived CurveTicks back to the hub
                    while _curve_publish_queue:
                        await ws.send(json.dumps(_curve_publish_queue.popleft()))

                    # Print summary every 10th tick
                    if tick_count % 10 == 0:
                        usdjpy = fx_spots["USD/JPY"].mid
                        usd_5y = usd_curve_points["USD_5Y"].rate * 100
                        jpy_10y = jpy_curve_points["JPY_10Y"].rate * 100
                        p_all = portfolios["ALL"]

                        print(
                            f"  [{tick_count:4d}] "
                            f"USD/JPY {usdjpy:.2f}  |  "
                            f"USD 5Y: {usd_5y:.2f}%  |  "
                            f"JPY 10Y: {jpy_10y:.3f}%  |  "
                            f"Portfolio NPV: ${p_all.total_npv:+,.0f}"
                        )

        except Exception as e:
            _log.warning(
                "Market Data connection lost (%s). Retrying in %ds...",
                e, RECONNECT_DELAY,
            )
            await asyncio.sleep(RECONNECT_DELAY)


def _start_md_consumer():
    """Run the market data consumer in a background thread with its own loop."""
    asyncio.run(_consume_and_publish())


print()
print("=" * 70)
print("  DEMO READY — Fully Reactive IRS Grid via Market Data Hub")
print("  Web UI:  http://localhost:10000")
print()
print("  Published tables (open in DH web IDE):")
print("    fx_spot_live            — FX spot rates (from market data server)")
print("    yield_curve_point_live  — yield curve points (@computed from FX)")
print("    interest_rate_swap_live — IRS pricing: NPV, DV01, PnL (@computed cascade)")
print("    swap_summary            — aggregate NPV + DV01 (ticking)")
print("    swap_portfolio_live     — portfolio breakdown: ALL / USD / JPY (@computed)")
print()
print("  Raw (append-only) tables: fx_spot_raw, yield_curve_point_raw,")
print("    interest_rate_swap_raw, swap_portfolio_raw")
print()
print("  Reactive chain (all from one batch_update):")
print("    FX bid/ask → @computed mid → @computed rate → @computed float_rate")
print("    → @computed npv → @computed total_npv")
print("    Every @effect fires automatically: DH push + CurveTick publish")
print()
print("  Requires: python -m marketdata.server  (running on port 8000)")
print("  Press Ctrl+C to stop.")
print("=" * 70)
print()

# Start the WS consumer in a daemon thread
_md_thread = threading.Thread(
    target=_start_md_consumer, daemon=True, name="irs-md-consumer",
)
_md_thread.start()

try:
    # Keep main thread alive
    while True:
        import time
        time.sleep(1)
except KeyboardInterrupt:
    print("\n  Shutting down...")
    print("  Done!")
