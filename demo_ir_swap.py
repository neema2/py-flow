#!/usr/bin/env python3
"""
Demo: Interest Rate Swap — Fully Reactive Grid via Market Data Hub
===================================================================

Consumes USD OIS swap rate ticks from the Market Data Server (port 8000).
Every computation AND every Deephaven push is expressed as @computed or
@effect — the WS consumer loop is a single batch_update() call.

Instruments are imported from instruments/:
    instruments/yield_curve.py — YieldCurvePoint, YieldCurve (interpolation)
    instruments/ir_swap.py     — SwapQuote, IRSwapFixedFloatApprox, SwapPortfolio

Usage:
  python3 demo_ir_swap.py
  Open http://localhost:10000 in your browser
"""

import asyncio
import json
import logging
import os
import sys
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

print("  Starting market data server...")
from marketdata.admin import MarketDataServer

_md_server = MarketDataServer(port=8000)
asyncio.run(_md_server.start())
print(f"  Market data server started on port {_md_server.port}")

# ── 2. Import instrument models from instruments/ ─────────────────────────
from streaming import agg, flush, get_tables

from marketmodel.yield_curve import YieldCurvePoint, YieldCurve
from marketmodel.curve_fitter import CurveFitter
from marketmodel.symbols import fit_symbol, tenor_name
from marketmodel.swap_curve import SwapQuote, SwapQuoteRisk
from instruments.ir_swap_fixed_floatapprox import IRSwapFixedFloatApprox, SwapPortfolio

# ── 3. Build reactive objects — cross-entity refs wired at construction ───
print("  Building reactive objects...")

# 1. Market Quotes (Inputs)
swap_quotes = {
    "IR_USD_OIS_QUOTE.1Y":  SwapQuote(symbol="IR_USD_OIS_QUOTE.1Y",  tenor=1.0,  rate=0.01),
    "IR_USD_OIS_QUOTE.5Y":  SwapQuote(symbol="IR_USD_OIS_QUOTE.5Y",  tenor=5.0,  rate=0.05),
    "IR_USD_OIS_QUOTE.10Y": SwapQuote(symbol="IR_USD_OIS_QUOTE.10Y", tenor=10.0, rate=0.10),
    "IR_USD_OIS_QUOTE.20Y": SwapQuote(symbol="IR_USD_OIS_QUOTE.20Y", tenor=20.0, rate=0.20),
}

# 2. Curve Pillars (Fitted Outputs)
curve_points = {}
for q_sym, q in swap_quotes.items():
    t_label = tenor_name(q.tenor)
    label = fit_symbol("USD", t_label)
    curve_points[label] = YieldCurvePoint(
        label=label, symbol=label, tenor_years=q.tenor,
        currency="USD", quote_ref=q, is_fitted=True,
    )

# 3. YieldCurve entity
usd_curve = YieldCurve(
    name="USD_OIS", currency="USD",
    points=list(curve_points.values())
)

# 4. Target Swaps for the Fitter
# To fit precisely, the solver targets a swap at each pillar tenor
target_swaps = []
for q in swap_quotes.values():
    target_swaps.append(IRSwapFixedFloatApprox(
        symbol=q.symbol, notional=50_000_000,
        fixed_rate=q.rate, tenor_years=q.tenor,
        curve=usd_curve, is_target=True,  # Hidden from live board
    ))

# 5. Global Fitter — the writer that publishes fitted rates
fitter = CurveFitter(
    name="USD_OIS_FITTER", currency="USD",
    target_swaps=target_swaps,
    quotes=list(swap_quotes.values()),
    curve=usd_curve,
    points=list(curve_points.values())
)
# Initial solve
print("  Solving initial curve...")
fitter.solve()
print("  Curve solved.")


# 6. Interest Rate Swaps (Portfolio)
# Slimming to one 7Y swap as requested to test interpolation risk split
swaps = {
    "USD-7Y": IRSwapFixedFloatApprox(
        symbol="USD-7Y", notional=100_000_000, fixed_rate=0.07,
        tenor_years=7.0, curve=usd_curve,
    )
}

# Swap portfolio — @computed aggregates react to child swap changes
portfolio_name = "USD_OIS_MGMT"
portfolio = SwapPortfolio(name=portfolio_name, swaps=list(swaps.values()))


# ── 4. Publish @ticking tables to DH global scope ────────────────────────
tables = get_tables()

# Aggregates
swap_summary = IRSwapFixedFloatApprox._ticking_live.agg_by([  # type: ignore[attr-defined]
    agg.sum(["TotalNPV=npv", "TotalDV01=dv01"]),
    agg.count("NumSwaps"),
    agg.avg(["AvgNPV=npv"]),
], by=[])
tables["swap_summary"] = swap_summary
tables["swap_risk_ladder"] = SwapQuoteRisk._ticking_live # type: ignore[attr-defined]
tables["swap_portfolio_live"] = SwapPortfolio._ticking_live    # type: ignore[attr-defined]
tables["yield_curve_live"] = usd_curve._ticking_live            # type: ignore[attr-defined]

print(f"  Publishing {len(tables)} tables to Deephaven...")
for name, tbl in tables.items():
    tbl.publish(name)
    print(f"    [DH] Published table: {name}")

# All initial DH pushes happened automatically via @effects during construction.
# Just flush the DH update graph once.
flush()

print(f"  Built: {len(swap_quotes)} OIS quotes, {len(curve_points)} curve points, "
      f"1 yield curve, {len(swaps)} swaps, 1 portfolio")
print("  All initial state pushed to DH via @effect (no manual push needed)")


# ── 5. Market Data Server WebSocket consumer ─────────────────────────────

MD_SERVER_URL = "ws://localhost:8000/md/subscribe"
RECONNECT_DELAY = 2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
_log = logging.getLogger("irs-md-consumer")


async def _consume_and_publish():
    """Connect to the Market Data Server, consume OIS swap ticks.

    Each OIS tick triggers the FULL reactive cascade via a single batch_update():
      batch_update(rate=...)
        → @effect on_rate        → DH write
        → @computed curve rate   → @effect → DH write
        → @computed float_rate   → @computed npv → @effect → DH write
        → @computed total_npv    → @effect → DH write

    No imperative push functions — every DH write is an @effect side-effect.
    """
    import websockets

    tick_count = 0

    while True:
        try:
            _log.info("Connecting to Market Data Server at %s ...", MD_SERVER_URL)
            async with websockets.connect(MD_SERVER_URL) as ws:
                # Subscribe to swap ticks only
                await ws.send(json.dumps({"types": ["swap"]}))
                _log.info("Connected — consuming USD OIS swap ticks, full reactive cascade active")

                # Publish initial CurveTicks back to hub
                for label, pt in curve_points.items():
                    ct = {
                        "type": "curve", "symbol": pt.symbol, "label": label,
                        "tenor_years": pt.tenor_years,
                        "rate": pt.rate,
                        "discount_factor": pt.discount_factor,
                        "currency": pt.currency,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                    await ws.send(json.dumps(ct))

                # Publish initial JacobianTicks back to hub
                for entry in usd_curve.jacobian:
                    jt = {
                        "type": "jacobian", "symbol": entry.symbol,
                        "output_tenor": entry.output_tenor,
                        "input_tenor": entry.input_tenor,
                        "value": entry.value,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                    await ws.send(json.dumps(jt))

                async for msg_str in ws:
                    tick = json.loads(msg_str)
                    if tick.get("type") != "swap":
                        continue

                    sq = swap_quotes.get(tick["symbol"])
                    if sq is None:
                        continue

                    # ── THE ONLY IMPERATIVE CALL ──────────────────────
                    # Everything else is reactive: @computed recalcs +
                    # @effect DH pushes all fire inside batch_update().
                    sq.batch_update(rate=tick["rate"])
                    
                    # Trigger the global fitting 
                    fitter.solve()
                    
                    flush()

                    tick_count += 1
                    print(f"  [Tick] Processed {tick['symbol']} #{tick_count}")

                    # Publish derived CurveTicks back to the hub
                    for label, pt in curve_points.items():
                        ct = {
                            "type": "curve", "symbol": pt.symbol, "label": label,
                            "tenor_years": pt.tenor_years,
                            "rate": pt.rate,
                            "discount_factor": pt.discount_factor,
                            "currency": pt.currency,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                        await ws.send(json.dumps(ct))

                    # Publish derived JacobianTicks back to the hub
                    for entry in usd_curve.jacobian:
                        jt = {
                            "type": "jacobian", "symbol": entry.symbol,
                            "output_tenor": entry.output_tenor,
                            "input_tenor": entry.input_tenor,
                            "value": entry.value,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                        await ws.send(json.dumps(jt))

                    # Print summary every tick (was every 5th)
                    if True:
                        # Use the new structured labels for lookup
                        lbl_5y = fit_symbol("USD", "5Y")
                        lbl_10y = fit_symbol("USD", "10Y")
                        
                        usd_5y = curve_points[lbl_5y].rate * 100
                        usd_10y = curve_points[lbl_10y].rate * 100

                        print("\n" + "-"*60)
                        print(f"  SUMMARY TICK #{tick_count} | {tick['symbol']} {tick['rate']:.4%}")
                        print(f"  USD 5Y: {usd_5y:.4f}%  |  USD 10Y: {usd_10y:.4f}%")
                        print("-"*60)
                        
                        try:
                            # Print a snapshot of the portfolio and risk ladder to console
                            port_df = tables["swap_portfolio_live"].snapshot()
                            risk_df = tables["swap_risk_ladder"].snapshot()
                            swap_df = tables["interest_rate_swap_live"].snapshot()
                            
                            print("  [Live Portfolio Snapshot]")
                            print(port_df.to_string(index=False))
                            
                            print("\n  [Live Swap NPV Breakdown]")
                            # Showing legs to confirm they are non-zero
                            cols = ["symbol", "fixed_leg_pv", "float_leg_pv", "npv", "dv01"]
                            print(swap_df[cols].to_string(index=False))

                            print("\n  [Live Risk Ladder Snapshot (Dedicated Table)]")
                            # Only show non-zero risks for brevity
                            if not risk_df.empty:
                                ladder_cols = ["quote", "risk", "equiv_notional"]
                                active_risk = risk_df[risk_df['risk'] != 0].copy()
                                # Format equiv_notional for easier reading
                                active_risk['equiv_notional'] = active_risk['equiv_notional'].apply(lambda x: f"{x/1e6:.1f}M")
                                print(active_risk[ladder_cols].to_string(index=False))
                            else:
                                print("  (empty ladder)")
                            print("-" * 60 + "\n")
                            sys.stdout.flush()
                        except Exception as snapshot_err:
                            print(f"  (Failed to take console snapshot: {snapshot_err})")

        except Exception as e:
            _log.warning(
                "Market Data connection lost (%s). Retrying in %ds...",
                e, RECONNECT_DELAY,
            )
            await asyncio.sleep(RECONNECT_DELAY)


def _start_md_consumer():
    """Run the market data consumer in a background thread with its own loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_consume_and_publish())


print()
print("=" * 70)
print("  DEMO READY — Fully Reactive IRS Grid via USD OIS Market Data")
print("  Web UI:  http://localhost:10000")
print()
print("  Published tables (open in DH web IDE):")
print("    swap_quote_live         — USD OIS par rates (from market data server)")
print("    yield_curve_point_live  — yield curve points (@computed from OIS quotes)")
print("    interest_rate_swap_live — IRS pricing: NPV, DV01, PnL (@computed cascade)")
print("    swap_summary            — aggregate NPV + DV01 (ticking summary)")
print("    swap_risk_ladder        — portfolio risk ladder: ∂Portfolio / ∂Quote")
print("    swap_portfolio_live     — portfolio total NPV (@computed status)")
print()
print("  Reactive chain (all from one batch_update):")
print("    OIS rate → @computed curve rate → @computed float_rate")
print("    → @computed npv → @computed total_npv")
print("    Every @effect fires automatically: DH push")
print()
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
    asyncio.run(_md_server.stop())
    print("  Done!")
