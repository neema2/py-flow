#!/usr/bin/env python3
"""
Demo: Backtest with Live TSDB — Proving Historical Market Data Works
=====================================================================

Connects to the running Market Data Server (which has TSDB enabled),
watches ticks accumulate in real time, then runs a simple moving-average
crossover backtest on the collected OHLCV bars.

This demo proves the full pipeline works end-to-end:
  SimulatorFeed → TickBus → TSDBConsumer → TSDB Backend → REST API → Backtest

Prerequisites:
  1. Install:    pip install -e ".[timeseries,marketdata]"
  2. Start server (pick one):
       TSDB_BACKEND=memory python -m marketdata.server       # in-memory, zero deps
       python -m marketdata.server                           # QuestDB (needs Java)
  3. Run:        python3 demo_backtest.py

Usage:  python3 demo_backtest.py [--collect 30] [--symbol AAPL] [--fast 3] [--slow 10]
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

# ── Config ────────────────────────────────────────────────────────────────────

MD_BASE = "http://localhost:8000"
POLL_INTERVAL = 3  # seconds between progress checks


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get(path: str, params: Optional[dict] = None) -> dict | list:
    """GET from market data server, raise on failure."""
    resp = httpx.get(f"{MD_BASE}{path}", params=params or {}, timeout=5)
    resp.raise_for_status()
    return resp.json()


def _check_server() -> bool:
    """Return True if market data server is reachable."""
    try:
        data = _get("/md/health")
        return data.get("status") == "ok"
    except Exception:
        return False


def _check_tsdb() -> bool:
    """Return True if TSDB endpoints are available (not 503)."""
    try:
        resp = httpx.get(f"{MD_BASE}/md/latest/equity", timeout=5)
        return resp.status_code != 503
    except Exception:
        return False


def _get_tick_counts() -> dict[str, int]:
    """Get approximate tick counts per type from TSDB."""
    counts = {}
    for msg_type in ("equity", "fx", "curve"):
        try:
            rows = _get(f"/md/latest/{msg_type}")
            counts[msg_type] = len(rows)
        except Exception:
            counts[msg_type] = 0
    return counts


def _get_history_count(msg_type: str, symbol: str) -> int:
    """Get number of stored ticks for a symbol."""
    try:
        rows = _get(f"/md/history/{msg_type}/{symbol}", {"limit": 10000})
        return len(rows)
    except Exception:
        return 0


# ── Phase 1: Data Collection ─────────────────────────────────────────────────

def collect_data(collect_secs: int, symbol: str) -> int:
    """Watch ticks accumulate in TSDB, printing live progress."""
    print(f"\n{'─' * 70}")
    print(f"  PHASE 1: Collecting live data ({collect_secs}s)")
    print(f"{'─' * 70}\n")

    start = time.monotonic()
    initial_count = _get_history_count("equity", symbol)
    last_count = initial_count

    while (elapsed := time.monotonic() - start) < collect_secs:
        remaining = int(collect_secs - elapsed)
        count = _get_history_count("equity", symbol)
        new_ticks = count - last_count
        total_new = count - initial_count

        # Also get FX and curve counts for a fuller picture
        fx_count = _get_history_count("fx", "EUR/USD")
        curve_count = _get_history_count("curve", "USD_5Y") if _tsdb_has_curves() else 0

        bar = "█" * min(50, total_new // 2) + "░" * max(0, 50 - total_new // 2)
        print(
            f"  [{remaining:3d}s] {symbol}: {count:,} ticks (+{new_ticks})  "
            f"| FX: {fx_count:,}  | Curves: {curve_count:,}  "
            f"[{bar}]",
            end="\r",
        )

        last_count = count
        time.sleep(POLL_INTERVAL)

    final_count = _get_history_count("equity", symbol)
    total_collected = final_count - initial_count
    print(f"\n\n  ✓ Collected {total_collected:,} new {symbol} ticks "
          f"(total in TSDB: {final_count:,})")
    return final_count


def _tsdb_has_curves() -> bool:
    """Check if curve_ticks table has data (only if IRS demo is running)."""
    try:
        rows = _get("/md/latest/curve")
        return len(rows) > 0
    except Exception:
        return False


# ── Phase 2: Query Bars ──────────────────────────────────────────────────────

def query_bars(symbol: str, interval: str = "5s") -> list[dict]:
    """Fetch OHLCV bars from the TSDB."""
    print(f"\n{'─' * 70}")
    print(f"  PHASE 2: Querying {interval} bars for {symbol}")
    print(f"{'─' * 70}\n")

    bars = _get(f"/md/bars/equity/{symbol}", {"interval": interval})
    print(f"  ✓ Got {len(bars)} bars at {interval} interval\n")

    if bars:
        # Show first and last few bars
        print(f"  {'Time':>23}  {'Open':>9}  {'High':>9}  {'Low':>9}  "
              f"{'Close':>9}  {'Volume':>8}  {'Trades':>7}")
        print(f"  {'─' * 23}  {'─' * 9}  {'─' * 9}  {'─' * 9}  "
              f"{'─' * 9}  {'─' * 8}  {'─' * 7}")

        display_bars = bars[:5] + ([{"sep": True}] if len(bars) > 10 else []) + bars[-5:]
        for bar in display_bars:
            if bar.get("sep"):
                print(f"  {'...':>23}")
                continue
            ts = bar["timestamp"][:23] if isinstance(bar["timestamp"], str) else str(bar["timestamp"])[:23]
            print(
                f"  {ts:>23}  {bar['open']:>9.2f}  {bar['high']:>9.2f}  "
                f"{bar['low']:>9.2f}  {bar['close']:>9.2f}  "
                f"{bar.get('volume', 0) or 0:>8,}  {bar.get('trade_count', 0):>7,}"
            )

    return bars


# ── Phase 3: Backtest — Moving Average Crossover ─────────────────────────────

def backtest_ma_crossover(
    bars: list[dict],
    symbol: str,
    fast_period: int = 5,
    slow_period: int = 20,
) -> dict:
    """Simple moving average crossover backtest.

    Rules:
      - BUY when fast MA crosses above slow MA
      - SELL when fast MA crosses below slow MA
      - One position at a time, 100 shares per trade
    """
    print(f"\n{'─' * 70}")
    print(f"  PHASE 3: Backtest — {fast_period}/{slow_period} MA Crossover on {symbol}")
    print(f"{'─' * 70}\n")

    if len(bars) < slow_period + 1:
        print(f"  ⚠ Need at least {slow_period + 1} bars, got {len(bars)}. "
              f"Try collecting more data or using a smaller interval.")
        return {"trades": [], "total_pnl": 0, "win_rate": 0}

    # Extract close prices
    closes = [b["close"] for b in bars]

    # Calculate MAs
    fast_ma = []
    slow_ma = []
    for i in range(len(closes)):
        if i >= fast_period - 1:
            fast_ma.append(sum(closes[i - fast_period + 1:i + 1]) / fast_period)
        else:
            fast_ma.append(None)
        if i >= slow_period - 1:
            slow_ma.append(sum(closes[i - slow_period + 1:i + 1]) / slow_period)
        else:
            slow_ma.append(None)

    # Generate signals
    trades = []
    position = None  # None = flat, "long" = holding
    share_size = 100

    for i in range(1, len(closes)):
        if fast_ma[i] is None or slow_ma[i] is None:
            continue
        if fast_ma[i - 1] is None or slow_ma[i - 1] is None:
            continue

        prev_above = fast_ma[i - 1] > slow_ma[i - 1]
        curr_above = fast_ma[i] > slow_ma[i]

        if not prev_above and curr_above and position is None:
            # BUY signal
            position = {"side": "BUY", "entry_price": closes[i], "entry_bar": i}

        elif prev_above and not curr_above and position is not None:
            # SELL signal (close long)
            pnl = (closes[i] - position["entry_price"]) * share_size
            trades.append({
                "entry_bar": position["entry_bar"],
                "exit_bar": i,
                "side": "LONG",
                "entry": position["entry_price"],
                "exit": closes[i],
                "pnl": pnl,
            })
            position = None

    # Close any open position at the last bar
    if position is not None:
        pnl = (closes[-1] - position["entry_price"]) * share_size
        trades.append({
            "entry_bar": position["entry_bar"],
            "exit_bar": len(closes) - 1,
            "side": "LONG (open)",
            "entry": position["entry_price"],
            "exit": closes[-1],
            "pnl": pnl,
        })

    # Results
    total_pnl = sum(t["pnl"] for t in trades)
    winners = [t for t in trades if t["pnl"] > 0]
    losers = [t for t in trades if t["pnl"] <= 0]
    win_rate = len(winners) / len(trades) * 100 if trades else 0

    # Print trade log
    if trades:
        print(f"  {'#':>3}  {'Side':<12}  {'Entry':>9}  {'Exit':>9}  {'P&L':>12}")
        print(f"  {'─' * 3}  {'─' * 12}  {'─' * 9}  {'─' * 9}  {'─' * 12}")
        for i, t in enumerate(trades, 1):
            pnl_str = f"${t['pnl']:>+,.2f}"
            marker = "✓" if t["pnl"] > 0 else "✗"
            print(
                f"  {i:>3}  {t['side']:<12}  {t['entry']:>9.2f}  "
                f"{t['exit']:>9.2f}  {pnl_str:>12}  {marker}"
            )
    else:
        print("  No trades generated (price may be trending without crossovers).")

    result = {
        "trades": trades,
        "total_pnl": total_pnl,
        "win_rate": win_rate,
        "num_trades": len(trades),
        "winners": len(winners),
        "losers": len(losers),
        "bars_used": len(bars),
        "fast_period": fast_period,
        "slow_period": slow_period,
    }
    return result


# ── Phase 4: Summary ──────────────────────────────────────────────────────────

def print_summary(result: dict, symbol: str, interval: str, total_ticks: int):
    """Print the final backtest summary."""
    print(f"\n{'═' * 70}")
    print(f"  BACKTEST RESULTS — {symbol} {result['fast_period']}/{result['slow_period']} MA Crossover")
    print(f"{'═' * 70}\n")

    print(f"  Data Source:     TSDB (via Market Data Server REST API)")
    print(f"  Symbol:          {symbol}")
    print(f"  Bar Interval:    {interval}")
    print(f"  Bars Used:       {result['bars_used']:,}")
    print(f"  Raw Ticks:       {total_ticks:,}")
    print()
    print(f"  Total Trades:    {result['num_trades']}")
    print(f"  Winners:         {result['winners']}")
    print(f"  Losers:          {result['losers']}")
    print(f"  Win Rate:        {result['win_rate']:.1f}%")
    print()

    pnl = result["total_pnl"]
    pnl_color = "\033[92m" if pnl >= 0 else "\033[91m"
    reset = "\033[0m"
    print(f"  Total P&L:       {pnl_color}${pnl:>+,.2f}{reset}")

    print(f"\n{'═' * 70}")
    print(f"  Pipeline: SimulatorFeed → TickBus → TSDBConsumer → TSDB Backend")
    print(f"           → REST /md/bars → MA Crossover Backtest")
    print(f"{'═' * 70}\n")


# ── Also show multi-asset TSDB snapshot ───────────────────────────────────────

def show_tsdb_snapshot():
    """Show a summary of all data in the TSDB across asset types."""
    print(f"\n{'─' * 70}")
    print(f"  TSDB Snapshot — All Asset Types")
    print(f"{'─' * 70}\n")

    for msg_type in ("equity", "fx", "curve"):
        try:
            latest = _get(f"/md/latest/{msg_type}")
            if not latest:
                print(f"  {msg_type.upper():>8}: (empty)")
                continue

            symbols = []
            for row in latest:
                sym = row.get("symbol") or row.get("pair") or row.get("label", "?")
                price = row.get("price") or row.get("mid") or row.get("rate", 0)
                symbols.append((sym, price))

            sym_str = ", ".join(f"{s}={p:.4f}" for s, p in sorted(symbols))
            print(f"  {msg_type.upper():>8}: {len(latest)} symbols — {sym_str}")
        except Exception as e:
            print(f"  {msg_type.upper():>8}: unavailable ({e})")

    print()


# ── FX Bars demo (if IRS demo is also running) ───────────────────────────────

def show_fx_bars_if_available():
    """If FX data exists, show a quick bar sample for EUR/USD."""
    try:
        bars = _get("/md/bars/fx/EUR/USD", {"interval": "5s"})
        if not bars or len(bars) < 3:
            return
        print(f"\n{'─' * 70}")
        print(f"  Bonus: EUR/USD 5s Bars (from TSDB)")
        print(f"{'─' * 70}\n")
        print(f"  {'Time':>23}  {'Open':>9}  {'High':>9}  {'Low':>9}  {'Close':>9}")
        print(f"  {'─' * 23}  {'─' * 9}  {'─' * 9}  {'─' * 9}  {'─' * 9}")
        for bar in bars[-10:]:
            ts = bar["timestamp"][:23] if isinstance(bar["timestamp"], str) else str(bar["timestamp"])[:23]
            print(
                f"  {ts:>23}  {bar['open']:>9.5f}  {bar['high']:>9.5f}  "
                f"{bar['low']:>9.5f}  {bar['close']:>9.5f}"
            )
        print(f"\n  ({len(bars)} total EUR/USD bars in TSDB)")
    except Exception:
        pass


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Backtest demo using live TSDB data from Market Data Server"
    )
    parser.add_argument("--collect", type=int, default=30,
                        help="Seconds to collect data before backtesting (default: 30)")
    parser.add_argument("--symbol", default="AAPL",
                        help="Equity symbol to backtest (default: AAPL)")
    parser.add_argument("--interval", default="5s",
                        help="Bar interval for backtest (default: 5s)")
    parser.add_argument("--fast", type=int, default=3,
                        help="Fast MA period (default: 3)")
    parser.add_argument("--slow", type=int, default=10,
                        help="Slow MA period (default: 10)")
    args = parser.parse_args()

    print()
    print("=" * 70)
    print("  Backtest Demo — Historical Market Data via TSDB")
    print("=" * 70)
    print()
    print("  Pipeline:  SimulatorFeed → TickBus → TSDB → REST /md/bars → Backtest")
    print()

    # Check server
    print("  Checking Market Data Server...", end=" ", flush=True)
    if not _check_server():
        print("FAILED")
        print("\n  ✗ Market Data Server not reachable at", MD_BASE)
        print("    Start it with:  python -m marketdata.server")
        sys.exit(1)
    print("OK")

    # Check TSDB
    print("  Checking TSDB backend...", end=" ", flush=True)
    if not _check_tsdb():
        print("FAILED")
        print("\n  ✗ TSDB backend not available (endpoints return 503)")
        print("    Start with TSDB enabled:")
        print("      TSDB_BACKEND=memory python -m marketdata.server")
        sys.exit(1)
    print("OK")
    print()

    # Phase 1: Collect data
    total_ticks = collect_data(args.collect, args.symbol)

    # Show TSDB snapshot
    show_tsdb_snapshot()

    # Phase 2: Query bars
    bars = query_bars(args.symbol, args.interval)

    if not bars:
        print("  ✗ No bars returned. The TSDB may need more time to accumulate data.")
        print("    Try running again with --collect 60")
        sys.exit(1)

    # Phase 3: Backtest
    result = backtest_ma_crossover(bars, args.symbol, args.fast, args.slow)

    # Phase 4: Summary
    print_summary(result, args.symbol, args.interval, total_ticks)

    # Bonus: FX bars if available
    show_fx_bars_if_available()


if __name__ == "__main__":
    main()
