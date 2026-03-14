"""
SimulatorFeed — Async Market Data Simulator
============================================
Generates realistic ticking price data using geometric Brownian motion.
Async rewrite of the original server/market_data.py.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Any

from marketdata.bus import TickBus
from marketdata.feed import MarketDataFeed
from marketdata.models import FXTick, Tick, SwapTick

logger = logging.getLogger(__name__)

SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "NFLX"]

BASE_PRICES = {
    "AAPL": 228.0, "GOOGL": 192.0, "MSFT": 415.0, "AMZN": 225.0,
    "TSLA": 355.0, "NVDA": 138.0, "META": 700.0, "NFLX": 1020.0,
}

# Random starting positions (shares held, negative = short)
POSITIONS = {sym: random.randint(-500, 500) for sym in SYMBOLS}

# ── FX pairs ─────────────────────────────────────────────────────────────────

FX_PAIRS = ["USD/JPY", "EUR/USD", "GBP/USD"]

FX_BASE: dict[str, dict[str, Any]] = {
    "USD/JPY": {"mid": 149.55, "spread": 0.10, "currency": "JPY"},
    "EUR/USD": {"mid": 1.0852, "spread": 0.0005, "currency": "USD"},
    "GBP/USD": {"mid": 1.2705, "spread": 0.0010, "currency": "USD"},
}

# ── Swap Quotes (OIS) ────────────────────────────────────────────────────────
SWAP_INSTRUMENTS = [
    "IR_USD_OIS_QUOTE.1Y",
    "IR_USD_OIS_QUOTE.5Y",
    "IR_USD_OIS_QUOTE.10Y",
    "IR_USD_OIS_QUOTE.20Y",
    "IR_JPY_OIS_QUOTE.1Y",
    "IR_JPY_OIS_QUOTE.5Y",
    "IR_JPY_OIS_QUOTE.10Y",
    "IR_JPY_OIS_QUOTE.20Y",
    "IR_JPY_XCCY_QUOTE.1Y",
    "IR_JPY_XCCY_QUOTE.5Y",
    "IR_JPY_XCCY_QUOTE.10Y",
    "IR_JPY_XCCY_QUOTE.20Y"
]

SWAP_BASE: dict[str, dict[str, Any]] = {
    # USD OIS
    "IR_USD_OIS_QUOTE.1Y":  {"mid": 0.01,   "spread": 0.0002, "tenor": 1.0,  "currency": "USD"},
    "IR_USD_OIS_QUOTE.5Y":  {"mid": 0.05,   "spread": 0.0003, "tenor": 5.0,  "currency": "USD"},
    "IR_USD_OIS_QUOTE.10Y": {"mid": 0.10,   "spread": 0.0004, "tenor": 10.0, "currency": "USD"},
    "IR_USD_OIS_QUOTE.20Y": {"mid": 0.20,   "spread": 0.0005, "tenor": 20.0, "currency": "USD"},
    
    # JPY OIS
    "IR_JPY_OIS_QUOTE.1Y":  {"mid": 0.001,  "spread": 0.0001, "tenor": 1.0,  "currency": "JPY"},
    "IR_JPY_OIS_QUOTE.5Y":  {"mid": 0.005,  "spread": 0.0002, "tenor": 5.0,  "currency": "JPY"},
    "IR_JPY_OIS_QUOTE.10Y": {"mid": 0.010,  "spread": 0.0003, "tenor": 10.0, "currency": "JPY"},
    "IR_JPY_OIS_QUOTE.20Y": {"mid": 0.020,  "spread": 0.0004, "tenor": 20.0, "currency": "JPY"},
    
    # JPY XCCY (basis spread)
    "IR_JPY_XCCY_QUOTE.1Y":  {"mid": -0.0010, "spread": 0.0001, "tenor": 1.0,  "currency": "JPY"},
    "IR_JPY_XCCY_QUOTE.5Y":  {"mid": -0.0020, "spread": 0.0002, "tenor": 5.0,  "currency": "JPY"},
    "IR_JPY_XCCY_QUOTE.10Y": {"mid": -0.0030, "spread": 0.0003, "tenor": 10.0, "currency": "JPY"},
    "IR_JPY_XCCY_QUOTE.20Y": {"mid": -0.0040, "spread": 0.0004, "tenor": 20.0, "currency": "JPY"},
}


class SimulatorFeed(MarketDataFeed):
    """Async market data simulator producing realistic ticking prices.

    Uses geometric Brownian motion with 0.2% std dev per tick.
    Publishes both price Ticks and RiskTicks to the TickBus.
    """

    def __init__(self, tick_interval: float = 1.0) -> None:
        self._tick_interval = tick_interval
        self._current_prices: dict[str, float] = dict(BASE_PRICES)
        self._current_fx: dict[str, float] = {
            pair: data["mid"] for pair, data in FX_BASE.items()
        }
        self._current_swaps: dict[str, float] = {
            sym: data["mid"] for sym, data in SWAP_BASE.items()
        }
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None

    @property
    def name(self) -> str:
        return "simulator"

    async def start(self, bus: TickBus) -> None:
        """Start generating ticks and publishing to the bus."""
        self._stop_event.clear()
        logger.info(
            "SimulatorFeed starting: %d equities + %d FX pairs, %.0fms interval",
            len(SYMBOLS), len(FX_PAIRS), self._tick_interval * 1000,
        )

        while not self._stop_event.is_set():
            try:
                now = datetime.now(timezone.utc)

                # ── Equity ticks ──────────────────────────────────────────
                for sym in SYMBOLS:
                    old = self._current_prices[sym]
                    move = random.gauss(0, 0.002)  # 0.2% std dev per tick
                    new_price = old * (1 + move)
                    self._current_prices[sym] = new_price

                    spread = new_price * 0.0001
                    bid = new_price - spread / 2
                    ask = new_price + spread / 2
                    volume = random.randint(100, 10_000)
                    change = new_price - old
                    change_pct = (change / old) * 100

                    tick = Tick(
                        symbol=sym,
                        price=new_price,
                        bid=bid,
                        ask=ask,
                        volume=volume,
                        change=change,
                        change_pct=change_pct,
                        timestamp=now,
                    )
                    await bus.publish(tick)

                # ── FX ticks ─────────────────────────────────────────────
                for pair in FX_PAIRS:
                    base = FX_BASE[pair]
                    old_mid = self._current_fx[pair]
                    mid_move = random.gauss(0, 0.0003) * old_mid  # ~3bp σ
                    new_mid = old_mid + mid_move
                    self._current_fx[pair] = new_mid

                    half_spread = base["spread"] / 2
                    bid = new_mid - half_spread
                    ask = new_mid + half_spread
                    spread_pips = base["spread"] * 10_000

                    fx_tick = FXTick(
                        symbol=pair,
                        pair=pair,
                        bid=round(bid, 5),
                        ask=round(ask, 5),
                        mid=round(new_mid, 5),
                        spread_pips=round(spread_pips, 1),
                        currency=base["currency"],
                        timestamp=now,
                    )
                    await bus.publish(fx_tick)

                # ── Swap ticks ─────────────────────────────────────────────
                for sym in SWAP_INSTRUMENTS:
                    base = SWAP_BASE[sym]
                    old_mid = self._current_swaps[sym]
                    mid_move = random.gauss(0, 0.0001)  # ~0.01bp absolute
                    new_mid = old_mid + mid_move
                    self._current_swaps[sym] = new_mid

                    half_spread = base["spread"] / 2
                    bid = new_mid - half_spread
                    ask = new_mid + half_spread

                    swap_tick = SwapTick(
                        symbol=sym,
                        currency=base["currency"],
                        tenor=base["tenor"],
                        bid=round(bid, 6),
                        ask=round(ask, 6),
                        rate=round(new_mid, 6),
                        timestamp=now,
                    )
                    await bus.publish(swap_tick)

                await asyncio.sleep(self._tick_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("SimulatorFeed error: %s", e)
                await asyncio.sleep(1)

        logger.info("SimulatorFeed stopped")

    async def stop(self) -> None:
        """Signal the feed to stop producing ticks."""
        self._stop_event.set()
