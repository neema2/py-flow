"""
marketmodel/swap_curve — Market data models for Swap Curves.

Provides:
  - SwapQuote: Live par rate quote for a benchmark swap (e.g. 10Y OIS).
  - SwapQuoteRisk: Risk of a portfolio sensitivity mapped back to a specific quote.
"""

from __future__ import annotations
from dataclasses import dataclass

from store.base import Storable
from reactive.computed import effect
from streaming import ticking

@ticking
@dataclass
class SwapQuote(Storable):
    """Live swap par rate quote (e.g. OIS)."""
    __key__ = "symbol"

    symbol: str = ""
    currency: str = "USD"
    tenor: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    rate: float = 0.0

    @effect("rate")
    def on_rate(self, value):
        self.tick()


@ticking
@dataclass
class SwapQuoteRisk(Storable):
    """One row in the portfolio risk ladder: ∂Portfolio / ∂Quote."""
    __key__ = "symbol"

    symbol: str = ""
    portfolio: str = ""
    quote: str = ""
    risk: float = 0.0
    equiv_notional: float = 0.0
