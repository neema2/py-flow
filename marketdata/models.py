"""
Market Data Models
==================
Pydantic v2 models shared across the market data pipeline.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field


class Tick(BaseModel):
    """A single equity price tick from a market data feed."""
    type: Literal["equity"] = "equity"
    symbol: str
    price: float
    bid: float
    ask: float
    volume: int
    change: float
    change_pct: float
    timestamp: datetime


class RiskTick(BaseModel):
    """Per-symbol risk snapshot computed from a price tick."""
    symbol: str
    position: int
    market_value: float
    unrealized_pnl: float
    delta: float
    gamma: float
    theta: float
    vega: float
    timestamp: datetime


class FXTick(BaseModel):
    """A single FX spot tick."""
    type: Literal["fx"] = "fx"
    symbol: str         # "USD/JPY" (alias for pair)
    pair: str           # "USD/JPY"
    bid: float
    ask: float
    mid: float
    spread_pips: float
    currency: str       # quote currency
    timestamp: datetime


class CurveTick(BaseModel):
    """A single yield curve point tick."""
    type: Literal["curve"] = "curve"
    symbol: str         # "IR_USD_DISC_USD.5Y"
    label: str          # "USD_5Y" (compat)
    tenor_years: float
    rate: float
    discount_factor: float
    currency: str       # "USD" / "JPY"
    timestamp: datetime


class SwapTick(BaseModel):
    """A single swap par rate quote (e.g. OIS)."""
    type: Literal["swap"] = "swap"
    symbol: str         # "USD_OIS_5Y"
    currency: str       # "USD"
    tenor: float        # 5.0
    rate: float
    bid: float
    ask: float
    timestamp: datetime


class JacobianTick(BaseModel):
    """A single fitter sensitivity entry: ∂pillar_rate / ∂quote_rate."""
    type: Literal["jacobian"] = "jacobian"
    symbol: str         # "IR_USD_YC_JACOBIAN.5Y.10Y"
    output_tenor: float
    input_tenor: float
    value: float
    timestamp: datetime


# Discriminated union of all market data message types
MarketDataMessage = Annotated[
    Tick | FXTick | CurveTick | SwapTick | JacobianTick,
    Field(discriminator="type"),
]


def get_symbol_key(msg: Any) -> str:
    """Extract the symbol/pair/label key from any market data message."""
    t = getattr(msg, "type", None)
    if t == "equity":
        return msg.symbol
    if t == "fx":
        return msg.symbol if hasattr(msg, "symbol") else msg.pair
    if t == "curve":
        return msg.symbol
    if t == "swap":
        return msg.symbol
    if t == "jacobian":
        return msg.symbol
    
    raise ValueError(f"Unknown message type: {t}")


class Subscription(BaseModel):
    """Client subscription request — which types/symbols to stream."""
    types: list[str] | None = None    # ["equity","fx","curve"] or None=all
    symbols: list[str] | None = None  # symbol/pair/label filter, None=all


class SnapshotResponse(BaseModel):
    """REST response for a single symbol snapshot."""
    symbol: str
    tick: Tick | None = None
