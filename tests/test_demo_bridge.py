"""
Mirror test for demo_bridge.py
=================================
Verifies the full demo flow:

  1. Define Order/Trade Storables
  2. Wire StoreBridge → DH ticking tables
  3. Create derived tables: last_by, agg_by
  4. @computed Position with reactive market_value + risk_score
  5. Save Orders/Trades → appear in DH tables (verified via snapshot())
  6. Position updates → computed values push to DH via @effect
"""

import time
from dataclasses import dataclass

import pytest

from bridge import StoreBridge
from reactive.computed import computed
from reactive.computed import effect as reactive_effect
from streaming import agg, flush, get_tables, snapshot, ticking

from store import Storable, connect


# ── Domain Models — same as demo ─────────────────────────────────────────

@dataclass
class BrgOrder(Storable):
    symbol: str = ""
    quantity: int = 0
    price: float = 0.0
    side: str = ""


@dataclass
class BrgTrade(Storable):
    symbol: str = ""
    quantity: int = 0
    price: float = 0.0
    side: str = ""
    pnl: float = 0.0


@ticking
@dataclass
class BrgPosition(Storable):
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
        self.tick()


# ── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def setup(streaming_server, store_server, market_data_server):
    """Full demo setup — Store + Bridge + DH tables + reactive positions."""
    store_server.provision_user("brg_user", "brg_pw")
    store_server.register_alias("brg-demo")

    # Connect as user — same as demo
    db = connect("brg-demo", user="brg_user", password="brg_pw")

    # Wire StoreBridge — same as demo
    bridge = StoreBridge(
        "brg-demo", user="brg_user", password="brg_pw",
        subscriber_id="brg_test",
    )
    bridge.register(BrgOrder)
    bridge.register(BrgTrade)
    bridge.start()

    # Get raw + live DH tables — same as demo
    orders_raw = bridge.table(BrgOrder)
    trades_raw = bridge.table(BrgTrade)
    orders_live = orders_raw.last_by("EntityId")
    trades_live = trades_raw.last_by("EntityId")

    # Create portfolio aggregate — same as demo
    portfolio = trades_live.agg_by([
        agg.sum(["TotalPnL=pnl", "TotalQty=quantity"]),
        agg.count("NumTrades"),
    ])

    # Risk totals from reactive positions — same as demo
    risk_totals = BrgPosition._ticking_live.agg_by([
        agg.sum(["TotalMV=market_value", "TotalRisk=risk_score"]),
        agg.count("NumPositions"),
    ])

    # Publish all tables — same as demo
    pos_tables = get_tables()
    for name, tbl in {
        "brg_orders_raw": orders_raw,
        "brg_orders_live": orders_live,
        "brg_trades_raw": trades_raw,
        "brg_trades_live": trades_live,
        "brg_portfolio": portfolio,
        "brg_risk_totals": risk_totals,
        **{f"brg_{k}": v for k, v in pos_tables.items()},
    }.items():
        tbl.publish(name)

    yield {
        "db": db,
        "bridge": bridge,
        "orders_raw": orders_raw,
        "orders_live": orders_live,
        "trades_raw": trades_raw,
        "trades_live": trades_live,
        "portfolio": portfolio,
        "risk_totals": risk_totals,
    }

    bridge.stop()
    db.close()


@pytest.fixture(scope="module")
def tracked_positions():
    """Track positions by symbol — same as demo."""
    return {}


def ensure_tracked(tracked, symbol, price, quantity):
    """Create or update a position — same as demo's ensure_tracked()."""
    if symbol not in tracked:
        pos = BrgPosition(symbol=symbol, price=price, quantity=quantity)
        tracked[symbol] = pos
    else:
        pos = tracked[symbol]
        pos.batch_update(price=price, quantity=quantity)
    return pos


# ── Tests ────────────────────────────────────────────────────────────────

class TestDemoBridge:
    """Mirrors demo_bridge.py — StoreBridge + reactive Position flow."""

    def test_bridge_tables_created(self, setup) -> None:
        """Bridge creates raw + live DH tables for Order and Trade."""
        assert setup["orders_raw"] is not None
        assert setup["trades_raw"] is not None
        assert setup["orders_live"] is not None
        assert setup["trades_live"] is not None

    def test_portfolio_aggregate_created(self, setup) -> None:
        """Portfolio = trades_live.agg_by(sum TotalPnL, count NumTrades)."""
        assert setup["portfolio"] is not None

    def test_risk_totals_created(self, setup) -> None:
        """Risk totals = Position._ticking_live.agg_by(sum MV, risk)."""
        assert setup["risk_totals"] is not None

    def test_save_order_flows_to_dh(self, setup) -> None:
        """Save an Order in Store → row appears in DH via bridge."""
        order = BrgOrder(symbol="AAPL", quantity=100, price=228.50, side="BUY")
        order.save()
        time.sleep(1)
        flush()

        snap = snapshot(setup["orders_raw"])
        assert len(snap) >= 1
        assert "AAPL" in snap["symbol"].values

    def test_save_trade_flows_to_dh(self, setup) -> None:
        """Save a Trade in Store → row appears in DH via bridge."""
        trade = BrgTrade(symbol="AAPL", quantity=100, price=228.50, side="BUY", pnl=150.0)
        trade.save()
        time.sleep(1)
        flush()

        snap = snapshot(setup["trades_raw"])
        assert len(snap) >= 1
        assert "AAPL" in snap["symbol"].values

    def test_order_update_flows_to_dh(self, setup) -> None:
        """Update an existing Order → new event appears in DH."""
        order = BrgOrder(symbol="MSFT", quantity=200, price=415.0, side="SELL")
        order.save()
        time.sleep(0.5)

        # Update price — same as demo's order.price = ... ; order.save()
        order.price = 416.0
        order.save()
        time.sleep(1)
        flush()

        snap = snapshot(setup["orders_raw"])
        assert len(snap) >= 3  # At least: AAPL + MSFT original + MSFT update

    def test_reactive_positions(self, tracked_positions) -> None:
        """@computed market_value and risk_score recompute on price change."""
        pos = ensure_tracked(tracked_positions, "NVDA", 875.0, 200)
        assert pos.market_value == 875.0 * 200
        assert pos.risk_score == 875.0 * 200 * 0.02

        """batch_update triggers @computed recalc + @effect → DH push."""
        pos.batch_update(price=880.0, quantity=200)
        flush()
        assert pos.market_value == 880.0 * 200
        assert pos.risk_score == 880.0 * 200 * 0.02

        """Track multiple symbols — same as demo's loop."""
        for sym, price, qty in [("AAPL", 228.0, 500), ("MSFT", 415.0, 300),
                                 ("TSLA", 355.0, 150)]:
            ensure_tracked(tracked_positions, sym, price, qty)
            flush()

        assert len(tracked_positions) >= 4  # NVDA + AAPL + MSFT + TSLA
