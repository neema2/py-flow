"""
ir_swap_float_float.py — Explicit Float/Float Interest Rate Swap

Uses separate projection and discount curves for two distinct floating legs.
Supports optional notional exchange at start and end of the swap.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from store import Storable
from reactive.computed import computed, effect
from reactive.computed_expr import computed_expr
from reactive.expr import Const, diff, eval_cached, If, Expr
import marketmodel.curve_fitter
from streaming import ticking
from instruments.ir_scheduling import payment_dates, reset_dates


@ticking(exclude={
    "leg1_discount_curve", "leg1_projection_curve",
    "leg2_discount_curve", "leg2_projection_curve"
})
@dataclass
class IRSwapFloatFloat(Storable):
    """IRS with two explicit floating legs tracking fwd inputs."""
    __key__ = "symbol"
    
    symbol: str = ""
    leg1_notional: float = 0.0
    leg1_currency: str = "USD"
    leg2_notional: float = 0.0
    leg2_currency: str = "USD"
    collateral_currency: str = "USD"
    initial_fx: float = 1.0  # rate to convert leg1 to leg2, e.g. leg2_notional = leg1_notional * initial_fx
    
    float_spread: float = 0.0
    
    tenor_years: float = 0.0
    
    # Optional curves for Leg 1
    leg1_discount_curve: object = field(default=None, repr=False)
    leg1_projection_curve: object = field(default=None, repr=False)
    
    # Optional curves for Leg 2
    leg2_discount_curve: object = field(default=None, repr=False)
    leg2_projection_curve: object = field(default=None, repr=False)

    # Leg1 receives normally, Leg2 pays. "RECEIVER" means we receive Leg1, pay Leg2.
    side: str = "RECEIVER"
    exchange_notional: bool = False
    is_target: bool = False

    def __post_init__(self):
        super().__post_init__()
        # If leg2_notional is 0, initialize it using initial_fx
        if self.leg2_notional == 0.0 and self.leg1_notional != 0.0:
            object.__setattr__(self, 'leg2_notional', self.leg1_notional * self.initial_fx)

    def target_dates(self) -> list[float]:
        return payment_dates(self.tenor_years)

    def reset_dates(self) -> list[float]:
        return reset_dates(self.tenor_years)

    @property
    def pillar_names(self) -> list[str]:
        names = set()
        for c in [self.leg1_discount_curve, self.leg1_projection_curve,
                  self.leg2_discount_curve, self.leg2_projection_curve]:
            if c:
                names.update(c.pillar_names)
        return sorted(list(names))

    def _safe_dt(self, t1: float, t2: float) -> float:
        return float(t2 - t1)

    @computed_expr
    def dv01(self) -> Expr:
        """Approximate DV01 using leg1's discount curve to support fitter scaling."""
        if not self.leg1_discount_curve:
            return Const(0.0)
        
        targets = self.target_dates()
        resets = self.reset_dates()
        pv = Const(0.0)
        for i in range(len(targets)):
            dt = Const(self._safe_dt(resets[i], targets[i]))
            df = self.leg1_discount_curve.df(targets[i])
            pv += df * dt
            
        return pv * 0.0001 * Const(self.leg1_notional)

    def _calc_leg_pv(self, notional: float, discount_curve: object, projection_curve: object, spread: float = 0.0) -> Expr:
        """Helper to calculate explicit floating PV for a single leg."""
        if not discount_curve or not projection_curve:
            return Const(0.0)
            
        targets = self.target_dates()
        resets = self.reset_dates()
        pv = Const(0.0)
        
        for i in range(len(targets)):
            start = resets[i]
            end = targets[i]
            dt = Const(self._safe_dt(start, end))
            
            rate = projection_curve.fwd(start, end) + Const(spread)
            df = discount_curve.df(end)
            
            pv += rate * df * Const(notional) * dt
            
        if self.exchange_notional:
            # Receive notional at maturity, pay notional at start
            df_end = discount_curve.df(targets[-1])
            # Assuming start df_start = 1.0
            pv += Const(notional) * df_end - Const(notional)
            
        return pv

    @computed_expr
    def leg1_float_leg_pv(self) -> Expr:
        """PV of Leg 1."""
        return self._calc_leg_pv(
            self.leg1_notional, 
            self.leg1_discount_curve, 
            self.leg1_projection_curve,
            self.float_spread
        )

    @computed_expr
    def leg2_float_leg_pv(self) -> Expr:
        """PV of Leg 2."""
        return self._calc_leg_pv(
            self.leg2_notional, 
            self.leg2_discount_curve, 
            self.leg2_projection_curve
        )

    @computed_expr
    def npv(self) -> Expr:
        """NPV: RECEIVER = Leg1_in_leg2_ccy - Leg2, PAYER = Leg2 - Leg1_in_leg2_ccy."""
        leg1_net = self.leg1_float_leg_pv() * Const(self.initial_fx)
        if self.side == "PAYER":
            return self.leg2_float_leg_pv() - leg1_net
        return leg1_net - self.leg2_float_leg_pv()

    @computed_expr
    def pnl_status(self) -> Expr:
        npv_val = self.npv()
        if npv_val is None:
            return Const("FLAT")
        return If(npv_val > Const(0), "PROFIT", If(npv_val < Const(0), "LOSS", "FLAT"))

    @effect("npv")
    def on_npv(self, value):
        if marketmodel.curve_fitter.IS_SOLVING or self.is_target:
            return
        self.tick()

    @computed_expr
    def risk(self) -> dict[str, Expr]:
        """∂npv/∂pillar_rate via symbolic differentiation."""
        expr = self.npv()
        if expr is None: return {}
        return {
            name: diff(expr, name)
            for name in self.pillar_names
        }

    def pillar_context(self) -> dict[str, float]:
        """Build a context dict from all associated curves' current pillar rates."""
        ctx = {}
        for c in [self.leg1_discount_curve, self.leg1_projection_curve,
                  self.leg2_discount_curve, self.leg2_projection_curve]:
            if c:
                for p in c._sorted_points():
                    ctx[p.name] = p.rate
        return ctx

