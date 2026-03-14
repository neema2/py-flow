"""
ir_swap — Interest Rate Swap instruments.

Two swap types with self-contained payoff logic:

  IRSwapFixedFloatApprox     — "Shortcut" float leg: N × (1 - DF_T).
                         Uses the telescoping identity so the float leg
                         needs only the maturity discount factor.

  IRSwapFixedFloat           — Explicit float leg: looks up fwd_at() at each
                          reset date, prices each floating coupon individually.
                          Supports separate projection_curve (for forwards)
                          and discount_curve (for discounting).

Both share:
  - The same rack_dates() → payment_dates() / reset_dates() schedule
  - The same fixed_leg logic (annuity × fixed_rate × notional)
  - @computed for reactivity, @ticking for streaming

Risk sensitivities are computed via the Expr tree architecture:
  - FixedFloatApproxSwapExpr builds the full pricing formula as an Expr tree
  - diff(npv_expr, pillar_label) → symbolic derivative Expr
  - Same tree compiles to Python (.eval) and SQL (.to_sql)

The swap does NOT know about YieldCurvePoints, pillar names, or how
the curve interpolates.  It only depends on the curve via:
    curve.fwd_at(tenor)
    curve.df_at(tenor)
    curve.df_array(tenors)
"""

from __future__ import annotations

from dataclasses import dataclass, field

from store import Storable
from reactive.computed import computed, effect
from reactive.expr import Const, diff, eval_cached, If
import marketmodel.curve_fitter
from streaming import ticking


from instruments.ir_scheduling import rack_dates, payment_dates, reset_dates

from reactive.computed import computed, effect
from reactive.computed_expr import computed_expr


# ═══════════════════════════════════════════════════════════════════════════
# IRSwapFixedFloatApprox  (shortcut float leg)
# ═══════════════════════════════════════════════════════════════════════════

@ticking(exclude={"curve", "risk"})
@dataclass
class IRSwapFixedFloatApprox(Storable):
    """IRS with telescoping float leg — single class for both reactive and Expr.

    Two execution paths from the same object:
      1. Reactive: @computed properties use curve.df_at() for live ticking
      2. Expr tree: npv_expr built at construction, compilable to SQL/Pure

    The Expr tree is built via __post_init__ when the curve supports
    df() (i.e. has pillar points).  Otherwise Expr fields are None.
    """
    __key__ = "symbol"

    symbol: str = ""
    notional: float = 0.0
    fixed_rate: float = 0.0
    tenor_years: float = 0.0
    currency: str = "USD"
    collateral_currency: str = "USD"
    side: str = "RECEIVER"    # "RECEIVER" (Receive Fixed, Pay Float) or "PAYER"
    curve: object = None      # anything with fwd_at(), df_at(), df_array()
    is_target: bool = False   # If True, hide from live portfolio ticking

    def __post_init__(self):
        super().__post_init__()
        if self.currency != self.collateral_currency:
            raise ValueError(f"IRSwapFixedFloatApprox cannot have differing currency ({self.currency}) and collateral_currency ({self.collateral_currency}). Please use IRSwapFixedFloat instead.")

    @property
    def pillar_names(self) -> list[str]:
        if getattr(self, "curve", None) is None: return []
        pts = getattr(self.curve, "_sorted_points", lambda: [])()
        return [p.name for p in pts]

    def coupon_payment_dates(self) -> list[float]:
        """Payment dates for this swap (short front stub, no 0.0)."""
        return payment_dates(self.tenor_years)

    # ── Symbolic + Reactive Properties ─────────────────────────────────

    @computed_expr
    def dv01(self) -> Expr:
        """DV01 = notional × Σ (period_i × df_i) × 0.0001."""
        dates = self.coupon_payment_dates()
        annuity = 0.0
        prev_t = 0.0
        for t in dates:
            df = self.curve.df(t)
            annuity = annuity + (t - prev_t) * df
            prev_t = t
        return self.notional * annuity * 0.0001

    @computed_expr
    def fixed_leg_pv(self) -> Expr:
        """PV of fixed leg = fixed_rate × DV01 × 10000."""
        return self.fixed_rate * self.dv01() * 10000.0

    @computed_expr
    def float_leg_pv(self) -> Expr:
        """PV of floating leg = notional × (1 - DF_maturity)."""
        df_T = self.curve.df(float(self.tenor_years))
        return self.notional * (1.0 - df_T)

    @computed_expr
    def npv(self) -> Expr:
        """NPV: RECEIVER = fixed - float, PAYER = float - fixed."""
        if self.side == "PAYER":
            return self.float_leg_pv() - self.fixed_leg_pv()
        return self.fixed_leg_pv() - self.float_leg_pv()

    @computed_expr
    def par_rate(self) -> Expr:
        """Par rate: the fixed_rate at which NPV = 0."""
        return self.float_leg_pv() / (self.dv01() * 10000.0)

    @computed
    def pnl_status(self) -> str:
        val = self.npv
        if val > 0:
            return "PROFIT"
        elif val < 0:
            return "LOSS"
        return "FLAT"

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

    def npv_sql(self) -> str:
        """Generate SQL for the NPV calculation."""
        expr = self.npv()
        if expr is None: return "0.0"
        return expr.to_sql()

    def pillar_context(self) -> dict[str, float]:
        """Build a context dict from the curve's current pillar rates."""
        pts = self.curve._sorted_points()
        return {p.name: p.rate for p in pts}



# ═══════════════════════════════════════════════════════════════════════════
# SwapPortfolio
# ═══════════════════════════════════════════════════════════════════════════

@ticking
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
        # Skip ticking during solver iterations.
        if marketmodel.curve_fitter.IS_SOLVING:
            return
        self.tick()



