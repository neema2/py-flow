"""
CurveFitter — Simultaneous solve for yield curve pillars.

This is the bridge between market data (SwapQuote) and the curve (YieldCurvePoints).
It solves for all pillar rates simultaneously by minimizing the squared NPVs
of all target swaps.

Uses Portfolio to build the pricing formula and its symbolic Jacobian
from the same Expr tree — the derivative can't drift out of sync with the
price.  The Jacobian Expr trees are built once at construction and evaluated
cheaply each solver iteration via eval_cached().
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field
import numpy as np
from scipy.optimize import least_squares

from reactive.computed import computed, effect
from streaming import ticking
from store import Storable

from reaktiv import batch

from marketmodel.yield_curve import YieldCurve, CurveJacobianEntry
from marketmodel.symbols import fit_symbol, jacobian_symbol

logger = logging.getLogger(__name__)

# Global flag to signal downstream components (like portfolios)
# to skip heavy effects (e.g. risk ladder calc) during solver iterations.
IS_SOLVING = False


@ticking(exclude={"target_swaps", "curve", "points", "quotes"})
@dataclass
class CurveFitter(Storable):
    """Fitter that solves for all curve points at once.

    Triggers a re-solve whenever any target swap's rate changes.
    The solver minimizes sum(swap.npv ** 2).
    """
    __key__ = "name"

    name: str = ""
    currency: str = "USD"
    
    # Inputs: target swaps whose NPVs we want to solve for zero
    target_swaps: list = field(default_factory=list)

    # Market quotes whose rate changes trigger a re-solve
    quotes: list = field(default_factory=list)
    
    # Curve we are fitting
    curve: YieldCurve | None = None
    points: list = field(default_factory=list)

    _is_solving: bool = False

    @computed
    def quote_trigger(self) -> float:
        """Dummy signal to watch all input quote rates."""
        # Using a sum as a simple trigger
        return sum(getattr(q, "rate", 0.0) for q in self.quotes)

    # @effect("quote_trigger")
    def on_market_change(self, value):
        """Re-solve the curve when any input quote rate changes."""
        self.solve()

    # ══════════════════════════════════════════════════════════════════
    # Solver (Portfolio + symbolic Jacobian)
    # ══════════════════════════════════════════════════════════════════

    def solve(self):
        """Solve using Portfolio — symbolic Jacobian from the same Expr tree.

        The Jacobian is derived from the SAME Expr as the objective,
        so it can't drift.  Each iteration evaluates the pre-built
        derivative DAGs via eval_cached().
        """
        global IS_SOLVING
        if self._is_solving:
            return
        if not self.target_swaps or not self.points:
            return

        import marketmodel.curve_fitter
        marketmodel.curve_fitter.IS_SOLVING = True
        try:
            from instruments.portfolio import Portfolio

            # Build the Portfolio from the target swaps
            portfolio = Portfolio(self.curve)
            for swap in self.target_swaps:
                portfolio.add_swap(getattr(swap, "symbol", "UNKNOWN"), swap)

            # Ordered lists for numpy conversion
            swap_names = portfolio.names
            pillar_names = portfolio.pillar_names

            # Pre-compute the Jacobian Expr trees (done once, reused every iteration)
            jac_exprs = portfolio.jacobian_exprs  # {name: {label: Expr}}

            # Initial guess from current quotes
            x0 = np.array([p.quote_ref.rate for p in self.points])

            def _build_ctx(x) -> dict:
                """Build a pillar rate context dict from the solver's x vector."""
                ctx = {}
                for swap in self.target_swaps:
                    if hasattr(swap, 'pillar_context'):
                        ctx.update(swap.pillar_context())
                
                ctx.update({
                    pt.name: float(rate_val)
                    for pt, rate_val in zip(self.points, x)
                })
                return ctx

            def objective(x):
                """Residual vector: NPV_i / notional_i for each target swap."""
                # 1. Update pillar rates (for reactive model sync)
                for pt, rate_val in zip(self.points, x):
                    pt.fitted_rate = float(rate_val)

                # 2. Evaluate residuals via Expr tree
                ctx = _build_ctx(x)
                residuals = portfolio.eval_residuals(ctx)
                result = np.array([residuals[name] for name in swap_names])

                print(f"    [Fitter] Iteration x={x} -> NormNPVs={result}")
                sys.stdout.flush()
                return result

            def jacobian(x):
                """∂residual_i / ∂pillar_j — evaluated from the symbolic Expr trees.

                The Jacobian Expr trees were built once at portfolio construction.
                Each call just evaluates them with the current pillar rates.
                Uses eval_cached because memoized diff() creates DAGs, not trees.
                """
                from reactive.expr import eval_cached
                ctx = _build_ctx(x)
                matrix = []
                for name in swap_names:
                    row = [eval_cached(jac_exprs[name][pillar_name], ctx) for pillar_name in pillar_names]
                    matrix.append(row)
                return np.array(matrix)

            # Solve!
            print(f"  [Fitter] Starting LM solve ({len(self.points)} pillars)...")
            res = least_squares(objective, x0, jac=jacobian, method='lm')
            print(f"  [Fitter] Solve complete. Status: {res.status}")
            sys.stdout.flush()

            if not res.success:
                logger.warning(f"Fitter solver failed: {res.message}")
            # Reset IS_SOLVING BEFORE applying final rates so effects fire
            marketmodel.curve_fitter.IS_SOLVING = False

            # Map results back to YieldCurvePoints with a batch update
            with batch():
                for pt, final_rate in zip(self.points, res.x):
                    pt.set_fitted_rate(float(final_rate))

            # Publish the Jacobian after batch closes and values propagate
            if res.success:
                print(f"  [Fitter] Starting Jacobian calculation...")
                sys.stdout.flush()
                self._publish_jacobian(res)
                print(f"  [Fitter] Jacobian published.")
                sys.stdout.flush()
        finally:
            marketmodel.curve_fitter.IS_SOLVING = False

    # ══════════════════════════════════════════════════════════════════
    # Shared: Jacobian publication
    # ══════════════════════════════════════════════════════════════════

    def _publish_jacobian(self, solver_res):
        """Publish the fitter sensitivity: ∂pillar_rate / ∂quote_rate.
        
        Populates CurveJacobianEntry objects in the curve.
        """
        if self.curve is None:
            return
            
        # For par swaps, the Jacobian (∂npv/∂pillar) is what we solved with.
        # But we want (∂pillar/∂quote).
        # Since NPV = (fixed - par_float) * dv01 * 10000:
        # ∂npv/∂fixed = +dv01 * 10000
        # So: ∂pillar/∂quote = -(Jacobian_pillars)^-1 * (∂npv/∂quote)
        
        j_pillars = solver_res.jac  # matrix[swaps, pillars]
        try:
            # Case 1: Over-determined or exact (smoothing fit)
            # Use Moore-Penrose pseudoinverse to find ∂pillar/∂quote
            j_inv = np.linalg.pinv(j_pillars)
            
            new_jacobian_entries = []
            for i, p_out in enumerate(self.points):
                for j, s_in in enumerate(self.target_swaps):
                    # For a par swap target: ∂NPV_scaled/∂quote = -dv01 * 10000 / notional
                    # (since NPV_scaled = (Float - Fixed)/Notional and Fixed = quote * dv01 * 10000)
                    dv01_val = getattr(s_in, "dv01", 0.0)
                    if dv01_val is None:
                        dv01_val = 0.0
                    
                    notional = getattr(s_in, "notional", getattr(s_in, "leg1_notional", 1.0))
                    dv01_scaled = (float(dv01_val) * 10000.0) / notional
                    
                    # Chain rule: ∂pillar/∂quote = - (Jacobian_pillars)^-1 * (∂NPV_scaled/∂quote)
                    # = - j_inv * (-dv01_scaled) = j_inv * dv01_scaled
                    val = j_inv[i, j] * dv01_scaled
                    
                    # Map to the corresponding quote symbol if possible
                    # target_swaps[j] usually corresponds to quotes[j]
                    q_sym = self.quotes[j].symbol if (self.quotes and j < len(self.quotes)) else s_in.symbol

                    symbol = jacobian_symbol(self.currency, p_out.name, q_sym)
                    entry = CurveJacobianEntry(
                        symbol=symbol,
                        output_tenor=p_out.tenor_years,
                        input_tenor=s_in.tenor_years,
                        value=float(val),
                        quote_symbol=q_sym
                    )
                    new_jacobian_entries.append(entry)
            
            self.curve.jacobian = new_jacobian_entries
            self.curve.tick()
            
        except (np.linalg.LinAlgError, ValueError) as e:
            logger.warning(f"Jacobian inversion failed: {e}")
