"""
yield_curve — YieldCurvePoint and YieldCurve with interpolation.

The curve owns the interpolation method.  Consumers (e.g. IRSwapFixedFloatApprox)
call fwd_at(t) / df_at(t) / df_array(tenors) without knowing how
interpolation works.  In future there will be many curve types with
different interpolation (linear, log-linear, cubic spline, etc.).

SQL compilation traces through this file — all math here must remain
SQL-translatable: only +, -, *, /, POWER, CASE.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from store import Storable
from reactive.computed import computed, effect
from reactive.expr import VariableMixin
from streaming import ticking


# ── Interpolation (SQL-translatable: only +, -, *, /) ────────────────────

def _interp(tenors: list[float], rates: list[float], t: float) -> float:
    """Linear interpolation with flat extrapolation.

    All operations are +, -, *, / — directly translatable to SQL.
    """
    n = len(tenors)
    if n == 0:
        return 0.0
    if t <= tenors[0]:
        return rates[0]
    if t >= tenors[n - 1]:
        return rates[n - 1]
    for i in range(n - 1):
        t1, t2 = tenors[i], tenors[i + 1]
        if t1 <= t <= t2:
            w = (t - t1) / (t2 - t1)
            return rates[i] + w * (rates[i + 1] - rates[i])
    return rates[n - 1]


# ── Domain Models ────────────────────────────────────────────────────────

def _point_tenor_key(p):
    """Sort key for YieldCurvePoint — avoids lambda in @computed."""
    return p.tenor_years

@ticking(exclude={"quote_ref", "fitted_rate"})
@dataclass
class YieldCurvePoint(Storable, VariableMixin):
    """Single yield curve pillar point — also an Expr leaf via VariableMixin.

    rate is @computed from quote_ref (typically a SwapQuote) — this is
    the curve fitting step.  Today it's a pass-through; in future it may
    be a complex solver (bootstrap, etc.).
    """
    __key__ = "name"

    name: str = ""
    symbol: str = ""       # IR_USD_YC_FIT.5Y
    tenor_years: float = 0.0
    fitted_rate: float = 0.0  # Set by CurveFitter
    currency: str = "USD"
    quote_ref: object = None      # The input quote (e.g. SwapQuote)
    is_fitted: bool = False       # If True, rate is set by a CurveFitter

    @computed
    def rate(self):
        """The pillar rate. Pass-through from quote unless is_fitted=True."""
        if self.is_fitted:
            return self.fitted_rate
        
        if self.quote_ref is None:
            return 0.0
        r = getattr(self.quote_ref, 'rate', 0.0)
        if r is None:
            return 0.0
        return float(r)

    def set_fitted_rate(self, value: float):
        """Update the rate from a solver."""
        self.fitted_rate = value
        import marketmodel.curve_fitter
        if not marketmodel.curve_fitter.IS_SOLVING:
            self.tick()

    @computed
    def discount_factor(self):
        return 1.0 / (1.0 + self.rate) ** self.tenor_years

    @effect("rate")
    def on_rate(self, value):
        import marketmodel.curve_fitter
        if marketmodel.curve_fitter.IS_SOLVING:
            return
        self.tick()


@dataclass
class CurveJacobianEntry(Storable):
    """One entry in the fitter's jacobian: ∂fit_rate[output] / ∂quote_rate[input].

    Symbol convention: IR_USD_YC_JACOBIAN.<output_tenor>.<input_tenor>

    For a pass-through fitter, the jacobian is identity:
        JACOBIAN.5Y.5Y = 1.0, JACOBIAN.5Y.10Y = 0.0

    For a bootstrap/solver fitter, off-diagonals can be non-zero.
    """
    __key__ = "symbol"

    symbol: str = ""                # IR_USD_YC_JACOBIAN.5Y.10Y
    output_tenor: float = 0.0       # the fitted point's tenor
    input_tenor: float = 0.0        # the quote's tenor
    value: float = 0.0              # ∂fit_rate / ∂quote_rate
    quote_symbol: str = ""          # the quote's symbol (e.g. IR_USD_OIS_QUOTE.5Y)


@ticking(exclude={"points", "jacobian"})
@dataclass
class YieldCurve(Storable):
    """Yield curve built from pillar YieldCurvePoints.

    Provides interpolated rate and discount factor queries for any tenor.
    All math is SQL-translatable (linear interpolation + power).

    The curve knows:
      - Its pillar points (YieldCurvePoint, output of the fitter)
      - Its jacobian (CurveJacobianEntry, ∂fit/∂quote from the fitter)

    Risk is computed via the Expr tree architecture:
      diff(npv_expr, pillar_name)  → ∂npv/∂pillar (symbolic differentiation)
      curve.risk_quote()            → ∂npv/∂quote  (jacobian multiplication)
    """
    __key__ = "name"

    name: str = ""
    currency: str = "USD"
    points: list = field(default_factory=list)
    jacobian: list = field(default_factory=list)  # list of CurveJacobianEntry

    @computed
    def point_count(self) -> int:
        return len(self.points)

    def _sorted_points(self):
        """Sort points by tenor — helper to avoid lambda in @computed."""
        return sorted(self.points, key=_point_tenor_key)

    @computed
    def pillar_tenors(self) -> list:
        """Sorted pillar tenors — recomputes when any point's rate changes."""
        pts = self._sorted_points()
        return [p.tenor_years for p in pts]

    @computed
    def pillar_rates(self) -> list:
        """Sorted pillar rates — triggers reactive dependency on all point rates."""
        pts = self._sorted_points()
        return [p.rate for p in pts]

    @computed
    def pillar_names(self) -> list:
        """Sorted pillar names."""
        pts = self._sorted_points()
        return [p.name for p in pts]

    def fwd_at(self, tenor: float, period: float = 1.0) -> float:
        """Forward rate for the period starting at tenor.

        Derived from discount factors:
            rate = (df(tenor) / df(tenor + period) - 1) / period

        This is the implied rate for borrowing from `tenor` to
        `tenor + period`.  SQL-translatable: just /, -, * on DFs.
        """
        dfs = self.df_array([tenor, tenor + period])
        if dfs[1] == 0.0:
            return 0.0
        return (dfs[0] / dfs[1] - 1.0) / period

    def fwd_array(self, tenors: list[float], period: float = 1.0) -> list[float]:
        """Batch forward rates — one df_array call for all tenors.
        For each tenor t, computes (df(t) / df(t+period) - 1) / period.
        """
        if not tenors:
            return []
        # Build pairs: [t0, t0+p, t1, t1+p, ...]
        all_tenors = []
        for t in tenors:
            all_tenors.append(t)
            all_tenors.append(t + period)
        all_dfs = self.df_array(all_tenors)

        rates = []
        for i in range(0, len(all_dfs), 2):
            df_start = all_dfs[i]
            df_end = all_dfs[i + 1]
            if df_end == 0.0:
                rates.append(0.0)
            else:
                rates.append((df_start / df_end - 1.0) / period)
        return rates

    def df_at(self, tenor: float) -> float:
        """Discount factor at any tenor: (1 + rate)^(-tenor).

        Uses _interp on pillar zero rates (not fwd_at, which is a forward rate).
        """
        r = _interp(self.pillar_tenors, self.pillar_rates, tenor)
        return (1.0 + r) ** (-tenor)

    def interp(self, t: float) -> "Expr":
        """Build an Expr tree for the interpolated rate at tenor t.

        The interpolation weights are constants (baked in at build time).
        The pillar rates are Variable leaf nodes (symbolic variables).

        Cached: calling interp(3.0) twice returns the SAME Expr object,
        enabling cross-swap sub-expression sharing.

        Returns an Expr that can:
          .eval(ctx)  → float (same as _interp)
          .to_sql()   → SQL expression
          diff(expr, "USD_OIS_5Y") → derivative Expr
        """
        cache = getattr(self, '_interp_cache', None)
        if cache is None:
            cache = {}
            object.__setattr__(self, '_interp_cache', cache)
        if t in cache:
            return cache[t]

        from reactive.expr import Const, Variable as PRExpr

        pts = self._sorted_points()
        n = len(pts)
        if n == 0:
            expr = Const(0.0)
            cache[t] = expr
            return expr

        tenors = [p.tenor_years for p in pts]

        # Use the point objects' names to build Variable leaves.
        # Variable inherits VariableMixin, matching the YieldCurvePoint
        # names, so diff() treats them identically.
        def _leaf(point):
            return PRExpr(point.name)

        # Flat extrapolation at boundaries
        if t <= tenors[0]:
            expr = _leaf(pts[0])
        elif t >= tenors[-1]:
            expr = _leaf(pts[-1])
        else:
            expr = _leaf(pts[-1])  # fallback
            for i in range(n - 1):
                t1, t2 = tenors[i], tenors[i + 1]
                if t1 <= t <= t2:
                    r1 = _leaf(pts[i])
                    r2 = _leaf(pts[i + 1])
                    w = Const((t - t1) / (t2 - t1))
                    expr = r1 + (r2 - r1) * w
                    break

        cache[t] = expr
        return expr

    def df(self, t: float) -> "Expr":
        """Build an Expr tree for the discount factor at tenor t.

        df(t) = (1 + interp(t)) ^ (-t)

        Cached: same tenor → same Expr object (enables sub-expression sharing
        across multiple swaps on the same curve).

        Returns an Expr with Variable leaves, suitable for eval/to_sql/diff.
        """
        cache = getattr(self, '_df_cache', None)
        if cache is None:
            cache = {}
            object.__setattr__(self, '_df_cache', cache)
        if t in cache:
            return cache[t]

        from reactive.expr import Const
        rate_expr = self.interp(t)
        expr = (Const(1.0) + rate_expr) ** Const(-t)
        cache[t] = expr
        return expr

    def fwd(self, start: float, end: float) -> "Expr":
        """Build an Expr tree for the forward rate between start and end.
        
        fwd(start, end) = (df(start) / df(end) - 1.0) / (end - start)
        """
        from reactive.expr import Const
        dt = end - start
        if dt <= 0:
            return Const(0.0)
            
        df_start = self.df(start)
        df_end = self.df(end)
        
        return (df_start / df_end - Const(1.0)) / Const(dt)



    def df_array(self, tenors: list[float]) -> list[float]:
        """Batch discount factors — single sort, linear sweep.

        For N tenors and M pillars, this is O(N log N + N + M) instead of
        O(N * M) from calling df_at() in a loop.
        """
        if not tenors:
            return []
        p_tenors = self.pillar_tenors
        p_rates = self.pillar_rates
        n = len(p_tenors)
        if n == 0:
            return [1.0] * len(tenors)

        # Sort query tenors but remember original order
        indexed = sorted(enumerate(tenors), key=lambda x: x[1])
        result = [0.0] * len(tenors)
        j = 0  # pillar index

        for orig_idx, t in indexed:
            # Advance pillar pointer
            while j < n - 1 and p_tenors[j + 1] < t:
                j += 1

            # Interpolate
            if t <= p_tenors[0]:
                r = p_rates[0]
            elif t >= p_tenors[n - 1]:
                r = p_rates[n - 1]
            elif p_tenors[j] == p_tenors[j + 1] if j < n - 1 else True:
                r = p_rates[j]
            else:
                t1, t2 = p_tenors[j], p_tenors[j + 1]
                w = (t - t1) / (t2 - t1)
                r = p_rates[j] + w * (p_rates[j + 1] - p_rates[j])

            result[orig_idx] = (1.0 + r) ** (-t)

        return result


    def risk_quote(self, pillar_risks: dict[str, float]) -> dict[str, float]:
        """Map ∂npv/∂pillar_rate → ∂npv/∂quote_rate using the fitter's jacobian.

        Applies the jacobian matrix:
            ∂npv/∂quote[j] = Σ_i  ∂npv/∂pillar[i]  ×  ∂pillar[i]/∂quote[j]

        The jacobian entries are CurveJacobianEntry objects stored in self.jacobian.
        For a pass-through fitter, the jacobian is identity.

        This is the final step in the risk chain:
          diff(npv_expr, label) → ∂npv/∂pillar  (symbolic differentiation)
          curve.risk_quote()    → ∂npv/∂quote   (jacobian)

        Returns {quote_symbol: ∂npv/∂quote_rate}.
        """
        if not self.jacobian:
            # No jacobian → assume identity (pass-through fitter)
            # pillar labels → quote symbols are the same
            return dict(pillar_risks)

        # Build quote-level risk from jacobian
        quote_risks: dict[str, float] = {}

        for entry in self.jacobian:
            # entry: ∂pillar[output_tenor] / ∂quote[input_tenor]
            # Find the pillar risk that corresponds to this entry's output
            pillar_name = None
            for pt in self._sorted_points():
                if pt.tenor_years == entry.output_tenor:
                    pillar_name = pt.name
                    break
            if pillar_name is None or pillar_name not in pillar_risks:
                continue

            pillar_risk = pillar_risks[pillar_name]

            # ∂npv/∂quote[j] += ∂npv/∂pillar[i] × ∂pillar[i]/∂quote[j]
            quote_key = entry.quote_symbol if entry.quote_symbol else entry.symbol
            quote_risks[quote_key] = quote_risks.get(quote_key, 0.0) + \
                pillar_risk * entry.value

        return quote_risks

    def benchmark_dv01s(self) -> dict[str, float]:
        """Return the DV01 (per 1M notional) for each benchmark quote.
        
        Calculated as the sensitivity of a 1M par swap at the quote's tenor 
        to its own market rate. This is the divisor for Equiv Notional.
        """
        # For simplicity in this demo, we approximate benchmark DV01 
        # as roughly Tenor * AvgDiscountFactor * 100 
        # (since ∂NPV/∂r ≈ Notional * Tenor * DF * 0.0001 per basis point)
        # For 1M notional, this is 100 * Tenor * DF.
        
        results = {}
        # Identify unique quotes from the jacobian
        quotes = {} # symbol -> tenor
        if not self.jacobian:
            # Identity case: pillars are quotes
            for pt in self.points:
                quotes[pt.symbol] = pt.tenor_years
        else:
            for entry in self.jacobian:
                quotes[entry.quote_symbol] = entry.input_tenor
                
        for q_sym, tenor in quotes.items():
            # More precise DV01 = 1M * Σ (period[i] * df[i]) * 0.0001
            # Assuming annual coupons for benchmark instruments:
            annuity = 0.0
            t = 1.0
            while t <= tenor:
                annuity += self.df_at(t)
                t += 1.0
            
            if t - 1.0 < tenor:
                annuity += (tenor - (t - 1.0)) * self.df_at(tenor)
            
            results[q_sym] = 100.0 * annuity # (1M * 0.0001 * annuity)
            
        return results

    @effect("pillar_rates")
    def on_rates_change(self, value):
        import marketmodel.curve_fitter
        if marketmodel.curve_fitter.IS_SOLVING:
            return
        self.tick()


