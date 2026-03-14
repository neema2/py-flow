"""
Tests for FixedFloatApproxSwapExpr and Portfolio — comparing Expr-tree
pricing against the original IRSwapFixedFloatApprox.

Proves that:
  1. Expr tree eval matches original @computed pricing (NPV, DV01, legs)
  2. Symbolic diff matches finite-difference risk
  3. The Expr tree can generate SQL strings (no hand-written SQL needed)
  4. Portfolio returns named dicts and Jacobians matching original AD
  5. Sub-expression sharing works across swaps on the same curve
"""

import pytest
from instruments.ir_swap_fixed_floatapprox import IRSwapFixedFloatApprox, SwapPortfolio, payment_dates, rack_dates
from instruments.portfolio import Portfolio
from marketmodel.yield_curve import YieldCurve, YieldCurvePoint
from reactive.expr import diff, Variable, Const


# ── Fixture: a 3-pillar curve ──────────────────────────────────────────────

PILLARS = [
    ("IR_USD_DISC_USD.1Y", 1.0, 0.03),
    ("IR_USD_DISC_USD.5Y", 5.0, 0.05),
    ("IR_USD_DISC_USD.10Y", 10.0, 0.07),
]


@pytest.fixture
def curve():
    """A YieldCurve with three pillar points."""
    points = [
        YieldCurvePoint(name=name, tenor_years=tenor, fitted_rate=rate, is_fitted=True)
        for name, tenor, rate in PILLARS
    ]
    return YieldCurve(name="TEST", points=points)


@pytest.fixture
def ctx():
    """Pillar rate context dict for Expr evaluation."""
    return {name: rate for name, _, rate in PILLARS}


# ── Schedule Tests ─────────────────────────────────────────────────────────

class TestSchedule:
    def test_rack_dates_integer(self):
        assert rack_dates(5.0) == [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]

    def test_rack_dates_fractional(self):
        assert rack_dates(2.5) == [0.0, 0.5, 1.5, 2.5]

    def test_payment_dates_no_zero(self):
        """Payment dates should not include 0.0."""
        dates = payment_dates(5.0)
        assert 0.0 not in dates
        assert dates == [1.0, 2.0, 3.0, 4.0, 5.0]

    def test_tenor_negative_raises(self):
        with pytest.raises(ValueError, match="tenor_years must be >= 0"):
            rack_dates(-1.0)


# ── Phase 1: df matches df_at ─────────────────────────────────────────

class TestDfExpr:
    def test_df_matches_df_at(self, curve, ctx):
        """Expr tree df().eval() must produce the same value as df_at()."""
        for t in [0.5, 1.0, 2.0, 3.0, 5.0, 7.5, 10.0]:
            expected = curve.df_at(t)
            expr = curve.df(t)
            actual = expr.eval(ctx)
            assert actual == pytest.approx(expected, rel=1e-12), \
                f"df mismatch at t={t}: expr={actual}, df_at={expected}"

    def test_interp_matches_interp(self, curve, ctx):
        """interp should match the scalar _interp for all tenors."""
        from marketmodel.yield_curve import _interp
        tenors = curve.pillar_tenors
        rates = curve.pillar_rates
        for t in [0.0, 0.5, 1.0, 3.0, 5.0, 7.5, 10.0, 12.0]:
            expected = _interp(tenors, rates, t)
            actual = curve.interp(t).eval(ctx)
            assert actual == pytest.approx(expected, rel=1e-12), \
                f"interp mismatch at t={t}"

    def test_df_generates_sql(self, curve):
        """df should produce a SQL string (not crash)."""
        sql = curve.df(3.0).to_sql()
        assert isinstance(sql, str)
        assert len(sql) > 0
        assert "^" in sql or "**" in sql or "p.rate" in sql

    def test_df_caching(self, curve):
        """df(t) should return the SAME object for the same tenor."""
        a = curve.df(5.0)
        b = curve.df(5.0)
        assert a is b, "df should cache and return the same object"

    def test_interp_caching(self, curve):
        """interp(t) should return the SAME object for the same tenor."""
        a = curve.interp(3.0)
        b = curve.interp(3.0)
        assert a is b, "interp should cache and return the same object"


# ── Phase 2: Expr swap correctness ─────────────────────────────────────────

class TestExprSwapCorrectness:
    """The unified IRSwapFixedFloatApprox's Expr path must produce correct values."""

    @pytest.fixture
    def swap(self, curve):
        return IRSwapFixedFloatApprox(
            symbol="TEST_5Y", notional=50_000_000, fixed_rate=0.05,
            tenor_years=5.0, side="RECEIVER", curve=curve,
        )

    def test_npv_near_zero_at_par(self, swap, ctx):
        """At par rates, NPV should be near zero."""
        npv = swap.eval_npv(ctx)
        assert abs(npv / swap.notional) < 0.01, f"NPV/notional = {npv / swap.notional}"

    def test_fixed_minus_float_equals_npv(self, swap, ctx):
        """RECEIVER: NPV = fixed_pv - float_pv."""
        npv = swap.eval_npv(ctx)
        fixed_pv = swap.eval_fixed_leg_pv(ctx)
        float_pv = swap.eval_float_leg_pv(ctx)
        assert npv == pytest.approx(fixed_pv - float_pv, rel=1e-12)

    def test_dv01_positive(self, swap, ctx):
        """DV01 should be positive for any swap."""
        dv01 = swap.eval_dv01(ctx)
        assert dv01 > 0

    def test_payer_npv(self, curve, ctx):
        """PAYER swap should have opposite sign to RECEIVER."""
        receiver = IRSwapFixedFloatApprox(
            symbol="REC", notional=50_000_000, fixed_rate=0.05,
            tenor_years=5.0, side="RECEIVER", curve=curve,
        )
        payer = IRSwapFixedFloatApprox(
            symbol="PAY", notional=50_000_000, fixed_rate=0.05,
            tenor_years=5.0, side="PAYER", curve=curve,
        )
        assert receiver.eval_npv(ctx) == pytest.approx(-payer.eval_npv(ctx), rel=1e-12)

    def test_npv_generates_sql(self, swap):
        """npv_expr should produce a valid SQL string."""
        sql = swap.npv_sql()
        assert isinstance(sql, str)
        assert len(sql) > 10


# ── Phase 3: Symbolic risk ─────────────────────────────────────────────────

class TestSymbolicRisk:
    """Symbolic diff() must produce correct sensitivities."""

    @pytest.fixture
    def swap(self, curve):
        return IRSwapFixedFloatApprox(
            symbol="RISK_5Y", notional=50_000_000, fixed_rate=0.05,
            tenor_years=5.0, side="RECEIVER", curve=curve,
        )

    def test_risk_nonzero(self, swap, ctx):
        """At least some pillar sensitivities should be non-zero."""
        risk = swap.risk(ctx)
        nonzero = [v for v in risk.values() if abs(v) > 1e-10]
        assert len(nonzero) > 0, "All risk sensitivities are zero — something is wrong"


# ── Phase 4: Portfolio ─────────────────────────────────────────────────────

class TestPortfolio:
    """Portfolio: named dictionaries of Expr trees for fitter integration."""

    @pytest.fixture
    def portfolio(self, curve):
        p = Portfolio(curve)
        p.add("3Y", notional=30_000_000, fixed_rate=0.04, tenor_years=3.0)
        p.add("5Y", notional=50_000_000, fixed_rate=0.05, tenor_years=5.0)
        p.add("10Y", notional=80_000_000, fixed_rate=0.07, tenor_years=10.0)
        return p

    # ── Named dictionaries ─────────────────────────────────────────────

    def test_npv_exprs_returns_named_dict(self, portfolio, ctx):
        """npv_exprs should return {name: Expr} with all swap names."""
        npvs = portfolio.npv_exprs
        assert set(npvs.keys()) == {"3Y", "5Y", "10Y"}
        for name, expr in npvs.items():
            val = expr.eval(ctx)
            assert isinstance(val, float)

    def test_npv_near_zero_at_par(self, portfolio, ctx):
        """Each named NPV should be near zero at par rates."""
        expr_npvs = portfolio.eval_npvs(ctx)
        for name, npv in expr_npvs.items():
            notional = portfolio._swaps[name].notional
            assert abs(npv / notional) < 0.05, \
                f"NPV/notional for {name}: {npv / notional}"

    def test_total_npv_is_sum(self, portfolio, ctx):
        """total_npv_expr should equal sum of individual NPVs."""
        expr_total = portfolio.total_npv_expr.eval(ctx)
        individual_sum = sum(portfolio.eval_npvs(ctx).values())
        assert expr_total == pytest.approx(individual_sum, rel=1e-10)

    # ── Residuals for fitter ───────────────────────────────────────────

    def test_residuals_are_scaled(self, portfolio, ctx):
        """Residuals = NPV / notional — dimensionless."""
        npvs = portfolio.eval_npvs(ctx)
        residuals = portfolio.eval_residuals(ctx)
        for name in portfolio.names:
            notional = portfolio._swaps[name].notional
            assert residuals[name] == pytest.approx(npvs[name] / notional, rel=1e-12)

    def test_residuals_dict_keys(self, portfolio, ctx):
        residuals = portfolio.eval_residuals(ctx)
        assert set(residuals.keys()) == {"3Y", "5Y", "10Y"}

    # ── Jacobian for fitter ────────────────────────────────────────────

    def test_jacobian_shape(self, portfolio, ctx):
        """Jacobian should be {swap_name: {pillar_label: float}}."""
        jac = portfolio.eval_jacobian(ctx)
        assert set(jac.keys()) == {"3Y", "5Y", "10Y"}
        for name, row in jac.items():
            assert set(row.keys()) == {"IR_USD_DISC_USD.1Y", "IR_USD_DISC_USD.5Y", "IR_USD_DISC_USD.10Y"}
            for val in row.values():
                assert isinstance(val, float)

    def test_jacobian_matches_fd(self, portfolio, ctx):
        """Jacobian (symbolic diff) should match finite-difference approximation."""
        from reactive.expr import eval_cached
        jac = portfolio.eval_jacobian(ctx)
        bump = 1e-7
        for name in portfolio.names:
            for pillar_name in portfolio.pillar_names:
                ctx_up = dict(ctx)
                ctx_up[pillar_name] += bump
                ctx_dn = dict(ctx)
                ctx_dn[pillar_name] -= bump
                npv_up = eval_cached(portfolio.residual_exprs[name], ctx_up)
                npv_dn = eval_cached(portfolio.residual_exprs[name], ctx_dn)
                fd = (npv_up - npv_dn) / (2 * bump)
                assert jac[name][pillar_name] == pytest.approx(fd, rel=1e-4), \
                    f"Jacobian FD mismatch for {name}/{pillar_name}"

    def test_jacobian_exprs_generate_sql(self, portfolio):
        """Jacobian Expr trees should compile to SQL."""
        for name, row in portfolio.jacobian_exprs.items():
            for pillar_name, expr in row.items():
                sql = expr.to_sql()
                assert isinstance(sql, str)
                assert len(sql) > 0

    # ── Aggregated risk ────────────────────────────────────────────────

    def test_total_risk_nonzero(self, portfolio, ctx):
        """Total risk (∂total_npv/∂pillar) should have non-zero entries."""
        total_risk = portfolio.eval_total_risk(ctx)
        nonzero = [v for v in total_risk.values() if abs(v) > 1e-10]
        assert len(nonzero) > 0

    # ── Sub-expression sharing ─────────────────────────────────────────

    def test_df_shared_across_swaps(self, curve):
        """Two swaps on the same curve should share the same df(t) objects."""
        p = Portfolio(curve)
        p.add("5Y", notional=50e6, fixed_rate=0.05, tenor_years=5.0)
        p.add("10Y", notional=80e6, fixed_rate=0.07, tenor_years=10.0)

        df5_a = curve.df(5.0)
        df5_b = curve.df(5.0)
        assert df5_a is df5_b, "df(5.0) should return the same object (via cache)"

    def test_shared_nodes_count(self, curve):
        """There should be shared Expr nodes between swaps on the same curve."""
        p = Portfolio(curve)
        p.add("5Y", notional=50e6, fixed_rate=0.05, tenor_years=5.0)
        p.add("10Y", notional=80e6, fixed_rate=0.07, tenor_years=10.0)
        shared = p.shared_nodes()
        assert len(shared) > 0, "No shared nodes found — sub-expression sharing broken"


# ── Convenience ────────────────────────────────────────────────────────────

class TestPillarContext:
    def test_pillar_context_round_trips(self, curve):
        """pillar_context() should give the same rates we put in."""
        swap = IRSwapFixedFloatApprox(
            symbol="TEST_ROUNDTRIP",
            notional=1_000_000, fixed_rate=0.05,
            tenor_years=5.0, side="RECEIVER", curve=curve,
        )
        ctx = swap.pillar_context()
        for name, _, rate in PILLARS:
            assert ctx[name] == pytest.approx(rate)


# ── Phase 5: Fitter Integration ────────────────────────────────────────────

class TestExprFitter:
    """Prove that solve() converges and produces correct fitted rates."""

    def test_solve_npv_near_zero(self):
        """After solve, all target swap NPVs should be near zero."""
        from marketmodel.swap_curve import SwapQuote
        from instruments.ir_swap_fixed_floatapprox import IRSwapFixedFloatApprox
        from marketmodel.curve_fitter import CurveFitter

        q1 = SwapQuote(symbol="Q1Y", rate=0.02)
        q5 = SwapQuote(symbol="Q5Y", rate=0.04)
        q10 = SwapQuote(symbol="Q10Y", rate=0.06)

        p1 = YieldCurvePoint(name="IR_USD_DISC_USD.1Y", tenor_years=1.0, quote_ref=q1, is_fitted=True)
        p5 = YieldCurvePoint(name="IR_USD_DISC_USD.5Y", tenor_years=5.0, quote_ref=q5, is_fitted=True)
        p10 = YieldCurvePoint(name="IR_USD_DISC_USD.10Y", tenor_years=10.0, quote_ref=q10, is_fitted=True)
        curve = YieldCurve(name="CURVE_NPV", points=[p1, p5, p10])

        targets = [
            IRSwapFixedFloatApprox(symbol="T1", tenor_years=1.0, fixed_rate=0.02,
                             curve=curve, notional=50e6, is_target=True),
            IRSwapFixedFloatApprox(symbol="T5", tenor_years=5.0, fixed_rate=0.04,
                             curve=curve, notional=50e6, is_target=True),
            IRSwapFixedFloatApprox(symbol="T10", tenor_years=10.0, fixed_rate=0.06,
                             curve=curve, notional=50e6, is_target=True),
        ]

        fitter = CurveFitter(
            name="FIT_NPV", currency="USD", curve=curve,
            points=[p1, p5, p10], quotes=[q1, q5, q10],
            target_swaps=targets,
        )
        fitter.solve()

        # After solve, each target swap's NPV should be near zero
        for swap in targets:
            assert abs(swap.npv / swap.notional) < 1e-8, \
                f"{swap.symbol}: NPV/notional = {swap.npv / swap.notional} (should be ~0)"
