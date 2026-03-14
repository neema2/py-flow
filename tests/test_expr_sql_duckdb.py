"""
Test: Expr tree Python eval vs SQL execution in DuckDB.

This is the ultimate "write once, run anywhere" proof.  The same Expr tree
produced by FixedFloatApproxSwapExpr and Portfolio is:
  1. Evaluated in Python via eval_cached(expr, ctx)
  2. Compiled to SQL via expr.to_sql() → wrapped in a CTE → executed in DuckDB
  3. Results compared to <1e-10 relative tolerance

Also tests that symbolic derivatives (risk) match between Python and SQL.

No external servers needed — DuckDB is in-process.
"""

import pytest
import duckdb

from instruments.ir_swap_fixed_floatapprox import IRSwapFixedFloatApprox
from instruments.portfolio import Portfolio, expr_to_executable_sql
from marketmodel.yield_curve import YieldCurve, YieldCurvePoint
from reactive.expr import diff, eval_cached


# ── Fixtures ────────────────────────────────────────────────────────────────

PILLARS = [
    ("IR_USD_DISC_USD.1Y", 1.0, 0.03),
    ("IR_USD_DISC_USD.5Y", 5.0, 0.05),
    ("IR_USD_DISC_USD.10Y", 10.0, 0.07),
]


@pytest.fixture
def curve():
    points = [
        YieldCurvePoint(name=label, tenor_years=tenor, fitted_rate=rate, is_fitted=True)
        for label, tenor, rate in PILLARS
    ]
    return YieldCurve(name="TEST", points=points)


@pytest.fixture
def ctx():
    return {label: rate for label, _, rate in PILLARS}


@pytest.fixture
def db():
    """In-process DuckDB connection — no server needed."""
    conn = duckdb.connect()
    yield conn
    conn.close()


def _sql_eval(db, expr, ctx):
    """Helper: compile Expr to SQL, execute in DuckDB, return float."""
    sql = expr_to_executable_sql(expr, ctx)
    return db.execute(sql).fetchone()[0]


@pytest.fixture
def portfolio(curve):
    p = Portfolio(curve)
    p.add("SWAP1", notional=10e6, fixed_rate=0.03, tenor_years=3.0)
    p.add("SWAP2", notional=10e7, fixed_rate=0.04, tenor_years=7.0)
    return p


# ── Single Swap: Python vs SQL ──────────────────────────────────────────────

class TestSingleSwapPythonVsSQL:
    """Same Expr tree → Python eval_cached() vs DuckDB execution."""

    @pytest.fixture
    def swap(self, curve):
        return IRSwapFixedFloatApprox(symbol="TEST", 
            notional=50_000_000, fixed_rate=0.05,
            tenor_years=5.0, side="RECEIVER", curve=curve,
        )

    def test_npv_matches(self, swap, ctx, db):
        py_val = eval_cached(swap.npv(), ctx)
        sql_val = _sql_eval(db, swap.npv(), ctx)
        assert sql_val == pytest.approx(py_val, rel=1e-10), \
            f"NPV: Python={py_val}, SQL={sql_val}"

    def test_fixed_pv_matches(self, swap, ctx, db):
        py_val = eval_cached(swap.fixed_leg_pv(), ctx)
        sql_val = _sql_eval(db, swap.fixed_leg_pv(), ctx)
        assert sql_val == pytest.approx(py_val, rel=1e-10)

    def test_float_pv_matches(self, swap, ctx, db):
        py_val = eval_cached(swap.float_leg_pv(), ctx)
        sql_val = _sql_eval(db, swap.float_leg_pv(), ctx)
        assert sql_val == pytest.approx(py_val, rel=1e-10)

    def test_dv01_matches(self, swap, ctx, db):
        py_val = eval_cached(swap.dv01(), ctx)
        sql_val = _sql_eval(db, swap.dv01(), ctx)
        assert sql_val == pytest.approx(py_val, rel=1e-10)

    def test_df_matches(self, curve, ctx, db):
        """Individual discount factor expr: Python vs SQL."""
        for t in [1.0, 3.0, 5.0, 7.5, 10.0]:
            expr = curve.df(t)
            py_val = eval_cached(expr, ctx)
            sql_val = _sql_eval(db, expr, ctx)
            assert sql_val == pytest.approx(py_val, rel=1e-12), \
                f"df({t}): Python={py_val}, SQL={sql_val}"


# ── Risk (symbolic derivative): Python vs SQL ───────────────────────────────

class TestRiskPythonVsSQL:
    """∂npv/∂pillar via diff() → same derivative Expr evaluated in Python and SQL."""

    @pytest.fixture
    def swap(self, curve):
        return IRSwapFixedFloatApprox(symbol="TEST", 
            notional=50_000_000, fixed_rate=0.05,
            tenor_years=5.0, side="RECEIVER", curve=curve,
        )

    def test_risk_per_pillar(self, swap, ctx, db):
        """Each ∂npv/∂pillar expression matches between Python and SQL."""
        for label in ctx:
            deriv_expr = diff(swap.npv(), label)
            py_val = eval_cached(deriv_expr, ctx)
            sql_val = _sql_eval(db, deriv_expr, ctx)
            assert sql_val == pytest.approx(py_val, rel=1e-8), \
                f"∂npv/∂{label}: Python={py_val}, SQL={sql_val}"


# ── Portfolio: Python vs SQL ────────────────────────────────────────────────

class TestPortfolioPythonVsSQL:
    """Portfolio — all named expressions match between Python and SQL."""

    @pytest.fixture
    def portfolio(self, curve):
        p = Portfolio(curve)
        p.add("3Y", notional=30_000_000, fixed_rate=0.04, tenor_years=3.0)
        p.add("5Y", notional=50_000_000, fixed_rate=0.05, tenor_years=5.0)
        p.add("10Y", notional=80_000_000, fixed_rate=0.07, tenor_years=10.0)
        return p

    def test_each_npv_matches(self, portfolio, ctx, db):
        for name, expr in portfolio.npv_exprs.items():
            py_val = eval_cached(expr, ctx)
            sql_val = _sql_eval(db, expr, ctx)
            assert sql_val == pytest.approx(py_val, rel=1e-10), \
                f"{name} NPV: Python={py_val}, SQL={sql_val}"

    def test_total_npv_matches(self, portfolio, ctx, db):
        py_val = eval_cached(portfolio.total_npv_expr, ctx)
        sql_val = _sql_eval(db, portfolio.total_npv_expr, ctx)
        assert sql_val == pytest.approx(py_val, rel=1e-10)

    def test_residuals_match(self, portfolio, ctx, db):
        for name, expr in portfolio.residual_exprs.items():
            py_val = eval_cached(expr, ctx)
            sql_val = _sql_eval(db, expr, ctx)
            assert sql_val == pytest.approx(py_val, rel=1e-10), \
                f"{name} residual: Python={py_val}, SQL={sql_val}"

    def test_jacobian_matches(self, portfolio, ctx, db):
        """Each ∂residual/∂pillar matches between Python and SQL."""
        for name, row in portfolio.jacobian_exprs.items():
            for label, expr in row.items():
                py_val = eval_cached(expr, ctx)
                sql_val = _sql_eval(db, expr, ctx)
                assert sql_val == pytest.approx(py_val, rel=1e-8), \
                    f"J[{name},{label}]: Python={py_val}, SQL={sql_val}"


# ── SQL is actually valid and non-trivial ───────────────────────────────────

class TestSQLQuality:
    """Verify the generated SQL is valid, non-trivial, and executable."""

    def test_sql_contains_pillar_references(self, curve, ctx):
        """NPV SQL should reference pillar labels."""
        swap = IRSwapFixedFloatApprox(symbol="TEST", 
            notional=50_000_000, fixed_rate=0.05,
            tenor_years=5.0, side="RECEIVER", curve=curve,
        )
        sql = expr_to_executable_sql(swap.npv(), ctx)
        for label in ctx:
            assert f'"{label}"' in sql, f"SQL should reference {label}"

    def test_sql_is_single_query(self, curve, ctx, db):
        """The generated SQL should be a single executable query."""
        swap = IRSwapFixedFloatApprox(symbol="TEST", 
            notional=1_000_000, fixed_rate=0.05,
            tenor_years=3.0, side="RECEIVER", curve=curve,
        )
        sql = expr_to_executable_sql(swap.npv(), ctx)
        # Should execute without error and return exactly one row
        result = db.execute(sql).fetchall()
        assert len(result) == 1
        assert isinstance(result[0][0], float)

    def test_different_rates_give_different_sql_results(self, curve, db):
        """Changing rates in the ctx should change the SQL output."""
        swap = IRSwapFixedFloatApprox(symbol="TEST", 
            notional=50_000_000, fixed_rate=0.05,
            tenor_years=5.0, side="RECEIVER", curve=curve,
        )
        ctx1 = {"IR_USD_DISC_USD.1Y": 0.03, "IR_USD_DISC_USD.5Y": 0.05, "IR_USD_DISC_USD.10Y": 0.07}
        ctx2 = {"IR_USD_DISC_USD.1Y": 0.04, "IR_USD_DISC_USD.5Y": 0.06, "IR_USD_DISC_USD.10Y": 0.08}

        val1 = _sql_eval(db, swap.npv(), ctx1)
        val2 = _sql_eval(db, swap.npv(), ctx2)
        assert val1 != pytest.approx(val2, rel=0.01), \
            "Different rates should give different NPVs"


# ── Optimized Portfolio SQL (DAGs + CTEs) ───────────────────────────────────

class TestOptimizedPortfolioSQL:
    """Verify Portfolio.to_sql_optimized() correctly uses CTEs and matches Python."""

    def test_optimized_sql_matches_python(self, portfolio, ctx, db):
        # Generate the single master query
        sql = portfolio.to_sql_optimized(ctx)
        
        # Execute in DuckDB
        results = db.execute(sql).df()
        
        # Compare each NPV
        py_npvs = portfolio.eval_npvs(ctx)
        for name, py_val in py_npvs.items():
            sql_val = results[f"{name}_NPV"].values[0]
            assert sql_val == pytest.approx(py_val, rel=1e-10)

    def test_optimized_sql_contains_ctes(self, portfolio, ctx):
        sql = portfolio.to_sql_optimized(ctx)
        # Should contain node references and a WITH clause
        assert "WITH " in sql
        assert "node_" in sql
        assert "val FROM" in sql

    def test_optimized_sql_is_pruned(self, curve):
        """Swaps should only have Jacobian columns for relevant pillars."""
        p = Portfolio(curve)
        # 3Y swap on 1Y/5Y/10Y curve
        p.add("SHORT", notional=10e6, fixed_rate=0.03, tenor_years=3.0)
        
        ctx = p.pillar_context()
        sql = p.to_sql_optimized(ctx)
        
        # In linear interp, 3Y depends on 1Y and 5Y, but NOT 10Y
        assert "SHORT_dNPV_dIR_USD_DISC_USD.1Y" in sql
        assert "SHORT_dNPV_dIR_USD_DISC_USD.5Y" in sql
        # This is the pruning check
        assert "SHORT_dNPV_dIR_USD_DISC_USD.10Y" not in sql
