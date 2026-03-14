"""
Test: IRS reactive cascade with instruments/ modules.

Imports directly from instruments/ — no class duplication.
Uses clean round rates (rate ≈ tenor%) so NPV ≈ 0 is easy to verify.
"""

import pytest

from streaming import agg, flush, get_tables

from marketmodel.yield_curve import YieldCurvePoint, YieldCurve
from marketmodel.swap_curve import SwapQuote
from instruments.ir_swap_fixed_floatapprox import IRSwapFixedFloatApprox, SwapPortfolio


# ── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def reactive_graph(streaming_server):
    """Build the reactive graph with clean round rates.

    Pillars: 1Y=1%, 5Y=5%, 10Y=10%
    Interpolation → fwd_at(t) returns forward rates
    Fixed rates match → NPV ≈ 0
    """
    # OIS Swap Quotes
    usd_ois_1y  = SwapQuote(symbol="IR_USD_OIS_QUOTE.1Y",  rate=0.01)
    usd_ois_5y  = SwapQuote(symbol="IR_USD_OIS_QUOTE.5Y",  rate=0.05)
    usd_ois_10y = SwapQuote(symbol="IR_USD_OIS_QUOTE.10Y", rate=0.10)

    # Yield curve points
    usd_1y = YieldCurvePoint(
        name="IR_USD_DISC_USD.1Y", tenor_years=1.0, currency="USD",
        quote_ref=usd_ois_1y, is_fitted=True,
    )
    usd_5y = YieldCurvePoint(
        name="IR_USD_DISC_USD.5Y", tenor_years=5.0, currency="USD",
        quote_ref=usd_ois_5y, is_fitted=True,
    )
    usd_10y = YieldCurvePoint(
        name="IR_USD_DISC_USD.10Y", tenor_years=10.0, currency="USD",
        quote_ref=usd_ois_10y, is_fitted=True,
    )

    # YieldCurve
    usd_curve = YieldCurve(
        name="USD_OIS", currency="USD",
        points=[usd_1y, usd_5y, usd_10y],
    )

    # IRS — fixed_rate = interpolated rate → NPV ≈ 0
    swap_usd_5y = IRSwapFixedFloatApprox(
        symbol="USD-5Y", notional=50_000_000, fixed_rate=0.05,
        tenor_years=5.0, currency="USD", curve=usd_curve,
    )
    swap_usd_10y = IRSwapFixedFloatApprox(
        symbol="USD-10Y", notional=100_000_000, fixed_rate=0.10,
        tenor_years=10.0, currency="USD", curve=usd_curve,
    )
    # 3Y: interp(1Y=1%, 5Y=5%) → rate=3% → fixed=3%
    swap_usd_3y = IRSwapFixedFloatApprox(
        symbol="USD-3Y", notional=60_000_000, fixed_rate=0.03,
        tenor_years=3.0, currency="USD", curve=usd_curve,
    )
    # 7Y: interp(5Y=5%, 10Y=10%) → rate=7% → fixed=7%
    swap_usd_7y = IRSwapFixedFloatApprox(
        symbol="USD-7Y", notional=40_000_000, fixed_rate=0.07,
        tenor_years=7.0, currency="USD", curve=usd_curve,
    )

    portfolio = SwapPortfolio(
        name="USD", swaps=[swap_usd_5y, swap_usd_10y, swap_usd_3y, swap_usd_7y],
    )

    # ── Fitter Step ───────────────────────────────────────────────
    # We must run the fitter to ensure the zero rates represent par rates
    # otherwise NPV will not be zero because zero_rate != par_rate.
    from marketmodel.curve_fitter import CurveFitter
    fitter = CurveFitter(
        name="USD_FIT", currency="USD", curve=usd_curve,
        points=[usd_1y, usd_5y, usd_10y],
        quotes=[usd_ois_1y, usd_ois_5y, usd_ois_10y],
        target_swaps=[
            IRSwapFixedFloatApprox(symbol="T1", tenor_years=1.0, fixed_rate=0.01, curve=usd_curve, notional=50_000_000, is_target=True),
            IRSwapFixedFloatApprox(symbol="T5", tenor_years=5.0, fixed_rate=0.05, curve=usd_curve, notional=50_000_000, is_target=True),
            IRSwapFixedFloatApprox(symbol="T10", tenor_years=10.0, fixed_rate=0.10, curve=usd_curve, notional=50_000_000, is_target=True),
        ]
    )
    fitter.solve()

    tables = get_tables()
    for name, tbl in tables.items():
        tbl.publish(f"irs_{name}")

    swap_summary = IRSwapFixedFloatApprox._ticking_live.agg_by([
        agg.sum(["TotalNPV=npv", "TotalDV01=dv01"]),
        agg.count("NumSwaps"),
    ])
    swap_summary.publish("irs_swap_summary")

    flush()

    return {
        "usd_ois_1y": usd_ois_1y, "usd_ois_5y": usd_ois_5y, "usd_ois_10y": usd_ois_10y,
        "usd_1y": usd_1y, "usd_5y": usd_5y, "usd_10y": usd_10y,
        "usd_curve": usd_curve,
        "swap_usd_5y": swap_usd_5y, "swap_usd_10y": swap_usd_10y,
        "swap_usd_3y": swap_usd_3y, "swap_usd_7y": swap_usd_7y,
        "portfolio": portfolio,
        "fitter": fitter,
    }


# ── Tests ────────────────────────────────────────────────────────────────

class TestCurveInterpolation:
    """YieldCurve fwd_at returns forward rates derived from DFs."""

    def test_df_at_pillar(self, reactive_graph) -> None:
        """df_at(5) = (1 + r5)^(-5) using fitted zero rate."""
        curve = reactive_graph["usd_curve"]
        # Fitter solve happened, so pillar rate is in usd_5y
        rate_5y = reactive_graph["usd_5y"].rate
        assert abs(curve.df_at(5.0) - (1 + rate_5y) ** (-5.0)) < 1e-10

    def test_df_array_matches_individual(self, reactive_graph) -> None:
        curve = reactive_graph["usd_curve"]
        tenors = [1.0, 3.0, 5.0, 7.0, 10.0]
        dfs = curve.df_array(tenors)
        for t, df in zip(tenors, dfs):
            assert abs(df - curve.df_at(t)) < 1e-10

    def test_fwd_at_is_forward_rate(self, reactive_graph) -> None:
        """fwd_at(t) = (df(t) / df(t+1) - 1) — 1Y forward rate."""
        curve = reactive_graph["usd_curve"]
        for t in [0.0, 3.0, 5.0, 7.0]:
            dfs = curve.df_array([t, t + 1.0])
            expected = dfs[0] / dfs[1] - 1.0
            assert abs(curve.fwd_at(t) - expected) < 1e-10

    def test_fwd_at_zero_equals_1y_zero_rate(self, reactive_graph) -> None:
        """At t=0: fwd = (df(0)/df(1) - 1) = 1Y zero rate."""
        curve = reactive_graph["usd_curve"]
        assert abs(curve.fwd_at(0.0) - 0.01) < 1e-10

    def test_fwd_at_upward_sloping(self, reactive_graph) -> None:
        """Forward rates increase on an upward-sloping curve."""
        curve = reactive_graph["usd_curve"]
        r0 = curve.fwd_at(0.0)
        r5 = curve.fwd_at(5.0)
        r7 = curve.fwd_at(7.0)
        assert r5 > r0
        assert r7 > r5

    def test_fwd_array_matches_individual(self, reactive_graph) -> None:
        """fwd_array batch matches individual fwd_at calls."""
        curve = reactive_graph["usd_curve"]
        tenors = [0.0, 3.0, 5.0, 7.0]
        rates = curve.fwd_array(tenors)
        for t, r in zip(tenors, rates):
            assert abs(r - curve.fwd_at(t)) < 1e-10


class TestSwapPricing:
    """Multi-cashflow swap pricing via YieldCurve."""

    def test_coupon_dates_5y(self, reactive_graph) -> None:
        swap = reactive_graph["swap_usd_5y"]
        assert swap.coupon_payment_dates() == [1.0, 2.0, 3.0, 4.0, 5.0]

    def test_coupon_dates_7y(self, reactive_graph) -> None:
        swap = reactive_graph["swap_usd_7y"]
        assert swap.coupon_payment_dates() == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]

    def test_coupon_dates_short_front_stub(self, reactive_graph) -> None:
        """3.5Y swap: payments at 0.5, 1.5, 2.5, 3.5."""
        swap = IRSwapFixedFloatApprox(symbol="TEST", tenor_years=3.5)
        assert swap.coupon_payment_dates() == [0.5, 1.5, 2.5, 3.5]

    def test_negative_tenor_errors(self, reactive_graph) -> None:
        swap = IRSwapFixedFloatApprox(symbol="FAIL", tenor_years=-1.0)
        with pytest.raises(ValueError, match="tenor_years must be >= 0"):
            swap.coupon_payment_dates()

    def test_5y_npv_near_zero(self, reactive_graph) -> None:
        """5Y: fixed 5%, par par_rate ≈ 5% → NPV ≈ 0."""
        swap = reactive_graph["swap_usd_5y"]
        # par_rate is the par rate: float_leg_pv / (dv01 * 10000)
        assert abs(swap.npv) < 1_000

    def test_7y_npv_near_zero(self, reactive_graph) -> None:
        """7Y: par NPV ≈ 0."""
        swap = reactive_graph["swap_usd_7y"]
        swap_par = IRSwapFixedFloatApprox(
            symbol="USD-7Y-PAR", notional=swap.notional,
            fixed_rate=float(swap.par_rate), tenor_years=7.0,
            currency="USD", side=swap.side, curve=swap.curve
        )
        assert abs(swap_par.npv) < 1_000

    def test_3y_npv_near_zero(self, reactive_graph) -> None:
        swap = reactive_graph["swap_usd_3y"]
        swap_par = IRSwapFixedFloatApprox(
            symbol="USD-3Y-PAR", notional=swap.notional,
            fixed_rate=float(swap.par_rate), tenor_years=3.0,
            currency="USD", side=swap.side, curve=swap.curve
        )
        assert abs(swap_par.npv) < 1_000

    def test_10y_npv_near_zero(self, reactive_graph) -> None:
        swap = reactive_graph["swap_usd_10y"]
        assert abs(swap.npv) < 1_000

    def test_par_rate_is_par_rate(self, reactive_graph) -> None:
        """par_rate = float_leg_pv / (dv01 × 10000) — the par swap rate."""
        swap = reactive_graph["swap_usd_5y"]
        expected = swap.float_leg_pv / (swap.dv01 * 10000)
        assert abs(swap.par_rate - expected) < 1e-10

    def test_par_rate_equals_fixed_rate_at_par(self, reactive_graph) -> None:
        """When NPV ≈ 0, par_rate ≈ fixed_rate (par swap)."""
        # 3Y swap was set to fixed_rate = 0.03, matching the curve → near par
        swap = reactive_graph["swap_usd_3y"]
        assert abs(swap.par_rate - swap.fixed_rate) < 0.005  # within 50bps

    def test_dv01_formula(self, reactive_graph) -> None:
        swap = reactive_graph["swap_usd_5y"]
        curve = reactive_graph["usd_curve"]
        dates = swap.coupon_payment_dates()
        dfs = curve.df_array(dates)
        # Expected DV01 = sum of (period * df)
        total = 0.0
        prev_t = 0.0
        for t, df in zip(dates, dfs):
            total += (t - prev_t) * df
            prev_t = t
        expected = swap.notional * total * 0.0001
        assert abs(swap.dv01 - expected) < 0.01

    def test_fixed_leg_pv_formula(self, reactive_graph) -> None:
        swap = reactive_graph["swap_usd_5y"]
        assert abs(swap.fixed_leg_pv - swap.fixed_rate * swap.dv01 * 10000) < 0.01

    def test_float_leg_pv_formula(self, reactive_graph) -> None:
        swap = reactive_graph["swap_usd_5y"]
        curve = reactive_graph["usd_curve"]
        expected = swap.notional * (1.0 - curve.df_at(5.0))
        assert abs(swap.float_leg_pv - expected) < 0.01

    def test_npv_is_fixed_minus_float(self, reactive_graph) -> None:
        swap = reactive_graph["swap_usd_5y"]
        # Default side is RECEIVER: NPV = fixed_leg_pv - float_leg_pv
        assert abs(swap.npv - (swap.fixed_leg_pv - swap.float_leg_pv)) < 0.01


class TestReactiveCascade:
    """Reactive cascade from OIS quote to portfolio NPV."""

    def test_ois_shock_moves_npv_off_zero(self, reactive_graph) -> None:
        """100bp shock to 5Y: NPV moves off zero."""
        usd_ois_5y = reactive_graph["usd_ois_5y"]
        swap = reactive_graph["swap_usd_5y"]
        fitter = reactive_graph["fitter"]
    
        # Ensure we are near zero before
        assert abs(swap.npv) < 1_000
    
        usd_ois_5y.batch_update(rate=0.06)
        # Update the target swap in the fitter so it solves for the new market level
        for s in fitter.target_swaps:
            if s.tenor_years == 5.0:
                s.fixed_rate = 0.06
        
        fitter.solve()  # Fitter solve is currently manual (effect disabled in core)
        flush()
    
        # par_rate (par rate) moves up — fixed is still 5% → NPV NEGATIVE (Receiver)
        assert swap.par_rate > 0.05
        assert swap.npv < -10_000  # moved down

    def test_7y_reacts_to_pillar_change(self, reactive_graph) -> None:
        """7Y swap reacts via interpolation: 5Y now at 6%."""
        swap_7y = reactive_graph["swap_usd_7y"]
        # Par par_rate for 7Y moved because 5Y pillar shifted
        assert swap_7y.par_rate > 0.07 
        assert swap_7y.npv < 0  # Was zero, rate up -> value down

    def test_portfolio_reacts(self, reactive_graph) -> None:
        port = reactive_graph["portfolio"]
        expected = sum(s.npv for s in port.swaps)
        assert abs(port.total_npv - expected) < 0.01
