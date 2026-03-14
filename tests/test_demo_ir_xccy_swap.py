"""
Test: Cross Currency Swap & Multiple Curve Fitting
"""

import pytest

from streaming import agg, flush, get_tables

from marketmodel.yield_curve import YieldCurvePoint, YieldCurve
from marketmodel.swap_curve import SwapQuote
from marketmodel.curve_fitter import CurveFitter
from instruments.ir_swap_fixed_floatapprox import IRSwapFixedFloatApprox
from instruments.ir_swap_float_float import IRSwapFloatFloat

@pytest.fixture(scope="module")
def xccy_system(streaming_server):
    # ── 1. USD PROJ/DISC OIS (Single Curve Fit) ──────────────────────────────
    usd_ois_1y  = SwapQuote(symbol="IR_USD_OIS_QUOTE.1Y",  rate=0.01)
    usd_ois_5y  = SwapQuote(symbol="IR_USD_OIS_QUOTE.5Y",  rate=0.05)
    usd_ois_10y = SwapQuote(symbol="IR_USD_OIS_QUOTE.10Y", rate=0.10)

    usd_1y = YieldCurvePoint(name="IR_USD_DISC_USD.1Y", tenor_years=1.0, currency="USD", quote_ref=usd_ois_1y, is_fitted=True)
    usd_5y = YieldCurvePoint(name="IR_USD_DISC_USD.5Y", tenor_years=5.0, currency="USD", quote_ref=usd_ois_5y, is_fitted=True)
    usd_10y = YieldCurvePoint(name="IR_USD_DISC_USD.10Y", tenor_years=10.0, currency="USD", quote_ref=usd_ois_10y, is_fitted=True)

    usd_curve = YieldCurve(
        name="USD_OIS", currency="USD",
        points=[usd_1y, usd_5y, usd_10y],
    )

    usd_fitter = CurveFitter(
        name="USD_FIT", currency="USD", curve=usd_curve,
        points=[usd_1y, usd_5y, usd_10y],
        quotes=[usd_ois_1y, usd_ois_5y, usd_ois_10y],
        target_swaps=[
            IRSwapFixedFloatApprox(symbol="USD_T1", tenor_years=1.0, fixed_rate=0.01, curve=usd_curve, notional=50_000_000, is_target=True),
            IRSwapFixedFloatApprox(symbol="USD_T5", tenor_years=5.0, fixed_rate=0.05, curve=usd_curve, notional=50_000_000, is_target=True),
            IRSwapFixedFloatApprox(symbol="USD_T10", tenor_years=10.0, fixed_rate=0.10, curve=usd_curve, notional=50_000_000, is_target=True),
        ]
    )
    usd_fitter.solve()


    # ── 2. JPY PROJ OIS (Single Curve Fit) ───────────────────────────────────
    jpy_ois_1y  = SwapQuote(symbol="IR_JPY_OIS_QUOTE.1Y",  rate=0.001)
    jpy_ois_5y  = SwapQuote(symbol="IR_JPY_OIS_QUOTE.5Y",  rate=0.005)
    jpy_ois_10y = SwapQuote(symbol="IR_JPY_OIS_QUOTE.10Y", rate=0.010)

    jpy_proj_1y = YieldCurvePoint(name="IR_JPY_DISC_JPY.1Y", tenor_years=1.0, currency="JPY", quote_ref=jpy_ois_1y, is_fitted=True)
    jpy_proj_5y = YieldCurvePoint(name="IR_JPY_DISC_JPY.5Y", tenor_years=5.0, currency="JPY", quote_ref=jpy_ois_5y, is_fitted=True)
    jpy_proj_10y = YieldCurvePoint(name="IR_JPY_DISC_JPY.10Y", tenor_years=10.0, currency="JPY", quote_ref=jpy_ois_10y, is_fitted=True)

    jpy_proj_curve = YieldCurve(
        name="JPY_OIS", currency="JPY",
        points=[jpy_proj_1y, jpy_proj_5y, jpy_proj_10y],
    )

    jpy_proj_fitter = CurveFitter(
        name="JPY_PROJ_FIT", currency="JPY", curve=jpy_proj_curve,
        points=[jpy_proj_1y, jpy_proj_5y, jpy_proj_10y],
        quotes=[jpy_ois_1y, jpy_ois_5y, jpy_ois_10y],
        target_swaps=[
            IRSwapFixedFloatApprox(symbol="JPY_T1", tenor_years=1.0, fixed_rate=0.001, curve=jpy_proj_curve, notional=50_000_000, is_target=True),
            IRSwapFixedFloatApprox(symbol="JPY_T5", tenor_years=5.0, fixed_rate=0.005, curve=jpy_proj_curve, notional=50_000_000, is_target=True),
            IRSwapFixedFloatApprox(symbol="JPY_T10", tenor_years=10.0, fixed_rate=0.010, curve=jpy_proj_curve, notional=50_000_000, is_target=True),
        ]
    )
    jpy_proj_fitter.solve()

    # ── 3. JPY DISC USD (Multi-Curve Basis Fit via FloatFloat) ──────────────
    jpy_xccy_1y  = SwapQuote(symbol="IR_JPY_XCCY_QUOTE.1Y",  rate=-0.001)
    jpy_xccy_5y  = SwapQuote(symbol="IR_JPY_XCCY_QUOTE.5Y",  rate=-0.002)
    jpy_xccy_10y = SwapQuote(symbol="IR_JPY_XCCY_QUOTE.10Y", rate=-0.003)

    jpy_disc_1y = YieldCurvePoint(name="IR_JPY_DISC_USD.1Y", tenor_years=1.0, currency="JPY", quote_ref=jpy_xccy_1y, is_fitted=True)
    jpy_disc_5y = YieldCurvePoint(name="IR_JPY_DISC_USD.5Y", tenor_years=5.0, currency="JPY", quote_ref=jpy_xccy_5y, is_fitted=True)
    jpy_disc_10y = YieldCurvePoint(name="IR_JPY_DISC_USD.10Y", tenor_years=10.0, currency="JPY", quote_ref=jpy_xccy_10y, is_fitted=True)

    jpy_disc_curve = YieldCurve(
        name="JPY_XCCY", currency="JPY",
        points=[jpy_disc_1y, jpy_disc_5y, jpy_disc_10y],
    )

    jpy_xccy_fitter = CurveFitter(
        name="JPY_XCCY_FIT", currency="JPY", curve=jpy_disc_curve,
        points=[jpy_disc_1y, jpy_disc_5y, jpy_disc_10y],
        quotes=[jpy_xccy_1y, jpy_xccy_5y, jpy_xccy_10y],
        target_swaps=[
            IRSwapFloatFloat(
                symbol="XCCY_T1", tenor_years=1.0, is_target=True,
                leg1_notional=100_000_000, leg1_discount_curve=jpy_disc_curve, leg1_projection_curve=jpy_proj_curve, float_spread=-0.001,
                leg2_notional=100_000_000, leg2_discount_curve=usd_curve, leg2_projection_curve=usd_curve,
                exchange_notional=True, side="RECEIVER"
            ),
            IRSwapFloatFloat(
                symbol="XCCY_T5", tenor_years=5.0, is_target=True,
                leg1_notional=100_000_000, leg1_discount_curve=jpy_disc_curve, leg1_projection_curve=jpy_proj_curve, float_spread=-0.002,
                leg2_notional=100_000_000, leg2_discount_curve=usd_curve, leg2_projection_curve=usd_curve,
                exchange_notional=True, side="RECEIVER"
            ),
            IRSwapFloatFloat(
                symbol="XCCY_T10", tenor_years=10.0, is_target=True,
                leg1_notional=100_000_000, leg1_discount_curve=jpy_disc_curve, leg1_projection_curve=jpy_proj_curve, float_spread=-0.003,
                leg2_notional=100_000_000, leg2_discount_curve=usd_curve, leg2_projection_curve=usd_curve,
                exchange_notional=True, side="RECEIVER"
            ),
        ]
    )
    jpy_xccy_fitter.solve()
    
    flush()

    return {
        "usd": {"curve": usd_curve, "fitter": usd_fitter},
        "jpy_proj": {"curve": jpy_proj_curve, "fitter": jpy_proj_fitter},
        "jpy_disc": {"curve": jpy_disc_curve, "fitter": jpy_xccy_fitter, "swaps": jpy_xccy_fitter.target_swaps},
    }

class TestXCCYFit:
    def test_usd_curve_solved(self, xccy_system):
        curve = xccy_system["usd"]["curve"]
        # Solved zero rates should be positive
        assert curve.points[0].rate > 0.0

    def test_jpy_proj_curve_solved(self, xccy_system):
        curve = xccy_system["jpy_proj"]["curve"]
        assert curve.points[0].rate > 0.0

    def test_jpy_xccy_swaps_priced_to_par(self, xccy_system):
        swaps = xccy_system["jpy_disc"]["swaps"]
        
        # After fitter.solve(), all target swaps must have npv ≈ 0.0
        for swap in swaps:
            assert abs(swap.npv) < 1_000

    def test_jpy_xccy_spread_moves_npv(self, xccy_system):
        swap_par = xccy_system["jpy_disc"]["swaps"][1] # 5Y
        
        # Par swap has spread=-0.002, so its NPV is ~0.
        npv_at_par = swap_par.npv
        
        # If the market spread tightens (-0.001), 
        # Leg 1 receives less spread (negative means we pay the spread). 
        # A tighter XCCY basis spread implies JPY is becoming less scarce vs USD.
        # So we receive more (-0.001 is greater than -0.002).
        usd_curve = xccy_system["usd"]["curve"]
        jpy_proj_curve = xccy_system["jpy_proj"]["curve"]
        jpy_disc_curve = xccy_system["jpy_disc"]["curve"]
        
        swap_tighter = IRSwapFloatFloat(
            symbol="XCCY_TEST", tenor_years=5.0,
            leg1_notional=100_000_000, leg1_discount_curve=jpy_disc_curve, leg1_projection_curve=jpy_proj_curve, float_spread=-0.001,
            leg2_notional=100_000_000, leg2_discount_curve=usd_curve, leg2_projection_curve=usd_curve,
            exchange_notional=True, side="RECEIVER"
        )
        
        # Should be a clear change (e.g. 10 bps * 5 years * 100M ≈ 500k)
        assert swap_tighter.npv > npv_at_par + 10_000
