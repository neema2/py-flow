import pytest
from instruments.ir_swap_float_float import IRSwapFloatFloat
from marketmodel.yield_curve import YieldCurve, YieldCurvePoint
from reactive.expr import eval_cached

PILLARS = [
    ("IR_USD_DISC_USD.1Y", 1.0, 0.03),
    ("IR_USD_DISC_USD.5Y", 5.0, 0.05),
    ("IR_USD_DISC_USD.10Y", 10.0, 0.07),
]

@pytest.fixture
def curve():
    points = [
        YieldCurvePoint(name=name, tenor_years=tenor, fitted_rate=rate, is_fitted=True)
        for name, tenor, rate in PILLARS
    ]
    return YieldCurve(name="TEST", points=points)

@pytest.fixture
def ctx():
    return {name: rate for name, _, rate in PILLARS}

def test_float_float_basic(curve, ctx):
    """
    Test basic explicit float/float swap evaluation.
    """
    swap = IRSwapFloatFloat(
        symbol="ff1",
        leg1_notional=100e6,
        leg2_notional=100e6,
        tenor_years=5.0,
        leg1_discount_curve=curve,
        leg1_projection_curve=curve,
        leg2_discount_curve=curve,
        leg2_projection_curve=curve,
        side="RECEIVER"
    )

    leg1_pv = eval_cached(swap.leg1_float_leg_pv(), ctx)
    leg2_pv = eval_cached(swap.leg2_float_leg_pv(), ctx)
    npv = eval_cached(swap.npv(), ctx)

    # Identical curves and notionals should mean PVs are the same, NPV is 0
    assert leg1_pv == pytest.approx(leg2_pv, abs=1e-8)
    assert npv == pytest.approx(0.0, abs=1e-8)

def test_float_float_exchange(curve, ctx):
    """
    Test notional exchange impact.
    """
    swap = IRSwapFloatFloat(
        symbol="ff1",
        leg1_notional=100e6,
        leg2_notional=100e6,
        tenor_years=5.0,
        leg1_discount_curve=curve,
        leg1_projection_curve=curve,
        leg2_discount_curve=curve,
        leg2_projection_curve=curve,
        exchange_notional=True,
        side="RECEIVER"
    )

    # If notionals are exchanged, the PV of the leg should be 100 * (1 - df) + (df - 1)*100 = 0? 
    # Wait, simple float leg (without exchange) PV = notional * (1 - df_end)
    # If we add exchange: notional * df_end - notional (pay at start)
    # Total = notional - notional*df_end + notional*df_end - notional = 0
    leg1_pv = eval_cached(swap.leg1_float_leg_pv(), ctx)
    
    assert leg1_pv == pytest.approx(0.0, abs=1e-6)
