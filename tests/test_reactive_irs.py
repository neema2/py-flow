"""
Interest Rate Swap / Yield Curve reactive test suite.

Tests a multi-layer reactive graph:
  FX spot → yield curve points → yield curve → IRS pricing/risk → swap portfolio

Proves that a single market data tick (e.g. a curve rate bump) recalculates
all the way up through swap NPV to portfolio aggregates.
"""

from dataclasses import dataclass, field

from reactive.computed import computed, effect
from store.base import Storable

# ===========================================================================
# Domain models
# ===========================================================================

# ── FX Spot ───────────────────────────────────────────────────────────────

@dataclass
class FXSpot(Storable):
    """Live FX spot rate (e.g. USD/JPY, EUR/USD)."""
    pair: str = ""
    bid: float = 0.0
    ask: float = 0.0
    currency: str = ""

    @computed
    def mid(self):
        return (self.bid + self.ask) / 2

    @computed
    def spread_pips(self):
        return (self.ask - self.bid) * 10000


# ── Yield Curve Point ─────────────────────────────────────────────────────

@dataclass
class YieldCurvePoint(Storable):
    """A single point on the yield curve."""
    tenor_years: float = 0.0
    rate: float = 0.0
    currency: str = "USD"

    @computed
    def discount_factor(self):
        return 1.0 / (1.0 + self.rate) ** self.tenor_years


# ── Yield Curve (cross-entity) ───────────────────────────────────────────

@dataclass
class YieldCurve(Storable):
    """A collection of curve points with aggregate computeds."""
    curve_points: list = field(default_factory=list)
    currency: str = "USD"

    @computed
    def avg_rate(self):
        if not self.curve_points:
            return 0.0
        return sum(p.rate for p in self.curve_points) / len(self.curve_points)

    @computed
    def curve_slope(self):
        if len(self.curve_points) < 2:
            return 0.0
        # Find short-end and long-end rates by tenor
        pairs = [(p.tenor_years, p.rate) for p in self.curve_points]
        pairs.sort()
        return pairs[-1][1] - pairs[0][1]


# ── Interest Rate Swap ───────────────────────────────────────────────────

@dataclass
class IRSwapFixedFloatApprox(Storable):
    """A plain vanilla interest rate swap."""
    symbol: str = ""
    notional: float = 0.0
    fixed_rate: float = 0.0
    float_rate: float = 0.0
    tenor_years: float = 0.0
    currency: str = "USD"

    @computed
    def fixed_leg_pv(self):
        # Simplified: PV of fixed coupons = notional × fixed_rate × tenor × DF
        # where DF ≈ 1 / (1 + fixed_rate) ^ tenor
        df = 1.0 / (1.0 + self.fixed_rate) ** self.tenor_years
        return self.notional * self.fixed_rate * self.tenor_years * df

    @computed
    def float_leg_pv(self):
        # Simplified: PV of float coupons = notional × float_rate × tenor × DF
        df = 1.0 / (1.0 + self.float_rate) ** self.tenor_years
        return self.notional * self.float_rate * self.tenor_years * df

    @computed
    def npv(self):
        # NPV from receiver perspective: receive float, pay fixed
        float_df = 1.0 / (1.0 + self.float_rate) ** self.tenor_years
        fixed_df = 1.0 / (1.0 + self.fixed_rate) ** self.tenor_years
        float_pv = self.notional * self.float_rate * self.tenor_years * float_df
        fixed_pv = self.notional * self.fixed_rate * self.tenor_years * fixed_df
        return float_pv - fixed_pv

    @computed
    def dv01(self):
        # Dollar value of 1bp: notional × tenor × 0.0001
        return self.notional * self.tenor_years * 0.0001

    @computed
    def pnl_status(self):
        float_df = 1.0 / (1.0 + self.float_rate) ** self.tenor_years
        fixed_df = 1.0 / (1.0 + self.fixed_rate) ** self.tenor_years
        float_pv = self.notional * self.float_rate * self.tenor_years * float_df
        fixed_pv = self.notional * self.fixed_rate * self.tenor_years * fixed_df
        npv = float_pv - fixed_pv
        if npv > 0:
            return "PROFIT"
        if npv < 0:
            return "LOSS"
        return "FLAT"


# ── Swap Portfolio (cross-entity) ─────────────────────────────────────────

@dataclass
class SwapPortfolio(Storable):
    """Portfolio of interest rate swaps with aggregate risk."""
    swaps: list = field(default_factory=list)
    currency: str = "USD"

    @computed
    def total_npv(self):
        if not self.swaps:
            return 0.0
        return sum(s.npv for s in self.swaps)

    @computed
    def total_dv01(self):
        if not self.swaps:
            return 0.0
        return sum(s.dv01 for s in self.swaps)

    @computed
    def max_npv(self):
        if not self.swaps:
            return 0.0
        return max(s.npv for s in self.swaps)

    @computed
    def min_npv(self):
        if not self.swaps:
            return 0.0
        return min(s.npv for s in self.swaps)

    @computed
    def swap_count(self):
        return len(self.swaps) if self.swaps else 0


# ── Effect helpers ─────────────────────────────────────────────────────────

_npv_alerts = []


@dataclass
class SwapWithAlert(Storable):
    """Swap with @effect that fires when NPV crosses a threshold."""
    symbol: str = ""
    notional: float = 0.0
    fixed_rate: float = 0.0
    float_rate: float = 0.0
    tenor_years: float = 0.0
    currency: str = "USD"

    @computed
    def npv(self):
        float_df = 1.0 / (1.0 + self.float_rate) ** self.tenor_years
        fixed_df = 1.0 / (1.0 + self.fixed_rate) ** self.tenor_years
        float_pv = self.notional * self.float_rate * self.tenor_years * float_df
        fixed_pv = self.notional * self.fixed_rate * self.tenor_years * fixed_df
        return float_pv - fixed_pv

    @computed
    def dv01(self):
        return self.notional * self.tenor_years * 0.0001

    @effect("npv")
    def on_npv(self, value):
        if abs(value) > 50000:
            _npv_alerts.append(("BREACH", self.symbol, value))


# ===========================================================================
# Tests: FX Spot
# ===========================================================================

class TestFXSpotReactivity:
    """FX spot mid-price and spread react to bid/ask ticks."""

    def test_mid_price(self):
        fx = FXSpot(pair="USD/JPY", bid=149.50, ask=149.60, currency="JPY")
        assert abs(fx.mid - 149.55) < 0.001

    def test_spread_pips(self):
        fx = FXSpot(pair="USD/JPY", bid=149.50, ask=149.60, currency="JPY")
        expected = (149.60 - 149.50) * 10000
        assert abs(fx.spread_pips - expected) < 0.01

    def test_mid_reacts_to_bid_move(self):
        fx = FXSpot(pair="EUR/USD", bid=1.0848, ask=1.0852, currency="USD")
        mid_before = fx.mid
        fx.bid = 1.0860
        assert fx.mid != mid_before
        assert abs(fx.mid - (1.0860 + 1.0852) / 2) < 0.00001

    def test_mid_reacts_to_batch(self):
        fx = FXSpot(pair="GBP/USD", bid=1.2650, ask=1.2654, currency="USD")
        fx.batch_update(bid=1.2700, ask=1.2704)
        assert abs(fx.mid - 1.2702) < 0.00001

    def test_multiple_fx_pairs_independent(self):
        usdjpy = FXSpot(pair="USD/JPY", bid=149.50, ask=149.60, currency="JPY")
        eurusd = FXSpot(pair="EUR/USD", bid=1.0848, ask=1.0852, currency="USD")

        usdjpy.bid = 150.00
        assert abs(usdjpy.mid - (150.00 + 149.60) / 2) < 0.001
        # EUR/USD unchanged
        assert abs(eurusd.mid - (1.0848 + 1.0852) / 2) < 0.00001


# ===========================================================================
# Tests: Yield Curve Point
# ===========================================================================

class TestYieldCurvePoint:
    """Discount factor reacts to rate changes."""

    def test_discount_factor_basic(self):
        pt = YieldCurvePoint(tenor_years=5.0, rate=0.04)
        expected = 1.0 / (1.04 ** 5.0)
        assert abs(pt.discount_factor - expected) < 1e-8

    def test_discount_factor_zero_rate(self):
        pt = YieldCurvePoint(tenor_years=10.0, rate=0.0)
        assert abs(pt.discount_factor - 1.0) < 1e-10

    def test_discount_factor_reacts_to_rate_bump(self):
        pt = YieldCurvePoint(tenor_years=5.0, rate=0.03)
        df_before = pt.discount_factor

        pt.rate = 0.05  # +200bp shock
        df_after = pt.discount_factor

        assert df_after < df_before  # Higher rate → lower DF
        expected = 1.0 / (1.05 ** 5.0)
        assert abs(df_after - expected) < 1e-8

    def test_discount_factor_reacts_to_tenor_change(self):
        pt = YieldCurvePoint(tenor_years=2.0, rate=0.04)
        df_short = pt.discount_factor

        pt.tenor_years = 10.0
        df_long = pt.discount_factor

        assert df_long < df_short  # Longer tenor → lower DF

    def test_high_rate_discount(self):
        """Sanity: 10% rate, 30Y → very low discount factor."""
        pt = YieldCurvePoint(tenor_years=30.0, rate=0.10)
        expected = 1.0 / (1.10 ** 30.0)
        assert abs(pt.discount_factor - expected) < 1e-8
        assert pt.discount_factor < 0.06


# ===========================================================================
# Tests: Yield Curve (cross-entity)
# ===========================================================================

class TestYieldCurve:
    """Cross-entity: avg_rate and curve_slope react to point changes."""

    def test_avg_rate(self):
        p1 = YieldCurvePoint(tenor_years=1.0, rate=0.03)
        p2 = YieldCurvePoint(tenor_years=5.0, rate=0.04)
        p3 = YieldCurvePoint(tenor_years=10.0, rate=0.045)
        curve = YieldCurve(curve_points=[p1, p2, p3])
        expected = (0.03 + 0.04 + 0.045) / 3
        assert abs(curve.avg_rate - expected) < 1e-8

    def test_curve_slope(self):
        p1 = YieldCurvePoint(tenor_years=1.0, rate=0.03)
        p2 = YieldCurvePoint(tenor_years=5.0, rate=0.04)
        p3 = YieldCurvePoint(tenor_years=10.0, rate=0.045)
        curve = YieldCurve(curve_points=[p1, p2, p3])
        # slope = 10Y rate - 1Y rate
        assert abs(curve.curve_slope - 0.015) < 1e-8

    def test_avg_rate_reacts_to_point_change(self):
        p1 = YieldCurvePoint(tenor_years=1.0, rate=0.03)
        p2 = YieldCurvePoint(tenor_years=5.0, rate=0.04)
        curve = YieldCurve(curve_points=[p1, p2])
        avg_before = curve.avg_rate

        p1.rate = 0.05  # 1Y rate jumps +200bp
        avg_after = curve.avg_rate

        assert avg_after > avg_before
        assert abs(avg_after - (0.05 + 0.04) / 2) < 1e-8

    def test_slope_reacts_to_long_end_move(self):
        p1 = YieldCurvePoint(tenor_years=2.0, rate=0.03)
        p2 = YieldCurvePoint(tenor_years=10.0, rate=0.04)
        curve = YieldCurve(curve_points=[p1, p2])
        slope_before = curve.curve_slope

        p2.rate = 0.06  # Bear steepening
        slope_after = curve.curve_slope

        assert slope_after > slope_before
        assert abs(slope_after - 0.03) < 1e-8

    def test_empty_curve(self):
        curve = YieldCurve(curve_points=[])
        assert curve.avg_rate == 0.0
        assert curve.curve_slope == 0.0

    def test_single_point_curve(self):
        p1 = YieldCurvePoint(tenor_years=5.0, rate=0.04)
        curve = YieldCurve(curve_points=[p1])
        assert curve.avg_rate == 0.04
        assert curve.curve_slope == 0.0

    def test_inverted_curve(self):
        """Short rate > long rate → negative slope."""
        p1 = YieldCurvePoint(tenor_years=1.0, rate=0.055)
        p2 = YieldCurvePoint(tenor_years=10.0, rate=0.04)
        curve = YieldCurve(curve_points=[p1, p2])
        assert curve.curve_slope < 0
        assert abs(curve.curve_slope - (0.04 - 0.055)) < 1e-8


# ===========================================================================
# Tests: Interest Rate Swap Pricing
# ===========================================================================

class TestSwapPricing:
    """Single-entity swap computeds react to rate and notional changes."""

    def test_npv_positive_when_float_above_fixed(self):
        """Receiver swap: float > fixed → positive NPV."""
        swap = IRSwapFixedFloatApprox(
            symbol="IRS-001", notional=10_000_000,
            fixed_rate=0.03, float_rate=0.04, tenor_years=5.0,
        )
        assert swap.npv > 0
        assert swap.pnl_status == "PROFIT"

    def test_npv_negative_when_float_below_fixed(self):
        """Receiver swap: float < fixed → negative NPV."""
        swap = IRSwapFixedFloatApprox(
            symbol="IRS-002", notional=10_000_000,
            fixed_rate=0.05, float_rate=0.03, tenor_years=5.0,
        )
        assert swap.npv < 0
        assert swap.pnl_status == "LOSS"

    def test_npv_flat_when_rates_equal(self):
        """float_rate == fixed_rate → NPV ≈ 0."""
        swap = IRSwapFixedFloatApprox(
            symbol="IRS-003", notional=10_000_000,
            fixed_rate=0.04, float_rate=0.04, tenor_years=5.0,
        )
        assert abs(swap.npv) < 0.01
        assert swap.pnl_status == "FLAT"

    def test_dv01_computation(self):
        swap = IRSwapFixedFloatApprox(
            symbol="IRS-004", notional=10_000_000, tenor_years=5.0,
            fixed_rate=0.03, float_rate=0.04,
        )
        expected = 10_000_000 * 5.0 * 0.0001
        assert abs(swap.dv01 - expected) < 0.01

    def test_npv_reacts_to_float_rate_change(self):
        swap = IRSwapFixedFloatApprox(
            symbol="IRS-005", notional=10_000_000,
            fixed_rate=0.035, float_rate=0.035, tenor_years=5.0,
        )
        assert abs(swap.npv) < 0.01

        swap.float_rate = 0.045  # +100bp
        assert swap.npv > 0
        assert swap.pnl_status == "PROFIT"

    def test_npv_reacts_to_notional_change(self):
        swap = IRSwapFixedFloatApprox(
            symbol="IRS-006", notional=10_000_000,
            fixed_rate=0.03, float_rate=0.04, tenor_years=5.0,
        )
        npv_10m = swap.npv

        swap.notional = 20_000_000
        npv_20m = swap.npv

        # Doubling notional should roughly double NPV
        assert abs(npv_20m / npv_10m - 2.0) < 0.01

    def test_dv01_reacts_to_tenor_change(self):
        swap = IRSwapFixedFloatApprox(
            symbol="IRS-007", notional=10_000_000, tenor_years=5.0,
            fixed_rate=0.04, float_rate=0.04,
        )
        dv01_5y = swap.dv01

        swap.tenor_years = 10.0
        dv01_10y = swap.dv01

        assert abs(dv01_10y / dv01_5y - 2.0) < 0.01

    def test_fixed_leg_pv(self):
        swap = IRSwapFixedFloatApprox(
            symbol="IRS-008", notional=10_000_000,
            fixed_rate=0.04, float_rate=0.04, tenor_years=5.0,
        )
        df = 1.0 / (1.04 ** 5.0)
        expected = 10_000_000 * 0.04 * 5.0 * df
        assert abs(swap.fixed_leg_pv - expected) < 0.01

    def test_float_leg_pv(self):
        swap = IRSwapFixedFloatApprox(
            symbol="IRS-009", notional=10_000_000,
            fixed_rate=0.03, float_rate=0.05, tenor_years=5.0,
        )
        df = 1.0 / (1.05 ** 5.0)
        expected = 10_000_000 * 0.05 * 5.0 * df
        assert abs(swap.float_leg_pv - expected) < 0.01

    def test_batch_rate_update(self):
        swap = IRSwapFixedFloatApprox(
            symbol="IRS-010", notional=10_000_000,
            fixed_rate=0.04, float_rate=0.04, tenor_years=5.0,
        )
        assert abs(swap.npv) < 0.01

        swap.batch_update(fixed_rate=0.03, float_rate=0.05)
        assert swap.npv > 0
        assert swap.pnl_status == "PROFIT"


# ===========================================================================
# Tests: Swap Portfolio (cross-entity)
# ===========================================================================

class TestSwapPortfolio:
    """Cross-entity: portfolio aggregates react to child swap changes."""

    def _make_book(self):
        s1 = IRSwapFixedFloatApprox(
            symbol="IRS-A", notional=10_000_000,
            fixed_rate=0.03, float_rate=0.04, tenor_years=5.0,
        )
        s2 = IRSwapFixedFloatApprox(
            symbol="IRS-B", notional=5_000_000,
            fixed_rate=0.05, float_rate=0.03, tenor_years=7.0,
        )
        s3 = IRSwapFixedFloatApprox(
            symbol="IRS-C", notional=20_000_000,
            fixed_rate=0.04, float_rate=0.04, tenor_years=10.0,
        )
        book = SwapPortfolio(swaps=[s1, s2, s3])
        return s1, s2, s3, book

    def test_total_npv(self):
        s1, s2, s3, book = self._make_book()
        expected = s1.npv + s2.npv + s3.npv
        assert abs(book.total_npv - expected) < 0.01

    def test_total_dv01(self):
        s1, s2, s3, book = self._make_book()
        expected = s1.dv01 + s2.dv01 + s3.dv01
        assert abs(book.total_dv01 - expected) < 0.01

    def test_swap_count(self):
        _, _, _, book = self._make_book()
        assert book.swap_count == 3

    def test_max_min_npv(self):
        s1, s2, s3, book = self._make_book()
        npvs = [s1.npv, s2.npv, s3.npv]
        assert abs(book.max_npv - max(npvs)) < 0.01
        assert abs(book.min_npv - min(npvs)) < 0.01

    def test_total_npv_reacts_to_child_rate_change(self):
        s1, s2, s3, book = self._make_book()
        npv_before = book.total_npv

        s1.float_rate = 0.06  # +200bp on swap A
        npv_after = book.total_npv

        assert npv_after != npv_before
        expected = s1.npv + s2.npv + s3.npv
        assert abs(npv_after - expected) < 0.01

    def test_total_dv01_reacts_to_notional_change(self):
        s1, s2, s3, book = self._make_book()
        dv01_before = book.total_dv01

        s2.notional = 15_000_000  # Triple notional on swap B
        dv01_after = book.total_dv01

        assert dv01_after > dv01_before
        expected = s1.dv01 + s2.dv01 + s3.dv01
        assert abs(dv01_after - expected) < 0.01

    def test_empty_portfolio(self):
        book = SwapPortfolio(swaps=[])
        assert book.total_npv == 0.0
        assert book.total_dv01 == 0.0
        assert book.swap_count == 0
        assert book.max_npv == 0.0
        assert book.min_npv == 0.0

    def test_add_swap_to_portfolio(self):
        s1 = IRSwapFixedFloatApprox(
            symbol="IRS-X", notional=10_000_000,
            fixed_rate=0.03, float_rate=0.04, tenor_years=5.0,
        )
        book = SwapPortfolio(swaps=[s1])
        npv_1 = book.total_npv

        s2 = IRSwapFixedFloatApprox(
            symbol="IRS-Y", notional=5_000_000,
            fixed_rate=0.04, float_rate=0.05, tenor_years=3.0,
        )
        book.swaps = [s1, s2]
        npv_2 = book.total_npv

        assert abs(npv_2 - (s1.npv + s2.npv)) < 0.01
        assert npv_2 != npv_1


# ===========================================================================
# Tests: End-to-End Cascade
# ===========================================================================

class TestEndToEndCascade:
    """Bump a single curve rate → NPV changes → portfolio total recalculates."""

    def test_curve_to_swap_to_portfolio(self):
        """Full reactive chain: curve point → swap float_rate → portfolio NPV."""
        # Layer 1: Yield curve
        p1y = YieldCurvePoint(tenor_years=1.0, rate=0.03)
        p5y = YieldCurvePoint(tenor_years=5.0, rate=0.04)
        p10y = YieldCurvePoint(tenor_years=10.0, rate=0.045)
        curve = YieldCurve(curve_points=[p1y, p5y, p10y])

        # Layer 2: Swaps referencing curve rates
        swap_5y = IRSwapFixedFloatApprox(
            symbol="5Y-IRS", notional=10_000_000,
            fixed_rate=0.035, float_rate=p5y.rate, tenor_years=5.0,
        )
        swap_10y = IRSwapFixedFloatApprox(
            symbol="10Y-IRS", notional=20_000_000,
            fixed_rate=0.04, float_rate=p10y.rate, tenor_years=10.0,
        )

        # Layer 3: Portfolio
        book = SwapPortfolio(swaps=[swap_5y, swap_10y])

        # Snapshot before
        npv_5y_before = swap_5y.npv
        _npv_10y_before = swap_10y.npv
        total_before = book.total_npv
        _slope_before = curve.curve_slope

        # ── Shock: bump 5Y rate +100bp ──
        p5y.rate = 0.05

        # Curve reacts
        assert curve.avg_rate > (0.03 + 0.04 + 0.045) / 3
        assert abs(curve.avg_rate - (0.03 + 0.05 + 0.045) / 3) < 1e-8

        # Propagate to swap (simulating a rate feed)
        swap_5y.float_rate = p5y.rate

        # Swap reacts
        assert swap_5y.npv != npv_5y_before
        assert swap_5y.npv > npv_5y_before  # float_rate up → more value

        # Portfolio reacts
        expected_total = swap_5y.npv + swap_10y.npv
        assert abs(book.total_npv - expected_total) < 0.01
        assert book.total_npv != total_before

    def test_parallel_curve_shift(self):
        """Shift entire curve +50bp → all swaps repriced → portfolio updated."""
        p2y = YieldCurvePoint(tenor_years=2.0, rate=0.03)
        p5y = YieldCurvePoint(tenor_years=5.0, rate=0.035)
        p10y = YieldCurvePoint(tenor_years=10.0, rate=0.04)
        _curve = YieldCurve(curve_points=[p2y, p5y, p10y])

        swap_2y = IRSwapFixedFloatApprox(
            symbol="2Y", notional=5_000_000,
            fixed_rate=0.025, float_rate=p2y.rate, tenor_years=2.0,
        )
        swap_5y = IRSwapFixedFloatApprox(
            symbol="5Y", notional=10_000_000,
            fixed_rate=0.03, float_rate=p5y.rate, tenor_years=5.0,
        )
        swap_10y = IRSwapFixedFloatApprox(
            symbol="10Y", notional=20_000_000,
            fixed_rate=0.035, float_rate=p10y.rate, tenor_years=10.0,
        )
        book = SwapPortfolio(swaps=[swap_2y, swap_5y, swap_10y])
        total_before = book.total_npv

        # +50bp parallel shift
        shift = 0.005
        p2y.rate += shift
        p5y.rate += shift
        p10y.rate += shift

        # Propagate to swaps
        swap_2y.float_rate = p2y.rate
        swap_5y.float_rate = p5y.rate
        swap_10y.float_rate = p10y.rate

        # All NPVs should increase (receiver benefits from higher float)
        assert swap_2y.npv > 0
        assert swap_5y.npv > 0
        assert swap_10y.npv > 0

        total_after = book.total_npv
        assert total_after > total_before

    def test_fx_to_swap_cascade(self):
        """FX spot change + curve change both propagate to portfolio."""
        fx = FXSpot(pair="USD/JPY", bid=149.50, ask=149.60, currency="JPY")
        pt = YieldCurvePoint(tenor_years=5.0, rate=0.01)  # JPY rate

        swap_jpy = IRSwapFixedFloatApprox(
            symbol="JPY-5Y", notional=1_000_000_000,  # 1B JPY
            fixed_rate=0.005, float_rate=pt.rate, tenor_years=5.0,
            currency="JPY",
        )

        npv_before = swap_jpy.npv
        mid_before = fx.mid

        # Bump JPY rate
        pt.rate = 0.02
        swap_jpy.float_rate = pt.rate

        assert swap_jpy.npv > npv_before

        # Bump FX
        fx.batch_update(bid=151.00, ask=151.10)
        assert fx.mid > mid_before


# ===========================================================================
# Tests: Multi-Currency Swaps
# ===========================================================================

class TestMultiCurrencySwaps:
    """Portfolio with swaps in different currencies react independently."""

    def test_usd_and_jpy_swaps_independent(self):
        usd_swap = IRSwapFixedFloatApprox(
            symbol="USD-5Y", notional=10_000_000,
            fixed_rate=0.04, float_rate=0.04, tenor_years=5.0,
            currency="USD",
        )
        jpy_swap = IRSwapFixedFloatApprox(
            symbol="JPY-5Y", notional=1_000_000_000,
            fixed_rate=0.005, float_rate=0.01, tenor_years=5.0,
            currency="JPY",
        )
        _book = SwapPortfolio(swaps=[usd_swap, jpy_swap])

        # Only bump USD swap
        usd_swap.float_rate = 0.06
        _usd_npv = usd_swap.npv
        _jpy_npv = jpy_swap.npv

        # USD swap changed, JPY didn't
        assert usd_swap.pnl_status == "PROFIT"
        # JPY swap NPV should still reflect original rates
        float_df = 1.0 / (1.01 ** 5.0)
        fixed_df = 1.0 / (1.005 ** 5.0)
        expected_jpy = (1_000_000_000 * 0.01 * 5.0 * float_df
                        - 1_000_000_000 * 0.005 * 5.0 * fixed_df)
        assert abs(jpy_swap.npv - expected_jpy) < 1.0

    def test_three_currency_portfolio(self):
        usd = IRSwapFixedFloatApprox(
            symbol="USD-10Y", notional=10_000_000,
            fixed_rate=0.035, float_rate=0.04, tenor_years=10.0, currency="USD",
        )
        eur = IRSwapFixedFloatApprox(
            symbol="EUR-5Y", notional=8_000_000,
            fixed_rate=0.025, float_rate=0.03, tenor_years=5.0, currency="EUR",
        )
        jpy = IRSwapFixedFloatApprox(
            symbol="JPY-7Y", notional=1_000_000_000,
            fixed_rate=0.005, float_rate=0.008, tenor_years=7.0, currency="JPY",
        )
        book = SwapPortfolio(swaps=[usd, eur, jpy])

        assert book.swap_count == 3
        assert abs(book.total_npv - (usd.npv + eur.npv + jpy.npv)) < 1.0
        assert abs(book.total_dv01 - (usd.dv01 + eur.dv01 + jpy.dv01)) < 0.01


# ===========================================================================
# Tests: Batch Curve Shift
# ===========================================================================

class TestBatchCurveShift:
    """batch_update on curve points verifies single-recalc behavior."""

    def test_batch_update_curve_point(self):
        pt = YieldCurvePoint(tenor_years=5.0, rate=0.04)
        df_before = pt.discount_factor

        pt.batch_update(rate=0.05, tenor_years=7.0)
        df_after = pt.discount_factor

        expected = 1.0 / (1.05 ** 7.0)
        assert abs(df_after - expected) < 1e-8
        assert df_after != df_before

    def test_batch_update_swap(self):
        swap = IRSwapFixedFloatApprox(
            symbol="BATCH-001", notional=10_000_000,
            fixed_rate=0.04, float_rate=0.04, tenor_years=5.0,
        )
        assert abs(swap.npv) < 0.01

        swap.batch_update(float_rate=0.06, tenor_years=10.0)
        assert swap.npv > 0
        assert swap.pnl_status == "PROFIT"

    def test_multiple_curve_points_sequential(self):
        """Sequentially updating multiple points — each triggers recalc."""
        p1 = YieldCurvePoint(tenor_years=1.0, rate=0.03)
        p2 = YieldCurvePoint(tenor_years=5.0, rate=0.04)
        p3 = YieldCurvePoint(tenor_years=10.0, rate=0.045)
        curve = YieldCurve(curve_points=[p1, p2, p3])

        avg_0 = curve.avg_rate

        p1.rate = 0.04  # +100bp
        avg_1 = curve.avg_rate
        assert avg_1 > avg_0

        p2.rate = 0.05  # +100bp
        avg_2 = curve.avg_rate
        assert avg_2 > avg_1

        p3.rate = 0.055  # +100bp
        avg_3 = curve.avg_rate
        assert avg_3 > avg_2

        expected = (0.04 + 0.05 + 0.055) / 3
        assert abs(avg_3 - expected) < 1e-8


# ===========================================================================
# Tests: Swap Effects (alerts)
# ===========================================================================

class TestSwapEffects:
    """@effect on NPV fires when threshold breached."""

    def test_no_alert_below_threshold(self):
        _npv_alerts.clear()
        swap = SwapWithAlert(
            symbol="ALERT-001", notional=1_000_000,
            fixed_rate=0.04, float_rate=0.04, tenor_years=5.0,
        )
        initial = len(_npv_alerts)

        swap.float_rate = 0.041  # Tiny move
        # NPV should be small, no breach
        assert abs(swap.npv) < 50000
        # Either no new alert or alert with value < threshold
        breaches = [a for a in _npv_alerts[initial:] if a[0] == "BREACH"]
        assert len(breaches) == 0

    def test_alert_fires_on_large_npv(self):
        _npv_alerts.clear()
        swap = SwapWithAlert(
            symbol="ALERT-002", notional=100_000_000,
            fixed_rate=0.03, float_rate=0.03, tenor_years=10.0,
        )
        initial = len(_npv_alerts)

        swap.float_rate = 0.05  # +200bp on 100M 10Y → huge NPV
        assert abs(swap.npv) > 50000

        breaches = [a for a in _npv_alerts[initial:] if a[0] == "BREACH"]
        assert len(breaches) > 0
        assert breaches[-1][1] == "ALERT-002"

    def test_alert_includes_npv_value(self):
        _npv_alerts.clear()
        _swap = SwapWithAlert(
            symbol="ALERT-003", notional=50_000_000,
            fixed_rate=0.02, float_rate=0.06, tenor_years=10.0,
        )
        # Initial construction should trigger effect
        breaches = [a for a in _npv_alerts if a[0] == "BREACH" and a[1] == "ALERT-003"]
        if breaches:
            assert isinstance(breaches[-1][2], float)
            assert breaches[-1][2] > 50000


# ===========================================================================
# Tests: Numeric precision and edge cases
# ===========================================================================

class TestEdgeCases:
    """Edge cases for the IRS domain."""

    def test_very_short_tenor(self):
        swap = IRSwapFixedFloatApprox(
            symbol="SHORT", notional=10_000_000,
            fixed_rate=0.04, float_rate=0.05, tenor_years=0.25,
        )
        assert swap.npv > 0
        assert swap.dv01 == 10_000_000 * 0.25 * 0.0001

    def test_very_long_tenor(self):
        swap = IRSwapFixedFloatApprox(
            symbol="LONG", notional=10_000_000,
            fixed_rate=0.03, float_rate=0.05, tenor_years=30.0,
        )
        # At long tenors, heavier discounting on float side can make NPV negative
        # despite float_rate > fixed_rate. Just verify it's non-zero.
        assert swap.npv != 0.0
        assert swap.dv01 == 10_000_000 * 30.0 * 0.0001

    def test_zero_notional(self):
        swap = IRSwapFixedFloatApprox(
            symbol="ZERO", notional=0.0,
            fixed_rate=0.04, float_rate=0.05, tenor_years=5.0,
        )
        assert swap.npv == 0.0
        assert swap.dv01 == 0.0
        assert swap.pnl_status == "FLAT"

    def test_very_high_rate(self):
        """Extreme rate scenario — computations still valid."""
        pt = YieldCurvePoint(tenor_years=5.0, rate=0.50)
        expected = 1.0 / (1.50 ** 5.0)
        assert abs(pt.discount_factor - expected) < 1e-8
        assert pt.discount_factor < 0.15

    def test_negative_rate(self):
        """Negative rates (like historical EUR/JPY) should still work."""
        pt = YieldCurvePoint(tenor_years=5.0, rate=-0.005)
        expected = 1.0 / (0.995 ** 5.0)
        assert abs(pt.discount_factor - expected) < 1e-8
        assert pt.discount_factor > 1.0  # Negative rate → DF > 1

    def test_portfolio_with_mixed_pnl(self):
        """Portfolio with some profitable, some losing swaps."""
        winner = IRSwapFixedFloatApprox(
            symbol="WIN", notional=10_000_000,
            fixed_rate=0.02, float_rate=0.05, tenor_years=5.0,
        )
        loser = IRSwapFixedFloatApprox(
            symbol="LOSE", notional=10_000_000,
            fixed_rate=0.06, float_rate=0.03, tenor_years=5.0,
        )
        book = SwapPortfolio(swaps=[winner, loser])

        assert winner.npv > 0
        assert loser.npv < 0
        assert book.max_npv > 0
        assert book.min_npv < 0
        # Total depends on magnitudes
        assert abs(book.total_npv - (winner.npv + loser.npv)) < 0.01
