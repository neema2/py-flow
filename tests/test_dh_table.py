"""
Tests for streaming.decorator — @ticking decorator (pure-Python parts).

Tests _to_snake_case and _resolve_column_specs which don't need Deephaven.
The full @ticking decorator and tick() are integration-tested via the demos.
"""

from dataclasses import dataclass, field

from reactive.computed import computed
from store.base import Storable
from streaming.decorator import _resolve_column_specs, _to_snake_case

# ── snake_case tests ──────────────────────────────────────────────────────

class TestToSnakeCase:
    def test_simple(self):
        assert _to_snake_case("Position") == "position"

    def test_two_words(self):
        assert _to_snake_case("SwapPortfolio") == "swap_portfolio"

    def test_three_words(self):
        assert _to_snake_case("IRSwapFixedFloatApprox") == "ir_swap_fixed_float_approx"

    def test_consecutive_caps(self):
        assert _to_snake_case("FXSpot") == "fx_spot"

    def test_long_consecutive_caps(self):
        assert _to_snake_case("YieldCurvePoint") == "yield_curve_point"

    def test_all_caps(self):
        assert _to_snake_case("USD") == "usd"

    def test_single_word_lower(self):
        assert _to_snake_case("trade") == "trade"


# ── Column spec resolution tests ─────────────────────────────────────────

class TestResolveColumnSpecs:
    def test_all_primitive_fields(self):
        @dataclass
        class Simple(Storable):
            _registry = None
            symbol: str = ""
            price: float = 0.0
            quantity: int = 0

        specs = _resolve_column_specs(Simple)
        assert specs == [
            ("symbol", "symbol", str),
            ("price", "price", float),
            ("quantity", "quantity", int),
        ]

    def test_skips_object_and_list(self):
        @dataclass
        class WithRef(Storable):
            _registry = None
            label: str = ""
            ref: object = None
            items: list = field(default_factory=list)

        specs = _resolve_column_specs(WithRef)
        names = [name for name, _, _ in specs]
        assert "label" in names
        assert "ref" not in names
        assert "items" not in names

    def test_exclude_fields(self):
        @dataclass
        class WithInternal(Storable):
            _registry = None
            label: str = ""
            base_rate: float = 0.0
            sensitivity: float = 0.5

        specs = _resolve_column_specs(WithInternal, exclude={"base_rate", "sensitivity"})
        names = [name for name, _, _ in specs]
        assert names == ["label"]

    def test_includes_computed(self):
        @dataclass
        class WithComputed(Storable):
            _registry = None
            price: float = 0.0
            quantity: int = 0

            @computed
            def market_value(self):
                return self.price * self.quantity

        specs = _resolve_column_specs(WithComputed)
        names = [name for name, _, _ in specs]
        assert "price" in names
        assert "quantity" in names
        assert "market_value" in names

    def test_computed_return_annotation_str(self):
        @dataclass
        class WithStatus(Storable):
            _registry = None
            value: float = 0.0

            @computed
            def status(self) -> str:
                return "OK" if self.value > 0 else "BAD"

        specs = _resolve_column_specs(WithStatus)
        status_spec = next(s for s in specs if s[0] == "status")
        assert status_spec[2] is str

    def test_computed_return_annotation_int(self):
        @dataclass
        class WithCount(Storable):
            _registry = None
            items: list = field(default_factory=list)

            @computed
            def count(self) -> int:
                return len(self.items)

        specs = _resolve_column_specs(WithCount)
        count_spec = next(s for s in specs if s[0] == "count")
        assert count_spec[2] is int

    def test_computed_no_annotation_defaults_float(self):
        @dataclass
        class WithCalc(Storable):
            _registry = None
            x: float = 0.0

            @computed
            def doubled(self):
                return self.x * 2

        specs = _resolve_column_specs(WithCalc)
        doubled_spec = next(s for s in specs if s[0] == "doubled")
        assert doubled_spec[2] is float

    def test_computed_sorted_alphabetically(self):
        @dataclass
        class MultiComputed(Storable):
            _registry = None
            x: float = 0.0

            @computed
            def zebra(self):
                return self.x

            @computed
            def alpha(self):
                return self.x

        specs = _resolve_column_specs(MultiComputed)
        computed_names = [name for name, _, _ in specs if name != "x"]
        assert computed_names == ["alpha", "zebra"]

    def test_exclude_computed(self):
        @dataclass
        class ExcludeCalc(Storable):
            _registry = None
            x: float = 0.0

            @computed
            def internal_calc(self):
                return self.x * 2

            @computed
            def public_calc(self):
                return self.x * 3

        specs = _resolve_column_specs(ExcludeCalc, exclude={"internal_calc"})
        names = [name for name, _, _ in specs]
        assert "internal_calc" not in names
        assert "public_calc" in names

    def test_fields_before_computed(self):
        """Dataclass fields come first (definition order), then computed (sorted)."""
        @dataclass
        class Ordered(Storable):
            _registry = None
            z_field: str = ""
            a_field: float = 0.0

            @computed
            def calc(self):
                return self.a_field

        specs = _resolve_column_specs(Ordered)
        names = [name for name, _, _ in specs]
        assert names == ["z_field", "a_field", "calc"]

    def test_private_fields_skipped(self):
        @dataclass
        class WithPrivate(Storable):
            _registry = None
            name: str = ""
            _internal: float = 0.0

        specs = _resolve_column_specs(WithPrivate)
        names = [name for name, _, _ in specs]
        assert "name" in names
        assert "_internal" not in names

    def test_empty_class(self):
        @dataclass
        class Empty(Storable):
            _registry = None

        specs = _resolve_column_specs(Empty)
        assert specs == []

    def test_bool_field(self):
        @dataclass
        class WithBool(Storable):
            _registry = None
            active: bool = True

        specs = _resolve_column_specs(WithBool)
        assert specs == [("active", "active", bool)]
