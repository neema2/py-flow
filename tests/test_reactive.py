"""
Tests for the reactive computation layer.

Uses generic @dataclass test classes (not trading-specific) to prove
the framework is domain-agnostic.
"""

import json
import math
import pytest
from dataclasses import dataclass, field
from typing import Optional

from store.base import Storable
from reactive.expr import Const, Field, BinOp, UnaryOp, Func, If, Coalesce, IsNull, StrOp, from_json
from reactive.computed import computed, effect, ComputedParseError


# ---------------------------------------------------------------------------
# Test domain classes — intentionally NOT trading-related
# ---------------------------------------------------------------------------

@dataclass
class Sensor(Storable):
    """A generic sensor reading."""
    name: str = ""
    value: float = 0.0
    threshold: float = 100.0
    unit: str = "celsius"

    @computed
    def doubled(self):
        return self.value * 2

    @computed
    def above_threshold(self):
        return self.value > self.threshold

    @computed
    def above(self):
        return self.value > self.threshold

    @computed
    def status(self):
        if self.value > self.threshold:
            return "ALERT"
        return "OK"

    @computed
    def celsius(self):
        return (self.value - 32) * 5 / 9


@dataclass
class Rectangle(Storable):
    """A shape with dimensions."""
    width: float = 0.0
    height: float = 0.0
    label: str = ""

    @computed
    def area(self):
        return self.width * self.height

    @computed
    def upper_label(self):
        return self.label.upper()


@dataclass
class Config(Storable):
    """A config with override/default pattern."""
    override: Optional[float] = None
    default: float = 10.0

    @computed
    def effective(self):
        if self.override is not None:
            return self.override
        return self.default


@dataclass
class Position(Storable):
    """A simple position for cross-entity tests."""
    symbol: str = ""
    quantity: int = 0
    price: float = 0.0

    @computed
    def mv(self):
        return self.price * self.quantity


@dataclass
class Portfolio(Storable):
    """Cross-entity aggregation via @computed."""
    positions: list = field(default_factory=list)

    @computed
    def total_mv(self):
        return sum(p.mv for p in self.positions) if self.positions else 0

    @computed
    def max_mv(self):
        return max(p.mv for p in self.positions) if self.positions else 0

    @computed
    def avg_mv(self):
        if not self.positions:
            return 0
        mvs = [p.mv for p in self.positions]
        return sum(mvs) / len(mvs)

    @computed
    def total_qty(self):
        return sum(p.quantity for p in self.positions) if self.positions else 0

    @computed
    def spread_mv(self):
        if len(self.positions) < 2:
            return 0
        mvs = [p.mv for p in self.positions]
        return max(mvs) - min(mvs)


# Effect test helpers
_effect_log = []


@dataclass
class SensorWithEffect(Storable):
    """Sensor with @effect for testing side-effect firing."""
    name: str = ""
    value: float = 0.0
    threshold: float = 100.0
    unit: str = "celsius"

    @computed
    def above_threshold(self):
        return self.value > self.threshold

    @effect("above_threshold")
    def on_threshold(self, value):
        _effect_log.append(("above_threshold", value))


_area_log = []


@dataclass
class RectangleWithEffect(Storable):
    """Rectangle with @effect for testing side-effect firing."""
    width: float = 0.0
    height: float = 0.0
    label: str = ""

    @computed
    def area(self):
        return self.width * self.height

    @effect("area")
    def on_area(self, value):
        _area_log.append(value)


_portfolio_log = []


@dataclass
class PortfolioWithEffect(Storable):
    """Portfolio with @effect for testing cross-entity effects."""
    positions: list = field(default_factory=list)

    @computed
    def total_mv(self):
        return sum(p.mv for p in self.positions) if self.positions else 0

    @effect("total_mv")
    def on_total_mv(self, value):
        _portfolio_log.append(("total_mv", value))


# ===========================================================================
# Expression tests
# ===========================================================================

class TestConst:
    def test_eval_number(self):
        assert Const(42).eval({}) == 42

    def test_eval_string(self):
        assert Const("hello").eval({}) == "hello"

    def test_eval_bool(self):
        assert Const(True).eval({}) is True

    def test_eval_none(self):
        assert Const(None).eval({}) is None

    def test_to_sql_number(self):
        assert Const(42).to_sql() == "42"

    def test_to_sql_string(self):
        assert Const("hello").to_sql() == "'hello'"

    def test_to_sql_bool(self):
        assert Const(True).to_sql() == "TRUE"
        assert Const(False).to_sql() == "FALSE"

    def test_to_sql_none(self):
        assert Const(None).to_sql() == "NULL"

    def test_to_pure_number(self):
        assert Const(42).to_pure() == "42"

    def test_to_pure_string(self):
        assert Const("hello").to_pure() == "'hello'"

    def test_to_pure_bool(self):
        assert Const(True).to_pure() == "true"
        assert Const(False).to_pure() == "false"


class TestField:
    def test_eval(self):
        assert Field("x").eval({"x": 10}) == 10

    def test_eval_string(self):
        assert Field("name").eval({"name": "alice"}) == "alice"

    def test_to_sql(self):
        assert Field("price").to_sql("data") == "(data->>'price')"

    def test_to_pure(self):
        assert Field("price").to_pure("$row") == "$row.price"


class TestBinOp:
    def test_add(self):
        expr = Field("a") + Field("b")
        assert expr.eval({"a": 3, "b": 7}) == 10

    def test_sub(self):
        expr = Field("a") - Field("b")
        assert expr.eval({"a": 10, "b": 3}) == 7

    def test_mul(self):
        expr = Field("a") * Field("b")
        assert expr.eval({"a": 4, "b": 5}) == 20

    def test_div(self):
        expr = Field("a") / Field("b")
        assert expr.eval({"a": 10, "b": 4}) == 2.5

    def test_mod(self):
        expr = Field("a") % Const(3)
        assert expr.eval({"a": 10}) == 1

    def test_pow(self):
        expr = Field("a") ** Const(2)
        assert expr.eval({"a": 5}) == 25

    def test_gt(self):
        expr = Field("a") > Const(5)
        assert expr.eval({"a": 10}) is True
        assert expr.eval({"a": 3}) is False

    def test_lt(self):
        expr = Field("a") < Const(5)
        assert expr.eval({"a": 3}) is True

    def test_ge(self):
        expr = Field("a") >= Const(5)
        assert expr.eval({"a": 5}) is True

    def test_le(self):
        expr = Field("a") <= Const(5)
        assert expr.eval({"a": 5}) is True

    def test_eq(self):
        expr = Field("a") == Const(5)
        assert expr.eval({"a": 5}) is True
        assert expr.eval({"a": 6}) is False

    def test_ne(self):
        expr = Field("a") != Const(5)
        assert expr.eval({"a": 6}) is True

    def test_and(self):
        expr = (Field("a") > Const(0)) & (Field("b") > Const(0))
        assert expr.eval({"a": 1, "b": 1}) is True
        assert expr.eval({"a": 1, "b": -1}) is False

    def test_or(self):
        expr = (Field("a") > Const(0)) | (Field("b") > Const(0))
        assert expr.eval({"a": -1, "b": 1}) is True
        assert expr.eval({"a": -1, "b": -1}) is False

    def test_nested_arithmetic(self):
        # (price - entry) * quantity
        expr = (Field("price") - Field("entry")) * Field("qty")
        assert expr.eval({"price": 230, "entry": 228, "qty": 100}) == 200

    def test_radd(self):
        expr = 10 + Field("a")
        assert expr.eval({"a": 5}) == 15

    def test_rmul(self):
        expr = 2 * Field("a")
        assert expr.eval({"a": 7}) == 14

    def test_to_sql_arithmetic(self):
        expr = Field("price") * Field("qty")
        sql = expr.to_sql("data")
        assert "(data->>'price')::float" in sql
        assert "(data->>'qty')::float" in sql
        assert "*" in sql

    def test_to_sql_comparison(self):
        expr = Field("age") > Const(18)
        sql = expr.to_sql("data")
        assert "(data->>'age')::float" in sql
        assert ">" in sql
        assert "18" in sql

    def test_to_sql_equality(self):
        expr = Field("status") == Const("active")
        sql = expr.to_sql("data")
        assert "(data->>'status')" in sql
        assert "=" in sql
        assert "'active'" in sql

    def test_to_sql_logical(self):
        expr = (Field("a") > Const(0)) & (Field("b") > Const(0))
        sql = expr.to_sql("data")
        assert "AND" in sql

    def test_to_pure_arithmetic(self):
        expr = (Field("price") - Field("entry")) * Field("qty")
        pure = expr.to_pure("$row")
        assert "$row.price" in pure
        assert "$row.entry" in pure
        assert "$row.qty" in pure

    def test_to_pure_logical(self):
        expr = (Field("a") > Const(0)) | (Field("b") > Const(0))
        pure = expr.to_pure("$row")
        assert "||" in pure


class TestUnaryOp:
    def test_neg(self):
        expr = -Field("x")
        assert expr.eval({"x": 5}) == -5

    def test_abs(self):
        expr = abs(Field("x"))
        assert expr.eval({"x": -7}) == 7

    def test_not(self):
        expr = ~(Field("x") > Const(0))
        assert expr.eval({"x": -1}) is True
        assert expr.eval({"x": 1}) is False

    def test_neg_to_sql(self):
        expr = -Field("x")
        assert "(-" in expr.to_sql("data")

    def test_abs_to_sql(self):
        expr = abs(Field("x"))
        assert "ABS(" in expr.to_sql("data")

    def test_not_to_sql(self):
        expr = ~(Field("x") > Const(0))
        assert "NOT" in expr.to_sql("data")

    def test_neg_to_pure(self):
        expr = -Field("x")
        assert "(-" in expr.to_pure("$row")

    def test_not_to_pure(self):
        expr = ~(Field("x") > Const(0))
        assert "!(" in expr.to_pure("$row")


class TestFunc:
    def test_sqrt(self):
        expr = Func("sqrt", [Field("x")])
        assert expr.eval({"x": 16}) == 4.0

    def test_ceil(self):
        expr = Func("ceil", [Field("x")])
        assert expr.eval({"x": 3.2}) == 4

    def test_floor(self):
        expr = Func("floor", [Field("x")])
        assert expr.eval({"x": 3.8}) == 3

    def test_round(self):
        expr = Func("round", [Field("x")])
        assert expr.eval({"x": 3.6}) == 4

    def test_min(self):
        expr = Func("min", [Field("a"), Field("b")])
        assert expr.eval({"a": 3, "b": 7}) == 3

    def test_max(self):
        expr = Func("max", [Field("a"), Field("b")])
        assert expr.eval({"a": 3, "b": 7}) == 7

    def test_log(self):
        expr = Func("log", [Const(math.e)])
        assert abs(expr.eval({}) - 1.0) < 1e-10

    def test_exp(self):
        expr = Func("exp", [Const(0)])
        assert expr.eval({}) == 1.0

    def test_sqrt_to_sql(self):
        expr = Func("sqrt", [Field("x")])
        assert "SQRT(" in expr.to_sql("data")
        assert "(data->>'x')::float" in expr.to_sql("data")

    def test_min_to_sql(self):
        expr = Func("min", [Field("a"), Field("b")])
        assert "LEAST(" in expr.to_sql("data")

    def test_max_to_sql(self):
        expr = Func("max", [Field("a"), Field("b")])
        assert "GREATEST(" in expr.to_sql("data")

    def test_sqrt_to_pure(self):
        expr = Func("sqrt", [Field("x")])
        assert expr.to_pure("$row") == "sqrt($row.x)"

    def test_ceil_to_pure(self):
        expr = Func("ceil", [Field("x")])
        assert "ceiling(" in expr.to_pure("$row")


class TestIf:
    def test_eval_true_branch(self):
        expr = If(Field("x") > Const(0), Field("x"), Const(0))
        assert expr.eval({"x": 5}) == 5

    def test_eval_false_branch(self):
        expr = If(Field("x") > Const(0), Field("x"), Const(0))
        assert expr.eval({"x": -3}) == 0

    def test_nested_if(self):
        expr = If(
            Field("age") < Const(18),
            Const("Minor"),
            If(Field("age") < Const(65), Const("Adult"), Const("Senior")),
        )
        assert expr.eval({"age": 10}) == "Minor"
        assert expr.eval({"age": 30}) == "Adult"
        assert expr.eval({"age": 70}) == "Senior"

    def test_to_sql(self):
        expr = If(Field("x") > Const(0), Field("x"), Const(0))
        sql = expr.to_sql("data")
        assert "CASE WHEN" in sql
        assert "THEN" in sql
        assert "ELSE" in sql
        assert "END" in sql

    def test_to_pure(self):
        expr = If(Field("x") > Const(0), Field("x"), Const(0))
        pure = expr.to_pure("$row")
        assert "if(" in pure
        assert "|" in pure


class TestCoalesce:
    def test_first_non_none(self):
        expr = Coalesce([Field("a"), Field("b"), Const(0)])
        assert expr.eval({"a": None, "b": 5}) == 5

    def test_all_none(self):
        expr = Coalesce([Field("a"), Field("b")])
        assert expr.eval({"a": None, "b": None}) is None

    def test_first_wins(self):
        expr = Coalesce([Field("a"), Field("b")])
        assert expr.eval({"a": 1, "b": 2}) == 1

    def test_to_sql(self):
        expr = Coalesce([Field("a"), Const(0)])
        sql = expr.to_sql("data")
        assert "COALESCE(" in sql

    def test_to_pure(self):
        expr = Coalesce([Field("a"), Const(0)])
        pure = expr.to_pure("$row")
        assert "isEmpty" in pure


class TestIsNull:
    def test_null(self):
        expr = IsNull(Field("x"))
        assert expr.eval({"x": None}) is True

    def test_not_null(self):
        expr = IsNull(Field("x"))
        assert expr.eval({"x": 5}) is False

    def test_to_sql(self):
        assert "IS NULL" in IsNull(Field("x")).to_sql("data")

    def test_to_pure(self):
        assert "isEmpty(" in IsNull(Field("x")).to_pure("$row")

    def test_is_null_method(self):
        expr = Field("x").is_null()
        assert expr.eval({"x": None}) is True


class TestStrOp:
    def test_length(self):
        expr = Field("name").length()
        assert expr.eval({"name": "alice"}) == 5

    def test_upper(self):
        expr = Field("name").upper()
        assert expr.eval({"name": "alice"}) == "ALICE"

    def test_lower(self):
        expr = Field("name").lower()
        assert expr.eval({"name": "ALICE"}) == "alice"

    def test_contains(self):
        expr = Field("name").contains(Const("li"))
        assert expr.eval({"name": "alice"}) is True
        assert expr.eval({"name": "bob"}) is False

    def test_starts_with(self):
        expr = Field("name").starts_with(Const("al"))
        assert expr.eval({"name": "alice"}) is True
        assert expr.eval({"name": "bob"}) is False

    def test_concat(self):
        expr = Field("first").concat(Const(" ")).concat(Field("last"))
        assert expr.eval({"first": "Jane", "last": "Doe"}) == "Jane Doe"

    def test_length_to_sql(self):
        assert "LENGTH(" in Field("name").length().to_sql("data")

    def test_upper_to_sql(self):
        assert "UPPER(" in Field("name").upper().to_sql("data")

    def test_lower_to_sql(self):
        assert "LOWER(" in Field("name").lower().to_sql("data")

    def test_contains_to_sql(self):
        sql = Field("name").contains(Const("li")).to_sql("data")
        assert "LIKE" in sql

    def test_starts_with_to_sql(self):
        sql = Field("name").starts_with(Const("al")).to_sql("data")
        assert "LIKE" in sql

    def test_concat_to_sql(self):
        sql = Field("a").concat(Field("b")).to_sql("data")
        assert "||" in sql

    def test_upper_to_pure(self):
        assert "toUpper(" in Field("name").upper().to_pure("$row")

    def test_lower_to_pure(self):
        assert "toLower(" in Field("name").lower().to_pure("$row")

    def test_contains_to_pure(self):
        pure = Field("name").contains(Const("li")).to_pure("$row")
        assert "contains(" in pure

    def test_starts_with_to_pure(self):
        pure = Field("name").starts_with(Const("al")).to_pure("$row")
        assert "startsWith(" in pure

    def test_concat_to_pure(self):
        pure = Field("a").concat(Field("b")).to_pure("$row")
        assert "+" in pure


class TestSerialization:
    def test_const_roundtrip(self):
        expr = Const(42)
        restored = from_json(expr.to_json())
        assert restored.eval({}) == 42

    def test_field_roundtrip(self):
        expr = Field("price")
        restored = from_json(expr.to_json())
        assert restored.eval({"price": 100}) == 100

    def test_binop_roundtrip(self):
        expr = Field("a") + Field("b")
        restored = from_json(expr.to_json())
        assert restored.eval({"a": 3, "b": 7}) == 10

    def test_nested_roundtrip(self):
        expr = (Field("price") - Field("entry")) * Field("qty")
        data = expr.to_json()
        restored = from_json(data)
        assert restored.eval({"price": 230, "entry": 228, "qty": 100}) == 200

    def test_if_roundtrip(self):
        expr = If(Field("x") > Const(0), Field("x"), Const(0))
        restored = from_json(expr.to_json())
        assert restored.eval({"x": 5}) == 5
        assert restored.eval({"x": -3}) == 0

    def test_func_roundtrip(self):
        expr = Func("sqrt", [Field("x")])
        restored = from_json(expr.to_json())
        assert restored.eval({"x": 16}) == 4.0

    def test_coalesce_roundtrip(self):
        expr = Coalesce([Field("a"), Const(0)])
        restored = from_json(expr.to_json())
        assert restored.eval({"a": None}) == 0

    def test_is_null_roundtrip(self):
        expr = IsNull(Field("x"))
        restored = from_json(expr.to_json())
        assert restored.eval({"x": None}) is True

    def test_strop_roundtrip(self):
        expr = Field("name").upper()
        restored = from_json(expr.to_json())
        assert restored.eval({"name": "alice"}) == "ALICE"

    def test_strop_with_arg_roundtrip(self):
        expr = Field("name").contains(Const("li"))
        restored = from_json(expr.to_json())
        assert restored.eval({"name": "alice"}) is True

    def test_json_string_roundtrip(self):
        expr = (Field("a") + Const(1)) * Field("b")
        json_str = json.dumps(expr.to_json())
        restored = from_json(json.loads(json_str))
        assert restored.eval({"a": 2, "b": 3}) == 9


# ===========================================================================
# @computed single-entity tests
# ===========================================================================

class TestReactiveComputed:
    def test_object_has_reactive(self):
        sensor = Sensor(name="temp", value=25.0, threshold=100.0, unit="celsius")
        assert hasattr(sensor, '_reactive')
        assert 'value' in object.__getattribute__(sensor, '_reactive')
        assert 'name' in object.__getattribute__(sensor, '_reactive')

    def test_computed_returns_correct_value(self):
        rect = Rectangle(width=10.0, height=5.0, label="test")
        assert rect.area == 50.0

    def test_setattr_triggers_recomputation(self):
        rect = Rectangle(width=10.0, height=5.0)
        assert rect.area == 50.0
        rect.width = 20.0
        assert rect.area == 100.0

    def test_setattr_syncs_attribute(self):
        sensor = Sensor(name="temp", value=25.0)
        sensor.value = 30.0
        assert sensor.value == 30.0

    def test_multiple_computeds_on_same_field(self):
        sensor = Sensor(name="temp", value=25.0, threshold=50.0)
        assert sensor.doubled == 50.0
        assert sensor.above is False

        sensor.value = 60.0
        assert sensor.doubled == 120.0
        assert sensor.above is True

    def test_works_with_different_storable_types(self):
        sensor = Sensor(name="temp", value=25.0)
        rect = Rectangle(width=10.0, height=5.0)

        assert sensor.doubled == 50.0
        assert rect.area == 50.0

        sensor.value = 100.0
        rect.width = 20.0

        assert sensor.doubled == 200.0
        assert rect.area == 100.0

    def test_computed_with_if_expression(self):
        sensor = Sensor(name="temp", value=25.0, threshold=50.0)
        assert sensor.status == "OK"
        sensor.value = 60.0
        assert sensor.status == "ALERT"

    def test_computed_with_coalesce_logic(self):
        cfg = Config(override=None, default=10.0)
        assert cfg.effective == 10.0
        cfg.override = 99.0
        assert cfg.effective == 99.0

    def test_computed_with_string_ops(self):
        rect = Rectangle(width=10.0, height=5.0, label="my rect")
        assert rect.upper_label == "MY RECT"

    def test_complex_expression(self):
        """Convert fahrenheit to celsius: (value - 32) * 5 / 9"""
        sensor = Sensor(name="temp", value=72.0, threshold=100.0, unit="fahrenheit")
        result = sensor.celsius
        expected = (72.0 - 32) * 5 / 9
        assert abs(result - expected) < 0.001

        sensor.value = 212.0
        result = sensor.celsius
        expected = (212.0 - 32) * 5 / 9
        assert abs(result - expected) < 0.001

    def test_batch_update(self):
        rect = Rectangle(width=10.0, height=5.0)
        assert rect.area == 50.0
        rect.batch_update(width=20.0, height=10.0)
        assert rect.area == 200.0

    def test_computed_sql_compilation(self):
        """@computed single-entity methods produce Expr for SQL."""
        expr = Rectangle.area.expr
        assert expr is not None
        sql = expr.to_sql("data")
        assert "(data->>'width')" in sql
        assert "(data->>'height')" in sql
        assert "*" in sql

    def test_computed_pure_compilation(self):
        """@computed single-entity methods produce Expr for Legend Pure."""
        expr = Sensor.doubled.expr
        assert expr is not None
        pure = expr.to_pure("$row")
        assert "$row.value" in pure

    def test_class_level_access_returns_descriptor(self):
        """Accessing @computed on the class returns the descriptor."""
        from reactive.computed import ComputedProperty
        assert isinstance(Rectangle.area, ComputedProperty)
        assert Rectangle.area.name == "area"


# ===========================================================================
# @effect tests
# ===========================================================================

class TestReactiveEffect:
    def test_effect_fires_on_change(self):
        _effect_log.clear()
        sensor = SensorWithEffect(name="temp", value=25.0, threshold=50.0)
        initial = len(_effect_log)
        sensor.value = 60.0
        assert len(_effect_log) > initial
        assert _effect_log[-1] == ("above_threshold", True)

    def test_effect_fires_on_batch(self):
        _area_log.clear()
        rect = RectangleWithEffect(width=10.0, height=5.0)
        initial = len(_area_log)
        rect.batch_update(width=20.0, height=10.0)
        assert rect.area == 200.0
        # Effect should have fired
        assert len(_area_log) > initial
        assert _area_log[-1] == 200.0

    def test_effect_requires_valid_computed(self):
        """@effect referencing nonexistent @computed raises at init."""
        @dataclass
        class BadEffect(Storable):
            value: float = 0.0

            @effect("nonexistent")
            def bad(self, value):
                pass

        with pytest.raises(ValueError, match="nonexistent"):
            BadEffect(value=1.0)


# ===========================================================================
# Cross-entity @computed tests
# ===========================================================================

class TestCrossEntityComputed:
    def test_sum_across_positions(self):
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        book = Portfolio(positions=[p1, p2])
        assert book.total_mv == 100 * 228.0 + 50 * 192.0

    def test_max_across_positions(self):
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        book = Portfolio(positions=[p1, p2])
        assert book.max_mv == 100 * 228.0

    def test_recomputes_on_member_update(self):
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        book = Portfolio(positions=[p1, p2])
        before = book.total_mv

        p1.price = 230.0
        after = book.total_mv

        assert after == 100 * 230.0 + 50 * 192.0
        assert after != before

    def test_single_position(self):
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        book = Portfolio(positions=[p1])
        assert book.total_mv == 100 * 228.0

    def test_empty_portfolio(self):
        book = Portfolio(positions=[])
        assert book.total_mv == 0

    def test_custom_reduce_average(self):
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        book = Portfolio(positions=[p1, p2])
        expected = (100 * 228.0 + 50 * 192.0) / 2
        assert book.avg_mv == expected

    def test_spread_between_positions(self):
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        book = Portfolio(positions=[p1, p2])
        assert book.spread_mv == (100 * 228.0) - (50 * 192.0)

    def test_total_quantity(self):
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        book = Portfolio(positions=[p1, p2])
        assert book.total_qty == 150

    def test_quantity_recomputes_on_update(self):
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        book = Portfolio(positions=[p1, p2])
        p1.quantity = 200
        assert book.total_qty == 250


class TestDynamicMembership:
    def test_add_position(self):
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        book = Portfolio(positions=[p1])
        assert book.total_mv == 100 * 228.0

        book.positions = [p1, p2]
        assert book.total_mv == 100 * 228.0 + 50 * 192.0

    def test_remove_position(self):
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        book = Portfolio(positions=[p1, p2])
        book.positions = [p1]
        assert book.total_mv == 100 * 228.0


class TestComputedOverride:
    """Tests for computed override (what-if) and clear_override."""

    def test_override_computed_value(self):
        pos = Position(symbol="AAPL", quantity=100, price=228.0)
        assert pos.mv == 22800.0
        pos.mv = 99999.0
        assert pos.mv == 99999.0

    def test_override_ripples_to_parent(self):
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        book = Portfolio(positions=[p1, p2])
        original_total = 100 * 228.0 + 50 * 192.0
        assert book.total_mv == original_total

        p1.mv = 50000.0
        assert book.total_mv == 50000.0 + 50 * 192.0

    def test_clear_override_reverts_to_formula(self):
        pos = Position(symbol="AAPL", quantity=100, price=228.0)
        assert pos.mv == 22800.0
        pos.mv = 99999.0
        assert pos.mv == 99999.0
        pos.clear_override("mv")
        assert pos.mv == 22800.0

    def test_clear_override_ripples_to_parent(self):
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        book = Portfolio(positions=[p1])
        p1.mv = 50000.0
        assert book.total_mv == 50000.0
        p1.clear_override("mv")
        assert book.total_mv == 22800.0

    def test_clear_override_on_field_raises(self):
        pos = Position(symbol="AAPL", quantity=100, price=228.0)
        with pytest.raises(ValueError, match="not a @computed"):
            pos.clear_override("price")

    def test_clear_override_on_nonexistent_raises(self):
        pos = Position(symbol="AAPL", quantity=100, price=228.0)
        with pytest.raises(ValueError, match="not a @computed"):
            pos.clear_override("bogus")

    def test_override_then_field_change_keeps_override(self):
        pos = Position(symbol="AAPL", quantity=100, price=228.0)
        pos.mv = 99999.0
        pos.price = 300.0  # field change — but mv is overridden
        assert pos.mv == 99999.0

    def test_clear_after_field_change_uses_new_field(self):
        pos = Position(symbol="AAPL", quantity=100, price=228.0)
        pos.mv = 99999.0
        pos.price = 300.0
        pos.clear_override("mv")
        assert pos.mv == 300.0 * 100  # formula with updated price

    def test_multiple_overrides_independent(self):
        rect = Rectangle(width=10.0, height=5.0)
        assert rect.area == 50.0
        rect.area = 999.0
        assert rect.area == 999.0
        assert rect.upper_label == ""  # other computed unaffected
        rect.clear_override("area")
        assert rect.area == 50.0


class TestCrossEntityEffect:
    def test_effect_fires_on_member_change(self):
        _portfolio_log.clear()
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        p2 = Position(symbol="GOOG", quantity=50, price=192.0)
        book = PortfolioWithEffect(positions=[p1, p2])
        initial = len(_portfolio_log)

        p1.price = 230.0
        assert len(_portfolio_log) > initial
        assert _portfolio_log[-1] == ("total_mv", 100 * 230.0 + 50 * 192.0)


# ===========================================================================
# Reactive internals coverage
# ===========================================================================

class TestReactiveInternals:
    """Tests for internal reactive wiring: _reactive dict, _effects list, field exclusion."""

    def test_effects_list_populated_for_effect_class(self):
        _portfolio_log.clear()
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        book = PortfolioWithEffect(positions=[p1])
        effects = object.__getattribute__(book, '_effects')
        assert isinstance(effects, list)
        assert len(effects) > 0

    def test_effects_list_empty_for_no_effects(self):
        pos = Position(symbol="AAPL", quantity=100, price=228.0)
        effects = object.__getattribute__(pos, '_effects')
        assert isinstance(effects, list)
        assert len(effects) == 0

    def test_store_fields_excluded_from_reactive(self):
        pos = Position(symbol="AAPL", quantity=100, price=228.0)
        reactive = object.__getattribute__(pos, '_reactive')
        # _store_* fields should NOT be reactive
        for key in reactive:
            assert not key.startswith('_'), f"Internal field '{key}' leaked into _reactive"

    def test_reactive_contains_fields_and_computeds(self):
        pos = Position(symbol="AAPL", quantity=100, price=228.0)
        reactive = object.__getattribute__(pos, '_reactive')
        # Fields
        assert 'symbol' in reactive
        assert 'quantity' in reactive
        assert 'price' in reactive
        # Computed
        assert 'mv' in reactive

    def test_reactive_node_has_read_and_write(self):
        pos = Position(symbol="AAPL", quantity=100, price=228.0)
        reactive = object.__getattribute__(pos, '_reactive')
        node = reactive['price']
        assert hasattr(node, 'read')
        assert hasattr(node, 'write')
        assert callable(node.read)
        assert callable(node.write)


# ===========================================================================
# AST validation rejection tests
# ===========================================================================

class TestComputedASTValidation:
    """Tests that @computed rejects unsupported Python constructs."""

    def test_lambda_rejected(self):
        with pytest.raises(ComputedParseError, match="Lambda"):
            @dataclass
            class Bad(Storable):
                items: list = field(default_factory=list)
                @computed
                def total(self):
                    return sum(map(lambda x: x, self.items))

    def test_try_except_rejected(self):
        with pytest.raises(ComputedParseError, match="try/except"):
            @dataclass
            class Bad(Storable):
                value: float = 0.0
                @computed
                def safe(self):
                    try:
                        return self.value
                    except Exception:
                        return 0.0

    def test_import_rejected(self):
        with pytest.raises(ComputedParseError, match="Import"):
            @dataclass
            class Bad(Storable):
                value: float = 0.0
                @computed
                def bad(self):
                    import os
                    return self.value

    def test_yield_rejected(self):
        with pytest.raises(ComputedParseError, match="yield"):
            @dataclass
            class Bad(Storable):
                value: float = 0.0
                @computed
                def bad(self):
                    yield self.value

    def test_nested_function_rejected(self):
        with pytest.raises(ComputedParseError, match="Nested function"):
            @dataclass
            class Bad(Storable):
                value: float = 0.0
                @computed
                def bad(self):
                    def helper():
                        return 1
                    return helper()

    def test_class_def_rejected(self):
        with pytest.raises(ComputedParseError, match="Class definition"):
            @dataclass
            class Bad(Storable):
                value: float = 0.0
                @computed
                def bad(self):
                    class Inner:
                        pass
                    return self.value


# ===========================================================================
# Computed referencing another computed (inline refs)
# ===========================================================================

@dataclass
class Circle(Storable):
    """Model where one computed references another computed."""
    radius: float = 1.0

    @computed
    def diameter(self):
        return self.radius * 2

    @computed
    def circumference(self):
        return self.diameter * 3.14159

    @computed
    def area(self):
        return self.radius * self.radius * 3.14159


class TestComputedReferencesComputed:
    """Tests that one @computed can depend on another @computed on the same object."""

    def test_computed_reads_another_computed(self):
        c = Circle(radius=5.0)
        assert c.diameter == 10.0
        assert abs(c.circumference - 31.4159) < 0.001

    def test_chained_computed_reacts_to_field_change(self):
        c = Circle(radius=5.0)
        assert c.diameter == 10.0
        c.radius = 10.0
        assert c.diameter == 20.0
        assert abs(c.circumference - 62.8318) < 0.001

    def test_override_intermediate_computed(self):
        c = Circle(radius=5.0)
        # Override diameter — circumference should use the override
        c.diameter = 100.0
        assert c.diameter == 100.0
        # circumference depends on diameter → should see override
        assert abs(c.circumference - 314.159) < 0.001

    def test_clear_intermediate_override(self):
        c = Circle(radius=5.0)
        c.diameter = 100.0
        c.clear_override("diameter")
        assert c.diameter == 10.0
        assert abs(c.circumference - 31.4159) < 0.001


# ===========================================================================
# _ReactiveProxy write-through test
# ===========================================================================

class TestReactiveProxySetattr:
    """Tests that writing through _ReactiveProxy delegates to __setattr__."""

    def test_proxy_setattr_updates_field(self):
        from reactive.computed import _ReactiveProxy
        pos = Position(symbol="AAPL", quantity=100, price=228.0)
        proxy = _ReactiveProxy(pos)
        proxy.price = 300.0
        assert pos.price == 300.0
        assert pos.mv == 300.0 * 100

    def test_proxy_setattr_triggers_recomputation(self):
        from reactive.computed import _ReactiveProxy
        p1 = Position(symbol="AAPL", quantity=100, price=228.0)
        book = Portfolio(positions=[p1])
        proxy = _ReactiveProxy(p1)
        proxy.price = 300.0
        assert book.total_mv == 300.0 * 100


# ===========================================================================
# ComputedProperty descriptor direct access
# ===========================================================================

# ===========================================================================
# Aggregate DSL helpers (group_by, rank_by)
# ===========================================================================

class TestGroupBy:
    """Tests for reactive.agg.group_by."""

    def test_basic_grouping(self):
        from reactive.agg import group_by
        result = group_by([("A", 10), ("B", 20), ("A", 30)])
        assert result == {"A": 40, "B": 20}

    def test_normalize(self):
        from reactive.agg import group_by
        result = group_by([("Tech", 300), ("Finance", 100)], normalize=True)
        assert result == {"Tech": 75.0, "Finance": 25.0}

    def test_empty(self):
        from reactive.agg import group_by
        assert group_by([]) == {}

    def test_single_group(self):
        from reactive.agg import group_by
        result = group_by([("X", 5), ("X", 10), ("X", 15)])
        assert result == {"X": 30}

    def test_normalize_single_group(self):
        from reactive.agg import group_by
        result = group_by([("X", 5), ("X", 10)], normalize=True)
        assert result == {"X": 100.0}

    def test_normalize_zero_total(self):
        from reactive.agg import group_by
        result = group_by([("A", 0), ("B", 0)], normalize=True)
        assert result == {"A": 0.0, "B": 0.0}


class TestRankBy:
    """Tests for reactive.agg.rank_by."""

    def test_basic_sort_desc(self):
        from reactive.agg import rank_by
        result = rank_by([("AAPL", 5000), ("NVDA", 8000), ("MSFT", 3000)])
        assert result[0]["label"] == "NVDA"
        assert result[0]["value"] == 8000
        assert result[-1]["label"] == "MSFT"

    def test_sort_asc(self):
        from reactive.agg import rank_by
        result = rank_by([("AAPL", 5000), ("NVDA", 8000)], desc=False)
        assert result[0]["label"] == "AAPL"

    def test_as_pct(self):
        from reactive.agg import rank_by
        result = rank_by([("A", 10), ("B", 0)], as_pct=True)
        assert result[0]["label"] == "A"
        assert result[0]["pct"] == 100.0
        assert result[1]["pct"] == 0.0

    def test_as_pct_all_zero(self):
        from reactive.agg import rank_by
        result = rank_by([("A", 0), ("B", 0)], as_pct=True)
        assert all(r["pct"] == 0.0 for r in result)

    def test_empty(self):
        from reactive.agg import rank_by
        assert rank_by([]) == []


class TestAggInsideComputed:
    """Test group_by/rank_by inside @computed on a Portfolio Storable."""

    def test_group_by_reactive(self):
        """group_by recomputes when child position price changes."""
        from reactive.agg import group_by

        @dataclass
        class TaggedPosition(Storable):
            symbol: str = ""
            price: float = 0.0
            quantity: int = 0
            label: str = ""

            @computed
            def mv(self):
                return self.price * self.quantity

        @dataclass
        class GroupPortfolio(Storable):
            positions: list = field(default_factory=list)

            @computed
            def weights(self):
                return group_by([(p.label, p.mv) for p in self.positions],
                                normalize=True)

        p1 = TaggedPosition(symbol="AAPL", price=100.0, quantity=10, label="Tech")
        p2 = TaggedPosition(symbol="JPM", price=50.0, quantity=10, label="Finance")
        port = GroupPortfolio(positions=[p1, p2])

        w = port.weights
        assert w["Tech"] == pytest.approx(66.7, abs=0.1)
        assert w["Finance"] == pytest.approx(33.3, abs=0.1)

        # Price tick → reactive recomputation
        p1.batch_update(price=200.0)
        w2 = port.weights
        assert w2["Tech"] == pytest.approx(80.0, abs=0.1)

    def test_rank_by_reactive(self):
        """rank_by recomputes when child position price changes."""
        from reactive.agg import rank_by

        @dataclass
        class RankedPosition(Storable):
            symbol: str = ""
            price: float = 0.0
            quantity: int = 0

            @computed
            def risk(self):
                return self.price * self.quantity * 0.02

        @dataclass
        class RiskPortfolio(Storable):
            positions: list = field(default_factory=list)

            @computed
            def risk_ranking(self):
                return rank_by([(p.symbol, p.risk) for p in self.positions],
                               as_pct=True)

        p1 = RankedPosition(symbol="AAPL", price=100.0, quantity=10)
        p2 = RankedPosition(symbol="NVDA", price=50.0, quantity=10)
        port = RiskPortfolio(positions=[p1, p2])

        r = port.risk_ranking
        assert r[0]["label"] == "AAPL"  # AAPL has higher risk

        # NVDA price spike → should flip ranking
        p2.batch_update(price=300.0)
        r2 = port.risk_ranking
        assert r2[0]["label"] == "NVDA"


class TestComputedPropertyDescriptor:
    """Tests for ComputedProperty.__get__ via direct descriptor call."""

    def test_class_level_returns_descriptor(self):
        from reactive.computed import ComputedProperty
        desc = Position.mv
        assert isinstance(desc, ComputedProperty)

    def test_descriptor_get_on_instance(self):
        from reactive.computed import ComputedProperty
        pos = Position(symbol="AAPL", quantity=100, price=228.0)
        desc = type(pos).__dict__['mv']
        # Direct descriptor call (bypasses __getattribute__)
        val = desc.__get__(pos, type(pos))
        assert val == 22800.0


# ---------------------------------------------------------------------------
# Async compatibility
# ---------------------------------------------------------------------------

class TestAsyncCompatibility:
    def test_batch_update_inside_running_event_loop(self):
        """batch_update() must not crash when called from inside an async context.

        Regression test: Storable._tick() used to call
        loop.run_until_complete(asyncio.sleep(0)) unconditionally, which raises
        RuntimeError('This event loop is already running') when called from
        within an async for / await context (e.g. a WebSocket consumer).
        """
        import asyncio

        effect_values = []

        @dataclass
        class AsyncSensor(Storable):
            name: str = ""
            value: float = 0.0

            @computed
            def doubled(self):
                return self.value * 2

            @effect("doubled")
            def on_doubled(self, v):
                effect_values.append(v)

        async def run():
            sensor = AsyncSensor(name="test", value=1.0)
            sensor.batch_update(value=5.0)
            assert sensor.doubled == 10.0
            sensor.batch_update(value=7.5)
            assert sensor.doubled == 15.0

        asyncio.run(run())
        assert 10.0 in effect_values
        assert 15.0 in effect_values
