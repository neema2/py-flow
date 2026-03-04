"""
@computed and @effect decorators for reactive Storable properties.

@computed parses a method's Python AST at decoration time and builds an Expr
tree (for single-entity arithmetic → compiles to Python/SQL/Legend Pure).
For cross-entity methods (iteration, aggregation), it falls back to a
proxy-based runtime evaluation.

@effect("computed_name") marks a method as a fire-and-forget side-effect
that runs whenever the named computed changes.

Both decorators produce descriptors that wire into Storable.__post_init__
to create reaktiv Signals/Computed/Effect instances automatically.
"""

import ast
import inspect
import textwrap
from collections.abc import Callable
from typing import Any

from reactive.expr import (
    BinOp,
    Coalesce,
    Const,
    Expr,
    Field,
    Func,
    If,
    IsNull,
    UnaryOp,
)

# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class ComputedParseError(Exception):
    """Raised when a @computed method body contains unsupported Python."""


# ---------------------------------------------------------------------------
# AST → Expr translator
# ---------------------------------------------------------------------------

# Python AST operator → our string operator
_BINOP_MAP = {
    ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/",
    ast.Mod: "%", ast.Pow: "**",
}

_CMPOP_MAP = {
    ast.Gt: ">", ast.Lt: "<", ast.GtE: ">=", ast.LtE: "<=",
    ast.Eq: "==", ast.NotEq: "!=",
    ast.Is: "==", ast.IsNot: "!=",
}

_BOOLOP_MAP = {
    ast.And: "and", ast.Or: "or",
}

# Built-in / math functions we support
_FUNC_MAP = {
    "abs": "abs", "round": "round", "min": "min", "max": "max",
    "sqrt": "sqrt", "ceil": "ceil", "floor": "floor",
    "log": "log", "exp": "exp",
}

# math.X qualified names
_MATH_FUNC_MAP = {
    "sqrt": "sqrt", "ceil": "ceil", "floor": "floor",
    "log": "log", "exp": "exp",
}


class _ASTTranslator(ast.NodeVisitor):
    """Walk a Python AST and produce an Expr tree.

    Raises ComputedParseError for unsupported constructs.
    Sets self.is_cross_entity = True if the body contains patterns that
    cannot be represented as a single-entity Expr (e.g. iteration).
    """

    def __init__(self, computed_names: set) -> None:
        self.computed_names = computed_names
        self.is_cross_entity = False

    def translate(self, func_def: ast.FunctionDef) -> "Expr | None":
        """Translate a function body to an Expr, or None for cross-entity."""
        body = func_def.body
        try:
            expr = self._translate_body(body)
            if self.is_cross_entity:
                return None
            return expr
        except ComputedParseError:
            # If translation fails, fall back to cross-entity (proxy-based).
            # Only truly unsupported constructs (validated earlier) raise.
            return None

    def _translate_body(self, stmts: list) -> Expr | None:
        """Translate a list of statements (function body) to an Expr."""
        if len(stmts) == 1:
            stmt = stmts[0]
            if isinstance(stmt, ast.Return):
                return self._translate_expr(stmt.value)
            if isinstance(stmt, ast.If):
                return self._translate_if_stmt(stmt, [])
            raise ComputedParseError(
                f"Unsupported statement: {type(stmt).__name__}"
            )

        # Multiple statements: must be if/elif/return chain
        first = stmts[0]
        rest = stmts[1:]
        if isinstance(first, ast.If):
            return self._translate_if_stmt(first, rest)
        raise ComputedParseError(
            f"Unsupported multi-statement body starting with {type(first).__name__}"
        )

    def _translate_if_stmt(self, if_node: ast.If, rest: list) -> Expr | None:
        """Translate an if statement to an If Expr.

        Pattern: if cond: return X [elif ...] [return Y]
        """
        cond = self._translate_expr(if_node.test)

        # then branch: must be a single return
        if (len(if_node.body) == 1
                and isinstance(if_node.body[0], ast.Return)):
            then = self._translate_expr(if_node.body[0].value)
        else:
            then = self._translate_body(if_node.body)

        # else branch
        if if_node.orelse:
            else_expr = self._translate_body(if_node.orelse)
        elif rest:
            else_expr = self._translate_body(rest)
        else:
            raise ComputedParseError(
                "if statement in @computed must have an else/return branch"
            )

        if cond is None or then is None or else_expr is None:
            self.is_cross_entity = True
            return None

        return If(cond, then, else_expr)

    def _translate_expr(self, node: ast.expr | None) -> Expr | None:
        """Translate a single AST expression node to an Expr."""
        if node is None:
            return Const(None)

        # --- Constants ---
        if isinstance(node, ast.Constant):
            return Const(node.value)

        # --- self.x ---
        if isinstance(node, ast.Attribute) and self._is_self(node.value):
            name = node.attr
            if name in self.computed_names:
                # Reference to another @computed → cross-entity (proxy-based)
                # so that computed overrides propagate correctly.
                self.is_cross_entity = True
                return None
            return Field(name)

        # --- p.x (attribute on non-self) — cross-entity ---
        if isinstance(node, ast.Attribute):
            self.is_cross_entity = True
            return None

        # --- Binary ops: a + b ---
        if isinstance(node, ast.BinOp):
            op_type = type(node.op)
            if op_type not in _BINOP_MAP:
                raise ComputedParseError(
                    f"Unsupported binary operator: {op_type.__name__}"
                )
            left = self._translate_expr(node.left)
            right = self._translate_expr(node.right)
            if left is None or right is None:
                self.is_cross_entity = True
                return None
            return BinOp(_BINOP_MAP[op_type], left, right)

        # --- Unary ops: -x, not x ---
        if isinstance(node, ast.UnaryOp):
            if isinstance(node.op, ast.USub):
                operand = self._translate_expr(node.operand)
                if operand is None:
                    self.is_cross_entity = True
                    return None
                return UnaryOp("neg", operand)
            if isinstance(node.op, ast.Not):
                operand = self._translate_expr(node.operand)
                if operand is None:
                    self.is_cross_entity = True
                    return None
                return UnaryOp("not", operand)
            raise ComputedParseError(
                f"Unsupported unary operator: {type(node.op).__name__}"
            )

        # --- Comparisons: a > b ---
        if isinstance(node, ast.Compare):
            if len(node.ops) != 1 or len(node.comparators) != 1:
                raise ComputedParseError(
                    "Chained comparisons not supported in @computed"
                )
            cmp_type = type(node.ops[0])
            if cmp_type not in _CMPOP_MAP:
                raise ComputedParseError(
                    f"Unsupported comparison: {cmp_type.__name__}"
                )
            left = self._translate_expr(node.left)
            right = self._translate_expr(node.comparators[0])
            if left is None or right is None:
                self.is_cross_entity = True
                return None
            return BinOp(_CMPOP_MAP[cmp_type], left, right)

        # --- Boolean ops: a and b, a or b ---
        if isinstance(node, ast.BoolOp):
            op_str = _BOOLOP_MAP.get(type(node.op))
            if op_str is None:
                raise ComputedParseError(
                    f"Unsupported boolean op: {type(node.op).__name__}"
                )
            result = self._translate_expr(node.values[0])
            for val in node.values[1:]:
                right = self._translate_expr(val)
                if result is None or right is None:
                    self.is_cross_entity = True
                    return None
                result = BinOp(op_str, result, right)
            return result

        # --- Ternary: x if cond else y ---
        if isinstance(node, ast.IfExp):
            cond = self._translate_expr(node.test)
            then = self._translate_expr(node.body)
            else_ = self._translate_expr(node.orelse)
            if cond is None or then is None or else_ is None:
                self.is_cross_entity = True
                return None
            return If(cond, then, else_)

        # --- Function calls: abs(x), round(x, 2), math.sqrt(x) ---
        if isinstance(node, ast.Call):
            func_name = self._get_func_name(node)
            if func_name is None:
                # Could be sum(), len() etc — cross-entity
                self.is_cross_entity = True
                return None
            args = []
            for arg in node.args:
                a = self._translate_expr(arg)
                if a is None:
                    self.is_cross_entity = True
                    return None
                args.append(a)
            if func_name == "abs" and len(args) == 1:
                return UnaryOp("abs", args[0])
            return Func(func_name, args)

        # --- Name (bare variable) — not self.x ---
        if isinstance(node, ast.Name):
            # Could be a builtin like True/False/None
            if node.id in ("True", "False", "None"):
                builtin_val: object = {"True": True, "False": False, "None": None}[node.id]
                return Const(builtin_val)
            # Unknown name — cross-entity variable (e.g. loop var)
            self.is_cross_entity = True
            return None

        # --- Generator expressions, list comps, etc — cross-entity ---
        if isinstance(node, (ast.GeneratorExp, ast.ListComp, ast.SetComp,
                             ast.DictComp)):
            self.is_cross_entity = True
            return None

        # --- Truly unsupported ---
        raise ComputedParseError(
            f"Unsupported expression in @computed: {type(node).__name__}"
        )

    def _is_self(self, node: ast.expr) -> bool:
        return isinstance(node, ast.Name) and node.id == "self"

    def _get_func_name(self, call: ast.Call) -> "str | None":
        """Extract a known function name, or None for unknown/cross-entity."""
        func = call.func
        # abs(x), round(x), min(a,b), max(a,b)
        if isinstance(func, ast.Name):
            if func.id in _FUNC_MAP:
                return _FUNC_MAP[func.id]
            return None  # unknown function → cross-entity
        # math.sqrt(x), math.log(x)
        if (isinstance(func, ast.Attribute)
                and isinstance(func.value, ast.Name)
                and func.value.id == "math"):
            if func.attr in _MATH_FUNC_MAP:
                return _MATH_FUNC_MAP[func.attr]
            return None
        return None


# ---------------------------------------------------------------------------
# _ReactiveProxy — used for cross-entity @computed evaluation
# ---------------------------------------------------------------------------

class _ReactiveProxy:
    """Proxy for `self` inside cross-entity @computed methods.

    Attribute reads are routed through Signals/Computeds so reaktiv
    can track dependencies. Used only at runtime inside Computed evaluation.
    """

    __slots__ = ("_obj",)

    def __init__(self, obj: Any) -> None:
        object.__setattr__(self, "_obj", obj)

    def __getattr__(self, name: str) -> Any:
        obj = object.__getattribute__(self, "_obj")
        # Route through _reactive dict (unified signals + computeds)
        reactive = object.__getattribute__(obj, "_reactive")
        node = reactive.get(name)
        if node is not None:
            return node.read()
        # Fallback to regular attribute
        return getattr(obj, name)

    def __setattr__(self, name: str, value: object) -> None:
        obj = object.__getattribute__(self, "_obj")
        setattr(obj, name, value)


# ---------------------------------------------------------------------------
# ComputedProperty descriptor
# ---------------------------------------------------------------------------

class ComputedProperty:
    """Descriptor for @computed methods.

    Class-level access returns the descriptor itself (for .expr, .name).
    Instance-level access calls the Computed signal → trackable by
    outer Computeds.
    """

    def __init__(self, fn: Callable[..., Any], expr: Expr | None, name: str) -> None:
        self.fn = fn          # original function
        self.expr = expr      # Expr tree (or None for cross-entity)
        self.name = name      # method name

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name

    def __get__(self, obj: Any, objtype: type | None = None) -> Any:
        if obj is None:
            return self  # class-level: Position.pnl → descriptor
        # __getattribute__ normally intercepts before this descriptor fires,
        # but handle the rare direct-descriptor-call case correctly.
        reactive = object.__getattribute__(obj, "_reactive")
        node = reactive.get(self.name)
        if node is not None:
            return node.read()
        return None

    def __repr__(self) -> str:
        kind = "single-entity" if self.expr is not None else "cross-entity"
        return f"<ComputedProperty {self.name!r} ({kind})>"


# ---------------------------------------------------------------------------
# EffectMethod descriptor
# ---------------------------------------------------------------------------

class EffectMethod:
    """Descriptor for @effect methods.

    Stores the target computed name and the callback function.
    Wiring happens in Storable.__post_init__.
    """

    def __init__(self, target_computed: str, fn: Callable[..., Any]) -> None:
        self.target_computed = target_computed
        self.fn = fn
        self.name = fn.__name__

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name

    def __get__(self, obj: Any, objtype: type | None = None) -> Any:
        if obj is None:
            return self
        # When accessed on an instance, return the bound method
        return self.fn.__get__(obj, objtype)

    def __repr__(self) -> str:
        return f"<EffectMethod {self.name!r} watches={self.target_computed!r}>"


# ---------------------------------------------------------------------------
# Public decorators
# ---------------------------------------------------------------------------

def _parse_computed_source(fn: Callable[..., Any]) -> "Expr | None":
    """Parse a @computed function's source and return an Expr (or None)."""
    # Collect names of other @computed methods from the class being defined.
    # At decoration time we don't have the full class yet, but we can detect
    # other ComputedProperty instances on the same class via the frame's locals.
    frame = inspect.currentframe()
    assert frame is not None and frame.f_back is not None and frame.f_back.f_back is not None
    frame_locals = frame.f_back.f_back.f_locals
    computed_names = set()
    for k, v in frame_locals.items():
        if isinstance(v, ComputedProperty):
            computed_names.add(k)

    try:
        source = inspect.getsource(fn)
    except OSError:
        # Can't get source (e.g. defined in REPL) — fall back to cross-entity
        return None

    source = textwrap.dedent(source)

    # Strip the @computed decorator line(s) from the source
    lines = source.split('\n')
    func_start = 0
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith('def '):
            func_start = i
            break
    source = '\n'.join(lines[func_start:])

    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        raise ComputedParseError(f"Syntax error in @computed: {e}") from e

    if not tree.body or not isinstance(tree.body[0], ast.FunctionDef):
        raise ComputedParseError("@computed must decorate a function")

    func_def = tree.body[0]

    # Validate: no unsupported top-level constructs
    _validate_no_unsupported(func_def)

    translator = _ASTTranslator(computed_names)
    expr = translator.translate(func_def)

    # For single-entity, inline references to other @computed
    if expr is not None:
        expr = _inline_computed_refs(expr, frame_locals)

    return expr


def _validate_no_unsupported(func_def: ast.FunctionDef) -> None:
    """Walk the AST and reject truly unsupported constructs."""
    for node in ast.walk(func_def):
        if isinstance(node, (ast.Try, ast.ExceptHandler)):
            raise ComputedParseError(
                "try/except is not supported in @computed"
            )
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            raise ComputedParseError(
                "Imports are not supported in @computed"
            )
        if isinstance(node, (ast.Yield, ast.YieldFrom)):
            raise ComputedParseError(
                "yield is not supported in @computed"
            )
        if isinstance(node, ast.Await):
            raise ComputedParseError(
                "await is not supported in @computed"
            )
        if isinstance(node, (ast.Global, ast.Nonlocal)):
            raise ComputedParseError(
                "global/nonlocal is not supported in @computed"
            )
        if isinstance(node, (ast.ClassDef,)):
            raise ComputedParseError(
                "Class definitions are not supported in @computed"
            )
        if isinstance(node, ast.FunctionDef) and node is not func_def:
            raise ComputedParseError(
                "Nested function definitions are not supported in @computed"
            )
        if isinstance(node, ast.Lambda):
            raise ComputedParseError(
                "Lambda is not supported in @computed"
            )


def _inline_computed_refs(expr: Expr, frame_locals: dict) -> Expr:
    """Replace Field("x") references to other @computed with their Expr."""
    if isinstance(expr, Field):
        name = expr.name
        if name in frame_locals and isinstance(frame_locals[name], ComputedProperty):
            other: ComputedProperty = frame_locals[name]
            resolved_expr = other.expr
            if resolved_expr is not None:
                return resolved_expr
        return expr  # type: ignore[return-value]  # Field is subclass of Expr
    if isinstance(expr, BinOp):
        new_left = _inline_computed_refs(expr.left, frame_locals)
        new_right = _inline_computed_refs(expr.right, frame_locals)
        if new_left is None or new_right is None:
            return expr  # type: ignore[return-value]
        return BinOp(
            expr.op,
            new_left,
            new_right,
        )
    if isinstance(expr, UnaryOp):
        return UnaryOp(expr.op, _inline_computed_refs(expr.operand, frame_locals))
    if isinstance(expr, If):
        return If(
            _inline_computed_refs(expr.condition, frame_locals),
            _inline_computed_refs(expr.then_, frame_locals),
            _inline_computed_refs(expr.else_, frame_locals),
        )
    if isinstance(expr, Func):
        return Func(expr.name, [_inline_computed_refs(a, frame_locals) for a in expr.args])
    if isinstance(expr, Coalesce):
        return Coalesce([_inline_computed_refs(e, frame_locals) for e in expr.exprs])
    if isinstance(expr, IsNull):
        return IsNull(_inline_computed_refs(expr.operand, frame_locals))
    return expr


def computed(fn: Callable[..., Any]) -> ComputedProperty:
    """Decorator: marks a method as a reactive computed property.

    Usage:
        @computed
        def pnl(self):
            return (self.current_price - self.avg_cost) * self.quantity
    """
    expr = _parse_computed_source(fn)
    return ComputedProperty(fn=fn, expr=expr, name=fn.__name__)


def effect(target_computed: str) -> Callable[..., EffectMethod]:
    """Decorator factory: marks a method as a side-effect watcher.

    Usage:
        @effect("pnl")
        def check_stop_loss(self, value):
            if value < -5000:
                send_alert(...)
    """
    def decorator(fn: Callable[..., Any]) -> EffectMethod:
        return EffectMethod(target_computed=target_computed, fn=fn)
    return decorator
