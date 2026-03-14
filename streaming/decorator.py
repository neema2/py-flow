"""
streaming.decorator — @ticking class decorator.

Auto-creates a TickingTable from a Storable dataclass, deriving column
schema from dataclass fields and @computed properties.

Usage::

    @ticking
    @dataclass
    class FXSpot(Storable):
        __key__ = "pair"
        pair: str = ""
        bid: float = 0.0
        ...

    @ticking(exclude={"base_rate", "sensitivity"})
    @dataclass
    class YieldCurvePoint(Storable):
        __key__ = "label"
        ...

Adds to the class:
    cls._ticking_table   TickingTable instance
    cls._ticking_live    LiveTable (last_by __key__)
    cls._ticking_cols    [(col_name, attr_name, python_type), ...]
    cls._ticking_name    snake_case name derived from class name
    self.tick()          instance method — writes all column values
"""

import re
from typing import Any

from streaming.table import LiveTable, TickingTable

# Global registry: table_name → (TickingTable, LiveTable)
_registry: dict[str, tuple[TickingTable, LiveTable]] = {}

# Primitive types that map to ticking table columns
_PRIMITIVE_TYPES = {str, float, int, bool}


def _to_snake_case(name: str) -> str:
    """Convert CamelCase class name to snake_case table name.

    FXSpot           → fx_spot
    YieldCurvePoint  → yield_curve_point
    IRSwapFixedFloatApprox → interest_rate_swap
    SwapPortfolio    → swap_portfolio
    """
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


def _resolve_column_specs(cls: type, exclude: set | None = None) -> list[tuple[str, str, type]]:
    """Pure-Python column resolution — no DH imports needed.

    Returns list of (col_name, attr_name, python_type).
    Skips non-primitive fields (object, list, etc.) and anything in exclude.
    """
    from reactive.computed import ComputedProperty

    exclude = set(exclude) if exclude else set()
    specs = []

    # 1. Dataclass fields (in definition order)
    for fname, fobj in cls.__dataclass_fields__.items():  # type: ignore[attr-defined]
        if fname in exclude or fname.startswith("_"):
            continue
        py_type = fobj.type
        if isinstance(py_type, str):
            py_type = {"str": str, "float": float, "int": int, "bool": bool}.get(py_type)
        if py_type not in _PRIMITIVE_TYPES:
            continue  # skip object, list, etc.
        specs.append((fname, fname, py_type))

    # 2. @computed properties (sorted for deterministic order)
    computed_names = sorted(
        name
        for name in dir(cls)
        if not name.startswith("_")
        and name not in exclude
        and isinstance(getattr(cls, name, None), ComputedProperty)
    )
    for name in computed_names:
        cp = getattr(cls, name)
        ret = getattr(cp.fn, "__annotations__", {}).get("return", float)
        
        # Handle "from __future__ import annotations" stringified types
        if isinstance(ret, str):
            ret = {"str": str, "float": float, "int": int, "bool": bool}.get(ret)

        if ret not in _PRIMITIVE_TYPES:
            # Skip non-primitive return types (list, dict, object) 
            # instead of defaulting to float.
            continue
            
        specs.append((name, name, ret))

    return specs


def _tick(self: Any) -> None:
    """Write all column values to the ticking table. Added to decorated classes."""
    cls = type(self)
    try:
        cls._ticking_table.write_row(*(getattr(self, attr) for _, attr, _ in cls._ticking_cols))
    except RuntimeError as e:
        if "Deephaven session not available" in str(e):
            # Silent fallback if DH is not running (e.g. offline tests)
            pass
        else:
            raise


def _apply_ticking(cls: type, exclude: set | None = None) -> type:
    """Core logic: create TickingTable, derive live table, attach to class."""
    # Require __key__
    key = getattr(cls, "__key__", None)
    if key is None:
        raise ValueError(
            f"@ticking on {cls.__name__} requires a __key__ class variable "
            f"(e.g. __key__ = 'symbol')"
        )

    # Resolve columns (pure Python types)
    col_specs = _resolve_column_specs(cls, exclude)
    if not col_specs:
        raise ValueError(f"@ticking on {cls.__name__}: no columns resolved")

    # Table name from class name
    table_name = _to_snake_case(cls.__name__)

    # Create TickingTable with Python-typed schema
    schema = {col_name: py_type for col_name, _, py_type in col_specs}
    tt = TickingTable(schema)

    # Derive live table (auto-locked via TickingTable.last_by)
    live = tt.last_by(key)

    # Attach to class
    cls._ticking_table = tt  # type: ignore[attr-defined]
    cls._ticking_live = live  # type: ignore[attr-defined]
    cls._ticking_cols = col_specs  # type: ignore[attr-defined]
    cls._ticking_name = table_name  # type: ignore[attr-defined]
    cls.tick = _tick  # type: ignore[attr-defined]

    # Register
    _registry[table_name] = (tt, live)

    return cls


def ticking(cls: type | None = None, *, exclude: set | None = None) -> type:
    """Class decorator: auto-create TickingTable + live table from Storable fields.

    Supports both bare and parameterized usage::

        @ticking                          # auto-infer all columns
        @ticking(exclude={"internal"})    # skip specific fields
    """
    if cls is not None:
        # Bare @ticking (no parentheses)
        return _apply_ticking(cls)
    # Parameterized @ticking(exclude=...)
    def decorator(cls: type) -> type:
        return _apply_ticking(cls, exclude=exclude)
    return decorator  # type: ignore[return-value]


def get_tables() -> dict:
    """Return dict of all registered tables: {name_raw: TickingTable, name_live: LiveTable}.

    Returns wrapped tables so all ops are auto-locked.
    """
    tables: dict[str, LiveTable] = {}
    for name, (tt, live) in _registry.items():
        tables[f"{name}_raw"] = tt          # TickingTable (inherits LiveTable)
        tables[f"{name}_live"] = live       # LiveTable from last_by
    return tables


def get_ticking_tables() -> dict:
    """Return dict of all registered TickingTable instances: {name: TickingTable}."""
    return {name: tt for name, (tt, _live) in _registry.items()}
