"""
Datacube configuration models — Legend-inspired frozen dataclasses.

All config types are immutable (frozen=True) so snapshots are hashable
and every mutation returns a new instance.  This mirrors Legend Studio's
``DataCubeSnapshot`` pattern.

Column separator for pivot result columns matches Legend's convention::

    PIVOT_COLUMN_NAME_SEPARATOR = "__|__"

    # single-pivot:   "BUY__|__quantity"
    # multi-pivot:    "BUY__|__LIMIT__|__quantity"
"""

from __future__ import annotations

import json
import dataclasses
from dataclasses import dataclass, field
from typing import Any


# ── Constants ──────────────────────────────────────────────────────────

PIVOT_COLUMN_NAME_SEPARATOR = "__|__"


# ── Leaf / group extended columns ─────────────────────────────────────

@dataclass(frozen=True)
class ExtendedColumn:
    """A virtual computed column (DuckDB SQL expression).

    Leaf-level  → computed on raw rows, BEFORE filter / groupBy / pivot.
    Group-level → computed on aggregated rows, AFTER groupBy / pivot.
    """
    name: str
    expression: str           # raw DuckDB SQL, e.g. "price * quantity"
    type: str = "float"       # result type hint


# ── Filters ───────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Filter:
    """A single filter predicate.

    Supported ops: eq, ne, gt, lt, ge, le, in, not_in, like, not_like,
    between, is_null, is_not_null, contains, starts_with, ends_with.
    """
    field: str
    op: str
    value: Any = None         # scalar, list, or (lo, hi) for between


# ── Sort ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Sort:
    """A single sort specification."""
    field: str
    descending: bool = False


# ── Joins ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class JoinSpec:
    """Declarative join between the datacube source and another table.

    ``on`` is a tuple of (left_col, right_col) pairs.
    For complex multi-table joins, use a raw SQL source string instead.
    """
    source: str                                   # table FQN or subquery
    on: tuple[tuple[str, str], ...] = ()          # ((left, right), …)
    join_type: str = "LEFT"                       # LEFT, INNER, RIGHT, FULL
    alias: str | None = None


# ── Per-column configuration (Legend-style) ───────────────────────────

@dataclass(frozen=True)
class DatacubeColumnConfig:
    """Per-column configuration — kind, aggregation, pivot participation, display.

    Auto-initialized from the column registry when available.
    """
    name: str
    type: str                                     # "str", "int", "float", "bool", "datetime"

    # ── Query behaviour ──
    kind: str = ""                                # "dimension" or "measure"
    aggregate_operator: str = ""                  # sum, avg, count, min, max, first, last, unique, std, var, …
    excluded_from_pivot: bool = True              # True → dimension, False → measure participates in HPivot
    is_selected: bool = True                      # include in query results
    hide_from_view: bool = False                  # selected but hidden in grid

    # ── Display ──
    display_name: str | None = None
    format: str | None = None                     # Python format string, e.g. ",.2f"
    decimals: int | None = None
    unit: str | None = None

    # ── Factory ──────────────────────────────────────────────────

    @staticmethod
    def from_column_def(col_def, field_name: str | None = None) -> DatacubeColumnConfig:
        """Build a column config from a :class:`store.registry.ColumnDef`.

        ``field_name`` overrides ``col_def.name`` (e.g. prefixed columns).
        """
        name = field_name or col_def.name
        python_type = col_def.python_type

        # Map Python type → string token
        type_map = {str: "str", int: "int", float: "float", bool: "bool"}
        type_str = type_map.get(python_type, "str")

        # Kind + pivot exclusion
        kind = col_def.role if col_def.role in ("dimension", "measure") else (
            "measure" if python_type in (int, float) else "dimension"
        )
        excluded = kind == "dimension"

        # Default aggregation
        agg = col_def.aggregation or ("sum" if kind == "measure" else "")

        return DatacubeColumnConfig(
            name=name,
            type=type_str,
            kind=kind,
            aggregate_operator=agg,
            excluded_from_pivot=excluded,
            display_name=col_def.display_name,
            format=col_def.format,
            unit=col_def.unit,
        )

    @staticmethod
    def from_type(name: str, python_type: type) -> DatacubeColumnConfig:
        """Build a minimal column config by inferring kind from type."""
        type_map = {str: "str", int: "int", float: "float", bool: "bool"}
        type_str = type_map.get(python_type, "str")
        kind = "measure" if python_type in (int, float) else "dimension"
        excluded = kind == "dimension"
        agg = "sum" if kind == "measure" else ""
        return DatacubeColumnConfig(
            name=name, type=type_str, kind=kind,
            aggregate_operator=agg, excluded_from_pivot=excluded,
        )

    # ── Mutation helpers (return new instance) ────────────────────

    def replace(self, **kwargs) -> DatacubeColumnConfig:
        """Return a copy with the given fields replaced."""
        return dataclasses.replace(self, **kwargs)


# ── Snapshot (immutable query state) ──────────────────────────────────

@dataclass(frozen=True)
class DatacubeSnapshot:
    """Immutable query state — the single source of truth for a datacube.

    Every user action produces a new snapshot via ``dataclasses.replace()``.
    Frozen → hashable → change-detection via ``hash(snapshot)``.
    """
    source: str = ""

    # Per-column configs
    columns: tuple[DatacubeColumnConfig, ...] = ()

    # VPivot (GROUP BY) — ordered row-grouping dimensions
    group_by: tuple[str, ...] = ()

    # HPivot (PIVOT) — columns whose distinct values become column headers
    # Multi-column: ("side",) or ("side", "order_type")
    pivot_by: tuple[str, ...] = ()
    pivot_values: tuple[str, ...] | None = None   # explicit, or None → auto-discover
    pivot_statistic_column: str | None = "Total"  # aggregated total across pivot values

    # Leaf-level extended columns (pre-aggregation)
    leaf_extended_columns: tuple[ExtendedColumn, ...] = ()

    # Group-level extended columns (post-aggregation)
    group_extended_columns: tuple[ExtendedColumn, ...] = ()

    # Filters (WHERE)
    filters: tuple[Filter, ...] = ()

    # Sorting
    sort: tuple[Sort, ...] = ()

    # Joins
    joins: tuple[JoinSpec, ...] = ()

    # Pagination
    limit: int | None = None
    offset: int | None = None

    # Drilldown state — list of {col: value} dicts for parent groups
    drill_path: tuple[dict, ...] = ()

    # Root aggregation + leaf count
    show_root_aggregation: bool = False
    show_leaf_count: bool = True

    # ── Convenience helpers ───────────────────────────────────────

    def replace(self, **kwargs) -> DatacubeSnapshot:
        """Return a copy with the given fields replaced."""
        return dataclasses.replace(self, **kwargs)

    def get_column(self, name: str) -> DatacubeColumnConfig | None:
        """Find a column config by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def set_column(self, name: str, **kwargs) -> DatacubeSnapshot:
        """Return a new snapshot with one column's config updated."""
        new_cols = []
        for col in self.columns:
            if col.name == name:
                new_cols.append(col.replace(**kwargs))
            else:
                new_cols.append(col)
        return self.replace(columns=tuple(new_cols))

    def selected_columns(self) -> list[DatacubeColumnConfig]:
        """Return columns where ``is_selected`` is True."""
        return [c for c in self.columns if c.is_selected]

    def dimension_columns(self) -> list[DatacubeColumnConfig]:
        """Return columns where kind == 'dimension'."""
        return [c for c in self.columns if c.kind == "dimension"]

    def measure_columns(self) -> list[DatacubeColumnConfig]:
        """Return columns where kind == 'measure'."""
        return [c for c in self.columns if c.kind == "measure"]

    def pivot_measure_columns(self) -> list[DatacubeColumnConfig]:
        """Return measure columns that participate in HPivot."""
        return [c for c in self.columns
                if c.kind == "measure" and not c.excluded_from_pivot]

    # ── Serialization ─────────────────────────────────────────────

    def to_dict(self) -> dict:
        """Serialize to a plain dict (JSON-safe)."""
        return dataclasses.asdict(self)

    def to_json(self) -> str:
        """Serialize to a JSON string."""
        return json.dumps(self.to_dict(), default=str)

    @classmethod
    def from_dict(cls, data: dict) -> DatacubeSnapshot:
        """Deserialize from a plain dict."""
        # Reconstruct nested frozen dataclasses
        columns = tuple(
            DatacubeColumnConfig(**c) for c in data.get("columns", ())
        )
        leaf_ext = tuple(
            ExtendedColumn(**e) for e in data.get("leaf_extended_columns", ())
        )
        group_ext = tuple(
            ExtendedColumn(**e) for e in data.get("group_extended_columns", ())
        )
        filters = tuple(Filter(**f) for f in data.get("filters", ()))
        sort = tuple(Sort(**s) for s in data.get("sort", ()))
        joins = tuple(
            JoinSpec(
                source=j["source"],
                on=tuple(tuple(pair) for pair in j.get("on", ())),
                join_type=j.get("join_type", "LEFT"),
                alias=j.get("alias"),
            )
            for j in data.get("joins", ())
        )
        drill_path = tuple(data.get("drill_path", ()))

        return cls(
            source=data.get("source", ""),
            columns=columns,
            group_by=tuple(data.get("group_by", ())),
            pivot_by=tuple(data.get("pivot_by", ())),
            pivot_values=(
                tuple(data["pivot_values"])
                if data.get("pivot_values") is not None
                else None
            ),
            pivot_statistic_column=data.get("pivot_statistic_column", "Total"),
            leaf_extended_columns=leaf_ext,
            group_extended_columns=group_ext,
            filters=filters,
            sort=sort,
            joins=joins,
            limit=data.get("limit"),
            offset=data.get("offset"),
            drill_path=drill_path,
            show_root_aggregation=data.get("show_root_aggregation", False),
            show_leaf_count=data.get("show_leaf_count", True),
        )

    @classmethod
    def from_json(cls, json_str: str) -> DatacubeSnapshot:
        """Deserialize from a JSON string."""
        return cls.from_dict(json.loads(json_str))
