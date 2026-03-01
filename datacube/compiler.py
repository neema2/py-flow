"""
Datacube SQL compiler — snapshot → DuckDB SQL string.

Stateless functions that compile a :class:`DatacubeSnapshot` into a
DuckDB-compatible SQL query.  The compilation pipeline follows Legend
Studio's query composition order::

    source
      → leaf extend (CTE)
      → filter (WHERE)
      → select (column projection)
      → pivot  (conditional aggregation in SELECT)
      → group by (GROUP BY)
      → group extend (post-agg CTE wrapper)
      → sort (ORDER BY)
      → limit / offset
"""

from __future__ import annotations

import logging
from itertools import product
from typing import Any, Sequence

from datacube.config import (
    DatacubeSnapshot,
    DatacubeColumnConfig,
    ExtendedColumn,
    Filter,
    JoinSpec,
    Sort,
    PIVOT_COLUMN_NAME_SEPARATOR,
)

logger = logging.getLogger(__name__)

# ── Public API ────────────────────────────────────────────────────────

def compile(snapshot: DatacubeSnapshot) -> str:  # noqa: A001 — shadowing builtin is intentional
    """Compile a datacube snapshot to a DuckDB SQL string.

    This is the main entry point.  The returned SQL is ready to execute
    against a DuckDB connection.
    """
    return _Compiler(snapshot).build()


def discover_pivot_values(
    conn,
    snapshot: DatacubeSnapshot,
) -> list[str]:
    """Run ``SELECT DISTINCT`` to discover HPivot column values.

    For multi-column pivot, returns the Cartesian-product labels joined
    by :data:`PIVOT_COLUMN_NAME_SEPARATOR`.

    Args:
        conn: A DuckDB connection (or Lakehouse with ``._ensure_conn()``).
        snapshot: The current datacube snapshot.

    Returns:
        Sorted list of distinct pivot value labels.
    """
    if not snapshot.pivot_by:
        return []

    # Build the FROM + leaf-extend source
    source_sql = _source_sql(snapshot)

    # For each pivot column, get distinct values
    all_values: list[list[str]] = []
    for pivot_col in snapshot.pivot_by:
        sql = f"SELECT DISTINCT \"{pivot_col}\" FROM ({source_sql}) AS _dc_src ORDER BY \"{pivot_col}\""
        try:
            result = conn.execute(sql).fetchall()
            vals = [str(row[0]) for row in result if row[0] is not None]
        except Exception:
            logger.warning("Failed to discover pivot values for %s", pivot_col)
            vals = []
        all_values.append(vals)

    if len(all_values) == 1:
        return all_values[0]

    # Multi-column: Cartesian product
    return [
        PIVOT_COLUMN_NAME_SEPARATOR.join(combo)
        for combo in product(*all_values)
    ]


# ── Aggregation SQL mapping ──────────────────────────────────────────

_AGG_SQL: dict[str, str] = {
    "sum":            "SUM({col})",
    "avg":            "AVG({col})",
    "count":          "COUNT({col})",
    "count_distinct": "COUNT(DISTINCT {col})",
    "min":            "MIN({col})",
    "max":            "MAX({col})",
    "first":          "FIRST({col})",
    "last":           "LAST({col})",
    "std":            "STDDEV_POP({col})",
    "std_sample":     "STDDEV_SAMP({col})",
    "var":            "VAR_POP({col})",
    "var_sample":     "VAR_SAMP({col})",
    "unique":         "FIRST({col})",
}


def _agg_expr(operator: str, col_ref: str) -> str:
    """Return DuckDB aggregation expression for the given operator."""
    template = _AGG_SQL.get(operator, "SUM({col})")
    return template.format(col=col_ref)


# ── Filter SQL compilation ───────────────────────────────────────────

def _quote_value(value: Any) -> str:
    """Quote a Python value for SQL injection into DuckDB."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    # String — escape single quotes
    return "'" + str(value).replace("'", "''") + "'"


def _compile_filter(f: Filter) -> str:
    """Compile one Filter to a SQL predicate string."""
    col = f'"{f.field}"'
    op = f.op.lower()

    if op == "eq":
        return f"{col} = {_quote_value(f.value)}"
    elif op == "ne":
        return f"{col} != {_quote_value(f.value)}"
    elif op == "gt":
        return f"{col} > {_quote_value(f.value)}"
    elif op == "lt":
        return f"{col} < {_quote_value(f.value)}"
    elif op == "ge":
        return f"{col} >= {_quote_value(f.value)}"
    elif op == "le":
        return f"{col} <= {_quote_value(f.value)}"
    elif op == "in":
        vals = ", ".join(_quote_value(v) for v in f.value)
        return f"{col} IN ({vals})"
    elif op == "not_in":
        vals = ", ".join(_quote_value(v) for v in f.value)
        return f"{col} NOT IN ({vals})"
    elif op == "like":
        return f"{col} LIKE {_quote_value(f.value)}"
    elif op == "not_like":
        return f"{col} NOT LIKE {_quote_value(f.value)}"
    elif op == "between":
        lo, hi = f.value
        return f"{col} BETWEEN {_quote_value(lo)} AND {_quote_value(hi)}"
    elif op == "is_null":
        return f"{col} IS NULL"
    elif op == "is_not_null":
        return f"{col} IS NOT NULL"
    elif op == "contains":
        escaped = str(f.value).replace("'", "''")
        return f"{col} LIKE '%{escaped}%'"
    elif op == "starts_with":
        escaped = str(f.value).replace("'", "''")
        return f"{col} LIKE '{escaped}%'"
    elif op == "ends_with":
        escaped = str(f.value).replace("'", "''")
        return f"{col} LIKE '%{escaped}'"
    else:
        raise ValueError(f"Unknown filter operator: {f.op!r}")


def _compile_filters(filters: Sequence[Filter]) -> str:
    """Compile a list of filters to a WHERE clause (without the WHERE keyword)."""
    if not filters:
        return ""
    parts = [_compile_filter(f) for f in filters]
    return " AND ".join(parts)


# ── Source SQL (with leaf extend + joins) ─────────────────────────────

def _source_sql(snapshot: DatacubeSnapshot) -> str:
    """Build the source SQL including leaf-level extended columns and joins.

    Returns a SQL expression that can be used as a subquery.
    """
    source = snapshot.source

    # Determine the base FROM clause
    # If source looks like a subquery (starts with '(' or 'SELECT')
    trimmed = source.strip()
    if trimmed.upper().startswith("SELECT") or trimmed.startswith("("):
        from_clause = f"({trimmed}) AS _dc_base"
    else:
        from_clause = f"{source} AS _dc_base"

    # Joins
    join_clauses = []
    for j in snapshot.joins:
        alias = j.alias or f"_j{len(join_clauses)}"
        on_parts = " AND ".join(
            f'_dc_base."{left}" = {alias}."{right}"'
            for left, right in j.on
        )
        join_clauses.append(
            f"{j.join_type} JOIN {j.source} AS {alias} ON {on_parts}"
        )

    joins_sql = " ".join(join_clauses)

    # Leaf extended columns
    if snapshot.leaf_extended_columns:
        ext_cols = ", ".join(
            f"({e.expression}) AS \"{e.name}\""
            for e in snapshot.leaf_extended_columns
        )
        return f"SELECT _dc_base.*, {ext_cols} FROM {from_clause} {joins_sql}".strip()
    elif joins_sql:
        return f"SELECT * FROM {from_clause} {joins_sql}".strip()
    else:
        # Simple case — no extensions, no joins, return source directly
        if trimmed.upper().startswith("SELECT") or trimmed.startswith("("):
            return trimmed
        return f"SELECT * FROM {source}"


# ── Compiler core ─────────────────────────────────────────────────────

class _Compiler:
    """Internal compiler: snapshot → DuckDB SQL."""

    def __init__(self, snapshot: DatacubeSnapshot):
        self.snap = snapshot

    # ── Main ──────────────────────────────────────────────────────

    def build(self) -> str:
        """Build the full SQL query."""
        has_groupby = bool(self.snap.group_by)
        has_pivot = bool(self.snap.pivot_by)
        has_group_ext = bool(self.snap.group_extended_columns)

        # Determine effective group-by columns (adjusted for drilldown)
        effective_group_by = self._effective_group_by()

        # 1. Inner query: source + leaf extend + joins
        inner_sql = _source_sql(self.snap)

        # 2. Build drilldown + user filters for WHERE
        all_filters = list(self.snap.filters)
        for drill in self.snap.drill_path:
            for col, val in drill.items():
                all_filters.append(Filter(field=col, op="eq", value=val))

        where_clause = _compile_filters(all_filters)

        # 3. Build SELECT + GROUP BY
        # When drilldown reaches max depth, effective_group_by is empty
        # even though the original group_by was set — show flat leaf rows.
        needs_aggregation = (has_groupby or has_pivot) and (
            bool(effective_group_by) or has_pivot
        )
        if needs_aggregation:
            select_parts, group_by_parts = self._build_aggregated_select(
                effective_group_by, has_pivot,
            )
        else:
            # Flat query — no aggregation
            select_parts = self._build_flat_select()
            group_by_parts = []

        select_clause = ", ".join(select_parts) if select_parts else "*"
        group_by_clause = ", ".join(group_by_parts)

        # 4. Assemble inner aggregated query
        parts = [f"SELECT {select_clause}"]
        parts.append(f"FROM ({inner_sql}) AS _dc_src")
        if where_clause:
            parts.append(f"WHERE {where_clause}")
        if group_by_clause:
            parts.append(f"GROUP BY {group_by_clause}")

        agg_sql = " ".join(parts)

        # 5. Group-level extend (post-aggregation CTE wrapper)
        if has_group_ext and (has_groupby or has_pivot):
            ext_cols = ", ".join(
                f"({e.expression}) AS \"{e.name}\""
                for e in self.snap.group_extended_columns
            )
            agg_sql = f"SELECT *, {ext_cols} FROM ({agg_sql}) AS _dc_agg"

        # 6. Sort
        order_clause = self._build_order_by()

        # 7. Assemble final query
        final_parts = [agg_sql]
        if order_clause:
            final_parts.append(f"ORDER BY {order_clause}")
        if self.snap.limit is not None:
            final_parts.append(f"LIMIT {self.snap.limit}")
        if self.snap.offset is not None:
            final_parts.append(f"OFFSET {self.snap.offset}")

        return " ".join(final_parts)

    # ── Drilldown ─────────────────────────────────────────────────

    def _effective_group_by(self) -> tuple[str, ...]:
        """Adjust group_by for drilldown depth.

        If the user has drilled down N levels, we group by the (N+1)th
        column only.  At max depth, we return an empty tuple (leaf rows).
        """
        if not self.snap.group_by or not self.snap.drill_path:
            return self.snap.group_by

        depth = len(self.snap.drill_path)
        if depth >= len(self.snap.group_by):
            # Max depth reached — show leaf rows (no GROUP BY)
            return ()
        # Group by the next level only
        return (self.snap.group_by[depth],)

    # ── Flat SELECT (no aggregation) ─────────────────────────────

    def _build_flat_select(self) -> list[str]:
        """Build SELECT for flat (non-aggregated) query."""
        # When leaf extensions or joins exist, use * to include
        # computed/joined columns that aren't in the snapshot
        if self.snap.leaf_extended_columns or self.snap.joins:
            return ["*"]
        selected = self.snap.selected_columns()
        if not selected:
            return ["*"]
        return [f'"{c.name}"' for c in selected]

    # ── Aggregated SELECT + GROUP BY ─────────────────────────────

    def _build_aggregated_select(
        self,
        effective_group_by: tuple[str, ...],
        has_pivot: bool,
    ) -> tuple[list[str], list[str]]:
        """Build SELECT and GROUP BY for aggregated query.

        Returns (select_parts, group_by_parts).
        """
        select_parts: list[str] = []
        group_by_parts: list[str] = []

        # Group-by columns go first in SELECT and GROUP BY
        for gb_col in effective_group_by:
            select_parts.append(f'"{gb_col}"')
            group_by_parts.append(f'"{gb_col}"')

        # Measures — either plain or pivoted
        measures = [
            c for c in self.snap.columns
            if c.kind == "measure" and c.is_selected
        ]

        if has_pivot and self.snap.pivot_by:
            self._add_pivot_measures(select_parts, measures)
        else:
            self._add_plain_measures(select_parts, measures)

        return select_parts, group_by_parts

    def _add_plain_measures(
        self,
        select_parts: list[str],
        measures: list[DatacubeColumnConfig],
    ) -> None:
        """Add non-pivoted aggregation expressions."""
        for m in measures:
            agg = _agg_expr(m.aggregate_operator, f'"{m.name}"')
            alias = f'"{m.name}"'
            select_parts.append(f"{agg} AS {alias}")

    def _add_pivot_measures(
        self,
        select_parts: list[str],
        measures: list[DatacubeColumnConfig],
    ) -> None:
        """Add pivoted conditional-aggregation expressions."""
        pivot_cols = self.snap.pivot_by
        pivot_values = self._resolve_pivot_values()

        # Separate pivot-participating measures from excluded ones
        pivot_measures = [m for m in measures if not m.excluded_from_pivot]
        excluded_measures = [m for m in measures if m.excluded_from_pivot]

        # Excluded measures get plain aggregation
        self._add_plain_measures(select_parts, excluded_measures)

        # Pivot measures: conditional aggregation per pivot-value combo
        for combo in pivot_values:
            # Build the CASE WHEN condition
            if isinstance(combo, str):
                # Single-column pivot
                condition = f"\"{pivot_cols[0]}\" = {_quote_value(combo)}"
                label = combo
            else:
                # Multi-column pivot: combo is a tuple
                conditions = []
                labels = []
                for i, val in enumerate(combo):
                    conditions.append(f"\"{pivot_cols[i]}\" = {_quote_value(val)}")
                    labels.append(str(val))
                condition = " AND ".join(conditions)
                label = PIVOT_COLUMN_NAME_SEPARATOR.join(labels)

            for m in pivot_measures:
                agg_template = _AGG_SQL.get(m.aggregate_operator, "SUM({col})")
                case_expr = f"CASE WHEN {condition} THEN \"{m.name}\" END"
                agg_expr_str = agg_template.format(col=case_expr)
                col_name = f"{label}{PIVOT_COLUMN_NAME_SEPARATOR}{m.name}"
                select_parts.append(f'{agg_expr_str} AS "{col_name}"')

        # Pivot statistic column ("Total")
        if self.snap.pivot_statistic_column:
            for m in pivot_measures:
                agg = _agg_expr(m.aggregate_operator, f'"{m.name}"')
                col_name = (
                    f"{self.snap.pivot_statistic_column}"
                    f"{PIVOT_COLUMN_NAME_SEPARATOR}{m.name}"
                )
                select_parts.append(f'{agg} AS "{col_name}"')

    def _resolve_pivot_values(self) -> list:
        """Return pivot values — explicit or placeholder.

        If ``pivot_values`` is None (auto-discover), we return a
        placeholder.  The engine must call ``discover_pivot_values()``
        first and set ``pivot_values`` on the snapshot before compiling.
        """
        if self.snap.pivot_values is not None:
            vals = list(self.snap.pivot_values)
        else:
            # Caller should have discovered values.  Return empty →
            # the query will have no pivot columns (still valid SQL).
            logger.warning(
                "pivot_values is None — call discover_pivot_values() first"
            )
            return []

        # For multi-column pivot, split combined labels back into tuples
        if len(self.snap.pivot_by) > 1:
            return [
                tuple(v.split(PIVOT_COLUMN_NAME_SEPARATOR))
                for v in vals
            ]
        return vals

    # ── ORDER BY ──────────────────────────────────────────────────

    def _build_order_by(self) -> str:
        """Build the ORDER BY clause."""
        if not self.snap.sort:
            return ""
        parts = []
        for s in self.snap.sort:
            direction = "DESC" if s.descending else "ASC"
            parts.append(f'"{s.field}" {direction}')
        return ", ".join(parts)
