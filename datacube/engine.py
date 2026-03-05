"""
Datacube engine — query execution + snapshot mutations.

The :class:`Datacube` class is the user-facing API.  It wraps an
immutable :class:`DatacubeSnapshot` and a DuckDB connection source.
Every mutation method returns a **new** Datacube (immutable pattern).

Sources:

- ``Lakehouse`` — uses its DuckDB connection.
- ``duckdb.DuckDBPyConnection`` — direct.
- Storable subclass — pulls data via ``cls.query()`` → Arrow → temp view.
- ``pa.Table`` / ``pd.DataFrame`` — registered as DuckDB temp view.
- Raw SQL string — used as-is.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    import duckdb
    import pandas as pd
    import pyarrow as pa
    from store import Storable


@runtime_checkable
class LakehouseProtocol(Protocol):
    """Structural type for the Lakehouse duck-typed interface."""

    def _ensure_conn(self) -> duckdb.DuckDBPyConnection: ...
    def _fqn(self, table_name: str) -> str: ...
    def table_info(self, table_name: str) -> list[dict]: ...

from datacube import compiler as _compiler
from datacube.config import (
    PIVOT_COLUMN_NAME_SEPARATOR,
    DatacubeColumnConfig,
    DatacubeSnapshot,
    ExtendedColumn,
    Filter,
    JoinSpec,
    Sort,
)

logger = logging.getLogger(__name__)


# Lakehouse internal columns to filter out of datacube schema
_LAKEHOUSE_INTERNAL_COLUMNS = frozenset({
    "_batch_id", "_batch_ts", "_is_current", "_updated_at",
    "_tx_time", "_valid_from", "_valid_to",
})


class Datacube:
    """Legend-inspired datacube engine over DuckDB.

    When the source is a **Lakehouse**, the datacube queries Iceberg
    tables directly from S3 — data never enters Python memory.  DuckDB's
    Iceberg extension reads Parquet files with partition pruning and
    predicate pushdown.

    Usage::

        dc = lakehouse.datacube("trades")
        dc = dc.set_group_by("sector", "symbol")
        dc = dc.set_pivot_by("side")
        result = dc.query_df()
    """

    def __init__(
        self,
        source: Any,
        *,
        snapshot: DatacubeSnapshot | None = None,
        source_name: str | None = None,
        lakehouse: LakehouseProtocol | None = None,
    ) -> None:
        """
        Args:
            source: A Lakehouse, DuckDB connection, Storable class,
                    PyArrow Table, pandas DataFrame, or raw SQL string.
            snapshot: Optional pre-built snapshot.  If None, one is
                      auto-created from the source.
            source_name: Override the source identifier in the snapshot
                         (e.g. a table FQN or short Lakehouse table name).
            lakehouse: Optional Lakehouse reference.  When provided for
                       non-Lakehouse sources (e.g. DataFrame), enables
                       cross-source joins with Lakehouse tables and
                       connection reuse.
        """
        self._lakehouse = None  # set when source is Lakehouse

        self._conn, resolved_name, columns = _resolve_source(
            source, lakehouse=lakehouse,
        )
        name = source_name or resolved_name

        # Detect Lakehouse source
        if _is_lakehouse(source):
            self._lakehouse = source
            # Auto-FQN: short table name → lakehouse.{ns}.{table}
            if name and not name.startswith("lakehouse."):
                name = source._fqn(name)
            # Rich schema via table_info()
            if not columns and source_name:
                columns = _columns_from_lakehouse(source, source_name)
        elif lakehouse is not None and _is_lakehouse(lakehouse):
            self._lakehouse = lakehouse

        if snapshot is not None:
            self._snapshot = snapshot
        else:
            # If no columns discovered yet but we have a source name,
            # try discovering from the connection
            if not columns and name:
                columns = _discover_columns_from_sql(self._conn, name)
            self._snapshot = DatacubeSnapshot(
                source=name,
                columns=tuple(columns),
            )

    # ── Properties ────────────────────────────────────────────────

    @property
    def snapshot(self) -> DatacubeSnapshot:
        """The current immutable snapshot."""
        return self._snapshot

    # ── Query ─────────────────────────────────────────────────────

    def query(self) -> pa.Table:
        """Execute the datacube query and return a PyArrow Table."""

        snap = self._ensure_pivot_values()
        sql = _compiler.compile(snap)
        logger.debug("Datacube SQL:\n%s", sql)
        return self._conn.execute(sql).fetch_arrow_table()

    def query_df(self) -> pd.DataFrame:
        """Execute and return a pandas DataFrame."""
        snap = self._ensure_pivot_values()
        sql = _compiler.compile(snap)
        logger.debug("Datacube SQL:\n%s", sql)
        return self._conn.execute(sql).fetchdf()

    def query_dicts(self) -> list[dict]:
        """Execute and return a list of dicts."""
        snap = self._ensure_pivot_values()
        sql = _compiler.compile(snap)
        logger.debug("Datacube SQL:\n%s", sql)
        result = self._conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row, strict=False)) for row in rows]

    def sql(self) -> str:
        """Return the compiled SQL without executing."""
        snap = self._ensure_pivot_values()
        return _compiler.compile(snap)

    # ── Snapshot mutations (return new Datacube) ──────────────────

    def _evolve(self, **kwargs: Any) -> Datacube:
        """Return a new Datacube with an updated snapshot."""
        new_snap = self._snapshot.replace(**kwargs)
        dc = Datacube.__new__(Datacube)
        dc._conn = self._conn
        dc._lakehouse = self._lakehouse
        dc._snapshot = new_snap
        return dc

    def set_group_by(self, *fields: str) -> Datacube:
        """Set VPivot (GROUP BY) dimensions.  Clears drilldown state."""
        return self._evolve(group_by=tuple(fields), drill_path=())

    def set_pivot_by(self, *fields: str) -> Datacube:
        """Set HPivot columns.  Pass no args to disable pivot.

        Multi-column pivot: ``dc.set_pivot_by("side", "order_type")``
        """
        return self._evolve(pivot_by=tuple(fields), pivot_values=None)

    def set_column(self, name: str, **kwargs: Any) -> Datacube:
        """Update one column's configuration (returns new Datacube)."""
        new_snap = self._snapshot.set_column(name, **kwargs)
        dc = Datacube.__new__(Datacube)
        dc._conn = self._conn
        dc._lakehouse = self._lakehouse
        dc._snapshot = new_snap
        return dc

    def add_filter(self, field: str, op: str, value: Any = None) -> Datacube:
        """Add a filter predicate."""
        f = Filter(field=field, op=op, value=value)
        return self._evolve(filters=(*self._snapshot.filters, f))

    def clear_filters(self) -> Datacube:
        """Remove all filters."""
        return self._evolve(filters=())

    def set_sort(self, *sorts: Sort | tuple) -> Datacube:
        """Set sort order.

        Each arg can be a ``Sort`` or a ``(field, descending)`` tuple.
        """
        parsed = []
        for s in sorts:
            if isinstance(s, Sort):
                parsed.append(s)
            elif isinstance(s, tuple):
                parsed.append(Sort(field=s[0], descending=s[1] if len(s) > 1 else False))
            else:
                parsed.append(Sort(field=s, descending=False))  # type: ignore[unreachable]
        return self._evolve(sort=tuple(parsed))

    def add_join(
        self,
        source: str,
        on: dict[str, str],
        join_type: str = "LEFT",
        alias: str | None = None,
    ) -> Datacube:
        """Add a table join.

        When a Lakehouse is attached, short table names are auto-resolved
        to fully qualified Iceberg names.
        """
        # Auto-FQN for join targets when Lakehouse is available
        resolved_source = source
        if self._lakehouse and not source.startswith("lakehouse."):
            resolved_source = self._lakehouse._fqn(source)
        j = JoinSpec(
            source=resolved_source,
            on=tuple(on.items()),
            join_type=join_type,
            alias=alias,
        )
        return self._evolve(joins=(*self._snapshot.joins, j))

    def add_leaf_extend(self, name: str, expression: str, type: str = "float") -> Datacube:
        """Add a leaf-level extended column (pre-aggregation).

        Also registers the column in the snapshot so it can be configured
        with ``set_column()`` (e.g. to set an aggregate operator).
        """
        e = ExtendedColumn(name=name, expression=expression, type=type)
        # Auto-register as a snapshot column if not already present
        existing_names = {c.name for c in self._snapshot.columns}
        if name not in existing_names:
            kind = "measure" if type in ("int", "float") else "dimension"
            agg = "sum" if kind == "measure" else ""
            new_col = DatacubeColumnConfig(
                name=name, type=type, kind=kind,
                aggregate_operator=agg,
                excluded_from_pivot=(kind == "dimension"),
            )
            return self._evolve(
                leaf_extended_columns=(*self._snapshot.leaf_extended_columns, e),
                columns=(*self._snapshot.columns, new_col),
            )
        return self._evolve(
            leaf_extended_columns=(*self._snapshot.leaf_extended_columns, e),
        )

    def add_group_extend(self, name: str, expression: str, type: str = "float") -> Datacube:
        """Add a group-level extended column (post-aggregation)."""
        e = ExtendedColumn(name=name, expression=expression, type=type)
        return self._evolve(
            group_extended_columns=(*self._snapshot.group_extended_columns, e),
        )

    def set_limit(self, limit: int | None, offset: int | None = None) -> Datacube:
        """Set pagination."""
        return self._evolve(limit=limit, offset=offset)

    def set_pivot_statistic(self, name: str | None) -> Datacube:
        """Set or disable the pivot statistic ("Total") column."""
        return self._evolve(pivot_statistic_column=name)

    # ── Drill-down ────────────────────────────────────────────────

    def drill_down(self, **row_values: Any) -> Datacube:
        """Drill into a row group.  Adds a pre-filter and advances depth.

        Example: ``dc.drill_down(sector="Tech")``
        """
        new_path = (*self._snapshot.drill_path, dict(row_values))
        return self._evolve(drill_path=new_path)

    def drill_up(self) -> Datacube:
        """Remove the last drill-down level."""
        if not self._snapshot.drill_path:
            return self
        return self._evolve(drill_path=self._snapshot.drill_path[:-1])

    def drill_reset(self) -> Datacube:
        """Reset drill-down to top level."""
        return self._evolve(drill_path=())

    # ── Introspection ─────────────────────────────────────────────

    def available_dimensions(self) -> list[str]:
        """Column names with kind == 'dimension'."""
        return [c.name for c in self._snapshot.dimension_columns()]

    def available_measures(self) -> list[str]:
        """Column names with kind == 'measure'."""
        return [c.name for c in self._snapshot.measure_columns()]

    def result_columns(self) -> list[str]:
        """Column names that would appear in the query result.

        If pivot is active, includes the generated pivot column names.
        """
        snap = self._ensure_pivot_values()
        sql = _compiler.compile(snap)
        # Use LIMIT 0 trick to get column names without data
        try:
            result = self._conn.execute(f"SELECT * FROM ({sql}) AS _rc LIMIT 0")
            return [desc[0] for desc in result.description]
        except Exception:
            return []

    def pivot_result_columns(self) -> list[str]:
        """Return only the generated pivot column names."""
        return [
            c for c in self.result_columns()
            if PIVOT_COLUMN_NAME_SEPARATOR in c
        ]

    # ── Serialization ─────────────────────────────────────────────

    def to_json(self) -> str:
        """Serialize the current snapshot to JSON."""
        return self._snapshot.to_json()

    # ── UI ─────────────────────────────────────────────────────────

    def show(self, port: int = 8050, open_browser: bool = True) -> None:
        """Open an interactive Perspective grid in the browser.

        Runs a Tornado server **in-process** (like ``plt.show()``).
        All SQL compilation and DuckDB pushdown happens in this process;
        Perspective is the Arrow renderer.

        Args:
            port: HTTP port (default 8050).
            open_browser: Auto-open a browser tab.
        """
        from datacube.server import run
        run(self, port=port, open_browser=open_browser)

    # ── Internal ──────────────────────────────────────────────────

    def _ensure_pivot_values(self) -> DatacubeSnapshot:
        """If pivot is active but values are None, discover them."""
        snap = self._snapshot
        if snap.pivot_by and snap.pivot_values is None:
            values = _compiler.discover_pivot_values(self._conn, snap)
            snap = snap.replace(pivot_values=tuple(values))
            # Cache for future calls
            self._snapshot = snap
        return snap


# ── Source resolution ─────────────────────────────────────────────────

def _is_lakehouse(obj: Any) -> bool:
    """Check if an object is a Lakehouse instance (duck-typed)."""
    return isinstance(obj, LakehouseProtocol)


def _columns_from_lakehouse(
    lakehouse: Any,
    table_name: str,
) -> list[DatacubeColumnConfig]:
    """Discover columns via Lakehouse ``table_info()`` (DESCRIBE).

    This is the Lakehouse fast path — richer than the ``LIMIT 0`` hack
    used for generic sources.  Filters out internal Lakehouse columns
    (``_batch_id``, ``_batch_ts``, ``_is_current``, etc.).
    """
    try:
        info = lakehouse.table_info(table_name)
    except Exception:
        logger.debug("table_info() failed for %s, falling back", table_name)
        return []

    # DuckDB type mapping
    _TYPE_MAP = {
        "VARCHAR": "str", "TEXT": "str",
        "INTEGER": "int", "INT": "int", "BIGINT": "int", "SMALLINT": "int",
        "TINYINT": "int", "HUGEINT": "int",
        "DOUBLE": "float", "FLOAT": "float", "REAL": "float",
        "DECIMAL": "float", "NUMERIC": "float",
        "BOOLEAN": "bool", "BOOL": "bool",
        "TIMESTAMP": "datetime", "TIMESTAMPTZ": "datetime",
        "TIMESTAMP WITH TIME ZONE": "datetime",
        "DATE": "datetime", "TIME": "datetime",
    }

    columns = []
    for col_info in info:
        name = col_info.get("column_name", col_info.get("Field", ""))
        if not name:
            # Try alternate key names from DESCRIBE output
            for key in col_info:
                if "name" in key.lower():
                    name = col_info[key]
                    break
        if not name:
            continue

        # Skip Lakehouse internal columns
        if name in _LAKEHOUSE_INTERNAL_COLUMNS:
            continue

        # Resolve type
        raw_type = str(col_info.get("column_type", col_info.get("Type", "VARCHAR"))).upper()
        # Strip precision/scale e.g. "DECIMAL(18,2)" → "DECIMAL"
        base_type = raw_type.split("(")[0].strip()
        type_str = _TYPE_MAP.get(base_type, "str")

        kind = "measure" if type_str in ("int", "float") else "dimension"
        agg = "sum" if kind == "measure" else ""

        columns.append(DatacubeColumnConfig(
            name=name, type=type_str, kind=kind,
            aggregate_operator=agg,
            excluded_from_pivot=(kind == "dimension"),
        ))

    return columns


def _resolve_source(
    source: Any,
    *,
    lakehouse: LakehouseProtocol | None = None,
) -> tuple[Any, str, list[DatacubeColumnConfig]]:
    """Resolve a source argument to (duckdb_conn, source_name, columns).

    Supports:
    - Lakehouse instance (direct Iceberg path — S3 → DuckDB, no Python)
    - duckdb.DuckDBPyConnection
    - Storable subclass (class, not instance)
    - PyArrow Table
    - pandas DataFrame
    - str (raw SQL or table FQN)

    When ``lakehouse`` is provided, non-Lakehouse sources register on
    the Lakehouse's DuckDB connection instead of creating a new one.
    This enables cross-source joins with Lakehouse tables.
    """
    import duckdb

    # Helper: get a DuckDB connection (prefer Lakehouse's if available)
    def _get_conn() -> duckdb.DuckDBPyConnection:
        if lakehouse is not None and isinstance(lakehouse, LakehouseProtocol):
            return lakehouse._ensure_conn()
        return duckdb.connect()

    # 1. String source — raw SQL or table name
    if isinstance(source, str):
        conn = _get_conn()
        return conn, source, _discover_columns_from_sql(conn, source)

    # 2. DuckDB connection
    if isinstance(source, duckdb.DuckDBPyConnection):
        return source, "", []

    # 3. Lakehouse — direct Iceberg path (S3 → DuckDB, no Python)
    if _is_lakehouse(source):
        conn = source._ensure_conn()
        return conn, "", []

    # 4. PyArrow Table
    try:
        import pyarrow as pa
        if isinstance(source, pa.Table):
            conn = _get_conn()
            view_name = f"_dc_arrow_{uuid.uuid4().hex[:8]}"
            conn.register(view_name, source)
            columns = _columns_from_arrow(source)
            return conn, view_name, columns
    except ImportError:
        pass

    # 5. Pandas DataFrame
    try:
        import pandas as pd
        if isinstance(source, pd.DataFrame):
            conn = _get_conn()
            view_name = f"_dc_df_{uuid.uuid4().hex[:8]}"
            conn.register(view_name, source)
            columns = _columns_from_df(source)
            return conn, view_name, columns
    except ImportError:
        pass

    # 6. Storable class
    if isinstance(source, type) and hasattr(source, 'type_name'):
        return _resolve_storable_source(source)

    raise TypeError(
        f"Unsupported datacube source: {type(source).__name__}. "
        f"Supported: Lakehouse, duckdb.DuckDBPyConnection, Storable class, "
        f"pa.Table, pd.DataFrame, str"
    )


def _resolve_storable_source(cls: type[Storable]) -> tuple[Any, str, list[DatacubeColumnConfig]]:
    """Pull Storable data → Arrow → DuckDB temp view."""
    import dataclasses as dc_mod

    import duckdb

    # Query all entities
    items = cls.query(limit=100000)
    if not items:
        conn = duckdb.connect()
        view_name = f"_dc_storable_{uuid.uuid4().hex[:8]}"
        return conn, view_name, _columns_from_storable_class(cls)

    # Convert to list of dicts
    rows: list[dict[str, Any]] = []
    for item in items:
        if dc_mod.is_dataclass(item):
            row = {}  # type: ignore[unreachable]
            for f in dc_mod.fields(item):
                if not f.name.startswith("_"):
                    row[f.name] = getattr(item, f.name)
            rows.append(row)

    if not rows:
        conn = duckdb.connect()
        view_name = f"_dc_storable_{uuid.uuid4().hex[:8]}"
        return conn, view_name, _columns_from_storable_class(cls)

    # Register as DuckDB temp view
    import pyarrow as pa
    keys = list(rows[0].keys())
    col_data = {k: [r.get(k) for r in rows] for k in keys}
    arrow_table = pa.table(col_data)

    conn = duckdb.connect()
    view_name = f"_dc_storable_{uuid.uuid4().hex[:8]}"
    conn.register(view_name, arrow_table)

    columns = _columns_from_storable_class(cls)
    return conn, view_name, columns


def _columns_from_storable_class(cls: type) -> list[DatacubeColumnConfig]:
    """Extract column configs from a Storable class using its registry."""
    import dataclasses as dc_mod

    columns = []
    if hasattr(cls, '_registry') and cls._registry is not None:
        registry = cls._registry
        if dc_mod.is_dataclass(cls):
            for f in dc_mod.fields(cls):
                if f.name.startswith("_"):
                    continue
                try:
                    col_def, _ = registry.resolve(f.name)
                    columns.append(DatacubeColumnConfig.from_column_def(col_def, f.name))
                except Exception:
                    ft = f.type if isinstance(f.type, type) else str
                    columns.append(DatacubeColumnConfig.from_type(f.name, ft))
    elif dc_mod.is_dataclass(cls):
        for f in dc_mod.fields(cls):
            if not f.name.startswith("_"):
                ft = f.type if isinstance(f.type, type) else str
                columns.append(DatacubeColumnConfig.from_type(f.name, ft))
    return columns


def _columns_from_arrow(table: Any) -> list[DatacubeColumnConfig]:
    """Extract column configs from a PyArrow Table schema."""
    import pyarrow as pa

    type_map = {
        pa.string(): "str",
        pa.utf8(): "str",
        pa.int32(): "int",
        pa.int64(): "int",
        pa.float32(): "float",
        pa.float64(): "float",
        pa.bool_(): "bool",
    }

    columns = []
    for field in table.schema:
        type_str = type_map.get(field.type, "str")
        kind = "measure" if type_str in ("int", "float") else "dimension"
        agg = "sum" if kind == "measure" else ""
        columns.append(DatacubeColumnConfig(
            name=field.name, type=type_str, kind=kind,
            aggregate_operator=agg, excluded_from_pivot=(kind == "dimension"),
        ))
    return columns


def _columns_from_df(df: Any) -> list[DatacubeColumnConfig]:
    """Extract column configs from a pandas DataFrame."""
    import numpy as np

    columns = []
    for col_name, dtype in df.dtypes.items():
        # Handle both numpy dtypes and pandas extension dtypes (e.g. StringDtype)
        try:
            is_int = np.issubdtype(dtype, np.integer)
            is_float = np.issubdtype(dtype, np.floating)
            is_bool = np.issubdtype(dtype, np.bool_)
        except TypeError:
            # Extension dtypes (StringDtype, Int64Dtype, etc.)
            dtype_str = str(dtype).lower()
            is_int = "int" in dtype_str and "interval" not in dtype_str
            is_float = "float" in dtype_str
            is_bool = "bool" in dtype_str

        if is_int:
            type_str, kind, agg = "int", "measure", "sum"
        elif is_float:
            type_str, kind, agg = "float", "measure", "sum"
        elif is_bool:
            type_str, kind, agg = "bool", "dimension", ""
        else:
            type_str, kind, agg = "str", "dimension", ""
        columns.append(DatacubeColumnConfig(
            name=str(col_name), type=type_str, kind=kind,
            aggregate_operator=agg, excluded_from_pivot=(kind == "dimension"),
        ))
    return columns


def _discover_columns_from_sql(conn: duckdb.DuckDBPyConnection, source: str) -> list[DatacubeColumnConfig]:
    """Discover columns by running LIMIT 0 against the source."""
    try:
        trimmed = source.strip()
        if trimmed.upper().startswith("SELECT") or trimmed.startswith("("):
            sql = f"SELECT * FROM ({trimmed}) AS _dc_discover LIMIT 0"
        else:
            sql = f"SELECT * FROM {source} LIMIT 0"
        result = conn.execute(sql)
        columns = []
        for desc in result.description:
            name = desc[0]
            # DuckDB type string → our type
            duckdb_type = str(desc[1]).upper() if len(desc) > 1 else ""
            if "INT" in duckdb_type:
                type_str, kind, agg = "int", "measure", "sum"
            elif "FLOAT" in duckdb_type or "DOUBLE" in duckdb_type or "DECIMAL" in duckdb_type or "NUMERIC" in duckdb_type:
                type_str, kind, agg = "float", "measure", "sum"
            elif "BOOL" in duckdb_type:
                type_str, kind, agg = "bool", "dimension", ""
            else:
                type_str, kind, agg = "str", "dimension", ""
            columns.append(DatacubeColumnConfig(
                name=name, type=type_str, kind=kind,
                aggregate_operator=agg, excluded_from_pivot=(kind == "dimension"),
            ))
        return columns
    except Exception:
        return []
