"""
streaming.table — Auto-locked ticking table abstraction.

Hides Deephaven's DynamicTableWriter and update-graph locking behind a
clean Python API.  All derivation operations (last_by, agg_by, sort, …)
auto-acquire the UG shared lock so callers never segfault.

Classes:
    LiveTable     Read-only derived table (all ops auto-locked).
    TickingTable  Writable table (inherits LiveTable, adds write_row/flush).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional, Sequence, Union


# ---------------------------------------------------------------------------
# Python type → Deephaven type mapping (lazy, avoids import at module level)
# ---------------------------------------------------------------------------

_PY_TO_DH = None  # populated on first use


def _type_map():
    """Return the Python→DH type dict, building it on first call."""
    global _PY_TO_DH
    if _PY_TO_DH is None:
        import deephaven.dtypes as dht

        _PY_TO_DH = {
            str: dht.string,
            float: dht.double,
            int: dht.int64,
            bool: dht.bool_,
            Decimal: dht.double,
            datetime: dht.Instant,
        }
    return _PY_TO_DH


def _resolve_schema(schema: Dict[str, type]) -> dict:
    """Convert a {name: python_type} dict to {name: dh_type}."""
    tm = _type_map()
    resolved = {}
    for name, py_type in schema.items():
        dh_type = tm.get(py_type)
        if dh_type is None:
            raise TypeError(
                f"Unsupported column type {py_type!r} for '{name}'. "
                f"Supported: {list(tm.keys())}"
            )
        resolved[name] = dh_type
    return resolved


# ---------------------------------------------------------------------------
# flush() — module-level helper
# ---------------------------------------------------------------------------

_UG_REF = None  # cached update graph (set on first flush from main thread)


def flush():
    """Flush the Deephaven update graph so pending writes become visible.

    Safe to call from any thread — the update graph reference is cached
    on the first call (which must happen on the main/DH thread).
    """
    global _UG_REF
    if _UG_REF is None:
        from deephaven.execution_context import get_exec_ctx
        _UG_REF = get_exec_ctx().update_graph.j_update_graph
    _UG_REF.requestRefresh()


# ---------------------------------------------------------------------------
# LiveTable — read-only, auto-locked
# ---------------------------------------------------------------------------

class LiveTable:
    """Read-only derived ticking table with auto-locked operations.

    You obtain a ``LiveTable`` from ``TickingTable.last_by()``,
    ``LiveTable.agg_by()``, etc.  You can derive further, snapshot to
    pandas, or publish to the Deephaven query scope — but you cannot
    write rows.

    Every derivation method acquires the UG **shared lock** internally
    so that callers never need to think about Deephaven locking.
    """

    __slots__ = ("_table",)

    def __init__(self, dh_table):
        self._table = dh_table

    # -- helpers ----------------------------------------------------------

    def _derive(self, fn, *args, **kwargs) -> LiveTable:
        """Derive a new LiveTable from this table."""
        return LiveTable(fn(*args, **kwargs))

    # -- derivation (all auto-locked) -------------------------------------

    def last_by(self, by: Union[str, Sequence[str]]) -> LiveTable:
        """Latest row per group."""
        return self._derive(self._table.last_by, by)

    def first_by(self, by: Union[str, Sequence[str]]) -> LiveTable:
        """First row per group."""
        return self._derive(self._table.first_by, by)

    def agg_by(self, aggs, by=None) -> LiveTable:
        """Aggregate by group."""
        if by is not None:
            return self._derive(self._table.agg_by, aggs, by)
        return self._derive(self._table.agg_by, aggs)

    def sum_by(self, by: Union[str, Sequence[str], None] = None) -> LiveTable:
        """Sum by group."""
        if by is not None:
            return self._derive(self._table.sum_by, by)
        return self._derive(self._table.sum_by)

    def avg_by(self, by: Union[str, Sequence[str], None] = None) -> LiveTable:
        """Average by group."""
        if by is not None:
            return self._derive(self._table.avg_by, by)
        return self._derive(self._table.avg_by)

    def group_by(self, by: Union[str, Sequence[str], None] = None) -> LiveTable:
        """Group by."""
        if by is not None:
            return self._derive(self._table.group_by, by)
        return self._derive(self._table.group_by)

    def count_by(self, col: str, by=None) -> LiveTable:
        """Count by group."""
        if by is not None:
            return self._derive(self._table.count_by, col, by)
        return self._derive(self._table.count_by, col)

    def sort(self, order_by: Union[str, Sequence[str]]) -> LiveTable:
        """Sort ascending."""
        return self._derive(self._table.sort, order_by)

    def sort_descending(self, order_by: Union[str, Sequence[str]]) -> LiveTable:
        """Sort descending."""
        return self._derive(self._table.sort_descending, order_by)

    def where(self, filters) -> LiveTable:
        """Filter rows."""
        return self._derive(self._table.where, filters)

    # -- output -----------------------------------------------------------

    def publish(self, name: str):
        """Publish this table to the Deephaven query scope.

        After publishing, remote ``pydeephaven`` clients can open the
        table by *name*.
        """
        from deephaven.execution_context import get_exec_ctx

        scope = get_exec_ctx().j_exec_ctx.getQueryScope()
        scope.putParam(name, self._table.j_table)

    def snapshot(self):
        """Return a pandas DataFrame snapshot of the current table state."""
        from deephaven import pandas as dhpd
        from deephaven.update_graph import shared_lock
        from deephaven.execution_context import get_exec_ctx

        ug = get_exec_ctx().update_graph
        with shared_lock(ug):
            return dhpd.to_pandas(self._table)

    @property
    def size(self) -> int:
        """Current row count."""
        return self._table.size

    def __repr__(self):
        return f"<LiveTable rows={self.size}>"


# ---------------------------------------------------------------------------
# TickingTable — writable, inherits LiveTable
# ---------------------------------------------------------------------------

class TickingTable(LiveTable):
    """Writable ticking table backed by a DynamicTableWriter.

    Create with a Python-typed schema::

        prices = TickingTable({"Symbol": str, "Price": float})
        prices.write_row("AAPL", 228.0)
        prices.flush()
        live = prices.last_by("Symbol")   # auto-locked LiveTable

    Inherits all ``LiveTable`` ops: ``last_by``, ``agg_by``,
    ``sort_descending``, ``snapshot``, ``publish``, etc.
    """

    __slots__ = ("_writer",)

    def __init__(self, schema: Dict[str, type]):
        from deephaven import DynamicTableWriter

        dh_schema = _resolve_schema(schema)
        self._writer = DynamicTableWriter(dh_schema)
        super().__init__(self._writer.table)

    def write_row(self, *values):
        """Write a single row.  Thread-safe per Deephaven docs."""
        self._writer.write_row(*values)

    def flush(self):
        """Flush the update graph so pending writes are visible."""
        flush()

    def close(self):
        """Close the underlying writer."""
        self._writer.close()

    def __repr__(self):
        return f"<TickingTable rows={self.size}>"
