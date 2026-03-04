"""
Datacube UI server — Tornado + Perspective flat grid.

Runs **in-process** (like ``plt.show()``).  The datacube engine does all
SQL compilation and DuckDB pushdown; Perspective renders the flat result.

Endpoints:

- ``/websocket`` — Perspective Arrow data channel
- ``/cmd``       — datacube command channel (JSON over WebSocket)

On schema change (group_by / pivot adds or removes columns), a new
Perspective table is created with an incremented name (``dc_0``, ``dc_1``, …)
and the client is told to reconnect.

Usage::

    from datacube.server import run
    run(datacube_instance, port=8050)
"""

from __future__ import annotations

import json
import logging
import webbrowser
from pathlib import Path
from typing import TYPE_CHECKING

import perspective
import perspective.handlers.tornado
import pyarrow as pa
import tornado.ioloop
import tornado.web
import tornado.websocket

if TYPE_CHECKING:
    from datacube.engine import Datacube

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"

# ── Arrow helpers ─────────────────────────────────────────────────


def _normalize_arrow(table: pa.Table) -> pa.Table:
    """Cast all numerics to float64 for Perspective compatibility."""
    cols, fields = [], []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_decimal(field.type) or pa.types.is_floating(field.type) or pa.types.is_integer(field.type):
            col = col.cast(pa.float64())
            fields.append(pa.field(field.name, pa.float64()))
        else:
            fields.append(field)
        cols.append(col)
    return pa.table(cols, schema=pa.schema(fields))


def _arrow_to_ipc(table: pa.Table) -> bytes:
    """Normalize and serialize a PyArrow Table to IPC stream bytes."""
    table = _normalize_arrow(table)
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return bytes(sink.getvalue().to_pybytes())


# ── State helpers ─────────────────────────────────────────────────


def _snapshot_state(dc: Datacube) -> dict:
    """JSON-serializable datacube state for the client."""
    snap = dc.snapshot
    return {
        "source": snap.source,
        "columns": [
            {
                "name": c.name,
                "type": c.type,
                "kind": c.kind,
                "aggregate_operator": c.aggregate_operator,
                "is_selected": c.is_selected,
            }
            for c in snap.columns
        ],
        "group_by": list(snap.group_by),
        "pivot_by": list(snap.pivot_by),
        "filters": [
            {"field": f.field, "op": f.op, "value": f.value}
            for f in snap.filters
        ],
        "sort": [
            {"field": s.field, "descending": s.descending}
            for s in snap.sort
        ],
        "drill_path": [dict(d) for d in snap.drill_path],
    }


# ── Tree builder ──────────────────────────────────────────────────


MAX_CHILDREN = 200  # limit leaf rows per expansion to keep UI responsive


def _pad_table(table: pa.Table, target_cols: list[str], target_schema: pa.Schema) -> pa.Table:
    """Pad a table with null columns to match target schema, in target column order."""
    arrays = {}
    n = table.num_rows
    for col_name in target_cols:
        if col_name in table.column_names:
            arrays[col_name] = table.column(col_name)
        else:
            field = target_schema.field(col_name)
            arrays[col_name] = pa.nulls(n, type=field.type)
    return pa.table(arrays)


def _get_source_schema(dc: Datacube) -> pa.Schema:
    """Get the normalized Arrow schema for the source.

    When pivot_by is active, returns the pivoted schema (using a sample
    GROUP BY on the first group field) so the tree builder's target
    schema matches what each level query actually returns.
    """
    snap = dc.snapshot
    if snap.pivot_by and snap.group_by:
        # Pivoted schema: GROUP BY first_field + pivot → get pivoted columns
        sample_dc = dc.set_group_by(snap.group_by[0]).set_limit(0)
        return _normalize_arrow(sample_dc.query()).schema
    # Flat schema: no grouping, no pivot
    base_dc = dc.set_group_by().set_pivot_by().set_limit(0)
    return _normalize_arrow(base_dc.query()).schema


def _build_tree_result(dc: Datacube, expanded_keys: set, source_schema: pa.Schema) -> pa.Table:
    """Build a hierarchical tree table with LAZY recursive expansion.

    Only queries levels that are actually expanded — unexpanded branches
    cost zero queries.  Each expanded node triggers exactly one
    ``GROUP BY next_field WHERE parent_filters`` query.

    With ``group_by(sector, symbol)``:
    - Level 0: ``GROUP BY sector`` — always queried (top-level groups)
    - Expand "Tech" → ``GROUP BY symbol WHERE sector='Tech'``
    - Expand "AAPL" → leaf rows ``WHERE sector='Tech' AND symbol='AAPL' LIMIT 200``

    Args:
        dc: Datacube with group_by already set.
        expanded_keys: set of tuples — key length encodes depth.
        source_schema: pre-computed schema from ``_get_source_schema()``.

    Returns:
        Arrow table with __tree__, then original source columns.
    """
    snap = dc.snapshot
    group_fields = list(snap.group_by)
    base_dc = dc.set_group_by()  # clear group_by for sub-level queries (keeps pivot_by)

    source_cols = [f.name for f in source_schema]

    # Target column order and schema
    all_cols = ["__tree__", *source_cols]
    fields = [pa.field("__tree__", pa.string())]
    for col_name in source_cols:
        fields.append(source_schema.field(col_name))
    target_schema = pa.schema(fields)

    chunks: list[pa.Table] = []
    indent = "\u00a0\u00a0\u00a0\u00a0\u00a0\u00a0"  # 6 non-breaking spaces per level (Perspective trims regular spaces)

    has_pivot = bool(snap.pivot_by)

    def _emit_leaves(parent_filters: list[tuple[str, object]], depth: int) -> None:
        """Fetch and emit leaf rows (no more group fields to expand)."""
        if has_pivot:
            return  # Cross-tab: only show aggregated group rows, not leaf rows
        leaf_dc = base_dc.set_limit(MAX_CHILDREN)
        for f, v in parent_filters:
            leaf_dc = leaf_dc.add_filter(f, "eq", v)
        leaves = _normalize_arrow(leaf_dc.query())
        if leaves.num_rows == 0:
            return
        labels = [f"{indent * depth}row {j}" for j in range(leaves.num_rows)]
        tree_arr = pa.array(labels, type=pa.string())
        leaf_block = leaves.append_column("__tree__", tree_arr)
        for pf, pv in parent_filters:
            if pf not in leaf_block.column_names:
                leaf_block = leaf_block.append_column(
                    pf, pa.array([pv] * leaves.num_rows, type=pa.string()))
        chunks.append(_pad_table(leaf_block, all_cols, target_schema))

    def _build_level(depth: int, parent_filters: list[tuple[str, object]]) -> None:
        """Lazily build tree rows — only query levels with expanded parents."""
        if depth >= len(group_fields):
            _emit_leaves(parent_filters, depth)
            return

        # Query THIS level: GROUP BY current_field + parent filters
        current_field = group_fields[depth]
        level_dc = base_dc.set_group_by(current_field)
        for f, v in parent_filters:
            level_dc = level_dc.add_filter(f, "eq", v)
        parents = _normalize_arrow(level_dc.query())

        for i in range(parents.num_rows):
            row_value = parents.column(current_field)[i].as_py()
            key = (*tuple(v for _, v in parent_filters), row_value)
            is_expanded = key in expanded_keys

            prefix = "▾ " if is_expanded else "▸ "
            label = f"{indent * depth}{prefix}{row_value}"

            # Emit parent row
            parent_slice = parents.slice(i, 1)
            tree_col = pa.array([label], type=pa.string())
            parent_row = parent_slice.append_column("__tree__", tree_col)
            for pf, pv in parent_filters:
                if pf not in parent_row.column_names:
                    parent_row = parent_row.append_column(
                        pf, pa.array([pv], type=pa.string()))
            chunks.append(_pad_table(parent_row, all_cols, target_schema))

            # LAZY: only recurse into children if this node is expanded
            if is_expanded:
                _build_level(depth + 1, [*parent_filters, (current_field, row_value)])

    _build_level(0, [])

    if not chunks:
        return pa.table({f.name: pa.array([], type=f.type) for f in target_schema})

    return pa.concat_tables(chunks)


# ── WebSocket command handler ─────────────────────────────────────


class CmdHandler(tornado.websocket.WebSocketHandler):
    """Datacube command channel — receives JSON, mutates engine, refreshes grid."""

    def check_origin(self, origin: str) -> bool:
        return True

    def open(self) -> None:  # type: ignore[override]
        logger.info("Command channel opened — full reset to initial state")
        state = self.application.dc_state  # type: ignore[attr-defined]  # Tornado dynamic attr
        dc = state["initial"]
        state["dc"] = dc
        state["expanded"] = set()

        # Reset to flat data — replace existing dc_0 content (don't recreate)
        arrow = _normalize_arrow(dc.query())
        ipc = _arrow_to_ipc(arrow)
        new_cols = sorted(arrow.column_names)
        old_cols = state["last_columns"]

        if new_cols != old_cols:
            # Schema changed (was grouped, now flat) — need new versioned table
            state["table_version"] += 1
            name = f"dc_{state['table_version']}"
            state["psp_table"] = state["psp_client"].table(ipc, name=name)
            state["last_columns"] = new_cols
            self.write_message(json.dumps({
                "type": "reload",
                "table_name": name,
                "state": _snapshot_state(dc),
                "sql": dc.sql(),
                "row_count": arrow.num_rows,
                "expanded": [],
            }))
        else:
            # Same schema — just replace data in current table
            state["psp_table"].replace(ipc)
            self.write_message(json.dumps({
                "type": "update",
                "state": _snapshot_state(dc),
                "sql": dc.sql(),
                "row_count": arrow.num_rows,
                "expanded": [],
            }))

    def on_message(self, message: str | bytes) -> None:
        try:
            msg = json.loads(message)
        except json.JSONDecodeError:
            self.write_message(json.dumps({"error": "Invalid JSON"}))
            return

        cmd = msg.get("cmd", "")
        state = self.application.dc_state  # type: ignore[attr-defined]
        dc = state["dc"]

        try:
            if cmd == "group_by":
                dc = dc.set_group_by(*msg.get("fields", []))
                state["expanded"] = set()  # clear expansions on group change
                state.pop("source_schema", None)  # schema depends on group/pivot
            elif cmd == "pivot_by":
                dc = dc.set_pivot_by(*msg.get("fields", []))
                state.pop("source_schema", None)  # pivoted columns change schema
            elif cmd == "column":
                name = msg["name"]
                kwargs = {k: v for k, v in msg.items() if k not in ("cmd", "name")}
                dc = dc.set_column(name, **kwargs)
            elif cmd == "filter":
                dc = dc.add_filter(msg["field"], msg["op"], msg.get("value"))
            elif cmd == "clear_filters":
                dc = dc.clear_filters()
            elif cmd == "sort":
                sorts = msg.get("sorts", [])
                dc = dc.set_sort(
                    *[(s["field"], s.get("descending", False)) for s in sorts],
                )
            elif cmd == "expand":
                # Toggle expansion of a group row.
                # key is an array of values, length = depth + 1.
                # e.g. ["Tech"] for depth 0, ["Tech", "AAPL"] for depth 1.
                key = tuple(msg.get("key", []))
                if not key:
                    self.write_message(json.dumps({"error": "expand requires key"}))
                    return
                expanded = state["expanded"]
                if key in expanded:
                    # Collapse: remove this key AND all descendants
                    to_remove = {k for k in expanded if k[:len(key)] == key}
                    expanded -= to_remove
                else:
                    expanded.add(key)
                # Don't change dc, just re-render
                self._refresh()
                return
            elif cmd == "collapse_all":
                state["expanded"] = set()
            elif cmd == "reset":
                dc = state["initial"]
                state["expanded"] = set()
            else:
                self.write_message(json.dumps({"error": f"Unknown: {cmd}"}))
                return

            state["dc"] = dc
            self._refresh()

        except Exception as e:
            logger.exception("Command failed: %s", cmd)
            self.write_message(json.dumps({"error": str(e)}))

    def _refresh(self) -> None:
        """Re-query datacube, update Perspective table, send state to client."""
        state = self.application.dc_state  # type: ignore[attr-defined]
        dc = state["dc"]

        try:
            sql = dc.sql()
            expanded = state["expanded"]

            # If group_by is active, build tree with expanded rows
            if dc.snapshot.group_by:
                # Cache source schema (LIMIT 0) — only re-query on source change
                if "source_schema" not in state:
                    state["source_schema"] = _get_source_schema(dc)
                arrow = _build_tree_result(dc, expanded, state["source_schema"])
            else:
                arrow = _normalize_arrow(dc.query())

            row_count = arrow.num_rows
            ipc = _arrow_to_ipc(arrow)

            new_cols = sorted(arrow.column_names)
            old_cols = state["last_columns"]

            if new_cols != old_cols:
                # Schema changed → new versioned table
                state["table_version"] += 1
                name = f"dc_{state['table_version']}"
                state["psp_table"] = state["psp_client"].table(ipc, name=name)
                state["last_columns"] = new_cols
                logger.info("Schema changed → %s (%d cols)", name, len(new_cols))
                self.write_message(json.dumps({
                    "type": "reload",
                    "table_name": name,
                    "state": _snapshot_state(dc),
                    "sql": sql,
                    "row_count": row_count,
                    "expanded": [list(k) for k in expanded],
                }))
            else:
                # Same schema → fast in-place replace
                state["psp_table"].replace(ipc)
                self.write_message(json.dumps({
                    "type": "update",
                    "state": _snapshot_state(dc),
                    "sql": sql,
                    "row_count": row_count,
                    "expanded": [list(k) for k in expanded],
                }))

        except Exception as e:
            logger.exception("Query failed")
            self.write_message(json.dumps({
                "error": str(e),
                "state": _snapshot_state(dc),
            }))


# ── Entry point ───────────────────────────────────────────────────


def run(dc: Datacube, port: int = 8050, open_browser: bool = True) -> None:
    """Start the datacube UI server (blocking).

    Args:
        dc: A Datacube instance.
        port: HTTP port (default 8050).
        open_browser: Whether to open a browser tab.
    """
    arrow = dc.query()
    ipc = _arrow_to_ipc(arrow)

    psp_server = perspective.Server()
    psp_client = psp_server.new_local_client()
    psp_table = psp_client.table(ipc, name="dc_0")

    app = tornado.web.Application(
        [
            (
                r"/websocket",
                perspective.handlers.tornado.PerspectiveTornadoHandler,
                {"perspective_server": psp_server},
            ),
            (r"/cmd", CmdHandler),
            (
                r"/(.*)",
                tornado.web.StaticFileHandler,
                {"path": str(STATIC_DIR), "default_filename": "index.html"},
            ),
        ],
        websocket_max_message_size=50 * 1024 * 1024,
    )

    app.dc_state = {  # type: ignore[attr-defined]
        "dc": dc,
        "initial": dc,
        "psp_table": psp_table,
        "psp_client": psp_client,
        "psp_server": psp_server,
        "last_columns": sorted(_normalize_arrow(arrow).column_names),
        "table_version": 0,
        "expanded": set(),
        "source_schema": _normalize_arrow(arrow).schema,  # cached, LIMIT 0 not needed here
    }

    app.listen(port)
    url = f"http://localhost:{port}"
    print(f"Datacube UI running at {url}")
    print("Press Ctrl+C to stop.")

    if open_browser:
        webbrowser.open(url)

    try:
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        print("\nStopped.")
