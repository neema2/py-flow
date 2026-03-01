"""
Lakehouse — User-Facing Iceberg Interface
===========================================
Read, ingest, and transform data in Apache Iceberg tables.

All reads and writes via DuckDB SQL (Iceberg extension with REST catalog).

Write modes (all include _batch_id, _batch_ts):
  - append:      Raw append.
  - snapshot:    Batch-level versioning (_is_current).
  - incremental: Row-level upsert by primary key (_is_current, _updated_at).
  - bitemporal:  Row-level + business/system time (_is_current, _tx_time, _valid_from, _valid_to).
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Optional, Union

import duckdb
import pyarrow as pa

logger = logging.getLogger(__name__)

DEFAULT_S3_ENDPOINT = "http://localhost:9002"
DEFAULT_S3_ACCESS_KEY = "minioadmin"
DEFAULT_S3_SECRET_KEY = "minioadmin"
DEFAULT_S3_REGION = "us-east-1"
DEFAULT_CATALOG_URI = "http://localhost:8181/catalog"
DEFAULT_WAREHOUSE = "lakehouse"

VALID_MODES = ("append", "snapshot", "incremental", "bitemporal")


class Lakehouse:
    """
    User-facing Iceberg interface: query, ingest, and transform.

    Usage::

        from lakehouse import Lakehouse

        # Via alias (registered by LakehouseServer)
        lh = Lakehouse("demo")

        # Or explicit params (backward compat)
        lh = Lakehouse(catalog_uri="http://localhost:8181/catalog")

        lh.query("SELECT * FROM lakehouse.default.events LIMIT 10")
        lh.ingest("my_signals", df, mode="append")
        lh.transform("daily_pnl", "SELECT ... GROUP BY ...", mode="snapshot")
        lh.close()
    """

    def __init__(
        self,
        alias_or_catalog: str | None = None,
        *,
        warehouse: str | None = None,
        s3_endpoint: str | None = None,
        s3_access_key: str | None = None,
        s3_secret_key: str | None = None,
        s3_region: str | None = None,
        namespace: str = "default",
        # Legacy positional compat
        catalog_uri: str | None = None,
    ):
        # Resolve alias
        resolved = self._resolve(alias_or_catalog, catalog_uri)

        self._catalog_uri = resolved.get("catalog_url") or os.environ.get("LAKEKEEPER_URI", DEFAULT_CATALOG_URI)
        self._warehouse = warehouse or resolved.get("warehouse") or os.environ.get("LAKEHOUSE_WAREHOUSE", DEFAULT_WAREHOUSE)
        self._s3_endpoint = s3_endpoint or resolved.get("s3_endpoint") or os.environ.get("S3_ENDPOINT", DEFAULT_S3_ENDPOINT)
        self._s3_access_key = s3_access_key or resolved.get("s3_access_key") or os.environ.get("S3_ACCESS_KEY", DEFAULT_S3_ACCESS_KEY)
        self._s3_secret_key = s3_secret_key or resolved.get("s3_secret_key") or os.environ.get("S3_SECRET_KEY", DEFAULT_S3_SECRET_KEY)
        self._s3_region = s3_region or resolved.get("s3_region") or os.environ.get("S3_REGION", DEFAULT_S3_REGION)
        self._namespace = namespace or resolved.get("namespace", "default")
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

    @staticmethod
    def _resolve(alias_or_catalog, catalog_uri) -> dict:
        """Resolve alias or explicit params."""
        if alias_or_catalog is not None:
            from lakehouse._registry import resolve_alias
            resolved = resolve_alias(alias_or_catalog)
            if resolved is not None:
                return resolved
            # Not an alias — treat as explicit catalog_uri
            return {"catalog_url": alias_or_catalog}
        if catalog_uri is not None:
            return {"catalog_url": catalog_uri}
        return {}

    # ── DuckDB connection (reads + writes) ────────────────────────────────

    def _ensure_conn(self) -> duckdb.DuckDBPyConnection:
        """Lazily initialize and configure the DuckDB connection."""
        if self._conn is not None:
            return self._conn

        self._conn = duckdb.connect()

        # Install and load extensions
        self._conn.execute("INSTALL iceberg; LOAD iceberg;")
        self._conn.execute("INSTALL httpfs; LOAD httpfs;")

        # Configure S3 credentials
        self._conn.execute(f"SET s3_endpoint='{self._s3_endpoint.replace('http://', '')}';")
        self._conn.execute(f"SET s3_access_key_id='{self._s3_access_key}';")
        self._conn.execute(f"SET s3_secret_access_key='{self._s3_secret_key}';")
        self._conn.execute(f"SET s3_region='{self._s3_region}';")
        self._conn.execute("SET s3_use_ssl=false;")
        self._conn.execute("SET s3_url_style='path';")

        # Attach the Iceberg REST catalog (Lakekeeper)
        self._conn.execute(f"""
            ATTACH '{self._warehouse}' AS lakehouse (
                TYPE ICEBERG,
                ENDPOINT '{self._catalog_uri}',
                AUTHORIZATION_TYPE 'none'
            );
        """)

        logger.info("DuckDB connected to Iceberg catalog at %s", self._catalog_uri)
        return self._conn

    def _fqn(self, table_name: str) -> str:
        """Fully qualified table name: lakehouse.{namespace}.{table}."""
        return f"lakehouse.{self._namespace}.{table_name}"

    def _exec(self, sql: str) -> None:
        """Execute a DuckDB SQL statement (no result)."""
        conn = self._ensure_conn()
        conn.execute(sql)

    def _register_arrow(self, name: str, data: pa.Table) -> None:
        """Register an Arrow table as a DuckDB temporary view."""
        conn = self._ensure_conn()
        conn.register(name, data)

    def _unregister(self, name: str) -> None:
        """Unregister a DuckDB temporary view."""
        conn = self._ensure_conn()
        conn.unregister(name)

    # ── Query (read) ──────────────────────────────────────────────────────

    def query(self, sql: str, params: list | None = None) -> list[dict]:
        """
        Execute a SQL query and return results as a list of dicts.

        Tables are accessible as lakehouse.default.<table_name>, e.g.:
            lh.query("SELECT * FROM lakehouse.default.events LIMIT 10")
        """
        conn = self._ensure_conn()
        try:
            if params:
                result = conn.execute(sql, params)
            else:
                result = conn.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error("Query failed: %s — %s", sql[:200], e)
            raise

    def query_arrow(self, sql: str, params: list | None = None) -> pa.Table:
        """Execute a SQL query and return results as a PyArrow Table."""
        conn = self._ensure_conn()
        if params:
            return conn.execute(sql, params).fetch_arrow_table()
        return conn.execute(sql).fetch_arrow_table()

    def query_df(self, sql: str, params: list | None = None):
        """Execute a SQL query and return results as a Pandas DataFrame."""
        conn = self._ensure_conn()
        if params:
            return conn.execute(sql, params).fetchdf()
        return conn.execute(sql).fetchdf()

    # ── Ingest (write) ────────────────────────────────────────────────────

    def ingest(
        self,
        table_name: str,
        data: Union[pa.Table, list[dict], "pd.DataFrame"],
        mode: str = "append",
        primary_key: str | None = None,
    ) -> int:
        """
        Write data into an Iceberg table.

        Args:
            table_name: Target table name (created automatically if missing).
            data: Data to write — PyArrow Table, list of dicts, or pandas DataFrame.
            mode: Write mode — "append", "snapshot", "incremental", "bitemporal".
            primary_key: Required for "incremental" and "bitemporal" modes.

        Returns:
            Number of rows written.
        """
        if mode not in VALID_MODES:
            raise ValueError(f"Invalid mode '{mode}'. Must be one of: {VALID_MODES}")
        if mode in ("incremental", "bitemporal") and not primary_key:
            raise ValueError(f"mode='{mode}' requires a primary_key")

        arrow_data = _to_arrow(data)
        if len(arrow_data) == 0:
            return 0

        if mode == "append":
            return self._write_append(table_name, arrow_data)
        elif mode == "snapshot":
            return self._write_snapshot(table_name, arrow_data)
        elif mode == "incremental":
            return self._write_incremental(table_name, arrow_data, primary_key)
        elif mode == "bitemporal":
            return self._write_bitemporal(table_name, arrow_data, primary_key)

    # ── Transform (SQL → write) ───────────────────────────────────────────

    def transform(
        self,
        table_name: str,
        sql: str,
        mode: str = "append",
        primary_key: str | None = None,
    ) -> int:
        """
        Run a SQL query and write the results into an Iceberg table.

        This is equivalent to: ingest(table_name, query_arrow(sql), mode, primary_key).
        The SQL should return the data you want to write — the system handles
        all versioning metadata automatically.

        Args:
            table_name: Target table name (created automatically if missing).
            sql: SQL query whose results will be written.
            mode: Write mode — "append", "snapshot", "incremental", "bitemporal".
            primary_key: Required for "incremental" and "bitemporal" modes.

        Returns:
            Number of rows written.
        """
        arrow_data = self.query_arrow(sql)
        return self.ingest(table_name, arrow_data, mode=mode, primary_key=primary_key)

    # ── Write modes (all via DuckDB SQL) ──────────────────────────────────

    def _write_append(self, table_name: str, data: pa.Table) -> int:
        """Append mode: raw append with _batch_id and _batch_ts only."""
        fqn = self._fqn(table_name)
        batch_id = str(uuid.uuid4())
        n = len(data)
        user_cols = ", ".join(data.column_names)

        view = f"_ingest_{table_name}_{uuid.uuid4().hex[:8]}"
        self._register_arrow(view, data)
        try:
            self._ensure_table_from_sql(fqn, f"""
                SELECT {user_cols},
                       CAST('{batch_id}' AS VARCHAR) AS _batch_id,
                       now() AS _batch_ts
                FROM {view} WHERE false
            """)
            self._exec(f"""
                INSERT INTO {fqn}
                SELECT {user_cols},
                       CAST('{batch_id}' AS VARCHAR) AS _batch_id,
                       now() AS _batch_ts
                FROM {view}
            """)
        finally:
            self._unregister(view)
        logger.info("Append %s: wrote %d rows to %s", batch_id[:8], n, table_name)
        return n

    def _write_snapshot(self, table_name: str, data: pa.Table) -> int:
        """Snapshot mode: batch-level versioning with _batch_id, _batch_ts, _is_current."""
        fqn = self._fqn(table_name)
        batch_id = str(uuid.uuid4())
        n = len(data)
        user_cols = ", ".join(data.column_names)

        view = f"_ingest_{table_name}_{uuid.uuid4().hex[:8]}"
        self._register_arrow(view, data)
        try:
            self._ensure_table_from_sql(fqn, f"""
                SELECT {user_cols},
                       CAST('{batch_id}' AS VARCHAR) AS _batch_id,
                       now() AS _batch_ts,
                       CAST(true AS BOOLEAN) AS _is_current
                FROM {view} WHERE false
            """)

            # Expire previous batch
            self._exec(f"""
                UPDATE {fqn} SET _is_current = false
                WHERE _is_current = true
            """)

            # Insert new batch
            self._exec(f"""
                INSERT INTO {fqn}
                SELECT {user_cols},
                       CAST('{batch_id}' AS VARCHAR) AS _batch_id,
                       now() AS _batch_ts,
                       CAST(true AS BOOLEAN) AS _is_current
                FROM {view}
            """)
        finally:
            self._unregister(view)

        logger.info("Snapshot %s: wrote %d rows to %s", batch_id[:8], n, table_name)
        return n

    def _write_incremental(self, table_name: str, data: pa.Table, primary_key: str) -> int:
        """Incremental mode: row-level upsert by primary key with soft delete."""
        if primary_key not in data.column_names:
            raise ValueError(f"primary_key '{primary_key}' not found in data columns: {data.column_names}")

        fqn = self._fqn(table_name)
        batch_id = str(uuid.uuid4())
        n = len(data)
        user_cols = ", ".join(data.column_names)

        view = f"_ingest_{table_name}_{uuid.uuid4().hex[:8]}"
        self._register_arrow(view, data)
        try:
            self._ensure_table_from_sql(fqn, f"""
                SELECT {user_cols},
                       CAST('{batch_id}' AS VARCHAR) AS _batch_id,
                       now() AS _batch_ts,
                       CAST(true AS BOOLEAN) AS _is_current,
                       now() AS _updated_at
                FROM {view} WHERE false
            """)

            # Expire matching current rows
            self._exec(f"""
                UPDATE {fqn}
                SET _is_current = false, _updated_at = now()
                WHERE _is_current = true
                  AND {primary_key} IN (SELECT {primary_key} FROM {view})
            """)

            # Insert new current versions
            self._exec(f"""
                INSERT INTO {fqn}
                SELECT {user_cols},
                       CAST('{batch_id}' AS VARCHAR) AS _batch_id,
                       now() AS _batch_ts,
                       CAST(true AS BOOLEAN) AS _is_current,
                       now() AS _updated_at
                FROM {view}
            """)
        finally:
            self._unregister(view)

        logger.info("Incremental %s: upserted %d rows in %s", batch_id[:8], n, table_name)
        return n

    def _write_bitemporal(self, table_name: str, data: pa.Table, primary_key: str) -> int:
        """Bitemporal mode: system time + business time versioning."""
        if primary_key not in data.column_names:
            raise ValueError(f"primary_key '{primary_key}' not found in data columns: {data.column_names}")

        fqn = self._fqn(table_name)
        batch_id = str(uuid.uuid4())
        n = len(data)

        # Build the valid_from/valid_to expressions — use user columns if present
        vf_expr = "_valid_from" if "_valid_from" in data.column_names else "now()"
        vt_expr = "_valid_to" if "_valid_to" in data.column_names else "CAST(NULL AS TIMESTAMPTZ)"

        # Columns to SELECT for INSERT (exclude _valid_from/_valid_to from user_cols if we're adding them)
        insert_user_cols = [c for c in data.column_names if c not in ("_valid_from", "_valid_to")]
        insert_user_cols_str = ", ".join(insert_user_cols)

        view = f"_ingest_{table_name}_{uuid.uuid4().hex[:8]}"
        self._register_arrow(view, data)
        try:
            self._ensure_table_from_sql(fqn, f"""
                SELECT {insert_user_cols_str},
                       CAST('{batch_id}' AS VARCHAR) AS _batch_id,
                       now() AS _batch_ts,
                       CAST(true AS BOOLEAN) AS _is_current,
                       now() AS _tx_time,
                       now() AS _valid_from,
                       CAST(NULL AS TIMESTAMPTZ) AS _valid_to
                FROM {view} WHERE false
            """)

            # Expire matching current rows — close their valid_to window
            self._exec(f"""
                UPDATE {fqn}
                SET _is_current = false, _valid_to = now(), _tx_time = now()
                WHERE _is_current = true
                  AND {primary_key} IN (SELECT {primary_key} FROM {view})
            """)

            # Insert new current versions
            self._exec(f"""
                INSERT INTO {fqn}
                SELECT {insert_user_cols_str},
                       CAST('{batch_id}' AS VARCHAR) AS _batch_id,
                       now() AS _batch_ts,
                       CAST(true AS BOOLEAN) AS _is_current,
                       now() AS _tx_time,
                       {vf_expr} AS _valid_from,
                       {vt_expr} AS _valid_to
                FROM {view}
            """)
        finally:
            self._unregister(view)

        logger.info("Bitemporal %s: upserted %d rows in %s", batch_id[:8], n, table_name)
        return n

    # ── Table management helpers ──────────────────────────────────────────

    def _ensure_namespace(self) -> None:
        """Ensure the Iceberg namespace exists in the catalog."""
        try:
            self._exec(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{self._namespace}")
        except Exception:
            pass  # Already exists or not supported

    def _ensure_table_from_view(self, fqn: str, view: str) -> None:
        """Create an Iceberg table from a DuckDB view schema if it doesn't exist."""
        conn = self._ensure_conn()
        try:
            conn.execute(f"SELECT 1 FROM {fqn} LIMIT 0")
        except duckdb.CatalogException:
            self._ensure_namespace()
            self._exec(f"CREATE TABLE {fqn} AS SELECT * FROM {view} WHERE false")
            logger.info("Created Iceberg table %s", fqn)

    def _ensure_table_from_sql(self, fqn: str, schema_sql: str) -> None:
        """Create an Iceberg table from a SQL schema query if it doesn't exist."""
        conn = self._ensure_conn()
        try:
            conn.execute(f"SELECT 1 FROM {fqn} LIMIT 0")
        except duckdb.CatalogException:
            self._ensure_namespace()
            self._exec(f"CREATE TABLE {fqn} AS {schema_sql}")
            logger.info("Created Iceberg table %s", fqn)

    # ── Metadata helpers ──────────────────────────────────────────────────

    def tables(self) -> list[str]:
        """List all tables in the lakehouse catalog."""
        conn = self._ensure_conn()
        result = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='lakehouse' AND table_schema='default'"
        ).fetchall()
        return [row[0] for row in result]

    def table_info(self, table_name: str) -> list[dict]:
        """Get column info for a specific table."""
        conn = self._ensure_conn()
        result = conn.execute(f"DESCRIBE lakehouse.default.{table_name}")
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def row_count(self, table_name: str) -> int:
        """Get the row count for a specific table."""
        conn = self._ensure_conn()
        result = conn.execute(
            f"SELECT count(*) FROM lakehouse.default.{table_name}"
        ).fetchone()
        return result[0] if result else 0

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("DuckDB connection closed")

    # ── Backward compatibility ────────────────────────────────────────────
    # Old method names from LakehouseQuery

    def sql(self, query_str: str, params: list | None = None) -> list[dict]:
        """Alias for query() — backward compatibility with LakehouseQuery."""
        return self.query(query_str, params)

    def sql_arrow(self, query_str: str, params: list | None = None) -> pa.Table:
        """Alias for query_arrow() — backward compatibility with LakehouseQuery."""
        return self.query_arrow(query_str, params)

    def sql_df(self, query_str: str, params: list | None = None):
        """Alias for query_df() — backward compatibility with LakehouseQuery."""
        return self.query_df(query_str, params)


# ── Deprecated alias ──────────────────────────────────────────────────────

LakehouseQuery = Lakehouse


# ── Module-level helpers ──────────────────────────────────────────────────


def _to_arrow(data) -> pa.Table:
    """Convert various data formats to a PyArrow Table."""
    if isinstance(data, pa.Table):
        return data

    # pandas DataFrame
    try:
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            return pa.Table.from_pandas(data)
    except ImportError:
        pass

    # list of dicts
    if isinstance(data, list):
        if len(data) == 0:
            return pa.table({})
        if isinstance(data[0], dict):
            # Build column-oriented from dicts
            keys = list(data[0].keys())
            columns = {k: [row.get(k) for row in data] for k in keys}
            return pa.table(columns)

    raise TypeError(f"Cannot convert {type(data).__name__} to Arrow Table. "
                    f"Supported: pa.Table, pd.DataFrame, list[dict]")
