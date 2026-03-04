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
from typing import TYPE_CHECKING, Any

import duckdb
import pyarrow as pa

if TYPE_CHECKING:
    import pandas as pd

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

    Row-Level Security (optional)::

        # When token is set, protected-table queries route through Flight SQL;
        # open tables still go direct to DuckDB (zero overhead).
        lh = Lakehouse("demo", token="alice-token")
        lh.query("SELECT * FROM lakehouse.default.sales_data")  # → RLS-filtered
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
        token: str | None = None,
        # Legacy positional compat
        catalog_uri: str | None = None,
    ) -> None:
        # Resolve alias
        resolved = self._resolve(alias_or_catalog, catalog_uri)

        self._catalog_uri = resolved.get("catalog_url") or os.environ.get("LAKEKEEPER_URI", DEFAULT_CATALOG_URI)
        self._warehouse = warehouse or resolved.get("warehouse") or os.environ.get("LAKEHOUSE_WAREHOUSE", DEFAULT_WAREHOUSE)
        self._s3_endpoint = s3_endpoint or resolved.get("s3_endpoint") or os.environ.get("S3_ENDPOINT", DEFAULT_S3_ENDPOINT)
        self._s3_access_key = s3_access_key or resolved.get("s3_access_key") or os.environ.get("S3_ACCESS_KEY", DEFAULT_S3_ACCESS_KEY)
        self._s3_secret_key = s3_secret_key or resolved.get("s3_secret_key") or os.environ.get("S3_SECRET_KEY", DEFAULT_S3_SECRET_KEY)
        self._s3_region = s3_region or resolved.get("s3_region") or os.environ.get("S3_REGION", DEFAULT_S3_REGION)
        self._namespace = namespace or resolved.get("namespace", "default")
        self._conn: duckdb.DuckDBPyConnection | None = None

        # ── RLS / Flight SQL (optional) ───────────────────────────────────
        self._token = token
        self._flight_host = resolved.get("flight_host")
        self._flight_port = resolved.get("flight_port")
        self._flight_client: Any = None  # pyarrow.flight.FlightClient (lazy)
        self._protected_tables: set[str] = set()

        if self._token and self._flight_host and self._flight_port:
            self._init_rls()

    @staticmethod
    def _resolve(alias_or_catalog: str | None, catalog_uri: str | None) -> dict:
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

    # ── RLS / Flight SQL helpers ──────────────────────────────────────────

    def _init_rls(self) -> None:
        """Initialize RLS: connect to Flight server and fetch protected table set."""
        try:
            import pyarrow.flight as flight
            location = flight.Location.for_grpc_tcp(self._flight_host, self._flight_port)
            self._flight_client = flight.FlightClient(location)

            # Authenticate with the bearer token
            self._flight_client.authenticate(
                _TokenClientAuthHandler(self._token or "")
            )

            # Fetch the set of protected tables from the server
            action = flight.Action("get_protected_tables", b"")
            results = list(self._flight_client.do_action(action))
            if results:
                import json
                self._protected_tables = set(json.loads(results[0].body.to_pybytes()))

            logger.info(
                "RLS initialized: %d protected tables via %s:%s",
                len(self._protected_tables), self._flight_host, self._flight_port,
            )
        except Exception as e:
            logger.error("Failed to initialize RLS Flight client: %s", e)
            self._flight_client = None
            self._protected_tables = set()

    def _ensure_flight(self) -> Any:
        """Return the authenticated Flight client."""
        if self._flight_client is None:
            raise RuntimeError(
                "Flight SQL client not initialized. "
                "Provide token= and ensure alias has flight_host/flight_port."
            )
        return self._flight_client

    def _is_flight_query(self, sql: str) -> bool:
        """Check if the SQL references any RLS-protected table."""
        if not self._token or not self._protected_tables:
            return False
        try:
            from lakehouse.rls_server import RLSRewriter
            tables = RLSRewriter.extract_tables(sql)
            return bool(tables & self._protected_tables)
        except Exception:
            return False

    def _flight_query_arrow(self, sql: str) -> pa.Table:
        """Execute a query via Flight SQL and return an Arrow Table."""
        import pyarrow.flight as flight
        client = self._ensure_flight()
        ticket = flight.Ticket(sql.encode("utf-8"))
        reader = client.do_get(ticket)
        return reader.read_all()

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

        When ``token`` is set and the query references a protected table,
        it is automatically routed through the Flight SQL server for RLS.
        """
        # RLS routing: protected tables → Flight SQL
        if self._is_flight_query(sql):
            table = self._flight_query_arrow(sql)
            columns = table.column_names
            rows = table.to_pydict()
            return [
                {col: rows[col][i] for col in columns}
                for i in range(table.num_rows)
            ]

        # Direct DuckDB path (open tables)
        conn = self._ensure_conn()
        try:
            if params:
                result = conn.execute(sql, params)
            else:
                result = conn.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]
        except Exception as e:
            logger.error("Query failed: %s — %s", sql[:200], e)
            raise

    def query_arrow(self, sql: str, params: list | None = None) -> pa.Table:
        """Execute a SQL query and return results as a PyArrow Table."""
        # RLS routing: protected tables → Flight SQL
        if self._is_flight_query(sql):
            return self._flight_query_arrow(sql)

        # Direct DuckDB path
        conn = self._ensure_conn()
        if params:
            return conn.execute(sql, params).fetch_arrow_table()
        return conn.execute(sql).fetch_arrow_table()

    def query_df(self, sql: str, params: list | None = None) -> pd.DataFrame:
        """Execute a SQL query and return results as a Pandas DataFrame."""
        # RLS routing: protected tables → Flight SQL
        if self._is_flight_query(sql):
            return self._flight_query_arrow(sql).to_pandas()

        # Direct DuckDB path
        conn = self._ensure_conn()
        if params:
            return conn.execute(sql, params).fetchdf()
        return conn.execute(sql).fetchdf()

    # ── Ingest (write) ────────────────────────────────────────────────────

    def ingest(
        self,
        table_name: str,
        data: pa.Table | list[dict] | pd.DataFrame | str,
        mode: str = "append",
        primary_key: str | None = None,
    ) -> int:
        """
        Write data into an Iceberg table.

        Args:
            table_name: Target table name (created automatically if missing).
            data: Data to write.  Accepts:
                  - **str**: File pointer (URL/S3/local path) or SQL query.
                    Data stays in DuckDB — never enters Python memory.
                  - **PyArrow Table**, **pandas DataFrame**, or **list[dict]**.
            mode: Write mode — "append", "snapshot", "incremental", "bitemporal".
            primary_key: Required for "incremental" and "bitemporal" modes.

        Returns:
            Number of rows written.

        Examples::

            # File pointer (zero Python transit)
            lh.ingest("taxi", "https://host/yellow_tripdata.parquet")
            lh.ingest("taxi", "s3://bucket/data.parquet")

            # SQL with column selection / renaming
            lh.ingest("taxi", "SELECT col AS new_name FROM 'url.parquet'")

            # Python objects (existing)
            lh.ingest("taxi", arrow_table)
            lh.ingest("taxi", dataframe)
        """
        if mode not in VALID_MODES:
            raise ValueError(f"Invalid mode '{mode}'. Must be one of: {VALID_MODES}")
        if mode in ("incremental", "bitemporal") and not primary_key:
            raise ValueError(f"mode='{mode}' requires a primary_key")

        # ── String source: URL, file path, or SQL ──
        if isinstance(data, str):
            return self._ingest_from_sql(table_name, data, mode, primary_key)

        # ── Python object source: Arrow, DataFrame, dicts ──
        arrow_data = _to_arrow(data)
        if len(arrow_data) == 0:
            return 0

        view = f"_ingest_{table_name}_{uuid.uuid4().hex[:8]}"
        self._register_arrow(view, arrow_data)
        try:
            col_names = list(arrow_data.column_names)
            n = len(arrow_data)
            return self._write_mode(table_name, view, col_names, n, mode, primary_key)
        finally:
            self._unregister(view)

    def _ingest_from_sql(
        self,
        table_name: str,
        source: str,
        mode: str,
        primary_key: str | None,
    ) -> int:
        """Ingest from a SQL string or file pointer — zero Python transit.

        For append/snapshot (source referenced once): streams directly.
        For incremental/bitemporal (source referenced twice): materializes
        into a DuckDB temp table first to avoid re-fetching.
        """
        source_sql = source.strip()
        if not source_sql.upper().startswith("SELECT"):
            source_sql = f"SELECT * FROM '{source_sql}'"

        # Discover column names from schema (metadata only, no data read)
        conn = self._ensure_conn()
        col_names = [
            row[0] for row in
            conn.execute(f"DESCRIBE ({source_sql})").fetchall()
        ]

        if mode in ("append", "snapshot"):
            # FAST PATH: stream directly from source → Iceberg, no temp table.
            # Wrap as subquery so it works as a FROM clause.
            source_ref = f"({source_sql})"
            row = conn.execute(f"SELECT count(*) FROM {source_ref}").fetchone()
            assert row is not None
            n = row[0]
            return self._write_mode(table_name, source_ref, col_names, n, mode, primary_key)
        else:
            # TEMP TABLE: incremental/bitemporal need source referenced twice.
            staging = f"_staging_{uuid.uuid4().hex[:8]}"
            try:
                self._exec(f"CREATE TEMP TABLE {staging} AS {source_sql}")
                row = conn.execute(f"SELECT count(*) FROM {staging}").fetchone()
                assert row is not None
                n = row[0]
                return self._write_mode(table_name, staging, col_names, n, mode, primary_key)
            finally:
                self._exec(f"DROP TABLE IF EXISTS {staging}")

    def _write_mode(
        self,
        table_name: str,
        source_ref: str,
        col_names: list[str],
        n: int,
        mode: str,
        primary_key: str | None,
    ) -> int:
        """Dispatch to the correct write mode."""
        if mode == "append":
            return self._write_append(table_name, source_ref, col_names, n)
        elif mode == "snapshot":
            return self._write_snapshot(table_name, source_ref, col_names, n)
        elif mode == "incremental":
            if primary_key is None:
                raise ValueError("primary_key is required for incremental mode")
            return self._write_incremental(table_name, source_ref, col_names, n, primary_key)
        elif mode == "bitemporal":
            if primary_key is None:
                raise ValueError("primary_key is required for bitemporal mode")
            return self._write_bitemporal(table_name, source_ref, col_names, n, primary_key)
        raise ValueError(f"Unknown write mode: {mode}")

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

        Data stays in DuckDB — no Python round-trip.

        Args:
            table_name: Target table name (created automatically if missing).
            sql: SQL query whose results will be written.
            mode: Write mode — "append", "snapshot", "incremental", "bitemporal".
            primary_key: Required for "incremental" and "bitemporal" modes.

        Returns:
            Number of rows written.
        """
        return self.ingest(table_name, sql, mode=mode, primary_key=primary_key)

    # ── Write modes (all via DuckDB SQL) ──────────────────────────────────

    def _write_append(self, table_name: str, source_ref: str, col_names: list[str], n: int) -> int:
        """Append mode: raw append with _batch_id and _batch_ts only."""
        fqn = self._fqn(table_name)
        batch_id = str(uuid.uuid4())
        user_cols = ", ".join(col_names)

        self._ensure_table_from_sql(fqn, f"""
            SELECT {user_cols},
                   CAST('{batch_id}' AS VARCHAR) AS _batch_id,
                   now() AS _batch_ts
            FROM {source_ref} WHERE false
        """)
        self._exec(f"""
            INSERT INTO {fqn}
            SELECT {user_cols},
                   CAST('{batch_id}' AS VARCHAR) AS _batch_id,
                   now() AS _batch_ts
            FROM {source_ref}
        """)
        logger.info("Append %s: wrote %d rows to %s", batch_id[:8], n, table_name)
        return n

    def _write_snapshot(self, table_name: str, source_ref: str, col_names: list[str], n: int) -> int:
        """Snapshot mode: batch-level versioning with _batch_id, _batch_ts, _is_current."""
        fqn = self._fqn(table_name)
        batch_id = str(uuid.uuid4())
        user_cols = ", ".join(col_names)

        self._ensure_table_from_sql(fqn, f"""
            SELECT {user_cols},
                   CAST('{batch_id}' AS VARCHAR) AS _batch_id,
                   now() AS _batch_ts,
                   CAST(true AS BOOLEAN) AS _is_current
            FROM {source_ref} WHERE false
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
            FROM {source_ref}
        """)

        logger.info("Snapshot %s: wrote %d rows to %s", batch_id[:8], n, table_name)
        return n

    def _write_incremental(self, table_name: str, source_ref: str, col_names: list[str], n: int, primary_key: str) -> int:
        """Incremental mode: row-level upsert by primary key with soft delete."""
        if primary_key not in col_names:
            raise ValueError(f"primary_key '{primary_key}' not found in data columns: {col_names}")

        fqn = self._fqn(table_name)
        batch_id = str(uuid.uuid4())
        user_cols = ", ".join(col_names)

        self._ensure_table_from_sql(fqn, f"""
            SELECT {user_cols},
                   CAST('{batch_id}' AS VARCHAR) AS _batch_id,
                   now() AS _batch_ts,
                   CAST(true AS BOOLEAN) AS _is_current,
                   now() AS _updated_at
            FROM {source_ref} WHERE false
        """)

        # Expire matching current rows
        self._exec(f"""
            UPDATE {fqn}
            SET _is_current = false, _updated_at = now()
            WHERE _is_current = true
              AND {primary_key} IN (SELECT {primary_key} FROM {source_ref})
        """)

        # Insert new current versions
        self._exec(f"""
            INSERT INTO {fqn}
            SELECT {user_cols},
                   CAST('{batch_id}' AS VARCHAR) AS _batch_id,
                   now() AS _batch_ts,
                   CAST(true AS BOOLEAN) AS _is_current,
                   now() AS _updated_at
            FROM {source_ref}
        """)

        logger.info("Incremental %s: upserted %d rows in %s", batch_id[:8], n, table_name)
        return n

    def _write_bitemporal(self, table_name: str, source_ref: str, col_names: list[str], n: int, primary_key: str) -> int:
        """Bitemporal mode: system time + business time versioning."""
        if primary_key not in col_names:
            raise ValueError(f"primary_key '{primary_key}' not found in data columns: {col_names}")

        fqn = self._fqn(table_name)
        batch_id = str(uuid.uuid4())

        # Build the valid_from/valid_to expressions — use user columns if present
        vf_expr = "_valid_from" if "_valid_from" in col_names else "now()"
        vt_expr = "_valid_to" if "_valid_to" in col_names else "CAST(NULL AS TIMESTAMPTZ)"

        # Columns to SELECT for INSERT (exclude _valid_from/_valid_to from user_cols if we're adding them)
        insert_user_cols = [c for c in col_names if c not in ("_valid_from", "_valid_to")]
        insert_user_cols_str = ", ".join(insert_user_cols)

        self._ensure_table_from_sql(fqn, f"""
            SELECT {insert_user_cols_str},
                   CAST('{batch_id}' AS VARCHAR) AS _batch_id,
                   now() AS _batch_ts,
                   CAST(true AS BOOLEAN) AS _is_current,
                   now() AS _tx_time,
                   now() AS _valid_from,
                   CAST(NULL AS TIMESTAMPTZ) AS _valid_to
            FROM {source_ref} WHERE false
        """)

        # Expire matching current rows — close their valid_to window
        self._exec(f"""
            UPDATE {fqn}
            SET _is_current = false, _valid_to = now(), _tx_time = now()
            WHERE _is_current = true
              AND {primary_key} IN (SELECT {primary_key} FROM {source_ref})
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
            FROM {source_ref}
        """)

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
        return [dict(zip(columns, row, strict=False)) for row in rows]

    def row_count(self, table_name: str) -> int:
        """Get the row count for a specific table."""
        conn = self._ensure_conn()
        result = conn.execute(
            f"SELECT count(*) FROM lakehouse.default.{table_name}"
        ).fetchone()
        return result[0] if result else 0

    # ── Datacube ────────────────────────────────────────────────────────

    def datacube(self, table_name: str, **kwargs: Any) -> Any:
        """Create a Datacube over a Lakehouse Iceberg table.

        Queries go directly from S3 → DuckDB → result (no Python).
        Short table names are auto-resolved to ``lakehouse.{ns}.{table}``.

        Usage::

            dc = lh.datacube("trades")
            dc = dc.set_group_by("sector").set_pivot_by("side")
            result = dc.query_df()
        """
        from datacube.engine import Datacube
        return Datacube(self, source_name=table_name, **kwargs)

    def close(self) -> None:
        """Close the DuckDB and Flight SQL connections."""
        if self._flight_client:
            self._flight_client.close()
            self._flight_client = None
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

    def sql_df(self, query_str: str, params: list | None = None) -> object:
        """Alias for query_df() — backward compatibility with LakehouseQuery."""
        return self.query_df(query_str, params)


# ── Flight SQL auth helper ────────────────────────────────────────────────


class _TokenClientAuthHandler:
    """Client-side auth handler for Flight SQL bearer-token authentication.

    Wraps a token string and implements the authenticate/get_token protocol.
    Extends flight.ClientAuthHandler when pyarrow.flight is available.
    """

    _token: bytes
    _session_token: bytes

    def __new__(cls, token: str) -> _TokenClientAuthHandler:
        try:
            import pyarrow.flight as _flight
            # Dynamically create a subclass that extends ClientAuthHandler
            if not issubclass(cls, _flight.ClientAuthHandler):
                bases = (_flight.ClientAuthHandler,)
                ns = {k: v for k, v in cls.__dict__.items() if k != "__dict__"}
                new_cls = type(cls.__name__, bases, ns)
                instance: _TokenClientAuthHandler = _flight.ClientAuthHandler.__new__(new_cls)  # type: ignore[assignment]
                instance._token = token.encode("utf-8")
                instance._session_token = b""
                return instance
        except ImportError:
            pass
        instance = object.__new__(cls)
        instance._token = token.encode("utf-8")
        instance._session_token = b""
        return instance

    def authenticate(self, outgoing: Any, incoming: Any) -> None:
        """Send token, receive session token."""
        outgoing.write(self._token)
        self._session_token = incoming.read()

    def get_token(self) -> bytes:
        """Return the session token for subsequent requests."""
        return self._session_token


# ── Deprecated alias ──────────────────────────────────────────────────────

LakehouseQuery = Lakehouse


# ── Module-level helpers ──────────────────────────────────────────────────


def _to_arrow(data: pa.Table | list | Any) -> pa.Table:
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
