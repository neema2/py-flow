"""
Tests for the agents/ package — PlatformAgents.

Four tiers:
1. Codegen integration — define_module, execute_python, inspect_registry (tmp dirs)
2. OLTP end-to-end   — real embedded Postgres via StoreServer
3. All-agent e2e     — every agent tool against real services
4. Eval framework     — scoring math (pure Python)
"""

import asyncio
import json
import os
import tempfile
import uuid

import agents._codegen as _cg
import pytest
from agents._codegen import create_codegen_tools
from agents._context import _PlatformContext
from agents._dashboard import create_dashboard_tools
from agents._datascience import create_datascience_tools
from agents._document import create_document_tools
from agents._eval.datasets import (
    ALL_EVAL_CASES,
    DATASCIENCE_EVAL_CASES,
    LAKEHOUSE_EVAL_CASES,
    OLTP_EVAL_CASES,
    QUERY_EVAL_CASES,
)
from agents._eval.framework import (
    DEFAULT_DIMENSIONS,
    AgentEval,
    AgentEvalCase,
    EvalPhase,
    _score_output_contains,
    _score_query_correctness,
    _score_schema_quality,
    _score_table_creation,
    _score_tool_selection,
)
from agents._eval.judges import (
    ANALYSIS_QUALITY_RUBRIC,
    CURATION_QUALITY_RUBRIC,
    DATA_MODEL_RUBRIC,
    METADATA_QUALITY_RUBRIC,
    STAR_SCHEMA_RUBRIC,
)
from agents._eval.scorers import (
    score_naming_conventions,
    score_sql_validity,
    score_star_schema_design,
)
from agents._feed import create_feed_tools
from agents._lakehouse import create_lakehouse_tools
from agents._oltp import create_oltp_tools
from agents._query import create_query_tools
from agents._team import _AGENT_DESCRIPTIONS, PlatformAgents
from agents._timeseries import create_timeseries_tools
from store.base import Storable
from store.columns import REGISTRY

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
requires_gemini = pytest.mark.skipif(not GEMINI_API_KEY, reason="GEMINI_API_KEY not set")


# Unique suffix for lakehouse table isolation
_UID = uuid.uuid4().hex[:8]


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def codegen_env(tmp_path):
    """Redirect codegen dirs to tmp and snapshot registry for cleanup."""
    col_dir = tmp_path / "columns" / "agent_generated"
    mod_dir = tmp_path / "models" / "agent_generated"
    col_dir.mkdir(parents=True)
    mod_dir.mkdir(parents=True)
    (col_dir / "__init__.py").write_text("")
    (mod_dir / "__init__.py").write_text("")

    orig_cols, orig_mods = _cg.COLUMNS_DIR, _cg.MODELS_DIR
    _cg.COLUMNS_DIR, _cg.MODELS_DIR = col_dir, mod_dir

    before = set(REGISTRY._columns.keys())
    yield col_dir, mod_dir

    _cg.COLUMNS_DIR, _cg.MODELS_DIR = orig_cols, orig_mods
    for k in list(REGISTRY._columns.keys()):
        if k not in before:
            del REGISTRY._columns[k]


@pytest.fixture(scope="module")
def _de_store(store_server):
    """Delegate to session-scoped store_server from conftest.py."""
    store_server.provision_user("agent_user", "agent_pw")
    store_server.register_alias("agent_test")
    return store_server


def _get_tool(tools, name):
    return next(t for t in tools if t.__name__ == name)


# ── Tool Inventory (regression guard) ─────────────────────────────────


_EXPECTED_TOOLS = {
    "oltp": (9, ["create_dataset", "insert_records", "query_dataset",
                  "inspect_registry", "define_module", "execute_python"]),
    "lakehouse": (7, ["list_lakehouse_tables", "design_star_schema", "build_datacube"]),
    "feed": (5, ["list_md_symbols", "describe_feed_setup"]),
    "timeseries": (6, ["list_tsdb_series", "get_bars"]),
    "document": (6, ["upload_document", "search_documents"]),
    "dashboard": (9, ["create_reactive_model", "create_ticking_table",
                       "inspect_registry", "define_module"]),
    "query": (7, ["query_store", "list_all_datasets"]),
    "datascience": (7, ["compute_statistics", "run_regression"]),
}

_TOOL_CREATORS = {
    "oltp": create_oltp_tools,
    "lakehouse": create_lakehouse_tools,
    "feed": create_feed_tools,
    "timeseries": create_timeseries_tools,
    "document": create_document_tools,
    "dashboard": create_dashboard_tools,
    "query": create_query_tools,
    "datascience": create_datascience_tools,
}


class TestToolInventory:

    @pytest.mark.parametrize("agent", list(_EXPECTED_TOOLS.keys()))
    def test_tool_count_and_names(self, agent):
        expected_count, must_have = _EXPECTED_TOOLS[agent]
        tools = _TOOL_CREATORS[agent](_PlatformContext())
        names = [t.__name__ for t in tools]
        assert len(names) == expected_count, f"{agent}: expected {expected_count}, got {len(names)}: {names}"
        for name in must_have:
            assert name in names, f"{agent} missing tool: {name}"


# ── Codegen Integration (tmp dirs, no services) ──────────────────────


class TestCodegenIntegration:

    def test_inspect_registry(self):
        inspect_fn = _get_tool(create_codegen_tools(_PlatformContext()), "inspect_registry")
        result = json.loads(inspect_fn(columns_json='["symbol", "zzz_fake"]'))
        assert result["symbol"]["exists"] is True
        assert result["zzz_fake"]["exists"] is False

    def test_inspect_summary(self):
        inspect_fn = _get_tool(create_codegen_tools(_PlatformContext()), "inspect_registry")
        result = json.loads(inspect_fn(columns_json="[]"))
        assert result["total_columns"] > 0

    def test_define_column_module(self, codegen_env):
        col_dir, _ = codegen_env
        define_fn = _get_tool(create_codegen_tools(_PlatformContext()), "define_module")
        code = (
            'from store.columns import REGISTRY\n'
            'REGISTRY.define("cg_test_col", float, role="measure", unit="USD", description="Test")\n'
        )
        r = json.loads(define_fn(module_name="test_cols", code=code, module_type="columns"))
        assert r["status"] == "success", f"Failed: {r}"
        assert (col_dir / "test_cols.py").exists()
        assert REGISTRY.has("cg_test_col")

    def test_define_model_module(self, codegen_env):
        _, _mod_dir = codegen_env
        ctx = _PlatformContext()
        define_fn = _get_tool(create_codegen_tools(ctx), "define_module")
        for n in ["cg_mod_a", "cg_mod_b"]:
            if not REGISTRY.has(n):
                REGISTRY.define(n, float, role="measure", unit="units", description=n)
        code = (
            'from dataclasses import dataclass\nfrom store.base import Storable\n\n'
            '@dataclass\nclass CGModel(Storable):\n    cg_mod_a: float = 0.0\n    cg_mod_b: float = 0.0\n'
        )
        r = json.loads(define_fn(module_name="test_model", code=code, module_type="models"))
        assert r["status"] == "success", f"Failed: {r}"
        assert "CGModel" in r["created_types"]
        assert ctx.get_storable_type("CGModel") is not None

    def test_execute_python(self):
        exec_fn = _get_tool(create_codegen_tools(_PlatformContext()), "execute_python")
        r = json.loads(exec_fn(code="result = 2 + 2"))
        assert r["status"] == "success" and r["result"] == 4

    def test_execute_python_print_capture(self):
        exec_fn = _get_tool(create_codegen_tools(_PlatformContext()), "execute_python")
        r = json.loads(exec_fn(code='print("hello sandbox")'))
        assert "hello sandbox" in r["output"]

    def test_execute_python_rejects_forbidden(self):
        exec_fn = _get_tool(create_codegen_tools(_PlatformContext()), "execute_python")
        assert json.loads(exec_fn(code="import os"))["status"] == "error"


# ── OLTP Codegen Pipeline (tmp dirs, no Postgres) ────────────────────


class TestOLTPCodegen:

    def test_create_dataset_full_pipeline(self, codegen_env):
        col_dir, mod_dir = codegen_env
        ctx = _PlatformContext()
        create_fn = _get_tool(create_oltp_tools(ctx), "create_dataset")

        fields = json.dumps([
            {"name": "cg_symbol", "type": "str"},
            {"name": "cg_price", "type": "float"},
            {"name": "cg_quantity", "type": "int"},
        ])
        result = json.loads(create_fn(name="CGTrade", fields_json=fields))
        assert result["status"] == "created", f"Failed: {result}"
        assert result["persistent"] is True

        # Files on disk
        assert (col_dir / "cgtrade_columns.py").exists()
        assert (mod_dir / "cgtrade_model.py").exists()

        # Column metadata
        assert REGISTRY.get("cg_symbol").role == "dimension"
        assert REGISTRY.get("cg_price").role == "measure"

        # Type in context, instantiable
        cls = ctx.get_storable_type("CGTrade")
        assert issubclass(cls, Storable)  # type: ignore[arg-type]
        obj = cls()  # type: ignore[misc]
        assert obj.cg_symbol == ""
        assert obj.cg_price == 0.0

    def test_create_reuses_existing_columns(self, codegen_env):
        ctx = _PlatformContext()
        create_fn = _get_tool(create_oltp_tools(ctx), "create_dataset")
        REGISTRY.define("cg_preexisting", float, role="measure", unit="USD", description="Pre-existing")
        fields = json.dumps([
            {"name": "cg_preexisting", "type": "float"},
            {"name": "cg_new_col", "type": "str"},
        ])
        result = json.loads(create_fn(name="CGMixed", fields_json=fields))
        assert "cg_preexisting" in result["existing_columns"]
        assert "cg_new_col" in result["new_columns"]

    def test_create_duplicate_rejected(self, codegen_env):
        ctx = _PlatformContext()
        create_fn = _get_tool(create_oltp_tools(ctx), "create_dataset")
        fields = json.dumps([{"name": "cg_dup_x", "type": "int"}])
        json.loads(create_fn(name="CGDup", fields_json=fields))
        assert "error" in json.loads(create_fn(name="CGDup", fields_json=fields))

    def test_create_invalid_type_rejected(self):
        create_fn = _get_tool(create_oltp_tools(_PlatformContext()), "create_dataset")
        assert "error" in json.loads(create_fn(
            name="Bad", fields_json=json.dumps([{"name": "x", "type": "complex_number"}]),
        ))

    def test_dashboard_bad_key_rejected(self, codegen_env):
        model_fn = _get_tool(create_dashboard_tools(_PlatformContext()), "create_reactive_model")
        assert "error" in json.loads(model_fn(
            name="BadKey", key_field="nonexistent",
            fields_json=json.dumps([{"name": "x", "type": "int"}]),
        ))


# ── Additional Service Fixtures ──────────────────────────────────────


@pytest.fixture(scope="module")
def lakehouse_stack(lakehouse_server):
    """Delegate to session-scoped lakehouse_server from conftest.py."""
    lakehouse_server.register_alias("agent_test")
    return lakehouse_server


@pytest.fixture(scope="module")
def media_store_fixture(_de_store, media_server):
    """MediaStore backed by real StoreServer + real MinIO (shared)."""
    from media import MediaStore
    from media.models import bootstrap_search_schema

    # Bootstrap search schema
    admin_conn = _de_store.admin_conn()
    bootstrap_search_schema(admin_conn)
    admin_conn.close()

    # Connect as user so MediaStore has a store connection
    from store.connection import connect
    info = _de_store.conn_info()
    conn = connect(host=info["host"], port=info["port"], dbname=info["dbname"],
                   user="agent_user", password="agent_pw")

    # Register media alias
    from media._registry import register_alias as _register_media_alias
    _register_media_alias(
        "agent_test",
        endpoint="localhost:9102",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="test-agent-media",
    )

    ms = MediaStore(
        s3_endpoint="localhost:9102",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
        s3_bucket="test-agent-media",
    )
    yield ms
    ms.close()
    conn.close()


# ── OLTP End-to-End (real Postgres) ──────────────────────────────────


class TestOLTPEndToEnd:
    """Real StoreServer → create dataset → insert records → query back."""

    def test_create_and_query_round_trip(self, _de_store, codegen_env):
        _col_dir, _mod_dir = codegen_env

        ctx = _PlatformContext(
            alias="agent_test",
            user="agent_user",
            password="agent_pw",
        )
        tools = create_oltp_tools(ctx)
        create_fn = _get_tool(tools, "create_dataset")
        insert_fn = _get_tool(tools, "insert_records")
        query_fn = _get_tool(tools, "query_dataset")

        # Create dataset
        fields = json.dumps([
            {"name": "e2e_symbol", "type": "str"},
            {"name": "e2e_price", "type": "float"},
            {"name": "e2e_quantity", "type": "int"},
        ])
        create_result = json.loads(create_fn(name="E2ETrade", fields_json=fields))
        assert create_result["status"] == "created", f"Create failed: {create_result}"

        # Insert records
        records = json.dumps([
            {"e2e_symbol": "AAPL", "e2e_price": 228.50, "e2e_quantity": 100},
            {"e2e_symbol": "GOOGL", "e2e_price": 192.30, "e2e_quantity": 50},
            {"e2e_symbol": "MSFT", "e2e_price": 415.00, "e2e_quantity": 75},
        ])
        insert_result = json.loads(insert_fn(type_name="E2ETrade", records_json=records))
        assert insert_result["inserted"] == 3, f"Insert failed: {insert_result}"
        assert insert_result["errors"] == 0
        assert len(insert_result["entity_ids"]) == 3

        # Query back
        query_result = json.loads(query_fn(type_name="E2ETrade", limit=10))
        assert query_result["count"] == 3, f"Query failed: {query_result}"
        symbols = {r["e2e_symbol"] for r in query_result["rows"]}
        assert symbols == {"AAPL", "GOOGL", "MSFT"}

    def test_insert_unknown_type_rejected(self, _de_store, codegen_env):
        ctx = _PlatformContext(alias="agent_test", user="agent_user", password="agent_pw")
        insert_fn = _get_tool(create_oltp_tools(ctx), "insert_records")
        result = json.loads(insert_fn(
            type_name="NonExistent",
            records_json=json.dumps([{"x": 1}]),
        ))
        assert "error" in result

    def test_query_unknown_type_rejected(self, _de_store, codegen_env):
        ctx = _PlatformContext(alias="agent_test", user="agent_user", password="agent_pw")
        query_fn = _get_tool(create_oltp_tools(ctx), "query_dataset")
        result = json.loads(query_fn(type_name="NonExistent"))
        assert "error" in result


# ── Feed Agent E2E (real MarketDataServer) ───────────────────────────


class TestFeedAgentE2E:
    """Feed agent tools against the real MarketDataServer."""

    def test_list_md_symbols(self, market_data_server):
        from marketdata._registry import register_alias as _reg_md
        _reg_md("agent_test", url=market_data_server.url, port=market_data_server.port)
        ctx = _PlatformContext(alias="agent_test")
        tools = create_feed_tools(ctx)
        result = json.loads(_get_tool(tools, "list_md_symbols")())
        assert "error" not in result
        # SimulatorFeed produces equity + fx symbols
        if isinstance(result, dict):
            assert len(result) > 0
        else:
            assert len(result) > 0

    def test_get_md_snapshot(self, market_data_server):
        ctx = _PlatformContext(alias="agent_test")
        tools = create_feed_tools(ctx)
        result = json.loads(_get_tool(tools, "get_md_snapshot")(msg_type="equity"))
        assert "error" not in result

    def test_get_feed_health(self, market_data_server):
        ctx = _PlatformContext(alias="agent_test")
        tools = create_feed_tools(ctx)
        result = json.loads(_get_tool(tools, "get_feed_health")())
        assert "error" not in result

    def test_publish_custom_tick(self, market_data_server):
        from datetime import datetime, timezone
        ctx = _PlatformContext(alias="agent_test")
        tools = create_feed_tools(ctx)
        tick = json.dumps({"type": "equity", "symbol": "TEST", "price": 99.99,
                           "bid": 99.98, "ask": 100.00, "volume": 1000,
                           "change": 0.5, "change_pct": 0.5,
                           "timestamp": datetime.now(timezone.utc).isoformat()})
        result = json.loads(_get_tool(tools, "publish_custom_tick")(tick_json=tick))
        assert "error" not in result


# ── Timeseries Agent E2E (real MarketDataServer + QuestDB) ───────────


class TestTimeseriesAgentE2E:
    """Timeseries agent tools against real MarketDataServer (embeds QuestDB)."""

    @staticmethod
    def _ensure_alias(market_data_server):
        """Register the market data alias for agent_test (idempotent)."""
        from marketdata._registry import register_alias as _reg_md
        _reg_md("agent_test", url=market_data_server.url, port=market_data_server.port)

    def test_list_tsdb_series(self, market_data_server):
        self._ensure_alias(market_data_server)
        ctx = _PlatformContext(alias="agent_test")
        tools = create_timeseries_tools(ctx)
        result = json.loads(_get_tool(tools, "list_tsdb_series")())
        assert "error" not in result
        assert "symbols" in result

    def test_get_bars(self, market_data_server):
        self._ensure_alias(market_data_server)
        ctx = _PlatformContext(alias="agent_test")
        tools = create_timeseries_tools(ctx)
        result = json.loads(_get_tool(tools, "get_bars")(msg_type="equity", symbol="AAPL", interval="1m"))
        assert "error" not in result
        assert result["bar_count"] > 0

    def test_get_tick_history(self, market_data_server):
        self._ensure_alias(market_data_server)
        ctx = _PlatformContext(alias="agent_test")
        tools = create_timeseries_tools(ctx)
        result = json.loads(_get_tool(tools, "get_tick_history")(msg_type="equity", symbol="AAPL", limit=50))
        assert "error" not in result
        assert result["tick_count"] > 0

    def test_compute_realized_vol(self, market_data_server):
        import time
        self._ensure_alias(market_data_server)
        ctx = _PlatformContext(alias="agent_test")
        tools = create_timeseries_tools(ctx)
        vol_fn = _get_tool(tools, "compute_realized_vol")

        # Retry for up to 10s — simulator needs time to generate enough bars
        deadline = time.time() + 10
        result = None
        while time.time() < deadline:
            result = json.loads(vol_fn(symbol="AAPL", msg_type="equity", window=5, interval="1s"))
            if "error" not in result:
                break
            time.sleep(1)

        assert result is not None and "error" not in result, (
            f"compute_realized_vol did not succeed after 10s: {result}"
        )
        assert "annualized_vol" in result
        assert result["annualized_vol"] >= 0

    def test_compare_cross_exchange(self, market_data_server):
        self._ensure_alias(market_data_server)
        ctx = _PlatformContext(alias="agent_test")
        tools = create_timeseries_tools(ctx)
        result = json.loads(_get_tool(tools, "compare_cross_exchange")(symbol="AAPL", msg_type="equity"))
        assert "error" not in result
        assert result["sources_compared"] >= 1


# ── Lakehouse Agent E2E (real LakehouseServer) ───────────────────────


class TestLakehouseAgentE2E:
    """Lakehouse agent tools against real Lakekeeper + MinIO + PG."""

    def test_list_lakehouse_tables(self, lakehouse_stack):
        ctx = _PlatformContext(alias="agent_test")
        tools = create_lakehouse_tools(ctx)
        result = json.loads(_get_tool(tools, "list_lakehouse_tables")())
        assert "error" not in result
        assert "tables" in result

    def test_create_and_query_lakehouse_table(self, lakehouse_stack):
        ctx = _PlatformContext(alias="agent_test")
        tools = create_lakehouse_tools(ctx)
        tbl = f"agent_test_{_UID}"

        # Ensure the default namespace exists in the Iceberg catalog
        lh = ctx.lakehouse
        lh._ensure_namespace()

        # Create table with inline SQL
        create_fn = _get_tool(tools, "create_lakehouse_table")
        result = json.loads(create_fn(
            table_name=tbl,
            sql="SELECT 'AAPL' as symbol, 228.5 as price, 100 as qty UNION ALL SELECT 'GOOGL', 192.3, 50",
            mode="append",
        ))
        assert result.get("status") == "created", f"Create failed: {result}"
        assert result["rows_written"] == 2

        # Query back
        query_fn = _get_tool(tools, "query_lakehouse")
        qr = json.loads(query_fn(sql=f"SELECT * FROM lakehouse.default.{tbl}"))
        assert qr["row_count"] == 2
        symbols = {r["symbol"] for r in qr["rows"]}
        assert symbols == {"AAPL", "GOOGL"}


# ── Quant Agent E2E (real LakehouseServer + MarketDataServer) ────────


class TestQuantAgentE2E:
    """Quant/DataScience agent tools against real Lakehouse + MarketData."""

    def test_run_sql_analysis(self, lakehouse_stack):
        ctx = _PlatformContext(alias="agent_test")
        tools = create_datascience_tools(ctx)
        tbl = f"quant_test_{_UID}"

        # Seed data directly via lakehouse
        lh = ctx.lakehouse
        lh.ingest(tbl, [
            {"symbol": "AAPL", "price": 228.5, "volume": 1000},
            {"symbol": "GOOGL", "price": 192.3, "volume": 2000},
            {"symbol": "MSFT", "price": 415.0, "volume": 500},
        ], mode="append")

        result = json.loads(_get_tool(tools, "run_sql_analysis")(
            sql=f"SELECT * FROM lakehouse.default.{tbl}",
            description="Test analysis",
        ))
        assert "error" not in result
        assert result["row_count"] == 3
        assert "numeric_stats" in result
        assert "price" in result["numeric_stats"]

    def test_compute_statistics(self, lakehouse_stack):
        ctx = _PlatformContext(alias="agent_test")
        tools = create_datascience_tools(ctx)
        tbl = f"quant_test_{_UID}"
        result = json.loads(_get_tool(tools, "compute_statistics")(
            sql=f"SELECT * FROM lakehouse.default.{tbl}",
            columns="price,volume",
        ))
        assert "error" not in result
        stats = result["statistics"]
        assert "price" in stats
        assert stats["price"]["count"] == 3

    def test_detect_anomalies(self, lakehouse_stack):
        ctx = _PlatformContext(alias="agent_test")
        tools = create_datascience_tools(ctx)
        tbl = f"quant_test_{_UID}"
        result = json.loads(_get_tool(tools, "detect_anomalies")(
            sql=f"SELECT * FROM lakehouse.default.{tbl}",
            column="price",
            method="zscore",
            threshold=1.0,
        ))
        assert "error" not in result
        assert result["total_rows"] == 3

    def test_run_regression(self, lakehouse_stack):
        ctx = _PlatformContext(alias="agent_test")
        tools = create_datascience_tools(ctx)
        tbl = f"quant_test_{_UID}"
        result = json.loads(_get_tool(tools, "run_regression")(
            sql=f"SELECT * FROM lakehouse.default.{tbl}",
            target="price",
            features="volume",
        ))
        assert "error" not in result
        assert "r_squared" in result
        assert "coefficients" in result

    def test_time_series_decompose(self, market_data_server):
        ctx = _PlatformContext(alias="agent_test")
        tools = create_datascience_tools(ctx)
        result = json.loads(_get_tool(tools, "time_series_decompose")(
            symbol="AAPL", msg_type="equity", interval="1m", window=5,
        ))
        if "error" not in result:
            assert "trend_direction" in result
        else:
            assert "Need at least" in result["error"]

    @requires_gemini
    def test_suggest_visualization(self, lakehouse_stack):
        ctx = _PlatformContext(alias="agent_test")
        tools = create_datascience_tools(ctx)
        result = _get_tool(tools, "suggest_visualization")(
            data_description="3 columns: symbol (str), price (float), volume (int), 100 rows",
            question="Show price distribution",
        )
        parsed = json.loads(result)
        assert "chart_type" in parsed or "error" not in parsed


# ── Document Agent E2E (real StoreServer + MinIO) ────────────────────


class TestDocumentAgentE2E:
    """Document agent tools against real StoreServer + S3."""

    def test_upload_and_list(self, store_server, media_store_fixture, tmp_path):
        ctx = _PlatformContext(alias="agent_test", user="agent_user", password="agent_pw")
        # Inject the real media store into ctx
        ctx._media_store_instance = media_store_fixture
        tools = create_document_tools(ctx)

        # Create a test file
        test_file = tmp_path / "test_doc.txt"
        test_file.write_text("This is a test document about interest rate swaps and credit risk.")

        # Upload
        upload_fn = _get_tool(tools, "upload_document")
        result = json.loads(upload_fn(
            file_path=str(test_file),
            title="Test IRS Document",
            tags="research,risk",
        ))
        assert result.get("status") == "uploaded", f"Upload failed: {result}"
        assert result["title"] == "Test IRS Document"

        # List
        list_fn = _get_tool(tools, "list_documents")
        lr = json.loads(list_fn(limit=10))
        assert lr["count"] >= 1

    def test_search(self, store_server, media_store_fixture):
        ctx = _PlatformContext(alias="agent_test", user="agent_user", password="agent_pw")
        ctx._media_store_instance = media_store_fixture
        tools = create_document_tools(ctx)

        search_fn = _get_tool(tools, "search_documents")
        result = json.loads(search_fn(query="interest rate", mode="text", limit=5))
        assert "error" not in result


# ── Dashboard Agent E2E (real StreamingServer + StoreServer) ─────────


class TestDashboardAgentE2E:
    """Dashboard agent tools against real Deephaven + StoreServer."""

    def test_list_ticking_tables(self, streaming_server):
        ctx = _PlatformContext(alias="agent_test")
        tools = create_dashboard_tools(ctx)
        result = json.loads(_get_tool(tools, "list_ticking_tables")())
        assert "error" not in result
        assert "tables" in result

    def test_create_ticking_table(self, streaming_server):
        ctx = _PlatformContext(alias="agent_test")
        tools = create_dashboard_tools(ctx)
        schema = json.dumps({"symbol": "str", "price": "float", "volume": "int"})
        result = json.loads(_get_tool(tools, "create_ticking_table")(
            name="test_agent_tbl", schema_json=schema,
        ))
        assert result.get("status") == "created", f"Create failed: {result}"

    def test_create_reactive_model(self, codegen_env):
        ctx = _PlatformContext()
        tools = create_dashboard_tools(ctx)
        fields = json.dumps([{"name": "rm_symbol", "type": "str"}, {"name": "rm_price", "type": "float"}])
        result = json.loads(_get_tool(tools, "create_reactive_model")(
            name="TestRMModel", key_field="rm_symbol", fields_json=fields,
        ))
        assert result.get("status") == "created", f"Create failed: {result}"
        assert result["persistent"] is True

    def test_setup_store_bridge(self, store_server, codegen_env):
        ctx = _PlatformContext(alias="agent_test", user="agent_user", password="agent_pw")
        # Register a type so setup_store_bridge can find it
        create_fn = _get_tool(create_oltp_tools(ctx), "create_dataset")
        fields = json.dumps([{"name": "br_symbol", "type": "str"}, {"name": "br_qty", "type": "int"}])
        json.loads(create_fn(name="BridgeTest", fields_json=fields))

        tools = create_dashboard_tools(ctx)
        result = json.loads(_get_tool(tools, "setup_store_bridge")(type_name="BridgeTest"))
        assert "error" not in result
        assert "bridge_config" in result
        assert "agent_user" in result["bridge_config"]["code"]


# ── Query Agent E2E (cross-store) ────────────────────────────────────


class TestQueryAgentE2E:
    """Query agent tools — cross-store access."""

    def test_query_store(self, store_server, codegen_env):
        ctx = _PlatformContext(alias="agent_test", user="agent_user", password="agent_pw")
        # Create and seed a type
        oltp_tools = create_oltp_tools(ctx)
        create_fn = _get_tool(oltp_tools, "create_dataset")
        fields = json.dumps([{"name": "qs_name", "type": "str"}, {"name": "qs_val", "type": "float"}])
        json.loads(create_fn(name="QueryStoreTest", fields_json=fields))
        insert_fn = _get_tool(oltp_tools, "insert_records")
        json.loads(insert_fn(type_name="QueryStoreTest",
                             records_json=json.dumps([{"qs_name": "A", "qs_val": 1.0}])))

        # Now use query agent
        tools = create_query_tools(ctx)
        result = json.loads(_get_tool(tools, "query_store")(
            type_name="QueryStoreTest", limit=10,
        ))
        assert "error" not in result
        assert result["count"] >= 1

    def test_list_all_datasets(self, store_server, codegen_env):
        ctx = _PlatformContext(alias="agent_test", user="agent_user", password="agent_pw")
        # Register a type so oltp shows up
        oltp_tools = create_oltp_tools(ctx)
        create_fn = _get_tool(oltp_tools, "create_dataset")
        fields = json.dumps([{"name": "ld_x", "type": "int"}])
        json.loads(create_fn(name="ListDatasetTest", fields_json=fields))

        tools = create_query_tools(ctx)
        result = json.loads(_get_tool(tools, "list_all_datasets")())
        assert "oltp" in result
        assert len(result["oltp"]) >= 1

    def test_describe_dataset(self, store_server, codegen_env):
        ctx = _PlatformContext(alias="agent_test", user="agent_user", password="agent_pw")
        # Must have QueryStoreTest from above
        tools = create_query_tools(ctx)
        result = json.loads(_get_tool(tools, "describe_dataset")(name="QueryStoreTest"))
        assert "error" not in result

    def test_get_md_snapshot_via_query(self, market_data_server):
        ctx = _PlatformContext(alias="agent_test")
        tools = create_query_tools(ctx)
        result = json.loads(_get_tool(tools, "get_md_snapshot")(msg_type="equity"))
        assert "error" not in result

    def test_query_lakehouse_via_query(self, lakehouse_stack):
        ctx = _PlatformContext(alias="agent_test")
        tools = create_query_tools(ctx)
        result = json.loads(_get_tool(tools, "query_lakehouse")(
            sql="SELECT 1 as test_col",
        ))
        assert "error" not in result
        assert result["row_count"] == 1


# ── Eval Scoring (pure math) ─────────────────────────────────────────


class TestEvalScoring:

    def test_tool_selection(self):
        assert _score_tool_selection(
            AgentEvalCase(expected_tools=["create_dataset"]),
            {"actual_tools": ["create_dataset", "insert_records"]},
        ) == 1.0
        assert _score_tool_selection(
            AgentEvalCase(expected_tools=["create_dataset", "insert_records"]),
            {"actual_tools": ["list_storable_types"]},
        ) == 0.0
        assert _score_tool_selection(
            AgentEvalCase(expected_tools=["create_dataset", "insert_records"]),
            {"actual_tools": ["create_dataset"]},
        ) == 0.5

    def test_output_contains(self):
        assert _score_output_contains(
            AgentEvalCase(expected_output_contains=["symbol", "price"]),
            {"actual_output": "Created table with symbol and price fields"},
        ) == 1.0
        score = _score_output_contains(
            AgentEvalCase(expected_output_contains=["symbol", "price", "volume"]),
            {"actual_output": "Created symbol and price"},
        )
        assert abs(score - 2/3) < 0.01

    def test_schema_quality(self):
        assert _score_schema_quality(
            AgentEvalCase(expected_schema={"fields": ["symbol", "price"]}),
            {"created_schema": {"fields": [{"name": "symbol"}, {"name": "price"}]}},
        ) == 1.0

    def test_table_creation(self):
        assert _score_table_creation(
            AgentEvalCase(expected_tables=["fact_trades", "dim_instrument"]),
            {"created_tables": ["fact_trades", "dim_instrument"]},
        ) == 1.0

    def test_query_correctness(self):
        assert _score_query_correctness(
            AgentEvalCase(expected_result=42), {"query_result": 42},
        ) == 1.0
        assert _score_query_correctness(
            AgentEvalCase(expected_result=100.0), {"query_result": 95.0},
        ) == 0.95

    def test_naming_conventions(self):
        good = score_naming_conventions(AgentEvalCase(), {"created_schema": {
            "type_name": "Trade",
            "fields": [{"name": "order_id"}, {"name": "trade_price"}],
        }})
        bad = score_naming_conventions(AgentEvalCase(), {"created_schema": {
            "type_name": "trade",
            "fields": [{"name": "OrderId"}, {"name": "PRICE"}],
        }})
        assert good == 1.0
        assert bad < 0.5

    def test_sql_validity(self):
        case = AgentEvalCase()
        assert score_sql_validity(case, {"generated_sql": "SELECT * FROM trades"}) == 1.0
        assert score_sql_validity(case, {"generated_sql": "DELETE FROM trades"}) == 0.3

    def test_star_schema_design(self):
        good = score_star_schema_design(AgentEvalCase(), {"star_schema_design": {
            "fact_tables": [{"name": "fact_trades", "columns": [{"role": "measure", "type": "float"}]}],
            "dimension_tables": [{"name": "dim_instrument", "columns": [{"role": "attribute", "type": "str"}]}],
            "relationships": [{"fact": "fact_trades", "dimension": "dim_instrument", "join_key": "symbol"}],
        }})
        empty = score_star_schema_design(AgentEvalCase(), {
            "star_schema_design": {"fact_tables": [], "dimension_tables": [], "relationships": []}
        })
        assert good >= 0.8
        assert empty == 0.0


# ── Eval Data Integrity ──────────────────────────────────────────────


class TestEvalDatasets:

    def test_case_counts(self):
        assert len(OLTP_EVAL_CASES) >= 5
        assert len(LAKEHOUSE_EVAL_CASES) >= 4
        assert len(QUERY_EVAL_CASES) >= 4
        assert len(DATASCIENCE_EVAL_CASES) >= 3
        assert len(ALL_EVAL_CASES) >= 20

    def test_case_integrity(self):
        for case in ALL_EVAL_CASES:
            assert case.tags, f"Missing tags: {case.input[:40]}"
            assert case.difficulty in ("basic", "intermediate", "advanced")

    def test_rubrics(self):
        for rubric in [DATA_MODEL_RUBRIC, CURATION_QUALITY_RUBRIC,
                       STAR_SCHEMA_RUBRIC, METADATA_QUALITY_RUBRIC,
                       ANALYSIS_QUALITY_RUBRIC]:
            assert isinstance(rubric, str) and len(rubric) > 50

    def test_dimensions_and_phases(self):
        assert len(DEFAULT_DIMENSIONS) >= 7
        assert EvalPhase.TOOL_SELECTION.value < EvalPhase.END_TO_END.value
        evaluator = AgentEval(agents={}, max_phase=EvalPhase.TOOL_SELECTION)
        assert all(d.phase == EvalPhase.TOOL_SELECTION for d in evaluator.active_dimensions)


# ── PlatformAgents ────────────────────────────────────────────────────


class TestPlatformAgents:

    def test_agent_descriptions(self):
        assert len(_AGENT_DESCRIPTIONS) == 8
        for name in ["oltp", "lakehouse", "feed", "timeseries",
                      "document", "dashboard", "query", "quant"]:
            assert name in _AGENT_DESCRIPTIONS

    @requires_gemini
    def test_team_construction(self):
        team = PlatformAgents()
        assert len(team) == 8
        assert "oltp" in team

    @requires_gemini
    def test_team_subset(self):
        team = PlatformAgents(agents=["oltp", "lakehouse"])
        assert len(team) == 2
        assert "feed" not in team

    @requires_gemini
    def test_typed_properties(self):
        team = PlatformAgents(agents=["oltp", "quant"])
        assert team.oltp is not None
        assert team.quant is not None
