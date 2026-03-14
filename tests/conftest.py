"""
Session-scoped test infrastructure — started once, shared across all test files.

Mirrors ``demo_platform_agents.py``: each server is self-contained with its
own embedded PG / MinIO / QuestDB on non-conflicting ports.  No shared backends.

Start order::

    1. StreamingServer   — JVM must start before test collection
    2. store_server      — StoreServer (embedded PG, auto-port via pgserver)
    3. media_server      — MediaServer (MinIO S3 on 9102/9103)
    4. tsdb_server       — TsdbServer  (QuestDB on 9200/9209/8922)
    5. market_data_server — MarketDataServer (FastAPI on 8765)
    6. lakehouse_server  — LakehouseServer (own PG + Lakekeeper + MinIO)
    7. scheduler_server  — SchedulerServer (own PG + WorkflowEngine)

Requires a ``.env`` file at the project root with::

    GEMINI_API_KEY=your-key-here
"""

import os
import tempfile
import sys
from pathlib import Path

import pytest

# ── Port isolation ───────────────────────────────────────────────────────────
# Set PORT_OFFSET env var to run multiple test suites in parallel.
# run_demo_tests.sh sets PORT_OFFSET=100 so demo tests don't collide with main.
_PORT_OFFSET = int(os.environ.get("PORT_OFFSET", "0"))

# Auto-increment offset for xdist parallel workers (gw0, gw1, etc.)
_xdist_worker = os.environ.get("PYTEST_XDIST_WORKER", "")
if _xdist_worker.startswith("gw"):
    try:
        _PORT_OFFSET += int(_xdist_worker[2:]) * 100
    except ValueError:
        pass

def _free_port(port: int) -> None:
    """Aggressively kill any process holding this port to survive SIGTERM-induced test cascades."""
    import subprocess
    try:
        subprocess.run(
            f"lsof -ti :{port} | xargs -r kill -9",
            shell=True,
            stderr=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL
        )
    except Exception:
        pass

# Ensure local source takes precedence over installed packages
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ── Load .env ────────────────────────────────────────────────────────────────
_env_file = Path(__file__).resolve().parent.parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if key and key not in os.environ:
            os.environ[key] = value

# Warn if key is missing — AI-dependent tests will fail naturally,
# but non-AI tests (store, scheduler, packaging, etc.) can still run.
import warnings as _warnings

if not os.environ.get("GEMINI_API_KEY"):
    _warnings.warn(
        "GEMINI_API_KEY not set — AI-dependent tests will be skipped or fail.\n"
        "Create a .env file: echo 'GEMINI_API_KEY=your-key-here' > .env",
        stacklevel=1,
    )


# ── 1. Streaming (Deephaven JVM) ─────────────────────────────────────────
# The JVM MUST start before any test file that imports deephaven is collected.
# But we skip it entirely when running tests that don't need it (e.g. unit tests).

def _any_test_needs_streaming() -> bool:
    """Check if any test file on the command line actually needs Deephaven."""
    args = sys.argv[1:]
    # No specific files or directory on command line → running full suite → need streaming
    if not args:
        return True
    
    # If any file or selector is on the command line, check it specifically.
    # VSCode passes lists like ['-p', 'vscode_pytest', '--rootdir=...', 'tests/test_foo.py::test_bar']
    for arg in args:
        if arg.startswith("-"):
            continue
        # Strip pytest selectors ("::") to get the pure file/path
        path_str = arg.split("::")[0]
        p = Path(path_str)
        
        # If any directory (e.g. 'tests/') is selected, start it.
        if p.is_dir():
            return True
            
        # If any file is selected, check its contents for 'streaming' or 'deephaven'.
        if p.is_file() and p.suffix == ".py":
            try:
                src = p.read_text()
                if "deephaven" in src or "streaming" in src:
                    return True
            except Exception:
                pass
                
    # Fallback to current directory check: if any .py file in tests/ needs it, return True
    # for VSCode discovery.
    return False

_streaming = None

def _get_streaming_server():
    """Lazily initialize the StreamingServer instance."""
    global _streaming
    if _streaming is None:
        port = 10000 + _PORT_OFFSET
        from streaming.admin import StreamingServer
        _streaming = StreamingServer(port=port, max_heap="1g")
        try:
            _streaming.start()
        except Exception as e:
            print(f"ERROR: StreamingServer failed to start: {e}. Some tests may fail.")

# Start streaming server early if any test needs it, so decorators can connect during collection.
if _any_test_needs_streaming():
    _get_streaming_server()


@pytest.fixture(scope="session")
def streaming_server():
    """Expose the already-running StreamingServer as a fixture."""
    if not _any_test_needs_streaming():
        pytest.skip("StreamingServer not started (no tests need it)")
    return _get_streaming_server()


# ── 2. StoreServer (embedded PostgreSQL) ─────────────────────────────────

@pytest.fixture(scope="session")
def store_server(tmp_path_factory):
    """Self-contained embedded PostgreSQL via pgserver.

    Uses Unix domain sockets — auto-assigned, no TCP port collisions.
    Each test file provisions its own users for isolation.
    """
    from store.admin import StoreServer

    tmp_dir = str(tmp_path_factory.mktemp("test_store"))
    srv = StoreServer(data_dir=tmp_dir, admin_password="test_admin_pw")
    srv.start()
    srv.register_alias("test")
    yield srv
    srv.stop()


# ── 3. MediaServer (MinIO S3) ───────────────────────────────────────────

@pytest.fixture(scope="session")
def media_server():
    """Self-contained MinIO for document/media storage.

    Each test file uses a different bucket for isolation.
    """
    import asyncio

    from media.admin import MediaServer

    api_port = 9102 + _PORT_OFFSET
    console_port = 9103 + _PORT_OFFSET
    _free_port(api_port)
    _free_port(console_port)

    srv = MediaServer(
        data_dir=tempfile.mkdtemp(prefix="test_media_"),
        api_port=api_port,
        console_port=console_port,
        bucket="test-media",
    )
    asyncio.run(srv.start())
    srv.register_alias("test")
    yield srv
    asyncio.run(srv.stop())


# ── 4. TsdbServer (QuestDB) ─────────────────────────────────────────────

@pytest.fixture(scope="session")
def tsdb_server():
    """Self-contained QuestDB for timeseries storage."""
    import asyncio

    from timeseries.admin import TsdbServer

    http_port = 9200 + _PORT_OFFSET
    ilp_port = 9209 + _PORT_OFFSET
    pg_port = 8922 + _PORT_OFFSET
    _free_port(http_port)
    _free_port(ilp_port)
    _free_port(pg_port)

    srv = TsdbServer(
        data_dir=tempfile.mkdtemp(prefix="test_tsdb_"),
        http_port=http_port,
        ilp_port=ilp_port,
        pg_port=pg_port,
    )
    asyncio.run(srv.start())
    srv.register_alias("test")
    yield srv
    asyncio.run(srv.stop())


# ── 5. MarketDataServer (FastAPI + SimulatorFeed) ────────────────────────

@pytest.fixture(scope="session")
def market_data_server(tsdb_server):
    """MarketDataServer on port 8765 — depends on tsdb_server for tick storage."""
    import asyncio
    import time

    import httpx
    from marketdata.admin import MarketDataServer

    port = 8765 + _PORT_OFFSET
    _free_port(port)
    srv = MarketDataServer(port=port, host="127.0.0.1")
    asyncio.run(srv.start())
    srv.register_alias("test")

    # Wait for simulator to produce tick data
    url = srv.url
    ready = False
    for _attempt in range(45):
        time.sleep(1)
        try:
            resp = httpx.get(f"{url}/md/latest/equity", timeout=2)
            if resp.status_code == 200:
                data = resp.json()
                if data and len(data) >= 8:
                    ready = True
                    break
        except Exception:
            continue

    if not ready:
        asyncio.run(srv.stop())
        pytest.fail("Market data server did not produce tick data within 45s")

    yield srv
    asyncio.run(srv.stop())


# ── 6. Lakehouse (Lakekeeper + Iceberg + own MinIO + own PG) ────────────

@pytest.fixture(scope="session")
def lakehouse_server():
    """Self-contained Lakehouse stack on non-default ports.

    Has its own embedded PG (port 5490) + Lakekeeper + MinIO.
    Uses /tmp to keep Unix socket path < 103 bytes (macOS limit).
    Includes RLS policies for ``sales_data`` table (test_lakehouse_rls_integration).
    """
    import asyncio

    from lakehouse.admin import LakehouseServer, RLSPolicy

    pg_port = 5490 + _PORT_OFFSET
    lakekeeper_port = 8183 + _PORT_OFFSET
    s3_api_port = 9004 + _PORT_OFFSET
    s3_console_port = 9005 + _PORT_OFFSET
    _free_port(pg_port)
    _free_port(lakekeeper_port)
    _free_port(s3_api_port)
    _free_port(s3_console_port)

    tmp_dir = tempfile.mkdtemp(prefix="tst_lh_", dir="/tmp")
    srv = LakehouseServer(
        data_dir=tmp_dir,
        pg_port=pg_port,
        lakekeeper_port=lakekeeper_port,
        s3_api_port=s3_api_port,
        s3_console_port=s3_console_port,
        rls_policies=[
            RLSPolicy(
                table_name="sales_data",
                acl_table="sales_acl",
                join_column="row_id",
                user_column="user_token",
            ),
        ],
        rls_users={"alice-token": "alice", "bob-token": "bob"},
    )
    asyncio.run(srv.start())
    srv.register_alias("test")
    yield srv
    asyncio.run(srv.stop())


# ── 7. Workflow (own embedded PG + DBOS engine) ─────────────────────────

@pytest.fixture(scope="session")
def workflow_server(tmp_path_factory):
    """Self-contained WorkflowServer — own embedded PG for durable workflows.

    pgserver auto-assigns Unix socket, so no port collision.
    """
    from workflow.admin import WorkflowServer

    tmp_dir = str(tmp_path_factory.mktemp("test_workflow"))
    srv = WorkflowServer(data_dir=tmp_dir)
    srv.start()
    srv.register_alias("test")
    yield srv
    srv.stop()


# ── 8. Scheduler (own embedded PG + WorkflowEngine) ─────────────────────

@pytest.fixture(scope="session")
def scheduler_server(tmp_path_factory):
    """Self-contained SchedulerServer — own embedded PG + workflow engine.

    pgserver auto-assigns Unix socket, so no port collision with store_server.
    """
    from scheduler.admin import SchedulerServer

    tmp_dir = str(tmp_path_factory.mktemp("test_scheduler"))
    srv = SchedulerServer(data_dir=tmp_dir)
    srv.start(poll_interval=0)
    yield srv
    srv.stop()
