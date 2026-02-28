"""
Session-scoped test infrastructure — started once, shared across all test files.

This is the equivalent of a GitHub Actions ``services:`` block.
Heavy servers (DH JVM, MarketDataServer) start once for the entire session.
Each test file publishes its own tables via the public client API.
"""

import os
import pytest

# ── Streaming (Deephaven JVM) ─────────────────────────────────────────────
# The JVM MUST start before any test file that imports deephaven is collected.
# Module-level code in conftest.py runs before test collection.
from streaming.admin import StreamingServer

_streaming = StreamingServer(port=10000, max_heap="512m")
_streaming.start()


@pytest.fixture(scope="session")
def streaming_server():
    """Expose the already-running StreamingServer as a fixture."""
    return _streaming


# ── Market Data Server (with QuestDB) ────────────────────────────────────

@pytest.fixture(scope="session")
def market_data_server(tmp_path_factory):
    """Start MarketDataServer with its own QuestDB on test-specific ports."""
    import asyncio
    import time
    import httpx
    from marketdata.admin import MarketDataServer

    tmp_dir = str(tmp_path_factory.mktemp("test_md_questdb"))
    os.environ["QUESTDB_DATA_DIR"] = tmp_dir
    os.environ["QUESTDB_HTTP_PORT"] = "19200"
    os.environ["QUESTDB_ILP_PORT"] = "19209"
    os.environ["QUESTDB_PG_PORT"] = "18922"

    server = MarketDataServer(port=18080, host="127.0.0.1")
    asyncio.run(server.start())

    # Wait for QuestDB to accumulate tick data from the simulator
    url = server.url
    ready = False
    for attempt in range(45):
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
        asyncio.run(server.stop())
        for key in ("QUESTDB_DATA_DIR", "QUESTDB_HTTP_PORT", "QUESTDB_ILP_PORT", "QUESTDB_PG_PORT"):
            os.environ.pop(key, None)
        pytest.fail("Market data server did not produce tick data within 45s")

    yield server

    asyncio.run(server.stop())
    for key in ("QUESTDB_DATA_DIR", "QUESTDB_HTTP_PORT", "QUESTDB_ILP_PORT", "QUESTDB_PG_PORT"):
        os.environ.pop(key, None)
