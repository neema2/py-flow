"""
Integration tests for platform tools — real PG + S3 object store + Gemini.

Tests tool registry, built-in search tools, and LLM → tool execution loop.
"""

import os
import json
import pytest
import asyncio

from ai._tools import ToolRegistry, create_search_tools
from ai._types import Tool

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
requires_gemini = pytest.mark.skipif(not GEMINI_API_KEY, reason="GEMINI_API_KEY not set")


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def pg_server():
    from store.server import StoreServer
    from media.models import bootstrap_search_schema, bootstrap_chunks_schema

    import tempfile
    server = StoreServer(data_dir=tempfile.mkdtemp(prefix="test_tools_"))
    server.start()
    server.provision_user("tools_user", "tools_pw")

    conn = server.admin_conn()
    bootstrap_search_schema(conn, embedding_dim=768)
    conn.close()

    conn = server.admin_conn()
    bootstrap_chunks_schema(conn, embedding_dim=768)
    conn.close()

    yield server
    server.stop()


@pytest.fixture(scope="module")
def s3_server():
    import objectstore
    import tempfile
    loop = asyncio.new_event_loop()
    store = loop.run_until_complete(objectstore.configure(
        "minio",
        data_dir=tempfile.mkdtemp(prefix="test_tools_s3_"),
        api_port=9032, console_port=9033,
    ))
    yield store
    # atexit handles cleanup


@pytest.fixture(scope="module")
def store_conn(pg_server):
    from store.connection import connect
    info = pg_server.conn_info()
    conn = connect(user="tools_user", host=info["host"], port=info["port"],
                   dbname=info["dbname"], password="tools_pw")
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def media_store(s3_server, store_conn):
    if not GEMINI_API_KEY:
        pytest.skip("GEMINI_API_KEY not set")

    from media import MediaStore
    from ai._embeddings import GeminiEmbeddings

    embedder = GeminiEmbeddings(api_key=GEMINI_API_KEY, dimension=768)
    ms = MediaStore(
        s3_endpoint="localhost:9032",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
        s3_bucket="test-tools",
        embedding_provider=embedder,
    )

    # Upload some test documents
    ms.upload(b"Credit default swaps are derivatives used to transfer credit risk.",
              filename="cds.txt", title="Credit Default Swaps", tags=["finance"])
    ms.upload(b"Machine learning algorithms can detect patterns in large datasets.",
              filename="ml.txt", title="Machine Learning Intro", tags=["ml"])
    ms.upload(b"The Federal Reserve raised interest rates by 25 basis points.",
              filename="fed.txt", title="Fed Rate Decision", tags=["finance", "macro"])

    yield ms
    ms.close()


@pytest.fixture(scope="module")
def registry(media_store):
    reg = ToolRegistry()
    for tool in create_search_tools(media_store):
        reg.register(tool)
    return reg


# ── Registry tests ───────────────────────────────────────────────────────


@requires_gemini
class TestToolRegistry:

    def test_register_and_get(self):
        reg = ToolRegistry()
        tool = Tool(name="test", description="A test tool",
                    parameters={"type": "object", "properties": {}},
                    fn=lambda: "ok")
        reg.register(tool)
        assert reg.get("test") is tool

    def test_unknown_tool_raises(self):
        reg = ToolRegistry()
        with pytest.raises(KeyError, match="Unknown tool"):
            reg.get("nonexistent")

    def test_list_declarations(self, registry):
        decls = registry.list_declarations()
        assert len(decls) >= 3  # search, semantic, hybrid, list
        names = [d["name"] for d in decls]
        assert "search_documents" in names
        assert "semantic_search" in names
        assert "hybrid_search" in names

    def test_tool_names(self, registry):
        assert "search_documents" in registry.tool_names
        assert "list_documents" in registry.tool_names


# ── Tool execution tests ─────────────────────────────────────────────────


@requires_gemini
class TestToolExecution:

    def test_search_documents_tool(self, registry):
        result = registry.execute("search_documents", {"query": "credit default swap"})
        data = json.loads(result)
        assert len(data) > 0
        titles = [r["title"] for r in data]
        assert "Credit Default Swaps" in titles

    def test_semantic_search_tool(self, registry):
        result = registry.execute("semantic_search", {"query": "derivatives for risk transfer"})
        data = json.loads(result)
        assert len(data) > 0

    def test_hybrid_search_tool(self, registry):
        result = registry.execute("hybrid_search", {"query": "interest rates monetary policy"})
        data = json.loads(result)
        assert len(data) > 0

    def test_list_documents_tool(self, registry):
        result = registry.execute("list_documents", {})
        data = json.loads(result)
        assert len(data) >= 3

    def test_search_with_limit(self, registry):
        result = registry.execute("search_documents", {"query": "finance", "limit": 1})
        data = json.loads(result)
        assert len(data) <= 1


# ── LLM + Tool integration ──────────────────────────────────────────────


@requires_gemini
class TestLLMToolIntegration:

    def test_llm_uses_tool(self, registry):
        """LLM generates a tool call, registry executes, LLM responds."""
        from ai._llm import GeminiLLM
        from ai._types import Message

        llm = GeminiLLM(api_key=GEMINI_API_KEY)
        messages = [
            Message(role="system", content="You are a helpful assistant with access to document search tools. Use them to answer questions."),
            Message(role="user", content="Search for documents about credit default swaps."),
        ]

        # Step 1: LLM should call a search tool
        response = llm.generate(messages, tools=registry.list_declarations(), temperature=0.0)
        assert len(response.tool_calls) > 0
        tc = response.tool_calls[0]
        assert tc.name in registry.tool_names

        # Step 2: Execute tool
        tool_result = registry.execute(tc.name, tc.arguments)

        # Step 3: Send result back
        assistant_msg = Message(role="assistant", content=response.content, tool_calls=response.tool_calls)
        assistant_msg._raw_content = response._raw_content
        messages.append(assistant_msg)
        messages.append(Message(role="tool", content=tool_result, name=tc.name, tool_call_id=tc.id))

        # Step 4: LLM generates final answer
        final = llm.generate(messages, tools=registry.list_declarations(), temperature=0.0)
        assert final.content
        # Should mention CDS or credit
        lower = final.content.lower()
        assert "credit" in lower or "cds" in lower or "swap" in lower
