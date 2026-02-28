"""
Integration tests for the AI class — the public user-facing API.

Tests the clean API: AI(api_key=...) as the single entry point.
Requires GEMINI_API_KEY env var. Tests skip if not set.
"""

import os
import json
import pytest
import asyncio

from ai import AI, Message, LLMResponse, RAGResult, ExtractionResult, Tool

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
requires_gemini = pytest.mark.skipif(not GEMINI_API_KEY, reason="GEMINI_API_KEY not set")


@pytest.fixture(scope="module")
def ai_client():
    if not GEMINI_API_KEY:
        pytest.skip("GEMINI_API_KEY not set")
    return AI(api_key=GEMINI_API_KEY)


# ── Generation ───────────────────────────────────────────────────────────


@requires_gemini
class TestGeneration:

    def test_generate_string(self, ai_client):
        """String shorthand for single user message."""
        response = ai_client.generate("What is 2 + 2? Answer with just the number.")
        assert isinstance(response, LLMResponse)
        assert "4" in response.content

    def test_generate_messages(self, ai_client):
        """Full conversation with Message list."""
        response = ai_client.generate([
            Message(role="system", content="You are a pirate."),
            Message(role="user", content="Say hello."),
        ], temperature=0.5)
        assert response.content

    def test_stream_string(self, ai_client):
        """Streaming with string shorthand."""
        chunks = list(ai_client.stream("Count from 1 to 3."))
        assert len(chunks) > 0
        full = "".join(chunks)
        assert "1" in full and "3" in full


# ── Extraction ───────────────────────────────────────────────────────────


@requires_gemini
class TestExtraction:

    def test_extract(self, ai_client):
        """Extract structured data from text."""
        result = ai_client.extract(
            text="Apple Inc. (AAPL) closed at $178.50 on March 15, 2024.",
            schema={
                "type": "object",
                "properties": {
                    "ticker": {"type": "string"},
                    "price": {"type": "number"},
                },
                "required": ["ticker", "price"],
            },
        )
        assert isinstance(result, ExtractionResult)
        assert result.data["ticker"] == "AAPL"
        assert result.data["price"] == 178.50


# ── RAG ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def pg_server():
    from store.server import ObjectStoreServer
    from store.schema import provision_user
    from media.models import bootstrap_search_schema, bootstrap_chunks_schema

    import tempfile
    server = ObjectStoreServer(data_dir=tempfile.mkdtemp(prefix="test_ai_client_"))
    server.start()

    conn = server.admin_conn()
    provision_user(conn, "ai_user", "ai_pw")
    conn.close()

    conn = server.admin_conn()
    bootstrap_search_schema(conn, embedding_dim=768)
    conn.close()

    conn = server.admin_conn()
    bootstrap_chunks_schema(conn, embedding_dim=768)
    conn.close()

    yield server
    server.stop()


@pytest.fixture(scope="module")
def minio_manager():
    from lakehouse.services import MinIOManager
    import tempfile
    minio = MinIOManager(data_dir=tempfile.mkdtemp(prefix="test_ai_client_minio_"), api_port=9052, console_port=9053)
    loop = asyncio.new_event_loop()
    loop.run_until_complete(minio.start())
    yield minio
    loop.run_until_complete(minio.stop())
    loop.close()


@pytest.fixture(scope="module")
def store_conn(pg_server):
    from store.connection import connect
    info = pg_server.conn_info()
    conn = connect(user="ai_user", host=info["host"], port=info["port"],
                   dbname=info["dbname"], password="ai_pw")
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def media_store(ai_client, minio_manager, store_conn):
    from media import MediaStore

    ms = MediaStore(
        s3_endpoint="localhost:9052",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
        s3_bucket="test-ai-client",
        ai=ai_client,
    )

    ms.upload(
        b"Credit default swaps (CDS) are financial derivatives that transfer credit risk.",
        filename="cds.txt", title="Credit Default Swaps",
    )
    ms.upload(
        b"The Federal Reserve raised interest rates by 25 basis points to 5.50 percent.",
        filename="fed.txt", title="Fed Rate Decision",
    )

    yield ms
    ms.close()


@requires_gemini
class TestRAG:

    def test_ask(self, ai_client, media_store):
        """RAG: ask a question grounded in documents."""
        result = ai_client.ask("What are credit default swaps?", documents=media_store)
        assert isinstance(result, RAGResult)
        assert result.answer
        assert len(result.sources) > 0
        assert "credit" in result.answer.lower() or "cds" in result.answer.lower()

    def test_ask_requires_documents(self, ai_client):
        """ask() without documents raises ValueError."""
        with pytest.raises(ValueError, match="documents="):
            ai_client.ask("What is 2+2?")


# ── Tool calling ─────────────────────────────────────────────────────────


@requires_gemini
class TestToolCalling:

    def test_run_tool_loop(self, ai_client):
        """run_tool_loop handles tool → execute → respond."""
        weather_tool = {
            "name": "get_weather",
            "description": "Get weather for a location.",
            "parameters": {
                "type": "object",
                "properties": {"location": {"type": "string"}},
                "required": ["location"],
            },
        }

        def execute_tool(name, arguments):
            return json.dumps({"temperature": "22°C", "condition": "sunny"})

        response = ai_client.run_tool_loop(
            "What's the weather in Tokyo?",
            tools=[weather_tool],
            execute_tool=execute_tool,
        )
        assert response.content
        assert "22" in response.content or "sunny" in response.content.lower()

    def test_search_tools(self, ai_client, media_store):
        """search_tools returns tool declarations."""
        tools = ai_client.search_tools(media_store)
        assert len(tools) >= 3
        names = [t["name"] for t in tools]
        assert "search_documents" in names
        assert "hybrid_search" in names


# ── Errors ───────────────────────────────────────────────────────────────


@requires_gemini
class TestErrors:

    def test_no_api_key(self):
        """Missing API key raises ValueError."""
        old = os.environ.pop("GEMINI_API_KEY", None)
        try:
            with pytest.raises(ValueError, match="API key required"):
                AI()
        finally:
            if old:
                os.environ["GEMINI_API_KEY"] = old

    def test_unknown_provider(self):
        """Unknown provider raises ValueError."""
        with pytest.raises(ValueError, match="Unknown provider"):
            AI(api_key="fake", provider="openai")


# ── Import hygiene ───────────────────────────────────────────────────────


@requires_gemini
class TestImportHygiene:

    def test_only_public_symbols(self):
        """ai.__all__ should have exactly 7 symbols."""
        import ai
        assert set(ai.__all__) == {
            "AI", "Message", "LLMResponse", "ToolCall",
            "RAGResult", "ExtractionResult", "Tool",
        }

    def test_no_provider_in_dir(self):
        """Provider classes should not appear in dir(ai)."""
        import ai
        public = [x for x in dir(ai) if not x.startswith("_")]
        assert "GeminiLLM" not in public
        assert "GeminiEmbeddings" not in public
        assert "EmbeddingProvider" not in public
        assert "LLMClient" not in public
        assert "RAGPipeline" not in public
        assert "ToolRegistry" not in public
