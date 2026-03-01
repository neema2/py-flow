"""
Integration tests for RAG pipeline — real PG + S3 object store + Gemini.

Tests the full retrieve → augment → generate flow.
"""

import os
import pytest
import asyncio

from ai._rag import RAGPipeline
from ai._types import RAGResult

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
requires_gemini = pytest.mark.skipif(not GEMINI_API_KEY, reason="GEMINI_API_KEY not set")


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def pg_server():
    from store.server import StoreServer
    from media.models import bootstrap_search_schema, bootstrap_chunks_schema

    import tempfile
    server = StoreServer(data_dir=tempfile.mkdtemp(prefix="test_rag_"))
    server.start()
    server.provision_user("rag_user", "rag_pw")

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
        data_dir=tempfile.mkdtemp(prefix="test_rag_s3_"),
        api_port=9042, console_port=9043,
    ))
    yield store
    # atexit handles cleanup


@pytest.fixture(scope="module")
def store_conn(pg_server):
    from store.connection import connect
    info = pg_server.conn_info()
    conn = connect(user="rag_user", host=info["host"], port=info["port"],
                   dbname=info["dbname"], password="rag_pw")
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
        s3_endpoint="localhost:9042",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
        s3_bucket="test-rag",
        embedding_provider=embedder,
    )

    # Upload documents with real content
    ms.upload(
        b"Credit default swaps (CDS) are financial derivatives that allow investors "
        b"to transfer credit risk. The buyer pays periodic premiums to the seller. "
        b"In case of a credit event such as default, the seller compensates the buyer. "
        b"CDS spreads widen during periods of financial stress.",
        filename="cds_explained.txt",
        title="Credit Default Swaps Explained",
        tags=["finance", "derivatives"],
    )
    ms.upload(
        b"The Federal Reserve raised the federal funds rate by 25 basis points "
        b"to a range of 5.25-5.50 percent. Chair Powell indicated that future "
        b"rate decisions would be data-dependent. Inflation remains above the "
        b"2 percent target but has shown signs of cooling.",
        filename="fed_decision.txt",
        title="Federal Reserve Rate Decision July 2024",
        tags=["finance", "macro"],
    )
    ms.upload(
        b"Convolutional neural networks (CNNs) are deep learning models specialized "
        b"for processing grid-like data such as images. They use convolutional layers "
        b"to automatically learn spatial hierarchies of features. Popular architectures "
        b"include ResNet, VGG, and Inception.",
        filename="cnn_overview.txt",
        title="CNN Architecture Overview",
        tags=["ml", "deep-learning"],
    )

    yield ms
    ms.close()


@pytest.fixture(scope="module")
def rag(media_store):
    from ai._llm import GeminiLLM
    llm = GeminiLLM(api_key=GEMINI_API_KEY)
    return RAGPipeline(llm=llm, media_store=media_store, search_mode="hybrid")


# ── Tests ─────────────────────────────────────────────────────────────────


@requires_gemini
class TestRAGPipeline:

    def test_rag_basic(self, rag):
        """Ask a question and get a grounded answer with sources."""
        result = rag.ask("What are credit default swaps?")
        assert isinstance(result, RAGResult)
        assert result.answer
        assert len(result.sources) > 0
        assert "credit" in result.answer.lower() or "cds" in result.answer.lower()

    def test_rag_cites_source(self, rag):
        """Answer should reference the source document."""
        result = rag.ask("What did the Federal Reserve do with interest rates?")
        assert result.answer
        lower = result.answer.lower()
        # Should mention rate/fed/basis points from the document
        assert "rate" in lower or "fed" in lower or "basis" in lower

    def test_rag_no_knowledge(self, rag):
        """Question not in docs — model should indicate insufficient information."""
        result = rag.ask("What is the current price of Bitcoin?")
        assert result.answer
        # Should NOT fabricate an answer
        lower = result.answer.lower()
        assert ("don't have" in lower or "not" in lower or "no " in lower
                or "cannot" in lower or "unavailable" in lower
                or "available documents" in lower)

    def test_rag_custom_system_prompt(self, rag):
        """Custom system prompt shapes behavior."""
        result = rag.ask(
            "Explain CNNs",
            system_prompt="You are a technical writer. Answer in exactly one sentence based on the provided documents.",
        )
        assert result.answer
        # Should be relatively short (one sentence)
        assert len(result.answer) < 500

    def test_rag_sources_populated(self, rag):
        """Sources should contain document metadata."""
        result = rag.ask("Tell me about neural networks")
        assert len(result.sources) > 0
        src = result.sources[0]
        assert "title" in src or "entity_id" in src

    def test_rag_semantic_mode(self, media_store):
        """RAG with semantic-only search mode."""
        from ai._llm import GeminiLLM
        llm = GeminiLLM(api_key=GEMINI_API_KEY)
        rag = RAGPipeline(llm=llm, media_store=media_store, search_mode="semantic")
        result = rag.ask("How do derivatives transfer risk?")
        assert result.answer
        assert len(result.sources) > 0
