"""
Integration tests for embed-on-upload pipeline.

Requires: embedded PG + S3-compatible object store + GEMINI_API_KEY.
Tests the full upload → chunk → embed → store → query flow.
"""

import os
import pytest
import asyncio

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
requires_gemini = pytest.mark.skipif(not GEMINI_API_KEY, reason="GEMINI_API_KEY not set")


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def pg_server():
    """Start embedded PG with search + chunks schema."""
    from store.server import StoreServer
    from media.models import bootstrap_search_schema, bootstrap_chunks_schema

    import tempfile
    server = StoreServer(data_dir=tempfile.mkdtemp(prefix="test_embed_upload_"))
    server.start()
    server.provision_user("emb_user", "emb_pw")

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
    """Start S3-compatible object store."""
    import objectstore
    import tempfile
    loop = asyncio.new_event_loop()
    store = loop.run_until_complete(objectstore.configure(
        "minio",
        data_dir=tempfile.mkdtemp(prefix="test_embed_s3_"),
        api_port=9022,
        console_port=9023,
    ))
    yield store
    # atexit handles cleanup


@pytest.fixture(scope="module")
def store_conn(pg_server):
    """Connect as emb_user."""
    from store.connection import connect
    info = pg_server.conn_info()
    conn = connect(user="emb_user", host=info["host"], port=info["port"],
                   dbname=info["dbname"], password="emb_pw")
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def media_store_no_embed(s3_server, store_conn):
    """MediaStore WITHOUT embedding provider."""
    from media import MediaStore
    ms = MediaStore(
        s3_endpoint="localhost:9022",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
        s3_bucket="test-embed",
    )
    yield ms
    ms.close()


@pytest.fixture(scope="module")
def media_store_with_embed(s3_server, store_conn):
    """MediaStore WITH Gemini embedding provider."""
    if not GEMINI_API_KEY:
        pytest.skip("GEMINI_API_KEY not set")

    from media import MediaStore
    from ai._embeddings import GeminiEmbeddings

    embedder = GeminiEmbeddings(api_key=GEMINI_API_KEY, dimension=768)
    ms = MediaStore(
        s3_endpoint="localhost:9022",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
        s3_bucket="test-embed",
        embedding_provider=embedder,
    )
    yield ms
    ms.close()


@pytest.fixture(scope="module")
def admin_db(pg_server):
    """Fresh admin connection for verification queries."""
    conn = pg_server.admin_conn()
    yield conn
    conn.close()


# ── Tests ─────────────────────────────────────────────────────────────────


class TestUploadWithoutEmbedder:
    """Upload without embedding provider — backward compatible."""

    def test_upload_no_embed(self, media_store_no_embed, admin_db):
        """Upload works without embedding provider. No chunks created."""
        doc = media_store_no_embed.upload(
            b"Plain text document about machine learning algorithms.",
            filename="no_embed.txt",
            title="No Embed Doc",
        )
        assert doc._store_entity_id is not None
        assert doc.has_text

        # No chunks should exist
        with admin_db.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM document_chunks WHERE entity_id = %s",
                (str(doc._store_entity_id),),
            )
            assert cur.fetchone()[0] == 0


@requires_gemini
class TestUploadWithEmbedder:
    """Upload with Gemini embedding provider — full pipeline."""

    def test_upload_creates_chunks(self, media_store_with_embed, admin_db):
        """Upload with embedder creates chunks in document_chunks."""
        text = (
            "Neural networks are computational models inspired by the human brain. "
            "They consist of layers of interconnected nodes that process information. "
            "Deep learning uses multiple layers to learn hierarchical representations. "
            "Convolutional neural networks are particularly effective for image recognition. "
            "Recurrent neural networks excel at sequential data processing. "
            "Transformer architectures have revolutionized natural language processing."
        )
        doc = media_store_with_embed.upload(
            text.encode(),
            filename="neural_nets.txt",
            title="Neural Network Overview",
            tags=["ml", "deep-learning"],
        )

        assert doc.has_text

        # Chunks should exist
        with admin_db.cursor() as cur:
            cur.execute(
                "SELECT chunk_index, token_count, embedding IS NOT NULL as has_emb "
                "FROM document_chunks WHERE entity_id = %s ORDER BY chunk_index",
                (str(doc._store_entity_id),),
            )
            rows = cur.fetchall()

        assert len(rows) >= 1, "Expected at least 1 chunk"
        for row in rows:
            assert row[2] is True, f"Chunk {row[0]} missing embedding"

    def test_document_embedding_stored(self, media_store_with_embed, admin_db):
        """Upload stores whole-document embedding in document_search."""
        doc = media_store_with_embed.upload(
            b"Quantum computing uses qubits for parallel computation.",
            filename="quantum.txt",
            title="Quantum Computing Intro",
        )

        with admin_db.cursor() as cur:
            cur.execute(
                "SELECT embedding IS NOT NULL FROM document_search WHERE entity_id = %s",
                (str(doc._store_entity_id),),
            )
            row = cur.fetchone()

        assert row is not None
        assert row[0] is True, "Document embedding not stored"

    def test_chunk_embedding_dimension(self, media_store_with_embed, admin_db):
        """Chunk embeddings should be 768-dimensional."""
        doc = media_store_with_embed.upload(
            b"Reinforcement learning trains agents through reward signals.",
            filename="rl.txt",
            title="RL Basics",
        )

        with admin_db.cursor() as cur:
            cur.execute("""
                SELECT vector_dims(embedding)
                FROM document_chunks
                WHERE entity_id = %s AND embedding IS NOT NULL
                LIMIT 1
            """, (str(doc._store_entity_id),))
            row = cur.fetchone()

        assert row is not None
        assert row[0] == 768

    def test_cosine_search_finds_document(self, media_store_with_embed, admin_db):
        """Cosine similarity query on chunks finds the uploaded document."""
        from ai._embeddings import GeminiEmbeddings

        doc = media_store_with_embed.upload(
            b"Black-Scholes option pricing model for European call and put options.",
            filename="options.txt",
            title="Options Pricing",
            tags=["finance", "derivatives"],
        )

        # Embed a search query
        embedder = GeminiEmbeddings(api_key=GEMINI_API_KEY, dimension=768)
        query_vec = embedder.embed_query("option pricing Black-Scholes")

        # Search chunks by cosine distance
        with admin_db.cursor() as cur:
            cur.execute("""
                SELECT dc.entity_id::text, dc.chunk_text,
                       dc.embedding <=> %s::vector AS distance
                FROM document_chunks dc
                WHERE dc.embedding IS NOT NULL
                ORDER BY distance ASC
                LIMIT 5
            """, (str(query_vec),))
            rows = cur.fetchall()

        assert len(rows) > 0
        # The options pricing doc should be in the top results
        found_ids = [row[0] for row in rows]
        assert str(doc._store_entity_id) in found_ids, (
            f"Expected {doc._store_entity_id} in results, got {found_ids}"
        )

    def test_binary_file_no_chunks(self, media_store_with_embed, admin_db):
        """Binary files (no extracted text) should not create chunks."""
        fake_png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 50
        doc = media_store_with_embed.upload(
            fake_png,
            filename="chart.png",
            title="Chart Image",
        )

        with admin_db.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM document_chunks WHERE entity_id = %s",
                (str(doc._store_entity_id),),
            )
            assert cur.fetchone()[0] == 0


@requires_gemini
class TestSemanticSearch:
    """Integration tests for semantic_search() on MediaStore."""

    def test_semantic_search_returns_results(self, media_store_with_embed):
        """semantic_search returns results ranked by cosine distance."""
        # Upload a doc first
        media_store_with_embed.upload(
            b"Gradient descent is an optimization algorithm used in machine learning.",
            filename="gradient.txt",
            title="Gradient Descent",
            tags=["ml"],
        )

        results = media_store_with_embed.semantic_search("optimization algorithms")
        assert len(results) > 0
        assert "distance" in results[0]
        assert "chunk_text" in results[0]
        assert "title" in results[0]
        # Distance should be ordered ascending (most similar first)
        distances = [r["distance"] for r in results]
        assert distances == sorted(distances)

    def test_semantic_search_relevance(self, media_store_with_embed):
        """Relevant documents rank higher than irrelevant ones."""
        media_store_with_embed.upload(
            b"Photosynthesis converts sunlight into chemical energy in plants.",
            filename="biology.txt",
            title="Photosynthesis",
            tags=["biology"],
        )
        media_store_with_embed.upload(
            b"Support vector machines classify data by finding optimal hyperplanes.",
            filename="svm.txt",
            title="Support Vector Machines",
            tags=["ml"],
        )

        results = media_store_with_embed.semantic_search("machine learning classification")
        assert len(results) >= 2

        # SVM doc should rank higher than photosynthesis for ML query
        titles = [r["title"] for r in results]
        svm_idx = next((i for i, t in enumerate(titles) if t == "Support Vector Machines"), None)
        bio_idx = next((i for i, t in enumerate(titles) if t == "Photosynthesis"), None)

        if svm_idx is not None and bio_idx is not None:
            assert svm_idx < bio_idx, (
                f"SVM (idx={svm_idx}) should rank higher than Photosynthesis (idx={bio_idx})"
            )

    def test_semantic_search_no_embedder_raises(self, media_store_no_embed):
        """semantic_search without embedding provider raises ValueError."""
        import pytest
        with pytest.raises(ValueError, match="embedding_provider"):
            media_store_no_embed.semantic_search("test query")


@requires_gemini
class TestHybridSearch:
    """Integration tests for hybrid_search() with RRF."""

    def test_hybrid_search_returns_results(self, media_store_with_embed):
        """hybrid_search returns results with rrf_score, text_rank, vector_distance."""
        media_store_with_embed.upload(
            b"Linear regression is a fundamental statistical method for prediction.",
            filename="regression.txt",
            title="Linear Regression",
            tags=["statistics"],
        )

        results = media_store_with_embed.hybrid_search("linear regression prediction")
        assert len(results) > 0
        r = results[0]
        assert "rrf_score" in r
        assert "text_rank" in r
        assert "vector_distance" in r
        assert "title" in r
        assert "entity_id" in r
        # RRF scores should be descending
        scores = [x["rrf_score"] for x in results]
        assert scores == sorted(scores, reverse=True)

    def test_hybrid_boosts_dual_match(self, media_store_with_embed):
        """Documents matching BOTH text and semantic should rank highest."""
        # Upload two docs — one matches keywords, one is semantically similar
        media_store_with_embed.upload(
            b"Decision trees split data based on feature thresholds for classification.",
            filename="decision_trees.txt",
            title="Decision Trees",
            tags=["ml"],
        )

        # This doc has the exact keywords "decision trees" AND is semantically relevant
        results = media_store_with_embed.hybrid_search("decision trees classification")
        assert len(results) > 0
        # The decision trees doc should be near the top
        titles = [r["title"] for r in results[:5]]
        assert "Decision Trees" in titles

    def test_hybrid_no_embedder_raises(self, media_store_no_embed):
        """hybrid_search without embedding provider raises ValueError."""
        with pytest.raises(ValueError, match="embedding_provider"):
            media_store_no_embed.hybrid_search("test query")
