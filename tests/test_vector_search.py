"""
Integration tests for pgvector on embedded PostgreSQL.

Verifies that pgvector extension works with pgserver, embedding columns
can store vectors, and HNSW cosine similarity queries return correct results.

Requires: embedded PG via ObjectStoreServer (real pgserver, no mocks).
"""

import uuid
import math
import pytest

from store.server import ObjectStoreServer
from store.schema import provision_user
from media.models import bootstrap_search_schema, bootstrap_chunks_schema


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def pg_server():
    """Start embedded PG, provision user, bootstrap search schema with pgvector."""
    import tempfile
    server = ObjectStoreServer(data_dir=tempfile.mkdtemp(prefix="test_vector_search_"))
    server.start()

    # Provision user + bootstrap search schema BEFORE any test connections
    conn = server.admin_conn()
    provision_user(conn, "vec_user", "vec_pw")
    conn.close()

    conn = server.admin_conn()
    bootstrap_search_schema(conn, embedding_dim=768)
    conn.close()

    conn = server.admin_conn()
    bootstrap_chunks_schema(conn, embedding_dim=768)
    conn.close()

    yield server
    server.stop()


@pytest.fixture()
def db(pg_server):
    """Fresh admin connection per test — avoids stale transaction locks."""
    conn = pg_server.admin_conn()
    yield conn
    conn.close()


# ── Helper ────────────────────────────────────────────────────────────────


def _make_vector(dim: int, value: float = 0.0) -> list[float]:
    """Create a vector of given dimension filled with a value."""
    return [value] * dim


def _make_unit_vector(dim: int, hot_index: int) -> list[float]:
    """Create a unit vector with 1.0 at hot_index, 0.0 elsewhere."""
    vec = [0.0] * dim
    vec[hot_index] = 1.0
    return vec


# ── Tests ─────────────────────────────────────────────────────────────────


class TestPgvectorExtension:
    """Verify pgvector extension is installed and functional."""

    def test_pgvector_extension_installed(self, db):
        """pgvector extension should be installed during server bootstrap."""
        with db.cursor() as cur:
            cur.execute(
                "SELECT extversion FROM pg_extension WHERE extname = 'vector'"
            )
            row = cur.fetchone()
            assert row is not None, "pgvector extension not installed"
            version = row[0]
            assert version is not None
            major, minor = version.split(".")[:2]
            assert int(major) >= 0 and int(minor) >= 6, (
                f"Expected pgvector >= 0.6, got {version}"
            )

    def test_embedding_column_exists(self, db):
        """document_search should have an embedding vector(768) column."""
        with db.cursor() as cur:
            cur.execute("""
                SELECT column_name, udt_name
                FROM information_schema.columns
                WHERE table_name = 'document_search'
                  AND column_name = 'embedding'
            """)
            row = cur.fetchone()
            assert row is not None, "embedding column not found"
            assert row[1] == "vector", f"Expected vector type, got {row[1]}"

    def test_hnsw_index_exists(self, db):
        """HNSW index should exist on the embedding column."""
        with db.cursor() as cur:
            cur.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = 'document_search'
                  AND indexname = 'idx_doc_search_embedding'
            """)
            row = cur.fetchone()
            assert row is not None, "HNSW index not found"
            assert "hnsw" in row[1].lower(), f"Expected HNSW index, got: {row[1]}"
            assert "vector_cosine_ops" in row[1].lower(), (
                f"Expected cosine ops, got: {row[1]}"
            )


class TestVectorOperations:
    """Test vector insert, query, and similarity ordering."""

    def test_insert_and_query_vector(self, db):
        """Insert vectors and verify cosine similarity ordering."""
        dim = 768
        v1 = _make_unit_vector(dim, 0)
        v2 = [0.0] * dim
        v2[0] = 0.9
        v2[1] = 0.1
        v3 = _make_unit_vector(dim, 1)

        ids = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]

        with db.cursor() as cur:
            for eid, vec in zip(ids, [v1, v2, v3]):
                cur.execute("""
                    INSERT INTO document_search (entity_id, owner, embedding)
                    VALUES (%s, 'vec_user', %s::vector)
                    ON CONFLICT (entity_id) DO UPDATE SET embedding = EXCLUDED.embedding
                """, (str(eid), str(vec)))
        db.commit()

        # Query: find most similar to v1
        query_vec = str(v1)
        with db.cursor() as cur:
            cur.execute("""
                SELECT entity_id::text, embedding <=> %s::vector AS distance
                FROM document_search
                WHERE embedding IS NOT NULL
                  AND entity_id = ANY(%s::uuid[])
                ORDER BY distance ASC
                LIMIT 3
            """, (query_vec, [str(eid) for eid in ids]))
            rows = cur.fetchall()

        assert len(rows) == 3
        # v1 should be closest to itself (distance ≈ 0)
        assert rows[0][0] == str(ids[0])
        assert rows[0][1] < 0.01

        # v2 should be second (very similar)
        assert rows[1][0] == str(ids[1])
        assert rows[1][1] < 0.1

        # v3 should be furthest (orthogonal → cosine distance = 1)
        assert rows[2][0] == str(ids[2])
        assert rows[2][1] > 0.9

    def test_null_embedding_excluded(self, db):
        """Rows with NULL embedding should be excluded from vector queries."""
        null_id = uuid.uuid4()
        with_id = uuid.uuid4()
        dim = 768
        vec = _make_unit_vector(dim, 5)

        with db.cursor() as cur:
            cur.execute("""
                INSERT INTO document_search (entity_id, owner)
                VALUES (%s, 'vec_user')
                ON CONFLICT (entity_id) DO NOTHING
            """, (str(null_id),))
            cur.execute("""
                INSERT INTO document_search (entity_id, owner, embedding)
                VALUES (%s, 'vec_user', %s::vector)
                ON CONFLICT (entity_id) DO UPDATE SET embedding = EXCLUDED.embedding
            """, (str(with_id), str(vec)))
        db.commit()

        query_vec = str(vec)
        with db.cursor() as cur:
            cur.execute("""
                SELECT entity_id::text
                FROM document_search
                WHERE embedding IS NOT NULL
                  AND entity_id = ANY(%s::uuid[])
                ORDER BY embedding <=> %s::vector ASC
            """, ([str(null_id), str(with_id)], query_vec))
            rows = cur.fetchall()

        result_ids = [row[0] for row in rows]
        assert str(with_id) in result_ids
        assert str(null_id) not in result_ids

    def test_embedding_dimension_mismatch(self, db):
        """Inserting a vector with wrong dimension should raise an error."""
        bad_id = uuid.uuid4()
        wrong_dim_vec = [1.0] * 100  # 100-dim, but column is 768-dim

        with db.cursor() as cur:
            with pytest.raises(Exception) as exc_info:
                cur.execute("""
                    INSERT INTO document_search (entity_id, owner, embedding)
                    VALUES (%s, 'vec_user', %s::vector)
                """, (str(bad_id), str(wrong_dim_vec)))
        db.rollback()
        assert "dimension" in str(exc_info.value).lower() or "expected" in str(exc_info.value).lower()

    def test_mixed_text_and_vector_search(self, db):
        """Combine tsvector text search with vector similarity."""
        dim = 768
        doc_id = uuid.uuid4()
        vec = _make_unit_vector(dim, 42)

        with db.cursor() as cur:
            cur.execute("""
                INSERT INTO document_search
                    (entity_id, owner, title, tsv, embedding)
                VALUES (
                    %s, 'vec_user', 'quantum computing research',
                    to_tsvector('english', 'quantum computing research paper'),
                    %s::vector
                )
                ON CONFLICT (entity_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    tsv = EXCLUDED.tsv,
                    embedding = EXCLUDED.embedding
            """, (str(doc_id), str(vec)))
        db.commit()

        query_vec = str(vec)
        with db.cursor() as cur:
            cur.execute("""
                SELECT entity_id, title,
                       ts_rank(tsv, websearch_to_tsquery('english', %s)) AS text_rank,
                       embedding <=> %s::vector AS vector_distance
                FROM document_search
                WHERE tsv @@ websearch_to_tsquery('english', %s)
                  AND embedding IS NOT NULL
                  AND entity_id = %s
            """, ("quantum computing", query_vec, "quantum computing", str(doc_id)))
            rows = cur.fetchall()

        assert len(rows) == 1
        row = rows[0]
        assert str(row[0]) == str(doc_id)
        assert row[1] == "quantum computing research"
        assert row[2] > 0  # text_rank > 0 (text matched)
        assert row[3] < 0.01  # vector_distance ≈ 0 (exact match)

    def test_configurable_embedding_dim(self, pg_server):
        """bootstrap_search_schema with same dim is idempotent."""
        conn = pg_server.admin_conn()
        bootstrap_search_schema(conn, embedding_dim=768)
        conn.close()


class TestDocumentChunks:
    """Integration tests for the document_chunks table."""

    def test_chunks_table_exists(self, db):
        """document_chunks table should exist with correct columns."""
        with db.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'document_chunks'
                ORDER BY ordinal_position
            """)
            cols = [row[0] for row in cur.fetchall()]
        assert 'chunk_id' in cols
        assert 'entity_id' in cols
        assert 'chunk_index' in cols
        assert 'chunk_text' in cols
        assert 'embedding' in cols
        assert 'start_char' in cols
        assert 'end_char' in cols
        assert 'token_count' in cols

    def test_chunks_hnsw_index_exists(self, db):
        """HNSW index should exist on the chunks embedding column."""
        with db.cursor() as cur:
            cur.execute("""
                SELECT indexname, indexdef FROM pg_indexes
                WHERE tablename = 'document_chunks'
                  AND indexname = 'idx_chunks_embedding'
            """)
            row = cur.fetchone()
        assert row is not None, "HNSW index on chunks not found"
        assert 'hnsw' in row[1].lower()

    def test_insert_chunks_with_embedding(self, db):
        """Insert chunks for a document and query by vector similarity."""
        doc_id = uuid.uuid4()
        dim = 768

        # Insert parent document first
        with db.cursor() as cur:
            cur.execute("""
                INSERT INTO document_search (entity_id, owner, title)
                VALUES (%s, 'vec_user', 'test doc for chunks')
                ON CONFLICT (entity_id) DO NOTHING
            """, (str(doc_id),))
        db.commit()

        # Insert 3 chunks with different embeddings
        vecs = [
            _make_unit_vector(dim, 0),
            _make_unit_vector(dim, 1),
            _make_unit_vector(dim, 2),
        ]
        with db.cursor() as cur:
            for i, vec in enumerate(vecs):
                cur.execute("""
                    INSERT INTO document_chunks
                        (entity_id, chunk_index, chunk_text, start_char, end_char, token_count, embedding)
                    VALUES (%s, %s, %s, %s, %s, %s, %s::vector)
                """, (str(doc_id), i, f'chunk {i} text', i * 100, (i + 1) * 100, 50, str(vec)))
        db.commit()

        # Query: find chunk most similar to vec[0]
        query_vec = str(vecs[0])
        with db.cursor() as cur:
            cur.execute("""
                SELECT chunk_index, embedding <=> %s::vector AS distance
                FROM document_chunks
                WHERE entity_id = %s
                  AND embedding IS NOT NULL
                ORDER BY distance ASC
            """, (query_vec, str(doc_id)))
            rows = cur.fetchall()

        assert len(rows) == 3
        assert rows[0][0] == 0  # chunk 0 is closest to itself
        assert rows[0][1] < 0.01

    def test_cascade_delete(self, db):
        """Deleting from document_search should cascade to document_chunks."""
        doc_id = uuid.uuid4()
        dim = 768

        with db.cursor() as cur:
            cur.execute("""
                INSERT INTO document_search (entity_id, owner, title)
                VALUES (%s, 'vec_user', 'doc to delete')
            """, (str(doc_id),))
            cur.execute("""
                INSERT INTO document_chunks
                    (entity_id, chunk_index, chunk_text, embedding)
                VALUES (%s, 0, 'chunk text', %s::vector)
            """, (str(doc_id), str(_make_unit_vector(dim, 10))))
        db.commit()

        # Verify chunk exists
        with db.cursor() as cur:
            cur.execute("SELECT count(*) FROM document_chunks WHERE entity_id = %s", (str(doc_id),))
            assert cur.fetchone()[0] == 1

        # Delete parent document
        with db.cursor() as cur:
            cur.execute("DELETE FROM document_search WHERE entity_id = %s", (str(doc_id),))
        db.commit()

        # Verify chunk was cascade-deleted
        with db.cursor() as cur:
            cur.execute("SELECT count(*) FROM document_chunks WHERE entity_id = %s", (str(doc_id),))
            assert cur.fetchone()[0] == 0
