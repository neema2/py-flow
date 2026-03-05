"""
Document Model — Storable type for unstructured data metadata.

Documents are stored as:
  - Binary content → S3-compatible storage (media bucket)
  - Metadata → PG object_events (via Storable)
  - Search index → PG document_search (tsvector + GIN)

The Document class inherits all Storable features:
  - Bi-temporal audit trail
  - RLS (owner/readers/writers)
  - Event sourcing (CREATED/UPDATED/DELETED)
  - Sync to Iceberg via SyncEngine
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from store.base import Storable

if TYPE_CHECKING:
    from store._types import Connection

logger = logging.getLogger(__name__)


@dataclass
class Document(Storable):
    """
    Metadata for an unstructured data file stored in S3.

    The actual binary content lives in S3 at s3://{s3_bucket}/{s3_key}.
    This Storable holds all searchable metadata and extracted text.
    """
    title: str = ""
    filename: str = ""
    content_type: str = ""
    size: int = 0
    s3_bucket: str = "media"
    s3_key: str = ""
    tags: list = field(default_factory=list)
    extracted_text: str = ""
    metadata: dict = field(default_factory=dict)

    @property
    def is_pdf(self) -> bool:
        return self.content_type == "application/pdf"

    @property
    def is_image(self) -> bool:
        return self.content_type.startswith("image/")

    @property
    def is_audio(self) -> bool:
        return self.content_type.startswith("audio/")

    @property
    def is_video(self) -> bool:
        return self.content_type.startswith("video/")

    @property
    def is_text(self) -> bool:
        return self.content_type.startswith("text/")

    @property
    def has_text(self) -> bool:
        return bool(self.extracted_text)


# ── PG Schema for Full-Text Search ──────────────────────────────────────


def bootstrap_search_schema(admin_conn: Connection, embedding_dim: int = 768) -> None:
    """
    Create the document_search table with tsvector + GIN index and pgvector embedding.

    This table is maintained alongside object_events and provides
    fast full-text search and vector similarity search over documents.

    Args:
        admin_conn: Admin PG connection.
        embedding_dim: Embedding vector dimension (default 768 for Gemini text-embedding-004).

    Idempotent — safe to call multiple times.
    """
    admin_conn.autocommit = True
    with admin_conn.cursor() as cur:
        # Full-text search table — owner/readers/writers for RLS (mirrors object_events)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS document_search (
                entity_id    UUID PRIMARY KEY,
                owner        TEXT NOT NULL DEFAULT current_user,
                readers      TEXT[] NOT NULL DEFAULT '{}',
                writers      TEXT[] NOT NULL DEFAULT '{}',
                title        TEXT NOT NULL DEFAULT '',
                filename     TEXT NOT NULL DEFAULT '',
                content_type TEXT NOT NULL DEFAULT '',
                tags         TEXT[] NOT NULL DEFAULT '{}',
                tsv          tsvector NOT NULL DEFAULT ''::tsvector,
                updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)

        # GIN index on tsvector for fast full-text search
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_doc_search_tsv
                ON document_search USING GIN (tsv);
        """)

        # Index on content_type for filtering
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_doc_search_type
                ON document_search (content_type);
        """)

        # GIN index on tags for tag-based filtering
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_doc_search_tags
                ON document_search USING GIN (tags);
        """)

        # ── pgvector: embedding column + HNSW index ──────────────────
        cur.execute(f"""
            ALTER TABLE document_search
                ADD COLUMN IF NOT EXISTS embedding vector({embedding_dim});
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_doc_search_embedding
                ON document_search USING hnsw (embedding vector_cosine_ops)
                WITH (m = 16, ef_construction = 64);
        """)

        # ── RLS: mirror object_events visibility ─────────────────────
        cur.execute("ALTER TABLE document_search ENABLE ROW LEVEL SECURITY;")
        cur.execute("ALTER TABLE document_search FORCE ROW LEVEL SECURITY;")

        for policy in ["ds_admin_all", "ds_user_select", "ds_user_insert", "ds_user_update", "ds_user_delete"]:
            cur.execute(f"DROP POLICY IF EXISTS {policy} ON document_search;")

        # Admin: full access
        cur.execute("""
            CREATE POLICY ds_admin_all ON document_search
                FOR ALL TO app_admin
                USING (true) WITH CHECK (true);
        """)

        # User SELECT: owner, or listed in readers/writers
        cur.execute("""
            CREATE POLICY ds_user_select ON document_search
                FOR SELECT TO app_user
                USING (
                    owner = current_user
                    OR current_user = ANY(readers)
                    OR current_user = ANY(writers)
                );
        """)

        # User INSERT: own docs only
        cur.execute("""
            CREATE POLICY ds_user_insert ON document_search
                FOR INSERT TO app_user
                WITH CHECK (owner = current_user);
        """)

        # User UPDATE: owner or writer
        cur.execute("""
            CREATE POLICY ds_user_update ON document_search
                FOR UPDATE TO app_user
                USING (
                    owner = current_user
                    OR current_user = ANY(writers)
                )
                WITH CHECK (
                    owner = current_user
                    OR current_user = ANY(writers)
                );
        """)

        # User DELETE: owner only
        cur.execute("""
            CREATE POLICY ds_user_delete ON document_search
                FOR DELETE TO app_user
                USING (owner = current_user);
        """)

        # Grant table access (RLS policies control what's visible)
        cur.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON document_search TO app_user;")

        logger.info("document_search table and indexes created")
    admin_conn.autocommit = False


def bootstrap_chunks_schema(admin_conn: Connection, embedding_dim: int = 768) -> None:
    """
    Create the document_chunks table for per-chunk embeddings.

    Each document can have multiple chunks, each with its own embedding vector.
    Chunks reference document_search via entity_id with CASCADE delete.

    Args:
        admin_conn: Admin PG connection.
        embedding_dim: Embedding vector dimension (default 768 for Gemini text-embedding-004).

    Idempotent — safe to call multiple times.
    """
    admin_conn.autocommit = True
    with admin_conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS document_chunks (
                chunk_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                entity_id    UUID NOT NULL REFERENCES document_search(entity_id) ON DELETE CASCADE,
                chunk_index  INTEGER NOT NULL,
                chunk_text   TEXT NOT NULL,
                start_char   INTEGER NOT NULL DEFAULT 0,
                end_char     INTEGER NOT NULL DEFAULT 0,
                token_count  INTEGER NOT NULL DEFAULT 0,
                embedding    vector({embedding_dim}),
                UNIQUE (entity_id, chunk_index)
            );
        """)

        # HNSW index for vector similarity search on chunks
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_chunks_embedding
                ON document_chunks USING hnsw (embedding vector_cosine_ops)
                WITH (m = 16, ef_construction = 64);
        """)

        # Lookup chunks by document
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_chunks_entity
                ON document_chunks (entity_id);
        """)

        # ── RLS: visibility inherits from parent document_search row ──
        cur.execute("ALTER TABLE document_chunks ENABLE ROW LEVEL SECURITY;")
        cur.execute("ALTER TABLE document_chunks FORCE ROW LEVEL SECURITY;")

        for policy in ["dc_admin_all", "dc_user_select", "dc_user_insert", "dc_user_update", "dc_user_delete"]:
            cur.execute(f"DROP POLICY IF EXISTS {policy} ON document_chunks;")

        # Admin: full access
        cur.execute("""
            CREATE POLICY dc_admin_all ON document_chunks
                FOR ALL TO app_admin
                USING (true) WITH CHECK (true);
        """)

        # User SELECT: can see chunks if they can see the parent document
        cur.execute("""
            CREATE POLICY dc_user_select ON document_chunks
                FOR SELECT TO app_user
                USING (
                    EXISTS (
                        SELECT 1 FROM document_search ds
                        WHERE ds.entity_id = document_chunks.entity_id
                          AND (ds.owner = current_user
                               OR current_user = ANY(ds.readers)
                               OR current_user = ANY(ds.writers))
                    )
                );
        """)

        # User INSERT: can insert chunks for docs they own
        cur.execute("""
            CREATE POLICY dc_user_insert ON document_chunks
                FOR INSERT TO app_user
                WITH CHECK (
                    EXISTS (
                        SELECT 1 FROM document_search ds
                        WHERE ds.entity_id = document_chunks.entity_id
                          AND ds.owner = current_user
                    )
                );
        """)

        # User UPDATE: can update chunks for docs they own or can write
        cur.execute("""
            CREATE POLICY dc_user_update ON document_chunks
                FOR UPDATE TO app_user
                USING (
                    EXISTS (
                        SELECT 1 FROM document_search ds
                        WHERE ds.entity_id = document_chunks.entity_id
                          AND (ds.owner = current_user
                               OR current_user = ANY(ds.writers))
                    )
                )
                WITH CHECK (
                    EXISTS (
                        SELECT 1 FROM document_search ds
                        WHERE ds.entity_id = document_chunks.entity_id
                          AND (ds.owner = current_user
                               OR current_user = ANY(ds.writers))
                    )
                );
        """)

        # User DELETE: can delete chunks for docs they own
        cur.execute("""
            CREATE POLICY dc_user_delete ON document_chunks
                FOR DELETE TO app_user
                USING (
                    EXISTS (
                        SELECT 1 FROM document_search ds
                        WHERE ds.entity_id = document_chunks.entity_id
                          AND ds.owner = current_user
                    )
                );
        """)

        # Grant table access (RLS policies control what's visible)
        cur.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON document_chunks TO app_user;")

        logger.info("document_chunks table and indexes created")
    admin_conn.autocommit = False


def upsert_search_index(conn: Connection, entity_id: str, owner: str,
                         readers: list, writers: list,
                         title: str, filename: str,
                         content_type: str, tags: list, extracted_text: str) -> None:
    """
    Insert or update the full-text search index for a document.

    The tsvector is built from title (weight A), filename (weight B),
    tags (weight B), and extracted_text (weight C).
    owner/readers/writers are copied from the Document Storable for RLS.
    """
    tags_text = " ".join(tags) if tags else ""

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO document_search
                (entity_id, owner, readers, writers, title, filename, content_type, tags, tsv, updated_at)
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                setweight(to_tsvector('english', coalesce(%s, '')), 'A') ||
                setweight(to_tsvector('english', coalesce(%s, '')), 'B') ||
                setweight(to_tsvector('english', coalesce(%s, '')), 'B') ||
                setweight(to_tsvector('english', coalesce(%s, '')), 'C'),
                now()
            )
            ON CONFLICT (entity_id) DO UPDATE SET
                owner = EXCLUDED.owner,
                readers = EXCLUDED.readers,
                writers = EXCLUDED.writers,
                title = EXCLUDED.title,
                filename = EXCLUDED.filename,
                content_type = EXCLUDED.content_type,
                tags = EXCLUDED.tags,
                tsv = EXCLUDED.tsv,
                updated_at = now()
        """, (entity_id, owner, readers or [], writers or [],
              title, filename, content_type, tags,
              title, filename, tags_text, extracted_text))
    conn.commit()


def delete_search_index(conn: Connection, entity_id: str) -> None:
    """Remove a document from the search index."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM document_search WHERE entity_id = %s", (entity_id,))
    conn.commit()


def search_documents(conn: Connection, query: str, content_type: str | None = None,
                      tags: list | None = None, limit: int = 50) -> list[dict]:
    """
    Full-text search over documents.

    Args:
        conn: PG connection.
        query: Search query string (PG tsquery syntax supported).
        content_type: Optional MIME type filter.
        tags: Optional tag filter (AND — all tags must match).
        limit: Max results.

    Returns:
        List of dicts with entity_id, title, filename, content_type, tags, rank.
    """
    conditions = ["tsv @@ websearch_to_tsquery('english', %s)"]
    params: list[Any] = [query]

    if content_type:
        conditions.append("content_type = %s")
        params.append(content_type)

    if tags:
        conditions.append("tags @> %s")
        params.append(tags)

    params.append(limit)

    where = " AND ".join(conditions)
    sql = f"""
        SELECT entity_id, title, filename, content_type, tags,
               ts_rank(tsv, websearch_to_tsquery('english', %s)) AS rank
        FROM document_search
        WHERE {where}
        ORDER BY rank DESC
        LIMIT %s
    """
    # First param is for ts_rank, then the where params, then limit
    all_params = [query, *params]

    with conn.cursor() as cur:
        cur.execute(sql, all_params)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return [dict(zip(columns, row, strict=False)) for row in rows]


def upsert_document_chunks(conn: Connection, entity_id: str, chunks: list, embeddings: list) -> None:
    """
    Replace all chunks for a document with new chunks and embeddings.

    Args:
        conn: PG connection.
        entity_id: Document entity_id.
        chunks: List of TextChunk objects from media.chunking.
        embeddings: List of embedding vectors (list[float]), one per chunk.
    """
    with conn.cursor() as cur:
        # Delete existing chunks (re-upload replaces all)
        cur.execute("DELETE FROM document_chunks WHERE entity_id = %s", (entity_id,))

        # Insert new chunks with embeddings
        for chunk, embedding in zip(chunks, embeddings, strict=False):
            cur.execute("""
                INSERT INTO document_chunks
                    (entity_id, chunk_index, chunk_text, start_char, end_char,
                     token_count, embedding)
                VALUES (%s, %s, %s, %s, %s, %s, %s::vector)
            """, (
                entity_id,
                chunk.chunk_index,
                chunk.text,
                chunk.start_char,
                chunk.end_char,
                chunk.token_count,
                str(embedding),
            ))
    conn.commit()


def update_document_embedding(conn: Connection, entity_id: str, embedding: list) -> None:
    """
    Update the whole-document embedding in document_search.

    Args:
        conn: PG connection.
        entity_id: Document entity_id.
        embedding: Embedding vector (list[float]).
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE document_search
            SET embedding = %s::vector
            WHERE entity_id = %s
        """, (str(embedding), entity_id))
    conn.commit()


def semantic_search_documents(conn: Connection, query_embedding: list, limit: int = 10) -> list[dict]:
    """
    Search document chunks by cosine similarity to a query embedding.

    Joins document_chunks with document_search to return chunk text
    alongside parent document metadata.

    Args:
        conn: PG connection.
        query_embedding: Query embedding vector (list[float]).
        limit: Max results (default: 10).

    Returns:
        List of dicts with entity_id, title, filename, content_type, tags,
        chunk_index, chunk_text, distance.
    """
    query_vec = str(query_embedding)
    sql = """
        SELECT ds.entity_id, ds.title, ds.filename, ds.content_type, ds.tags,
               dc.chunk_index, dc.chunk_text,
               dc.embedding <=> %s::vector AS distance
        FROM document_chunks dc
        JOIN document_search ds ON ds.entity_id = dc.entity_id
        WHERE dc.embedding IS NOT NULL
        ORDER BY distance ASC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (query_vec, limit))
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return [dict(zip(columns, row, strict=False)) for row in rows]


def hybrid_search_documents(
    conn: Connection,
    query: str,
    query_embedding: list,
    limit: int = 10,
    k: int = 60,
    text_weight: float = 1.0,
    semantic_weight: float = 1.0,
) -> list[dict]:
    """
    Hybrid search combining full-text (tsvector) and semantic (pgvector) results
    using Reciprocal Rank Fusion (RRF).

    RRF_score(doc) = text_weight / (k + text_rank) + semantic_weight / (k + semantic_rank)

    Args:
        conn: PG connection.
        query: Text search query string.
        query_embedding: Query embedding vector (list[float]).
        limit: Max results to return (default: 10).
        k: RRF constant (default: 60). Higher = more even blending.
        text_weight: Weight for full-text search leg (default: 1.0).
        semantic_weight: Weight for semantic search leg (default: 1.0).

    Returns:
        List of dicts with entity_id, title, filename, content_type, tags,
        chunk_text, rrf_score, text_rank, vector_distance.
        Sorted by rrf_score descending (best first).
    """
    # Fetch candidates from both search legs (2× limit for enough overlap)
    candidate_limit = limit * 3

    # Full-text search (document-level)
    text_results = search_documents(conn, query, limit=candidate_limit)

    # Semantic search (chunk-level)
    semantic_results = semantic_search_documents(conn, query_embedding, limit=candidate_limit)

    # Build RRF scores keyed by entity_id
    # For text results: use entity_id as key
    # For semantic results: use entity_id (may have multiple chunks — take best)
    rrf_scores = {}  # entity_id → {rrf_score, text_rank, vector_distance, metadata}

    # Text leg
    for rank_idx, result in enumerate(text_results):
        eid = str(result["entity_id"])
        rrf_contribution = text_weight / (k + rank_idx + 1)
        if eid not in rrf_scores:
            rrf_scores[eid] = {
                "entity_id": eid,
                "title": result.get("title", ""),
                "filename": result.get("filename", ""),
                "content_type": result.get("content_type", ""),
                "tags": result.get("tags", []),
                "chunk_text": "",
                "rrf_score": 0.0,
                "text_rank": result.get("rank", 0.0),
                "vector_distance": None,
            }
        rrf_scores[eid]["rrf_score"] += rrf_contribution
        rrf_scores[eid]["text_rank"] = result.get("rank", 0.0)

    # Semantic leg
    for rank_idx, result in enumerate(semantic_results):
        eid = str(result["entity_id"])
        rrf_contribution = semantic_weight / (k + rank_idx + 1)
        if eid not in rrf_scores:
            rrf_scores[eid] = {
                "entity_id": eid,
                "title": result.get("title", ""),
                "filename": result.get("filename", ""),
                "content_type": result.get("content_type", ""),
                "tags": result.get("tags", []),
                "chunk_text": result.get("chunk_text", ""),
                "rrf_score": 0.0,
                "text_rank": None,
                "vector_distance": result.get("distance"),
            }
        rrf_scores[eid]["rrf_score"] += rrf_contribution
        # Keep best (smallest) vector distance and best chunk text
        if rrf_scores[eid]["vector_distance"] is None or (
            result.get("distance") is not None
            and result["distance"] < rrf_scores[eid]["vector_distance"]
        ):
            rrf_scores[eid]["vector_distance"] = result.get("distance")
            rrf_scores[eid]["chunk_text"] = result.get("chunk_text", "")

    # Sort by RRF score descending
    merged = sorted(rrf_scores.values(), key=lambda x: x["rrf_score"], reverse=True)
    return merged[:limit]
