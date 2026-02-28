"""
Document Model — Storable type for unstructured data metadata.

Documents are stored as:
  - Binary content → MinIO S3 (media bucket)
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
from typing import Optional

from store.base import Storable

logger = logging.getLogger(__name__)


@dataclass
class Document(Storable):
    """
    Metadata for an unstructured data file stored in S3.

    The actual binary content lives in MinIO at s3://{s3_bucket}/{s3_key}.
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


def bootstrap_search_schema(admin_conn) -> None:
    """
    Create the document_search table with tsvector + GIN index.

    This table is maintained alongside object_events and provides
    fast full-text search over extracted document text.

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


def upsert_search_index(conn, entity_id: str, owner: str,
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


def delete_search_index(conn, entity_id: str) -> None:
    """Remove a document from the search index."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM document_search WHERE entity_id = %s", (entity_id,))
    conn.commit()


def search_documents(conn, query: str, content_type: Optional[str] = None,
                      tags: Optional[list] = None, limit: int = 50) -> list[dict]:
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
    params = [query]

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
    all_params = [query] + params

    with conn.cursor() as cur:
        cur.execute(sql, all_params)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return [dict(zip(columns, row)) for row in rows]
