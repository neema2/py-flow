"""
MediaStore — User-facing API for unstructured data.

Upload, download, search, and manage documents, images, audio, and video
with full-text search and bi-temporal audit trail.

All binary content is stored in MinIO S3. Metadata is stored as Document
Storable objects in PG with tsvector-indexed full-text search.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional, Union

from media.s3 import S3Client
from media.models import (
    Document,
    bootstrap_search_schema,
    upsert_search_index,
    delete_search_index,
    search_documents,
    semantic_search_documents,
    hybrid_search_documents,
    upsert_document_chunks,
    update_document_embedding,
)
from media.extraction import extract_text, detect_content_type
from media.chunking import chunk_text

logger = logging.getLogger(__name__)


class MediaStore:
    """
    User-facing API for unstructured data storage and search.

    Usage::

        from media import MediaStore

        ms = MediaStore(s3_endpoint="localhost:9002")

        # Upload
        doc = ms.upload("reports/q1.pdf", title="Q1 Report", tags=["research"])

        # Search
        results = ms.search("interest rate swap")

        # Download
        data = ms.download(doc)

        ms.close()
    """

    def __init__(
        self,
        s3_endpoint: str = "localhost:9002",
        s3_access_key: str = "minioadmin",
        s3_secret_key: str = "minioadmin",
        s3_bucket: str = "media",
        s3_secure: bool = False,
        embedding_provider=None,
    ):
        self._s3 = S3Client(
            endpoint=s3_endpoint,
            access_key=s3_access_key,
            secret_key=s3_secret_key,
            bucket=s3_bucket,
            secure=s3_secure,
        )
        self._s3.ensure_bucket()
        self._bucket = s3_bucket
        self._embedder = embedding_provider

    # ── Upload ────────────────────────────────────────────────────────────

    def upload(
        self,
        source: Union[str, Path, bytes],
        *,
        filename: str = "",
        title: str = "",
        content_type: str = "",
        tags: Optional[list[str]] = None,
        metadata: Optional[dict] = None,
        extract: bool = True,
    ) -> Document:
        """
        Upload a file and create a Document with extracted text.

        Args:
            source: File path (str/Path) or raw bytes.
            filename: Override filename (required if source is bytes).
            title: Document title (defaults to filename).
            content_type: MIME type (auto-detected from filename if empty).
            tags: Optional list of tags.
            metadata: Optional arbitrary key-value metadata.
            extract: Whether to extract text for search (default: True).

        Returns:
            Saved Document Storable with S3 key and extracted text.
        """
        # Read file data
        if isinstance(source, (str, Path)):
            path = Path(source)
            if not path.exists():
                raise FileNotFoundError(f"File not found: {source}")
            data = path.read_bytes()
            if not filename:
                filename = path.name
        elif isinstance(source, bytes):
            data = source
            if not filename:
                raise ValueError("filename is required when source is bytes")
        else:
            raise TypeError(f"source must be str, Path, or bytes, got {type(source).__name__}")

        # Detect content type
        if not content_type:
            content_type = detect_content_type(filename)

        if not title:
            title = filename

        # Create Document (not saved yet — we need entity_id for S3 key)
        doc = Document(
            title=title,
            filename=filename,
            content_type=content_type,
            size=len(data),
            s3_bucket=self._bucket,
            tags=tags or [],
            metadata=metadata or {},
        )

        # Extract text
        if extract:
            extracted = extract_text(data, content_type, filename)
            if extracted:
                doc.extracted_text = extracted
                logger.info("Extracted %d chars from %s", len(extracted), filename)

        # Save to PG first to get entity_id
        doc.save()

        # Build S3 key and upload
        s3_key = f"media/{doc._store_entity_id}/{filename}"
        self._s3.upload(s3_key, data, content_type)
        doc.s3_key = s3_key
        doc.save()  # update with S3 key

        # Update search index
        self._update_search_index(doc)

        # Chunk and embed (if embedding provider configured)
        self._embed_document(doc)

        logger.info("Uploaded %s (%d bytes, %s) → %s",
                     filename, len(data), content_type, s3_key)
        return doc

    # ── Download ──────────────────────────────────────────────────────────

    def download(self, doc: Union[Document, str]) -> bytes:
        """
        Download file content from S3.

        Args:
            doc: Document object or entity_id string.

        Returns:
            Raw bytes of the file.
        """
        if isinstance(doc, str):
            doc = Document.find(doc)
            if doc is None:
                raise ValueError(f"Document not found: {doc}")

        if not doc.s3_key:
            raise ValueError(f"Document {doc._store_entity_id} has no S3 key")

        return self._s3.download(doc.s3_key)

    def download_to(self, doc: Union[Document, str], path: Union[str, Path]) -> Path:
        """
        Download file content to a local path.

        Args:
            doc: Document object or entity_id string.
            path: Local file path to write to.

        Returns:
            Path to the written file.
        """
        data = self.download(doc)
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path

    # ── Search ────────────────────────────────────────────────────────────

    def search(
        self,
        query: str,
        content_type: Optional[str] = None,
        tags: Optional[list[str]] = None,
        limit: int = 50,
    ) -> list[dict]:
        """
        Full-text search over documents.

        Uses PG tsvector with weighted ranking:
          - Title (weight A — highest)
          - Filename + tags (weight B)
          - Extracted text (weight C)

        Args:
            query: Search query (natural language — PG websearch syntax).
            content_type: Optional MIME type filter.
            tags: Optional tag filter (all tags must match).
            limit: Max results (default: 50).

        Returns:
            List of dicts with entity_id, title, filename, content_type, tags, rank.
        """
        from store.connection import get_connection
        conn = get_connection()
        return search_documents(conn.conn, query, content_type, tags, limit)

    def semantic_search(
        self,
        query: str,
        limit: int = 10,
    ) -> list[dict]:
        """
        Semantic search — embed query and find most similar document chunks.

        Uses the embedding provider to convert the query to a vector,
        then searches document_chunks by cosine distance.

        Args:
            query: Natural language search query.
            limit: Max results (default: 10).

        Returns:
            List of dicts with entity_id, title, filename, content_type, tags,
            chunk_index, chunk_text, distance (cosine distance, lower = more similar).

        Raises:
            ValueError: If no embedding provider is configured.
        """
        if not self._embedder:
            raise ValueError(
                "semantic_search requires an embedding_provider. "
                "Pass embedding_provider= to MediaStore constructor."
            )

        query_embedding = self._embedder.embed_query(query)

        from store.connection import get_connection
        conn = get_connection()
        return semantic_search_documents(conn.conn, query_embedding, limit)

    def hybrid_search(
        self,
        query: str,
        limit: int = 10,
        k: int = 60,
        text_weight: float = 1.0,
        semantic_weight: float = 1.0,
    ) -> list[dict]:
        """
        Hybrid search — combines full-text and semantic search with RRF.

        Reciprocal Rank Fusion merges text search (keyword matching) and
        semantic search (embedding similarity) for best-of-both-worlds retrieval.

        Args:
            query: Natural language search query.
            limit: Max results (default: 10).
            k: RRF constant (default: 60). Higher = more even blending.
            text_weight: Weight for text search leg (default: 1.0).
            semantic_weight: Weight for semantic search leg (default: 1.0).

        Returns:
            List of dicts with entity_id, title, filename, content_type, tags,
            chunk_text, rrf_score, text_rank, vector_distance.
            Sorted by rrf_score descending (best first).

        Raises:
            ValueError: If no embedding provider is configured.
        """
        if not self._embedder:
            raise ValueError(
                "hybrid_search requires an embedding_provider. "
                "Pass embedding_provider= to MediaStore constructor."
            )

        query_embedding = self._embedder.embed_query(query)

        from store.connection import get_connection
        conn = get_connection()
        return hybrid_search_documents(
            conn.conn, query, query_embedding,
            limit=limit, k=k,
            text_weight=text_weight, semantic_weight=semantic_weight,
        )

    # ── List ──────────────────────────────────────────────────────────────

    def list(
        self,
        content_type: Optional[str] = None,
        tags: Optional[list[str]] = None,
        limit: int = 100,
    ) -> list[Document]:
        """
        List documents, optionally filtered by content_type and/or tags.

        Uses Storable.query() for pagination and RLS-enforced access control.
        """
        filters = {}
        if content_type:
            filters["content_type"] = content_type

        result = Document.query(filters=filters, limit=limit)
        docs = list(result)

        # Filter by tags in-memory (Storable.query doesn't support array contains)
        if tags:
            docs = [d for d in docs if all(t in d.tags for t in tags)]

        return docs

    # ── Delete ────────────────────────────────────────────────────────────

    def delete(self, doc: Union[Document, str]) -> None:
        """
        Soft-delete a document (Storable semantics).

        The S3 object is NOT deleted — it's retained for audit trail.
        The search index entry is removed.
        """
        if isinstance(doc, str):
            doc = Document.find(doc)
            if doc is None:
                raise ValueError(f"Document not found: {doc}")

        # Remove from search index
        try:
            from store.connection import get_connection
            conn = get_connection()
            delete_search_index(conn.conn, str(doc._store_entity_id))
        except Exception as e:
            logger.warning("Failed to remove search index: %s", e)

        # Soft-delete the Storable
        doc.delete()
        logger.info("Deleted document %s (%s)", doc._store_entity_id, doc.filename)

    # ── Internal ──────────────────────────────────────────────────────────

    def _update_search_index(self, doc: Document) -> None:
        """Update the full-text search index for a document."""
        try:
            from store.connection import get_connection
            conn = get_connection()
            upsert_search_index(
                conn.conn,
                entity_id=str(doc._store_entity_id),
                owner=doc._store_owner or conn.user,
                readers=getattr(doc, '_store_readers', []) or [],
                writers=getattr(doc, '_store_writers', []) or [],
                title=doc.title,
                filename=doc.filename,
                content_type=doc.content_type,
                tags=doc.tags,
                extracted_text=doc.extracted_text,
            )
        except Exception as e:
            logger.warning("Failed to update search index for %s: %s",
                           doc._store_entity_id, e)

    def _embed_document(self, doc: Document) -> None:
        """Chunk text and generate embeddings for a document."""
        if not self._embedder:
            return
        if not doc.extracted_text:
            return

        try:
            # Chunk the extracted text
            chunks = chunk_text(doc.extracted_text)
            if not chunks:
                return

            # Embed all chunks in one batch
            chunk_texts = [c.text for c in chunks]
            embeddings = self._embedder.embed(chunk_texts)

            # Store chunks + embeddings in document_chunks table
            from store.connection import get_connection
            conn = get_connection()
            entity_id = str(doc._store_entity_id)
            upsert_document_chunks(conn.conn, entity_id, chunks, embeddings)

            # Store whole-document embedding (title + first chunk)
            doc_text = f"{doc.title}. {chunks[0].text}" if doc.title else chunks[0].text
            doc_embedding = self._embedder.embed_query(doc_text)
            update_document_embedding(conn.conn, entity_id, doc_embedding)

            logger.info("Embedded %s: %d chunks → %d-dim vectors",
                         doc.filename, len(chunks), self._embedder.dimension)
        except Exception as e:
            logger.warning("Failed to embed document %s: %s",
                           doc._store_entity_id, e)

    def close(self) -> None:
        """Clean up resources."""
        logger.info("MediaStore closed")
