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
)
from media.extraction import extract_text, detect_content_type

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

    def close(self) -> None:
        """Clean up resources."""
        logger.info("MediaStore closed")
