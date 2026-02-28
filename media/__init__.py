"""
Media — Unstructured Data Storage & Search
============================================
Upload, download, and search documents, images, audio, and video
with full-text search and bi-temporal audit trail.

Binary content stored in MinIO S3. Metadata stored as Document
Storable objects in PG with tsvector-indexed full-text search.

Usage::

    from media import MediaStore, Document

    ms = MediaStore(s3_endpoint="localhost:9002")

    # Upload
    doc = ms.upload("reports/q1.pdf", title="Q1 Report", tags=["research"])

    # Search
    results = ms.search("interest rate swap")

    # Download
    data = ms.download(doc)

    ms.close()
"""

from media.store import MediaStore
from media.models import Document

__all__ = [
    "MediaStore",
    "Document",
]
