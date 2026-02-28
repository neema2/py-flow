"""
Media columns — columns for unstructured data (Document Storable).

Covers file metadata, S3 storage references, and extracted text.
"""

from store.columns import REGISTRY

# ── File metadata ─────────────────────────────────────────────────

REGISTRY.define("filename", str,
    description="Original filename of uploaded file",
    semantic_type="identifier",
    role="dimension",
    synonyms=["file_name", "name"],
)

REGISTRY.define("content_type", str,
    description="MIME content type (e.g., application/pdf, image/png)",
    semantic_type="label",
    role="dimension",
    synonyms=["mime_type", "media_type"],
)

REGISTRY.define("size", int,
    description="File size in bytes",
    semantic_type="count",
    role="measure",
    unit="bytes",
)

# ── S3 storage ────────────────────────────────────────────────────

REGISTRY.define("s3_bucket", str,
    description="S3 bucket name where the file is stored",
    semantic_type="identifier",
    role="attribute",
)

REGISTRY.define("s3_key", str,
    description="S3 object key (path) for the stored file",
    semantic_type="identifier",
    role="attribute",
)

# ── Text extraction / search ──────────────────────────────────────

REGISTRY.define("extracted_text", str,
    description="Text extracted from document for full-text search",
    semantic_type="free_text",
    role="attribute",
    nullable=True,
)

# ── Arbitrary metadata ────────────────────────────────────────────

REGISTRY.define("metadata", dict,
    description="Arbitrary key-value metadata",
    role="attribute",
    nullable=True,
)
