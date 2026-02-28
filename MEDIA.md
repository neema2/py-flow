# Media Store — Unstructured Data Storage & Search

Upload, download, and search documents, images, audio, and video with full-text search, bi-temporal audit trail, and RLS access control.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  MediaStore API                                                 │
│                                                                 │
│  ms.upload(file)  ──→ MinIO S3 (binary content)                 │
│                   ──→ PG object_events (Document Storable)      │
│                   ──→ PG document_search (tsvector + GIN)       │
│                   ──→ text extraction (PDF, text, md, HTML)     │
│                                                                 │
│  ms.search("query")  ──→ PG tsvector full-text search (RLS)    │
│  ms.download(doc)    ──→ MinIO S3 → bytes                       │
│  ms.list()           ──→ PG Storable.query() (RLS)              │
│                                                                 │
│  SyncEngine (existing)  ──→ Document metadata → Iceberg         │
│  Lakehouse.query()      ──→ DuckDB SQL over documents           │
└─────────────────────────────────────────────────────────────────┘
```

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Binary storage** | MinIO S3 | File content (media bucket) |
| **Metadata** | PG object_events (Storable) | Bi-temporal, RLS, event-sourced |
| **Search index** | PG document_search (tsvector + GIN) | Full-text search with ranking |
| **Text extraction** | pymupdf, beautifulsoup4 | PDF, HTML text extraction |
| **OLAP** | Iceberg + DuckDB (via SyncEngine) | Analytical queries on metadata |

---

## Quick Start

```bash
pip install -e ".[media]"
python3 demo_media.py
```

```python
from media import MediaStore

ms = MediaStore(s3_endpoint="localhost:9002")

doc = ms.upload("reports/q1.pdf", title="Q1 Report", tags=["research"])
results = ms.search("interest rate swap")
data = ms.download(doc)

ms.close()
```

---

## Package Layout

```
media/
├── __init__.py        # Public API: MediaStore, Document
├── store.py           # MediaStore class: upload, download, search, list, delete
├── models.py          # Document Storable + document_search schema (RLS)
├── extraction.py      # Text extraction: PDF, text, markdown, HTML
└── s3.py              # S3Client wrapper (MinIO upload/download/presign)
```

---

## Upload

```python
# From file path
doc = ms.upload("reports/q1_research.pdf", title="Q1 Research", tags=["research", "Q1"])

# From bytes
doc = ms.upload(image_bytes, filename="chart.png", content_type="image/png")

# Skip text extraction
doc = ms.upload(large_video, filename="recording.mp4", extract=False)
```

On upload, MediaStore:

1. Reads file content (path or bytes)
2. Detects content type from filename
3. Extracts searchable text (if supported)
4. Saves Document metadata to PG (Storable)
5. Uploads binary to MinIO at `media/{entity_id}/{filename}`
6. Updates the full-text search index

---

## Text Extraction

| Content type | Library | Status |
|-------------|---------|--------|
| `application/pdf` | pymupdf | ✅ Phase 1 |
| `text/plain` | built-in | ✅ Phase 1 |
| `text/markdown` | built-in (regex) | ✅ Phase 1 |
| `text/html` | beautifulsoup4 | ✅ Phase 1 |
| `text/csv` | built-in | ✅ Phase 1 |
| `image/*` | pytesseract (OCR) | Phase 2 |
| `audio/*` | whisper | Phase 2 |
| `video/*` | extract audio → whisper | Phase 2 |

Images, audio, and video are stored and downloadable in Phase 1 — text extraction comes in Phase 2.

---

## Full-Text Search

PG `tsvector` with weighted ranking and GIN index:

```python
# Basic search
results = ms.search("interest rate swap pricing")

# Filter by content type
results = ms.search("risk report", content_type="application/pdf")

# Filter by tags (AND — all must match)
results = ms.search("volatility", tags=["research", "equity"])
```

### Search Weights

| Weight | Source | Ranking |
|--------|--------|---------|
| **A** (highest) | Document title | Title matches rank first |
| **B** | Filename + tags | Filename/tag matches rank second |
| **C** | Extracted text | Full content matches rank third |

### Result Format

```python
[
    {
        "entity_id": "6704799a-...",
        "title": "Interest Rate Swap Research",
        "filename": "irs_research.txt",
        "content_type": "text/plain",
        "tags": ["research", "rates"],
        "rank": 1.0,
    },
    ...
]
```

---

## Download

```python
# By Document object
data = ms.download(doc)

# By entity_id string
data = ms.download("6704799a-72a1-43dd-9375-ca297e7364d5")

# To local file
ms.download_to(doc, "local/copy.pdf")
```

---

## List & Filter

```python
# All documents
docs = ms.list()

# By content type
pdfs = ms.list(content_type="application/pdf")

# By tags
research = ms.list(tags=["research"])
```

Uses `Storable.query()` — RLS enforced, only returns documents the user owns or has been shared.

---

## Delete

```python
ms.delete(doc)              # by Document object
ms.delete("entity-id...")   # by entity_id string
```

Soft-delete (Storable semantics) — the S3 object is retained for audit trail.

---

## Document Model

`Document` is a Storable — inherits bi-temporal audit trail, RLS, event sourcing:

| Field | Type | Description |
|-------|------|-------------|
| `title` | `str` | Document title |
| `filename` | `str` | Original filename |
| `content_type` | `str` | MIME type |
| `size` | `int` | File size in bytes |
| `s3_bucket` | `str` | S3 bucket (default: "media") |
| `s3_key` | `str` | S3 object key |
| `tags` | `list` | Classification tags |
| `extracted_text` | `str` | Extracted searchable text |
| `metadata` | `dict` | Arbitrary key-value metadata |

### Storable Features (free)

```python
from media import Document

# Find by ID
doc = Document.find(entity_id)

# Version history
history = doc.history()

# Audit trail
audit = doc.audit()

# Share with another user
doc.share("bob", mode="read")

# Bi-temporal query
doc = Document.as_of(entity_id, tx_time=yesterday)
```

---

## Access Control (RLS)

The `document_search` table mirrors `object_events` RLS policies:

| Operation | Policy |
|-----------|--------|
| **SELECT** | `owner = current_user OR current_user = ANY(readers) OR current_user = ANY(writers)` |
| **INSERT** | `owner = current_user` |
| **UPDATE** | `owner = current_user OR current_user = ANY(writers)` |
| **DELETE** | `owner = current_user` |

When a Document is shared via `.share()`, the search index is updated with the new `readers`/`writers` on the next upsert.

---

## OLAP Integration

Document metadata syncs to Iceberg automatically — `Document` is a Storable, so `SyncEngine` picks it up. Query via DuckDB:

```sql
-- Document counts by type
SELECT json_extract_string(data, '$.content_type') as ctype, count(*)
FROM lakehouse.default.events
WHERE type_name = 'Document'
GROUP BY ctype

-- Or build a dedicated documents table
lh.transform("documents",
    "SELECT entity_id, json_extract_string(data, '$.title') as title, ... "
    "FROM lakehouse.default.events WHERE type_name = 'Document'",
    mode="incremental", primary_key="entity_id")
```

---

## Dependencies

```toml
[project.optional-dependencies]
media = [
    "minio>=7.0",
    "pymupdf>=1.24.0",
    "beautifulsoup4>=4.12.0",
]
```

---

## Phase 2 (future)

- **Vector/semantic search**: pgvector or LanceDB for embeddings
- **OCR**: pytesseract for image text extraction
- **Audio transcription**: openai-whisper or faster-whisper
- **Video**: frame extraction + audio transcription
- **Chunking**: split large documents for RAG pipelines
- **Thumbnails**: auto-generate for images/video/PDF pages
