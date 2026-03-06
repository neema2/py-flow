"""
Integration tests for the media package.

Requires: embedded PG (via StoreServer) + S3-compatible object store.
Tests the full upload → extract → search → download → delete flow.
"""

import asyncio

import pytest

# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def _provision_media_user(store_server):
    """Provision media_user on the shared store_server."""
    store_server.provision_user("media_user", "media_pw")
    return store_server



@pytest.fixture(scope="session")
def store_conn(_provision_media_user):
    """Connect to the object store as media_user."""
    from store.connection import connect
    info = _provision_media_user.conn_info()
    conn = connect(user="media_user", host=info["host"], port=info["port"],
                    dbname=info["dbname"], password="media_pw")
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def search_schema(_provision_media_user, store_conn):
    """Bootstrap the document_search table."""
    from media.models import bootstrap_search_schema
    admin_conn = _provision_media_user.admin_conn()
    bootstrap_search_schema(admin_conn)
    admin_conn.close()
    return True


@pytest.fixture(scope="session")
def media_store(media_server, store_conn, search_schema):
    """Create a MediaStore connected to test object store."""
    from media import MediaStore
    ms = MediaStore(
        s3_endpoint="localhost:9102",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
        s3_bucket="test-media",
    )
    yield ms
    ms.close()


# ── Tests ─────────────────────────────────────────────────────────────────


class TestMediaIntegration:
    """Full integration tests: upload → extract → search → download → delete."""

    def test_upload_text_file(self, media_store):
        """Upload a plain text file and verify metadata."""
        content = b"This is a test document about interest rate swaps and bond pricing."
        doc = media_store.upload(
            content,
            filename="test_doc.txt",
            title="Test Document",
            tags=["test", "finance"],
        )

        assert doc.entity_id is not None
        assert doc.title == "Test Document"
        assert doc.filename == "test_doc.txt"
        assert doc.content_type == "text/plain"
        assert doc.size == len(content)
        assert doc.s3_key.startswith("media/")
        assert doc.s3_key.endswith("test_doc.txt")
        assert doc.tags == ["test", "finance"]
        assert "interest rate swaps" in doc.extracted_text

    def test_upload_markdown_file(self, media_store):
        """Upload a markdown file — text extracted with markup stripped."""
        md_content = b"""# Research Report

## Summary
This report covers **credit default swap** pricing models
and *volatility surface* construction.

### Key Findings
- CDS spreads widened in Q1
- Implied vol skew increased for short-dated options
"""
        doc = media_store.upload(
            md_content,
            filename="research.md",
            title="Q1 Research Report",
            tags=["research", "credit"],
        )

        assert doc.content_type == "text/markdown"
        assert doc.has_text
        assert "credit default swap" in doc.extracted_text
        assert "volatility surface" in doc.extracted_text
        # Markdown syntax should be stripped
        assert "**" not in doc.extracted_text
        assert "#" not in doc.extracted_text

    def test_upload_html_file(self, media_store):
        """Upload an HTML file — text extracted with tags stripped."""
        html = b"""<html>
<head><title>Trading Report</title></head>
<body>
<h1>Daily Trading Summary</h1>
<p>The portfolio generated <b>$2.5M</b> in realized P&L from equity derivatives.</p>
<script>alert('xss')</script>
</body>
</html>"""
        doc = media_store.upload(
            html,
            filename="report.html",
            title="Trading Report",
            tags=["trading", "daily"],
        )

        assert doc.content_type == "text/html"
        assert doc.has_text
        assert "equity derivatives" in doc.extracted_text
        assert "<" not in doc.extracted_text
        assert "alert" not in doc.extracted_text

    def test_upload_pdf(self, media_store):
        """Upload a PDF file — text extracted via pymupdf."""
        import pymupdf
        doc = pymupdf.open()
        page = doc.new_page()
        page.insert_text((72, 72), "Collateralized debt obligation pricing\n"  # type: ignore[attr-defined]
                         "using Monte Carlo simulation for tranche valuation.", fontsize=12)
        pdf_bytes = doc.tobytes()
        doc.close()

        result = media_store.upload(
            pdf_bytes,
            filename="cdo_pricing.pdf",
            title="CDO Pricing Model",
            tags=["research", "credit"],
        )

        assert result.content_type == "application/pdf"
        assert result.has_text
        assert "Collateralized debt obligation" in result.extracted_text
        assert "Monte Carlo" in result.extracted_text
        assert result.s3_key.endswith("cdo_pricing.pdf")

    def test_search_pdf_content(self, media_store):
        """Full-text search finds content extracted from PDF."""
        results = media_store.search("collateralized debt obligation")
        assert len(results) > 0
        titles = [r["title"] for r in results]
        assert "CDO Pricing Model" in titles

    def test_upload_binary_no_extraction(self, media_store):
        """Upload a binary file (image) — no text extraction."""
        fake_png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100
        doc = media_store.upload(
            fake_png,
            filename="chart.png",
            title="Portfolio Chart",
            tags=["chart"],
            extract=True,  # extraction should return None for images
        )

        assert doc.content_type == "image/png"
        assert not doc.has_text
        assert doc.size == len(fake_png)
        assert doc.s3_key.endswith("chart.png")

    def test_download(self, media_store):
        """Upload and download — content roundtrips correctly."""
        content = b"Portfolio risk report: VaR = $1.2M at 99% confidence."
        doc = media_store.upload(
            content,
            filename="risk_report.txt",
            title="Risk Report",
        )

        downloaded = media_store.download(doc)
        assert downloaded == content

    def test_download_by_entity_id(self, media_store):
        """Download using entity_id string."""
        content = b"Test download by ID"
        doc = media_store.upload(content, filename="by_id.txt")

        downloaded = media_store.download(str(doc.entity_id))
        assert downloaded == content

    def test_search_full_text(self, media_store):
        """Full-text search finds documents by extracted text."""
        results = media_store.search("interest rate swaps")
        assert len(results) > 0
        # The test_doc.txt we uploaded should be found
        titles = [r["title"] for r in results]
        assert "Test Document" in titles

    def test_search_by_content_type(self, media_store):
        """Search filtered by content type."""
        results = media_store.search("report", content_type="text/markdown")
        assert len(results) > 0
        assert all(r["content_type"] == "text/markdown" for r in results)

    def test_search_by_tags(self, media_store):
        """Search filtered by tags."""
        results = media_store.search("credit default swap", tags=["research"])
        assert len(results) > 0
        for r in results:
            assert "research" in r["tags"]

    def test_search_no_results(self, media_store):
        """Search for something that doesn't exist."""
        results = media_store.search("quantum_chromodynamics_hadron_collider")
        assert len(results) == 0

    def test_search_ranked(self, media_store):
        """Search results are ranked by relevance."""
        results = media_store.search("credit default swap")
        assert len(results) > 0
        # Results should have rank scores
        assert all("rank" in r for r in results)
        # Should be in descending order
        ranks = [r["rank"] for r in results]
        assert ranks == sorted(ranks, reverse=True)

    def test_list_documents(self, media_store):
        """List all documents."""
        docs = media_store.list()
        assert len(docs) >= 4  # at least the ones we uploaded

    def test_list_by_content_type(self, media_store):
        """List filtered by content type."""
        docs = media_store.list(content_type="text/markdown")
        assert len(docs) >= 1
        assert all(d.content_type == "text/markdown" for d in docs)

    def test_list_by_tags(self, media_store):
        """List filtered by tags."""
        docs = media_store.list(tags=["research"])
        assert len(docs) >= 1
        assert all("research" in d.tags for d in docs)

    def test_document_storable_features(self, media_store):
        """Document inherits Storable features: find, history, audit."""
        content = b"Storable features test"
        doc = media_store.upload(content, filename="storable_test.txt", title="Storable Test")

        # find
        from media.models import Document
        found = Document.find(doc.entity_id)
        assert found is not None
        assert found.title == "Storable Test"

        # history
        history = found.history()
        assert len(history) >= 1  # at least CREATED + UPDATED (for s3_key)

    def test_delete_document(self, media_store):
        """Delete a document — soft delete, S3 object retained."""
        content = b"To be deleted"
        doc = media_store.upload(content, filename="delete_me.txt", title="Delete Me")
        entity_id = doc.entity_id

        media_store.delete(doc)

        # Should not be findable after delete
        from media.models import Document
        found = Document.find(entity_id)
        assert found is None

        # But S3 object is still there (retained for audit)
        from objectstore import S3Client
        s3 = S3Client(endpoint="localhost:9102", bucket="test-media")
        assert s3.exists(doc.s3_key)
