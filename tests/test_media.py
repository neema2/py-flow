"""
Unit tests for the media package — extraction, content type detection, and models.

No external services needed (no S3, no PG).
"""

import pytest
from media.extraction import (
    _extract_html,
    _extract_markdown,
    _extract_pdf,
    _extract_plain,
    detect_content_type,
    extract_text,
)
from media.models import Document

# ── Content type detection ────────────────────────────────────────────────


class TestDetectContentType:
    @pytest.mark.parametrize("filename,expected", [
        ("report.pdf", "application/pdf"),
        ("notes.txt", "text/plain"),
        ("README.md", "text/markdown"),
        ("page.html", "text/html"),
        ("data.csv", "text/csv"),
        ("photo.jpg", "image/jpeg"),
        ("icon.png", "image/png"),
        ("logo.svg", "image/svg+xml"),
        ("song.mp3", "audio/mpeg"),
        ("movie.mp4", "video/mp4"),
        ("file.xyz", "application/octet-stream"),
        ("noext", "application/octet-stream"),
        ("REPORT.PDF", "application/pdf"),
    ])
    def test_detect(self, filename, expected):
        assert detect_content_type(filename) == expected


# ── PDF extraction ────────────────────────────────────────────────────────


def _make_pdf(text: str) -> bytes:
    """Create a minimal PDF with the given text."""
    pymupdf = pytest.importorskip("pymupdf")
    doc = pymupdf.Document()
    page = doc.new_page()
    assert page is not None
    page.insert_text((72, 72), text, fontsize=12)
    data = doc.tobytes()
    doc.close()
    return data


class TestExtractPDF:
    def test_basic_extraction(self):
        data = _make_pdf("Interest rate swap pricing models.")
        result = _extract_pdf(data)
        assert result is not None
        assert "Interest rate swap" in result

    def test_multiline(self):
        data = _make_pdf("Line one.\nLine two.\nLine three.")
        result = _extract_pdf(data)
        assert "Line one" in result  # type: ignore[operator]
        assert "Line three" in result  # type: ignore[operator]

    def test_via_dispatcher(self):
        data = _make_pdf("Credit default swap valuation.")
        result = extract_text(data, "application/pdf")
        assert result is not None
        assert "Credit default swap" in result

    def test_empty_pdf(self):
        pymupdf = pytest.importorskip("pymupdf")
        doc = pymupdf.Document()
        doc.new_page()  # blank page
        data = doc.tobytes()
        doc.close()
        result = _extract_pdf(data)
        assert result is None

    def test_invalid_pdf(self):
        result = _extract_pdf(b"not a pdf")
        assert result is None


# ── Plain text extraction ─────────────────────────────────────────────────


class TestExtractPlain:
    def test_utf8_and_whitespace(self):
        assert _extract_plain(b"hello world") == "hello world"
        assert _extract_plain(b"  hello  ") == "hello"

    def test_empty_and_fallback(self):
        assert _extract_plain(b"") is None
        assert _extract_plain(b"   ") is None
        assert "caf" in _extract_plain("café résumé".encode("latin-1"))  # type: ignore[operator]


# ── Markdown extraction ───────────────────────────────────────────────────


class TestExtractMarkdown:
    def test_strips_formatting(self):
        md = b"# Title\n## Subtitle\nThis is **bold** and *italic* text"
        result = _extract_markdown(md)
        assert "Title" in result and "#" not in result  # type: ignore[operator]
        assert "bold" in result and "**" not in result  # type: ignore[operator]

    def test_strips_links_and_code(self):
        md = b"Check [this link](https://example.com)\n```python\nprint('hello')\n```\nMore text"
        result = _extract_markdown(md)
        assert "this link" in result and "https://" not in result  # type: ignore[operator]
        assert "More text" in result and "print" not in result  # type: ignore[operator]

    def test_empty(self):
        assert _extract_markdown(b"") is None


# ── HTML extraction ───────────────────────────────────────────────────────


class TestExtractHTML:
    def test_strips_tags_scripts_style(self):
        html = b"<html><script>alert('xss')</script><style>body{color:red}</style><body><p>Hello <b>world</b></p></body></html>"
        result = _extract_html(html)
        assert "Hello" in result and "world" in result and "<" not in result  # type: ignore[operator]
        assert "alert" not in result and "color" not in result  # type: ignore[operator]

    def test_empty(self):
        assert _extract_html(b"") is None
        assert _extract_html(b"<html></html>") is None


# ── extract_text dispatcher ───────────────────────────────────────────────


class TestExtractText:
    def test_plain_text(self):
        result = extract_text(b"Hello world", "text/plain")
        assert result == "Hello world"

    def test_markdown(self):
        result = extract_text(b"# Title\nBody", "text/markdown")
        assert "Title" in result  # type: ignore[operator]
        assert "#" not in result  # type: ignore[operator]

    def test_html(self):
        result = extract_text(b"<p>Content</p>", "text/html")
        assert "Content" in result  # type: ignore[operator]

    def test_unknown_returns_none(self):
        result = extract_text(b"\x00\x01\x02", "image/png")
        assert result is None

    def test_text_subtype_fallback(self):
        result = extract_text(b"data,values", "text/csv")
        assert result == "data,values"

    def test_markdown_by_filename(self):
        result = extract_text(b"# Title\nBody", "application/octet-stream", filename="README.md")
        assert "Title" in result  # type: ignore[operator]


# ── Document model ────────────────────────────────────────────────────────


class TestDocument:
    @pytest.mark.parametrize("content_type,prop", [
        ("application/pdf", "is_pdf"),
        ("image/png", "is_image"),
        ("audio/mpeg", "is_audio"),
        ("video/mp4", "is_video"),
        ("text/plain", "is_text"),
    ])
    def test_type_properties(self, content_type, prop):
        doc = Document(content_type=content_type)
        assert getattr(doc, prop) is True

    def test_has_text_and_defaults(self):
        doc = Document()
        assert doc.title == "" and doc.size == 0 and doc.s3_bucket == "media"
        assert not doc.has_text
        doc.extracted_text = "some content"
        assert doc.has_text
