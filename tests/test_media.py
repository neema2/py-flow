"""
Unit tests for the media package — extraction, content type detection, and models.

No external services needed (no MinIO, no PG).
"""

import pytest
from media.extraction import (
    extract_text,
    detect_content_type,
    _extract_plain,
    _extract_markdown,
    _extract_html,
    _extract_pdf,
)
from media.models import Document


# ── Content type detection ────────────────────────────────────────────────


class TestDetectContentType:
    def test_pdf(self):
        assert detect_content_type("report.pdf") == "application/pdf"

    def test_txt(self):
        assert detect_content_type("notes.txt") == "text/plain"

    def test_markdown(self):
        assert detect_content_type("README.md") == "text/markdown"
        assert detect_content_type("doc.markdown") == "text/markdown"

    def test_html(self):
        assert detect_content_type("page.html") == "text/html"
        assert detect_content_type("page.htm") == "text/html"

    def test_csv(self):
        assert detect_content_type("data.csv") == "text/csv"

    def test_images(self):
        assert detect_content_type("photo.jpg") == "image/jpeg"
        assert detect_content_type("photo.jpeg") == "image/jpeg"
        assert detect_content_type("icon.png") == "image/png"
        assert detect_content_type("anim.gif") == "image/gif"
        assert detect_content_type("pic.webp") == "image/webp"
        assert detect_content_type("logo.svg") == "image/svg+xml"

    def test_audio(self):
        assert detect_content_type("song.mp3") == "audio/mpeg"
        assert detect_content_type("clip.wav") == "audio/wav"
        assert detect_content_type("track.ogg") == "audio/ogg"
        assert detect_content_type("music.flac") == "audio/flac"

    def test_video(self):
        assert detect_content_type("movie.mp4") == "video/mp4"
        assert detect_content_type("clip.mkv") == "video/x-matroska"
        assert detect_content_type("vid.avi") == "video/x-msvideo"
        assert detect_content_type("rec.mov") == "video/quicktime"

    def test_unknown(self):
        assert detect_content_type("file.xyz") == "application/octet-stream"
        assert detect_content_type("noext") == "application/octet-stream"

    def test_case_insensitive(self):
        assert detect_content_type("REPORT.PDF") == "application/pdf"
        assert detect_content_type("Photo.JPG") == "image/jpeg"


# ── PDF extraction ────────────────────────────────────────────────────────


def _make_pdf(text: str) -> bytes:
    """Create a minimal PDF with the given text."""
    import pymupdf
    doc = pymupdf.open()
    page = doc.new_page()
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
        assert "Line one" in result
        assert "Line three" in result

    def test_via_dispatcher(self):
        data = _make_pdf("Credit default swap valuation.")
        result = extract_text(data, "application/pdf")
        assert result is not None
        assert "Credit default swap" in result

    def test_empty_pdf(self):
        import pymupdf
        doc = pymupdf.open()
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
    def test_utf8(self):
        assert _extract_plain(b"hello world") == "hello world"

    def test_utf8_with_whitespace(self):
        assert _extract_plain(b"  hello  ") == "hello"

    def test_empty(self):
        assert _extract_plain(b"") is None
        assert _extract_plain(b"   ") is None

    def test_latin1_fallback(self):
        # Latin-1 encoded text with special chars
        text = "café résumé"
        data = text.encode("latin-1")
        result = _extract_plain(data)
        assert result is not None
        assert "caf" in result


# ── Markdown extraction ───────────────────────────────────────────────────


class TestExtractMarkdown:
    def test_strips_headers(self):
        md = b"# Title\n## Subtitle\nBody text"
        result = _extract_markdown(md)
        assert "Title" in result
        assert "#" not in result

    def test_strips_bold_italic(self):
        md = b"This is **bold** and *italic* text"
        result = _extract_markdown(md)
        assert "bold" in result
        assert "italic" in result
        assert "**" not in result
        assert "*italic*" not in result

    def test_strips_links(self):
        md = b"Check [this link](https://example.com) out"
        result = _extract_markdown(md)
        assert "this link" in result
        assert "https://" not in result

    def test_strips_code_blocks(self):
        md = b"Text\n```python\nprint('hello')\n```\nMore text"
        result = _extract_markdown(md)
        assert "Text" in result
        assert "More text" in result
        assert "print" not in result

    def test_empty(self):
        assert _extract_markdown(b"") is None


# ── HTML extraction ───────────────────────────────────────────────────────


class TestExtractHTML:
    def test_strips_tags(self):
        html = b"<html><body><p>Hello <b>world</b></p></body></html>"
        result = _extract_html(html)
        assert "Hello" in result
        assert "world" in result
        assert "<" not in result

    def test_strips_scripts(self):
        html = b"<html><script>alert('xss')</script><p>Content</p></html>"
        result = _extract_html(html)
        assert "Content" in result
        assert "alert" not in result

    def test_strips_style(self):
        html = b"<html><style>body{color:red}</style><p>Text</p></html>"
        result = _extract_html(html)
        assert "Text" in result
        assert "color" not in result

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
        assert "Title" in result
        assert "#" not in result

    def test_html(self):
        result = extract_text(b"<p>Content</p>", "text/html")
        assert "Content" in result

    def test_unknown_returns_none(self):
        result = extract_text(b"\x00\x01\x02", "image/png")
        assert result is None

    def test_text_subtype_fallback(self):
        result = extract_text(b"data,values", "text/csv")
        assert result == "data,values"

    def test_markdown_by_filename(self):
        result = extract_text(b"# Title\nBody", "application/octet-stream", filename="README.md")
        assert "Title" in result


# ── Document model ────────────────────────────────────────────────────────


class TestDocument:
    def test_properties(self):
        doc = Document(content_type="application/pdf")
        assert doc.is_pdf
        assert not doc.is_image
        assert not doc.is_audio
        assert not doc.is_video
        assert not doc.is_text

    def test_is_image(self):
        doc = Document(content_type="image/png")
        assert doc.is_image
        assert not doc.is_pdf

    def test_is_audio(self):
        doc = Document(content_type="audio/mpeg")
        assert doc.is_audio

    def test_is_video(self):
        doc = Document(content_type="video/mp4")
        assert doc.is_video

    def test_is_text(self):
        doc = Document(content_type="text/plain")
        assert doc.is_text

    def test_has_text(self):
        doc = Document(extracted_text="")
        assert not doc.has_text
        doc.extracted_text = "some content"
        assert doc.has_text

    def test_default_values(self):
        doc = Document()
        assert doc.title == ""
        assert doc.filename == ""
        assert doc.content_type == ""
        assert doc.size == 0
        assert doc.s3_bucket == "media"
        assert doc.s3_key == ""
        assert doc.tags == []
        assert doc.extracted_text == ""
        assert doc.metadata == {}
