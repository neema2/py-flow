"""
Text Extraction — Extract searchable text from various file formats.

Supported formats (Phase 1):
  - application/pdf → pymupdf
  - text/plain → as-is
  - text/markdown → strip markup
  - text/html → beautifulsoup4 (strip tags)

Phase 2 (future):
  - image/* → OCR via pytesseract
  - audio/* → transcription via whisper
  - video/* → extract audio → transcribe
"""

from __future__ import annotations

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)


def extract_text(data: bytes, content_type: str, filename: str = "") -> Optional[str]:
    """
    Extract searchable text from binary data based on content type.

    Args:
        data: Raw file bytes.
        content_type: MIME type (e.g., "application/pdf", "text/plain").
        filename: Optional filename for format detection fallback.

    Returns:
        Extracted text, or None if extraction is not supported.
    """
    ct = content_type.lower().strip()

    if ct == "application/pdf":
        return _extract_pdf(data)
    elif ct in ("text/plain", "text/csv"):
        return _extract_plain(data)
    elif ct == "text/markdown" or (filename and filename.endswith(".md")):
        return _extract_markdown(data)
    elif ct == "text/html" or ct == "application/xhtml+xml":
        return _extract_html(data)
    elif ct.startswith("text/"):
        return _extract_plain(data)
    else:
        logger.debug("No text extraction for content_type=%s", content_type)
        return None


def _extract_pdf(data: bytes) -> Optional[str]:
    """Extract text from PDF using pymupdf."""
    try:
        import pymupdf
    except ImportError:
        logger.warning("pymupdf not installed — cannot extract PDF text. pip install pymupdf")
        return None

    try:
        doc = pymupdf.open(stream=data, filetype="pdf")
        pages = []
        for page in doc:
            text = page.get_text()
            if text:
                pages.append(text.strip())
        doc.close()
        result = "\n\n".join(pages)
        logger.debug("Extracted %d chars from PDF (%d pages)", len(result), len(pages))
        return result if result else None
    except Exception as e:
        logger.error("PDF extraction failed: %s", e)
        return None


def _extract_plain(data: bytes) -> Optional[str]:
    """Extract text from plain text files."""
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = data.decode("latin-1")
        except Exception:
            return None
    return text.strip() if text.strip() else None


def _extract_markdown(data: bytes) -> Optional[str]:
    """Extract text from markdown, stripping common markup."""
    text = _extract_plain(data)
    if not text:
        return None

    # Strip common markdown syntax
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)  # headers
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)                # bold
    text = re.sub(r"\*(.+?)\*", r"\1", text)                    # italic
    text = re.sub(r"`{1,3}[^`]*`{1,3}", "", text)               # inline code
    text = re.sub(r"```[\s\S]*?```", "", text)                   # code blocks
    text = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", text)        # links
    text = re.sub(r"^[-*+]\s+", "", text, flags=re.MULTILINE)   # list items
    text = re.sub(r"^>\s+", "", text, flags=re.MULTILINE)       # blockquotes
    text = re.sub(r"---+", "", text)                             # horizontal rules
    text = re.sub(r"\n{3,}", "\n\n", text)                      # excess newlines

    return text.strip() if text.strip() else None


def _extract_html(data: bytes) -> Optional[str]:
    """Extract text from HTML using beautifulsoup4."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning("beautifulsoup4 not installed — cannot extract HTML text. pip install beautifulsoup4")
        # Fallback: strip tags with regex
        text = _extract_plain(data)
        if text:
            text = re.sub(r"<[^>]+>", " ", text)
            text = re.sub(r"\s+", " ", text)
            return text.strip() if text.strip() else None
        return None

    try:
        soup = BeautifulSoup(data, "html.parser")
        # Remove script and style elements
        for tag in soup(["script", "style", "head"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        logger.debug("Extracted %d chars from HTML", len(text))
        return text if text else None
    except Exception as e:
        logger.error("HTML extraction failed: %s", e)
        return None


# ── Content type detection ────────────────────────────────────────────────

MIME_MAP = {
    ".pdf": "application/pdf",
    ".txt": "text/plain",
    ".md": "text/markdown",
    ".markdown": "text/markdown",
    ".html": "text/html",
    ".htm": "text/html",
    ".csv": "text/csv",
    ".json": "application/json",
    ".xml": "application/xml",
    # Images
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".bmp": "image/bmp",
    ".webp": "image/webp",
    ".svg": "image/svg+xml",
    ".tiff": "image/tiff",
    ".ico": "image/x-icon",
    # Audio
    ".mp3": "audio/mpeg",
    ".wav": "audio/wav",
    ".ogg": "audio/ogg",
    ".flac": "audio/flac",
    ".m4a": "audio/mp4",
    # Video
    ".mp4": "video/mp4",
    ".mkv": "video/x-matroska",
    ".avi": "video/x-msvideo",
    ".mov": "video/quicktime",
    ".webm": "video/webm",
}


def detect_content_type(filename: str) -> str:
    """Detect MIME type from filename extension."""
    lower = filename.lower()
    for ext, mime in MIME_MAP.items():
        if lower.endswith(ext):
            return mime
    return "application/octet-stream"
