"""
Document Agent — Unstructured Document Structuring
====================================================
Process docs, extract structured data, set up search pipelines.

Tools:
    - upload_document          — upload with auto-embed
    - list_documents           — browse corpus
    - search_documents         — text/semantic/hybrid search
    - extract_structured_data  — AI.extract() on doc text
    - bulk_upload              — batch process a folder
    - tag_documents            — search + auto-tag

Usage::

    from agents._document import create_document_agent

    agent = create_document_agent(ctx)
    result = agent.run("Upload and tag all PDFs in the reports folder")
"""

from __future__ import annotations

import json
import logging
from typing import Any

from ai import Agent, tool

from agents._context import _PlatformContext

logger = logging.getLogger(__name__)

DOCUMENT_SYSTEM_PROMPT = """\
You are the Document Agent — a platform specialist that manages unstructured \
data: PDFs, text files, CSVs, HTML, and other documents.

You can:
1. Upload documents to the MediaStore (with automatic text extraction, chunking, and embedding).
2. List and browse the document corpus with filters.
3. Search documents using full-text, semantic, or hybrid search.
4. Extract structured data from documents using AI (e.g. extract key facts, tables, entities).
5. Bulk-upload entire directories of files.
6. Auto-tag documents based on content.

When uploading documents:
- Suggest meaningful titles and tags for discoverability.
- Use semantic search mode for meaning-based queries and text search for exact keywords.
- When extracting structured data, define clear schemas matching the user's needs.

Always report what was uploaded/found with counts and key details.
"""


def create_document_tools(ctx: _PlatformContext) -> list:
    """Create Document agent tools bound to a _PlatformContext."""
    from media.store import MediaStore

    def _get_ms() -> MediaStore:
        if ctx.media_store is None:
            raise RuntimeError("No MediaStore configured in _PlatformContext")
        return ctx.media_store

    @tool
    def upload_document(file_path: str, title: str = "", tags: str = "") -> str:
        """Upload a document to the MediaStore with automatic text extraction and embedding.

        Args:
            file_path: Local file path or URL to the document.
            title: Human-readable title for the document.
            tags: Comma-separated tags for categorization (e.g. "research,quarterly,risk").
        """
        try:
            ms = _get_ms()
            tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
            doc = ms.upload(file_path, title=title or "", tags=tag_list or None)
            return json.dumps({
                "status": "uploaded",
                "entity_id": str(doc._store_entity_id),
                "title": doc.title,
                "filename": doc.filename,
                "content_type": doc.content_type,
                "size": doc.size,
                "tags": doc.tags,
            }, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def list_documents(content_type: str = "", tags: str = "", limit: int = 20) -> str:
        """List documents in the MediaStore with optional filters.

        Args:
            content_type: Optional MIME type filter (e.g. "application/pdf").
            tags: Optional comma-separated tags to filter by.
            limit: Maximum documents to return (default 20).
        """
        try:
            ms = _get_ms()
            tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else None
            docs = ms.list(
                content_type=content_type or None,
                tags=tag_list,
                limit=limit,
            )
            result = [{
                "entity_id": str(d._store_entity_id),
                "title": d.title,
                "filename": d.filename,
                "content_type": d.content_type,
                "tags": d.tags,
                "size": d.size,
            } for d in docs]
            return json.dumps({"count": len(result), "documents": result}, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def search_documents(query: str, mode: str = "hybrid", limit: int = 10) -> str:
        """Search documents using text, semantic, or hybrid search.

        Args:
            query: Search query (natural language or keywords).
            mode: Search mode — "text" (keyword), "semantic" (meaning), or "hybrid" (best of both).
            limit: Maximum results (default 10).
        """
        try:
            ms = _get_ms()
            if mode == "semantic":
                results = ms.semantic_search(query, limit=limit)
            elif mode == "text":
                results = ms.search(query, limit=limit)
            else:  # hybrid
                results = ms.hybrid_search(query, limit=limit)
            return json.dumps({"query": query, "mode": mode, "count": len(results),
                              "results": results}, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def extract_structured_data(document_id: str, schema_json: str) -> str:
        """Extract structured data from a document using AI.

        Downloads the document text, then uses AI.extract() to pull out
        structured fields matching the provided JSON schema.

        Args:
            document_id: Entity ID of the document to extract from.
            schema_json: JSON schema defining the fields to extract.
                         Example: {"type": "object", "properties": {"company": {"type": "string"},
                         "revenue": {"type": "number"}}, "required": ["company"]}
        """
        if ctx.ai is None:
            return json.dumps({"error": "No AI configured in _PlatformContext"})

        try:
            ms = _get_ms()
            schema = json.loads(schema_json)

            # Get document text
            from media.models import Document
            doc = Document.find(document_id)
            if doc is None:
                return json.dumps({"error": f"Document {document_id} not found"})

            text = doc.extracted_text or ""
            if not text:
                # Try downloading and extracting
                data = ms.download(doc)
                from media.extraction import extract_text
                text = extract_text(data, doc.content_type) or ""

            if not text:
                return json.dumps({"error": "No text content available in document"})

            # Extract structured data
            result = ctx.ai.extract(text[:10000], schema=schema)
            return json.dumps({
                "document_id": document_id,
                "extracted": result.data,
                "raw_response": result.raw_response[:500] if result.raw_response else "",
            }, default=str)
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid schema JSON: {e}"})
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def bulk_upload(directory: str, tags: str = "", content_type_filter: str = "") -> str:
        """Upload all files in a directory to the MediaStore.

        Args:
            directory: Local directory path containing files to upload.
            tags: Comma-separated tags to apply to all uploaded documents.
            content_type_filter: Optional filter — only upload files matching this MIME type prefix
                                 (e.g. "application/pdf", "text/").
        """
        import os
        try:
            ms = _get_ms()
            tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []

            if not os.path.isdir(directory):
                return json.dumps({"error": f"Directory not found: {directory}"})

            uploaded = []
            errors = []
            for filename in os.listdir(directory):
                filepath = os.path.join(directory, filename)
                if not os.path.isfile(filepath):
                    continue

                # Filter by content type if specified
                if content_type_filter:
                    from media.extraction import detect_content_type
                    ct = detect_content_type(filepath)
                    if not ct.startswith(content_type_filter):
                        continue

                try:
                    doc = ms.upload(filepath, title=filename, tags=tag_list or None)
                    uploaded.append({
                        "filename": filename,
                        "entity_id": str(doc._store_entity_id),
                        "content_type": doc.content_type,
                    })
                except Exception as e:
                    errors.append({"filename": filename, "error": str(e)})

            return json.dumps({
                "directory": directory,
                "uploaded": len(uploaded),
                "errors": len(errors),
                "files": uploaded[:20],
                "error_details": errors[:5],
            }, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def tag_documents(query: str, tags: str, search_mode: str = "hybrid") -> str:
        """Search for documents and auto-tag all matches.

        Args:
            query: Search query to find documents to tag.
            tags: Comma-separated tags to apply to matching documents.
            search_mode: Search mode — "text", "semantic", or "hybrid".
        """
        try:
            ms = _get_ms()
            tag_list = [t.strip() for t in tags.split(",") if t.strip()]
            if not tag_list:
                return json.dumps({"error": "No tags provided"})

            # Search for matching documents
            if search_mode == "semantic":
                results = ms.semantic_search(query, limit=50)
            elif search_mode == "text":
                results = ms.search(query, limit=50)
            else:
                results = ms.hybrid_search(query, limit=50)

            # Tag each result
            tagged = 0
            for r in results:
                try:
                    doc_id = r.get("entity_id") or r.get("document_id")
                    if doc_id:
                        from media.models import Document
                        doc = Document.find(doc_id)
                        if doc:
                            existing_tags = set(doc.tags or [])
                            new_tags = list(existing_tags | set(tag_list))
                            doc.tags = new_tags
                            doc.save()
                            tagged += 1
                except Exception:
                    continue

            return json.dumps({
                "query": query,
                "tags_applied": tag_list,
                "documents_found": len(results),
                "documents_tagged": tagged,
            })
        except Exception as e:
            return json.dumps({"error": str(e)})

    return [upload_document, list_documents, search_documents,
            extract_structured_data, bulk_upload, tag_documents]


def create_document_agent(ctx: _PlatformContext, **kwargs: Any) -> Agent:
    """Create a Document Agent bound to a _PlatformContext."""
    tools = create_document_tools(ctx)
    return Agent(
        tools=tools,
        system_prompt=DOCUMENT_SYSTEM_PROMPT,
        name="document",
        **kwargs,
    )
