"""
Platform Tools — Callable tools that LLMs can use to interact with the platform.

Each tool has a JSON schema declaration (for the LLM) and an execute function.
Tools are registered in a ToolRegistry which provides declarations for the
LLM's tools= parameter and dispatches execution.

Usage::

    from ai.tools import ToolRegistry, create_search_tools

    registry = ToolRegistry()
    for tool in create_search_tools(media_store):
        registry.register(tool)

    # Pass declarations to LLM
    response = llm.generate(messages, tools=registry.list_declarations())

    # Execute tool calls
    for tc in response.tool_calls:
        result = registry.execute(tc.name, tc.arguments)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class Tool:
    """A callable tool with JSON schema declaration."""
    name: str
    description: str
    parameters: dict            # JSON Schema for arguments
    fn: Callable[..., str]      # Execute function → returns JSON string


class ToolRegistry:
    """Registry of tools that an LLM can call."""

    def __init__(self):
        self._tools: dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        """Register a tool."""
        self._tools[tool.name] = tool
        logger.debug("Registered tool: %s", tool.name)

    def get(self, name: str) -> Tool:
        """Get a tool by name. Raises KeyError if not found."""
        if name not in self._tools:
            raise KeyError(f"Unknown tool: {name}. Available: {list(self._tools.keys())}")
        return self._tools[name]

    def list_declarations(self) -> list[dict]:
        """Return tool declarations in the format expected by LLMClient.generate(tools=...)."""
        return [
            {
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.parameters,
            }
            for tool in self._tools.values()
        ]

    def execute(self, name: str, arguments: dict) -> str:
        """
        Execute a tool by name with the given arguments.

        Args:
            name: Tool name.
            arguments: Arguments dict (from LLM ToolCall.arguments).

        Returns:
            JSON string result from the tool.

        Raises:
            KeyError: If tool not found.
        """
        tool = self.get(name)
        logger.info("Executing tool %s with args: %s", name, arguments)
        return tool.fn(**arguments)

    @property
    def tool_names(self) -> list[str]:
        return list(self._tools.keys())


# ── Built-in platform tools ──────────────────────────────────────────────


def create_search_tools(media_store) -> list[Tool]:
    """
    Create search tools that operate on a MediaStore.

    Args:
        media_store: A MediaStore instance (with or without embedding_provider).

    Returns:
        List of Tool instances for document search.
    """
    tools = []

    # Full-text search
    def _search_documents(query: str, content_type: str = "", limit: int = 10) -> str:
        results = media_store.search(
            query,
            content_type=content_type or None,
            limit=limit,
        )
        return json.dumps(results, default=str)

    tools.append(Tool(
        name="search_documents",
        description="Full-text keyword search over documents. Returns documents matching the query terms.",
        parameters={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query (keywords).",
                },
                "content_type": {
                    "type": "string",
                    "description": "Optional MIME type filter (e.g. 'application/pdf').",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results to return (default: 10).",
                },
            },
            "required": ["query"],
        },
        fn=_search_documents,
    ))

    # Semantic search (only if embedding provider is available)
    if hasattr(media_store, '_embedder') and media_store._embedder:
        def _semantic_search(query: str, limit: int = 10) -> str:
            results = media_store.semantic_search(query, limit=limit)
            return json.dumps(results, default=str)

        tools.append(Tool(
            name="semantic_search",
            description="Semantic similarity search over document chunks. Finds documents by meaning, not just keywords.",
            parameters={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language search query.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return (default: 10).",
                    },
                },
                "required": ["query"],
            },
            fn=_semantic_search,
        ))

        def _hybrid_search(query: str, limit: int = 10) -> str:
            results = media_store.hybrid_search(query, limit=limit)
            return json.dumps(results, default=str)

        tools.append(Tool(
            name="hybrid_search",
            description="Combined keyword + semantic search using Reciprocal Rank Fusion. Best for general queries.",
            parameters={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query (natural language).",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return (default: 10).",
                    },
                },
                "required": ["query"],
            },
            fn=_hybrid_search,
        ))

    # List documents
    def _list_documents(content_type: str = "", limit: int = 20) -> str:
        docs = media_store.list(
            content_type=content_type or None,
            limit=limit,
        )
        return json.dumps([
            {
                "entity_id": str(d._store_entity_id),
                "title": d.title,
                "filename": d.filename,
                "content_type": d.content_type,
                "tags": d.tags,
                "size": d.size,
            }
            for d in docs
        ], default=str)

    tools.append(Tool(
        name="list_documents",
        description="List available documents, optionally filtered by content type.",
        parameters={
            "type": "object",
            "properties": {
                "content_type": {
                    "type": "string",
                    "description": "Optional MIME type filter.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max documents to return (default: 20).",
                },
            },
        },
        fn=_list_documents,
    ))

    return tools
