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

import inspect
import json
import logging
from typing import Optional, get_type_hints

from ai._types import Tool

logger = logging.getLogger(__name__)


# ── Python type → JSON Schema mapping ────────────────────────────────────

_PY_TO_JSON_TYPE = {
    str: "string",
    int: "integer",
    float: "number",
    bool: "boolean",
}


def _param_schema(py_type) -> dict:
    """Convert a Python type hint to a JSON Schema fragment."""
    origin = getattr(py_type, "__origin__", None)
    if origin is list:
        args = getattr(py_type, "__args__", ())
        items = _param_schema(args[0]) if args else {"type": "string"}
        return {"type": "array", "items": items}
    if origin is dict:
        return {"type": "object"}
    return {"type": _PY_TO_JSON_TYPE.get(py_type, "string")}


def _schema_from_function(fn) -> dict:
    """Build a JSON Schema parameters dict from a function's type hints and docstring."""
    hints = get_type_hints(fn)
    sig = inspect.signature(fn)
    properties = {}
    required = []

    # Parse param descriptions from docstring
    param_docs = _parse_param_docs(fn.__doc__ or "")

    for name, param in sig.parameters.items():
        if name == "self" or name == "return":
            continue
        py_type = hints.get(name, str)
        if py_type is inspect.Parameter.empty:
            py_type = str
        prop = _param_schema(py_type)
        if name in param_docs:
            prop["description"] = param_docs[name]
        properties[name] = prop
        if param.default is inspect.Parameter.empty:
            required.append(name)

    schema = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def _parse_param_docs(docstring: str) -> dict[str, str]:
    """Extract parameter descriptions from a docstring (Google/numpy style)."""
    result = {}
    if not docstring:
        return result
    lines = docstring.split("\n")
    in_args = False
    for line in lines:
        stripped = line.strip()
        if stripped.lower() in ("args:", "parameters:", "params:"):
            in_args = True
            continue
        if in_args:
            if not stripped or (not line[0].isspace() and stripped.endswith(":")):
                in_args = False
                continue
            # "name: description" or "name (type): description"
            if ":" in stripped:
                parts = stripped.split(":", 1)
                param_part = parts[0].strip()
                desc = parts[1].strip()
                # Handle "name (type)" format
                if "(" in param_part:
                    param_part = param_part.split("(")[0].strip()
                if param_part and desc:
                    result[param_part] = desc
    return result


def tool(fn):
    """
    Decorator that converts a typed Python function into a Tool.

    The function name becomes the tool name, the docstring's first line
    becomes the description, and type hints are converted to JSON Schema.

    Usage::

        @tool
        def get_price(symbol: str) -> str:
            \"\"\"Get the current price of a stock.

            Args:
                symbol: Ticker symbol (e.g. AAPL).
            \"\"\"
            return json.dumps({"price": 150.25})

    The decorated function gains a `_tool` attribute with the Tool object.
    """
    doc = fn.__doc__ or ""
    description = doc.strip().split("\n")[0] if doc.strip() else fn.__name__
    parameters = _schema_from_function(fn)

    t = Tool(
        name=fn.__name__,
        description=description,
        parameters=parameters,
        fn=fn,
    )
    fn._tool = t
    return fn


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

    def register_decorated(self, *fns) -> None:
        """Batch-register functions decorated with @tool."""
        for fn in fns:
            if hasattr(fn, '_tool'):
                self.register(fn._tool)
            elif isinstance(fn, Tool):
                self.register(fn)
            else:
                raise TypeError(
                    f"{fn} is not a @tool-decorated function or Tool instance"
                )

    @property
    def tool_names(self) -> list[str]:
        return list(self._tools.keys())

    @classmethod
    def from_platform(
        cls,
        media_store=None,
        lakehouse=None,
    ) -> "ToolRegistry":
        """Create a ToolRegistry with built-in platform tools.

        Args:
            media_store: A MediaStore instance — registers search/list tools.
            lakehouse: A Lakehouse instance — registers query/list_tables tools.

        Returns:
            A populated ToolRegistry.
        """
        registry = cls()
        if media_store is not None:
            for t in create_search_tools(media_store):
                registry.register(t)
        if lakehouse is not None:
            for t in create_lakehouse_tools(lakehouse):
                registry.register(t)
        return registry


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


def create_lakehouse_tools(lakehouse) -> list[Tool]:
    """
    Create tools that operate on a Lakehouse.

    Args:
        lakehouse: A Lakehouse instance.

    Returns:
        List of Tool instances for lakehouse queries.
    """
    tools = []

    def _query_lakehouse(sql: str) -> str:
        """Execute a SQL query against the Iceberg lakehouse."""
        rows = lakehouse.query(sql)
        return json.dumps(rows, default=str)

    tools.append(Tool(
        name="query_lakehouse",
        description="Execute a read-only SQL query against the Iceberg lakehouse. Returns rows as JSON.",
        parameters={
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "SQL query to execute (read-only).",
                },
            },
            "required": ["sql"],
        },
        fn=_query_lakehouse,
    ))

    def _list_tables(namespace: str = "default") -> str:
        """List tables in the lakehouse."""
        tables = lakehouse.list_tables(namespace=namespace)
        return json.dumps(tables, default=str)

    tools.append(Tool(
        name="list_tables",
        description="List available tables in the Iceberg lakehouse.",
        parameters={
            "type": "object",
            "properties": {
                "namespace": {
                    "type": "string",
                    "description": "Iceberg namespace (default: 'default').",
                },
            },
        },
        fn=_list_tables,
    ))

    return tools
