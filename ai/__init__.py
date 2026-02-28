"""
AI package — embeddings, LLM integration, and agent framework.

Phase 1: Embedding providers (Gemini primary).
Phase 2: LLM client with tool calling (Gemini primary).
"""

from ai.embeddings import EmbeddingProvider, GeminiEmbeddings
from ai.llm import LLMClient, GeminiLLM, Message, LLMResponse, ToolCall
from ai.tools import Tool, ToolRegistry, create_search_tools

__all__ = [
    "EmbeddingProvider", "GeminiEmbeddings",
    "LLMClient", "GeminiLLM", "Message", "LLMResponse", "ToolCall",
    "Tool", "ToolRegistry", "create_search_tools",
]
