"""
Agent Memory — Persistent conversation storage backed by the object store.

Conversations are stored as Storable objects with RLS, bi-temporal audit,
and optional LLM-powered summarization for long conversations.

Usage::

    from ai import Agent
    from ai.memory import AgentMemory

    memory = AgentMemory(store_conn=conn)
    agent = Agent(tools=[...], memory=memory)

    result = agent.run("Analyze AAPL")
    # Conversation auto-saved

    agent.load_conversation(conversation_id)
    convos = agent.list_conversations(limit=10)
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field

from ai._types import Message
from ai.client import AI
from store import Storable, UserConnection

logger = logging.getLogger(__name__)


@dataclass
class Conversation(Storable):
    """A persisted agent conversation.

    Stored in the object store as a Storable — inherits bi-temporal audit,
    RLS, event sourcing, and Iceberg sync for free.
    """
    agent_name: str = ""
    messages: list = field(default_factory=list)
    summary: str = ""
    message_count: int = 0
    metadata: dict = field(default_factory=dict)

    @property
    def id(self) -> str:
        """Conversation ID (alias for entity_id)."""
        return self.entity_id or ""

    @property
    def created_at(self) -> str:
        """Creation timestamp as ISO string."""
        return str(self.tx_time or "")

    @property
    def updated_at(self) -> str:
        """Last update timestamp as ISO string."""
        return str(self.tx_time or "")

    def to_messages(self) -> list[Message]:
        """Convert stored message dicts back to Message objects."""
        return [
            Message(
                role=m["role"],
                content=m.get("content", ""),
                name=m.get("name", ""),
                tool_call_id=m.get("tool_call_id", ""),
            )
            for m in self.messages
        ]


class AgentMemory:
    """
    Persistent conversation memory backed by the object store.

    Stores conversations as Storable objects in object_events —
    no separate table or raw SQL needed.

    Args:
        store_conn: A UserConnection (from ``connect()``).
        auto_summarize: Summarize conversations longer than this many messages (0 = disabled).
    """

    def __init__(self, store_conn: UserConnection | None = None, auto_summarize: int = 20) -> None:
        self._conn = store_conn
        self._auto_summarize = auto_summarize

    def _require_conn(self) -> UserConnection:
        assert self._conn is not None, "AgentMemory requires a store connection"
        return self._conn

    def save(
        self,
        conversation_id: str,
        messages: list[Message],
        agent_name: str = "",
        metadata: dict | None = None,
        ai: AI | None = None,
    ) -> Conversation:
        """
        Save a conversation.

        Args:
            conversation_id: Unique ID for the conversation.
            messages: List of Message objects.
            agent_name: Name of the agent.
            metadata: Optional metadata dict.
            ai: Optional AI instance for auto-summarization.

        Returns:
            The saved Conversation.
        """
        msg_dicts = [
            {"role": m.role, "content": m.content, "name": m.name, "tool_call_id": m.tool_call_id}
            for m in messages
            if m.role != "system"  # Don't persist system prompts
        ]

        summary = ""
        if self._auto_summarize > 0 and len(msg_dicts) > self._auto_summarize and ai is not None:
            summary = self._summarize(messages, ai)

        if self._conn is not None:
            # Check if conversation already exists
            existing = Conversation.find(conversation_id)
            if existing is not None:
                # Update existing conversation
                existing.messages = msg_dicts
                existing.summary = summary or existing.summary
                existing.message_count = len(msg_dicts)
                existing.metadata = metadata or existing.metadata
                existing.agent_name = agent_name or existing.agent_name
                existing.save()
                return existing
            else:
                # Create new conversation with the provided ID
                convo = Conversation(
                    agent_name=agent_name,
                    messages=msg_dicts,
                    summary=summary,
                    message_count=len(msg_dicts),
                    metadata=metadata or {},
                )
                convo._store_entity_id = conversation_id
                convo.save()
                return convo
        else:
            # No connection — return an in-memory conversation
            convo = Conversation(
                agent_name=agent_name,
                messages=msg_dicts,
                summary=summary,
                message_count=len(msg_dicts),
                metadata=metadata or {},
            )
            convo._store_entity_id = conversation_id
            return convo

    def load(self, conversation_id: str) -> Conversation | None:
        """Load a conversation by ID. Returns None if not found."""
        if self._conn is None:
            return None
        try:
            return Conversation.find(conversation_id)
        except Exception as e:
            logger.warning("Failed to load conversation %s: %s", conversation_id, e)
            return None

    def list_conversations(
        self,
        agent_name: str | None = None,
        limit: int = 20,
    ) -> list[Conversation]:
        """List conversations, optionally filtered by agent name."""
        if self._conn is None:
            return []
        try:
            filters = {}
            if agent_name:
                filters["agent_name"] = agent_name
            result = Conversation.query(filters=filters or None, limit=limit)
            return list(result.items)
        except Exception as e:
            logger.warning("Failed to list conversations: %s", e)
            return []

    def delete(self, conversation_id: str) -> bool:
        """Delete a conversation by ID."""
        if self._conn is None:
            return False
        try:
            convo = Conversation.find(conversation_id)
            if convo is None:
                return False
            convo.delete()
            return True
        except Exception as e:
            logger.warning("Failed to delete conversation %s: %s", conversation_id, e)
            return False

    def new_id(self) -> str:
        """Generate a new conversation ID."""
        return str(uuid.uuid4())

    def _summarize(self, messages: list[Message], ai: AI) -> str:
        """Summarize a conversation using the LLM."""
        try:
            # Build a condensed version of the conversation
            lines = []
            for m in messages:
                if m.role == "system":
                    continue
                prefix = m.role.upper()
                content = m.content[:200] if m.content else "(tool call)"
                lines.append(f"{prefix}: {content}")

            text = "\n".join(lines[-30:])  # Last 30 messages

            response = ai.generate(
                [
                    Message(role="system", content="Summarize this conversation in 2-3 sentences. Be specific about what was discussed and any conclusions reached."),
                    Message(role="user", content=text),
                ],
                temperature=0.3,
                max_tokens=200,
            )
            return response.content.strip()
        except Exception as e:
            logger.warning("Failed to summarize conversation: %s", e)
            return ""
