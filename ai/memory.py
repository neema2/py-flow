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

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from ai._types import Message

logger = logging.getLogger(__name__)

_TABLE = "agent_conversations"


def bootstrap_conversations_table(admin_conn, grant_to: str = "") -> None:
    """Create the agent_conversations table using an admin connection.

    Args:
        admin_conn: A raw psycopg2 connection or admin_conn() from StoreServer.
        grant_to: Optional role to GRANT ALL to.
    """
    cursor = admin_conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {_TABLE} (
            id TEXT PRIMARY KEY,
            agent_name TEXT DEFAULT '',
            messages JSONB DEFAULT '[]'::jsonb,
            summary TEXT DEFAULT '',
            message_count INTEGER DEFAULT 0,
            metadata JSONB DEFAULT '{{}}'::jsonb,
            owner TEXT DEFAULT current_user,
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now()
        )
    """)
    if grant_to:
        cursor.execute(f"GRANT ALL ON {_TABLE} TO {grant_to}")
    admin_conn.commit()
    logger.info("Bootstrapped %s table", _TABLE)


@dataclass
class Conversation:
    """A persisted agent conversation."""
    id: str = ""
    agent_name: str = ""
    messages: list[dict] = field(default_factory=list)
    summary: str = ""
    message_count: int = 0
    created_at: str = ""
    updated_at: str = ""
    metadata: dict = field(default_factory=dict)

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

    Stores conversations as JSON in the object_events table.
    Supports save, load, list, and auto-summarization.

    Args:
        store_conn: A store connection (from ``connect()``).
        auto_summarize: Summarize conversations longer than this many messages (0 = disabled).
    """

    def __init__(self, store_conn=None, auto_summarize: int = 20):
        self._conn = store_conn
        self._auto_summarize = auto_summarize

    def save(
        self,
        conversation_id: str,
        messages: list[Message],
        agent_name: str = "",
        metadata: Optional[dict] = None,
        ai=None,
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

        now = datetime.now(timezone.utc).isoformat()
        convo = Conversation(
            id=conversation_id,
            agent_name=agent_name,
            messages=msg_dicts,
            summary=summary,
            message_count=len(msg_dicts),
            created_at=now,
            updated_at=now,
            metadata=metadata or {},
        )

        if self._conn is not None:
            self._write_to_db(convo)

        return convo

    def load(self, conversation_id: str) -> Optional[Conversation]:
        """Load a conversation by ID. Returns None if not found."""
        if self._conn is None:
            return None

        try:
            cursor = self._conn.conn.cursor()
            cursor.execute(
                f"SELECT id, agent_name, messages, summary, message_count, metadata, "
                f"created_at, updated_at FROM {_TABLE} WHERE id = %s",
                (conversation_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return Conversation(
                id=row[0],
                agent_name=row[1],
                messages=row[2] if isinstance(row[2], list) else json.loads(row[2]),
                summary=row[3] or "",
                message_count=row[4],
                metadata=row[5] if isinstance(row[5], dict) else json.loads(row[5] or "{}"),
                created_at=str(row[6]),
                updated_at=str(row[7]),
            )
        except Exception as e:
            logger.warning("Failed to load conversation %s: %s", conversation_id, e)
            return None

    def list_conversations(
        self,
        agent_name: Optional[str] = None,
        limit: int = 20,
    ) -> list[Conversation]:
        """List conversations, optionally filtered by agent name."""
        if self._conn is None:
            return []

        try:
            cursor = self._conn.conn.cursor()
            sql = (
                f"SELECT id, agent_name, '[]'::jsonb, summary, message_count, metadata, "
                f"created_at, updated_at FROM {_TABLE}"
            )
            params: list = []
            if agent_name:
                sql += " WHERE agent_name = %s"
                params.append(agent_name)
            sql += " ORDER BY updated_at DESC LIMIT %s"
            params.append(limit)
            cursor.execute(sql, params)

            return [
                Conversation(
                    id=row[0],
                    agent_name=row[1],
                    messages=[],  # Don't load messages for listing
                    summary=row[3] or "",
                    message_count=row[4],
                    metadata=row[5] if isinstance(row[5], dict) else json.loads(row[5] or "{}"),
                    created_at=str(row[6]),
                    updated_at=str(row[7]),
                )
                for row in cursor.fetchall()
            ]
        except Exception as e:
            logger.warning("Failed to list conversations: %s", e)
            return []

    def delete(self, conversation_id: str) -> bool:
        """Delete a conversation by ID."""
        if self._conn is None:
            return False
        try:
            cursor = self._conn.conn.cursor()
            cursor.execute(f"DELETE FROM {_TABLE} WHERE id = %s", (conversation_id,))
            self._conn.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.warning("Failed to delete conversation %s: %s", conversation_id, e)
            return False

    def new_id(self) -> str:
        """Generate a new conversation ID."""
        return str(uuid.uuid4())

    def _write_to_db(self, convo: Conversation) -> None:
        """Upsert a conversation to the database."""
        try:
            cursor = self._conn.conn.cursor()
            cursor.execute(
                f"""
                INSERT INTO {_TABLE} (id, agent_name, messages, summary, message_count, metadata, updated_at)
                VALUES (%s, %s, %s::jsonb, %s, %s, %s::jsonb, now())
                ON CONFLICT (id) DO UPDATE SET
                    messages = EXCLUDED.messages,
                    summary = EXCLUDED.summary,
                    message_count = EXCLUDED.message_count,
                    metadata = EXCLUDED.metadata,
                    updated_at = now()
                """,
                (
                    convo.id,
                    convo.agent_name,
                    json.dumps(convo.messages),
                    convo.summary,
                    convo.message_count,
                    json.dumps(convo.metadata),
                ),
            )
            self._conn.conn.commit()
        except Exception as e:
            logger.warning("Failed to save conversation %s: %s", convo.id, e)
            try:
                self._conn.conn.rollback()
            except Exception:
                pass

    def _summarize(self, messages: list[Message], ai) -> str:
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
