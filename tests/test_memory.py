"""
Tests for AgentMemory — persistent conversation storage.

Uses real embedded PostgreSQL (StoreServer) and public Store APIs only.
No mocks.
"""

import tempfile

import pytest
from ai._types import Message
from ai.memory import AgentMemory
from store.admin import StoreServer
from store.connection import UserConnection

# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def server(store_server):
    """Delegate to session-scoped store_server from conftest.py."""
    return store_server


@pytest.fixture(scope="module")
def conn_info(server):
    return server.conn_info()


@pytest.fixture(scope="module")
def _provision_users(server):
    server.provision_user("mem_user", "mem_pw")


@pytest.fixture()
def conn(conn_info, _provision_users):
    c = UserConnection(
        user="mem_user", password="mem_pw",
        host=conn_info["host"], port=conn_info["port"], dbname=conn_info["dbname"],
    )
    yield c
    c.close()


@pytest.fixture()
def memory(conn):
    return AgentMemory(store_conn=conn)


# ── Tests ─────────────────────────────────────────────────────────────────

class TestAgentMemory:

    def test_save_and_load(self, memory):
        """Save a conversation, then load it back by ID."""
        cid = memory.new_id()
        msgs = [
            Message(role="user", content="Hello"),
            Message(role="assistant", content="Hi there!"),
        ]
        saved = memory.save(cid, msgs, agent_name="test-agent")
        assert saved.agent_name == "test-agent"
        assert saved.message_count == 2

        loaded = memory.load(cid)
        assert loaded is not None
        assert loaded.message_count == 2
        assert loaded.agent_name == "test-agent"
        assert len(loaded.messages) == 2
        assert loaded.messages[0]["role"] == "user"
        assert loaded.messages[1]["content"] == "Hi there!"

    def test_save_update_existing(self, memory):
        """Saving with the same conversation_id updates the record."""
        cid = memory.new_id()
        msgs1 = [Message(role="user", content="First")]
        memory.save(cid, msgs1, agent_name="agent-v1")

        msgs2 = [
            Message(role="user", content="First"),
            Message(role="assistant", content="Reply"),
        ]
        updated = memory.save(cid, msgs2, agent_name="agent-v2")
        assert updated.message_count == 2
        assert updated.agent_name == "agent-v2"

        loaded = memory.load(cid)
        assert loaded is not None
        assert loaded.message_count == 2

    def test_load_nonexistent_returns_none(self, memory):
        """Loading a conversation that doesn't exist returns None."""
        result = memory.load(memory.new_id())
        assert result is None

    def test_list_conversations(self, memory):
        """List conversations, optionally filtered by agent_name."""
        cid1 = memory.new_id()
        cid2 = memory.new_id()
        memory.save(cid1, [Message(role="user", content="A")], agent_name="alpha")
        memory.save(cid2, [Message(role="user", content="B")], agent_name="beta")

        all_convos = memory.list_conversations()
        assert len(all_convos) >= 2

        alpha_only = memory.list_conversations(agent_name="alpha")
        assert all(c.agent_name == "alpha" for c in alpha_only)

    def test_delete(self, memory):
        """Delete a conversation by ID."""
        cid = memory.new_id()
        memory.save(cid, [Message(role="user", content="Deletable")])

        assert memory.load(cid) is not None
        assert memory.delete(cid) is True
        assert memory.load(cid) is None

    def test_delete_nonexistent_returns_false(self, memory):
        """Deleting a nonexistent conversation returns False."""
        assert memory.delete(memory.new_id()) is False

    def test_to_messages_round_trip(self, memory):
        """Messages survive save → load → to_messages() round-trip."""
        cid = memory.new_id()
        msgs = [
            Message(role="user", content="Question?"),
            Message(role="assistant", content="Answer."),
        ]
        memory.save(cid, msgs)
        loaded = memory.load(cid)
        assert loaded is not None

        restored = loaded.to_messages()
        assert len(restored) == 2
        assert restored[0].role == "user"
        assert restored[0].content == "Question?"
        assert restored[1].role == "assistant"
        assert restored[1].content == "Answer."

    def test_metadata_persisted(self, memory):
        """Metadata dict is persisted and loaded back."""
        cid = memory.new_id()
        meta = {"model": "gemini-2.0", "tokens": 150}
        memory.save(cid, [Message(role="user", content="Hi")], metadata=meta)

        loaded = memory.load(cid)
        assert loaded is not None
        assert loaded.metadata["model"] == "gemini-2.0"
        assert loaded.metadata["tokens"] == 150

    def test_no_connection_returns_in_memory(self):
        """AgentMemory without a connection returns in-memory conversations."""
        mem = AgentMemory(store_conn=None)
        convo = mem.save("fake-id", [Message(role="user", content="X")])
        assert convo.message_count == 1
        # load/list/delete all return empty without a connection
        assert mem.load("fake-id") is None
        assert mem.list_conversations() == []
        assert mem.delete("fake-id") is False
