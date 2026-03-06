"""
Tests for UserConnection + Active Record API.

Tests cover: connect(), register_alias(), save(), delete(), find(),
query(), transition(), share/unshare(), history(), audit(), refresh(),
context manager, and no-connection error.

Run with: pytest tests/test_connection.py -v
"""

import os
import sys
import tempfile
from dataclasses import dataclass

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from store.admin import StoreServer
from store.base import Storable
from store.connection import _set_active, active_connection, connect
from store.state_machine import StateMachine, Transition

# ── Test models ──────────────────────────────────────────────────────────────

@dataclass
class Item(Storable):
    name: str = ""
    value: float = 0.0


class ItemLifecycle(StateMachine):
    initial = "ACTIVE"
    transitions = [
        Transition("ACTIVE", "ARCHIVED"),
        Transition("ARCHIVED", "ACTIVE"),
    ]

Item._state_machine = ItemLifecycle


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def server(store_server):
    """Delegate to session-scoped store_server from conftest.py."""
    return store_server


@pytest.fixture(scope="module")
def conn_info(server):
    return server.conn_info()


@pytest.fixture(scope="module")
def _provision_users(server):
    server.provision_user("alice", "alice_pw")
    server.provision_user("bob", "bob_pw")


@pytest.fixture()
def alice_db(conn_info, _provision_users):
    """UserConnection as alice (explicit params)."""
    db = connect(
        host=conn_info["host"], port=conn_info["port"],
        dbname=conn_info["dbname"],
        user="alice", password="alice_pw",
    )
    yield db
    db.close()


@pytest.fixture()
def bob_db(conn_info, _provision_users):
    """UserConnection as bob (explicit params)."""
    db = connect(
        host=conn_info["host"], port=conn_info["port"],
        dbname=conn_info["dbname"],
        user="bob", password="bob_pw",
    )
    yield db
    db.close()


# ═════════════════════════════════════════════════════════════════════════════
# Connection & Alias Tests
# ═════════════════════════════════════════════════════════════════════════════

class TestConnect:

    def test_connect_explicit_params(self, alice_db):
        assert alice_db.user == "alice"
        assert alice_db.alias is None

    def test_connect_alias(self, server, _provision_users):
        server.register_alias("test_db")
        db = connect("test_db", user="alice", password="alice_pw")
        assert db.alias == "test_db"
        assert db.user == "alice"
        db.close()

    def test_context_manager(self, conn_info, _provision_users):
        with connect(host=conn_info["host"], port=conn_info["port"],
                     dbname=conn_info["dbname"],
                     user="alice", password="alice_pw") as db:
            assert active_connection() is db
        # After exit, connection is deactivated
        _set_active(None)

    def test_no_connection_raises(self):
        _set_active(None)
        with pytest.raises(RuntimeError, match="No active connection"):
            active_connection()

    def test_repr(self, alice_db):
        assert "alice" in repr(alice_db)


# ═════════════════════════════════════════════════════════════════════════════
# Active Record: save / find / delete
# ═════════════════════════════════════════════════════════════════════════════

class TestSaveAndFind:

    def test_save_creates_new_entity(self, alice_db):
        item = Item(name="widget", value=42.0)
        entity_id = item.save()
        assert entity_id is not None
        assert item.entity_id == entity_id
        assert item.version == 1

    def test_save_updates_existing(self, alice_db):
        item = Item(name="gadget", value=10.0)
        item.save()
        assert item.version == 1

        item.value = 20.0
        entity_id = item.save()
        assert item.version == 2
        assert entity_id == item.entity_id

    def test_find_by_id(self, alice_db):
        item = Item(name="findme", value=99.0)
        entity_id = item.save()

        found = Item.find(entity_id)
        assert found is not None
        assert found.name == "findme"
        assert found.value == 99.0

    def test_find_nonexistent_returns_none(self, alice_db):
        found = Item.find("00000000-0000-0000-0000-000000000000")
        assert found is None

    def test_delete(self, alice_db):
        item = Item(name="deleteme", value=1.0)
        entity_id = item.save()
        item.delete()

        found = Item.find(entity_id)
        assert found is None


# ═════════════════════════════════════════════════════════════════════════════
# Active Record: query / count
# ═════════════════════════════════════════════════════════════════════════════

class TestQueryAndCount:

    def test_query_with_filter(self, alice_db):
        Item(name="q_alpha", value=1.0).save()
        Item(name="q_beta", value=2.0).save()

        results = Item.query(filters={"name": "q_alpha"})
        assert any(i.name == "q_alpha" for i in results)
        assert not any(i.name == "q_beta" for i in results)

    def test_count(self, alice_db):
        before = Item.count()
        Item(name="counter", value=0.0).save()
        after = Item.count()
        assert after >= before + 1


# ═════════════════════════════════════════════════════════════════════════════
# Active Record: transition
# ═════════════════════════════════════════════════════════════════════════════

class TestTransition:

    def test_transition(self, alice_db):
        item = Item(name="stateful", value=5.0)
        item.save()
        assert item.state == "ACTIVE"

        item.transition("ARCHIVED")
        assert item.state == "ARCHIVED"

    def test_transition_round_trip(self, alice_db):
        item = Item(name="roundtrip", value=5.0)
        item.save()
        item.transition("ARCHIVED")
        item.transition("ACTIVE")
        assert item.state == "ACTIVE"


# ═════════════════════════════════════════════════════════════════════════════
# Active Record: history / audit / refresh
# ═════════════════════════════════════════════════════════════════════════════

class TestHistoryAuditRefresh:

    def test_history(self, alice_db):
        item = Item(name="hist", value=1.0)
        item.save()
        item.value = 2.0
        item.save()
        item.value = 3.0
        item.save()

        versions = item.history()
        assert len(versions) == 3
        assert versions[0].value == 1.0
        assert versions[2].value == 3.0

    def test_audit(self, alice_db):
        item = Item(name="audited", value=1.0)
        item.save()
        item.value = 2.0
        item.save()

        trail = item.audit()
        assert len(trail) == 2
        assert trail[0]["event_type"] == "CREATED"
        assert trail[1]["event_type"] == "UPDATED"

    def test_refresh(self, alice_db):
        item = Item(name="refreshable", value=10.0)
        item.save()

        # Simulate external change via internal client
        item2 = Item.find(item.entity_id)  # type: ignore[arg-type]
        item2.value = 99.0  # type: ignore[union-attr]
        item2.save()  # type: ignore[union-attr]

        # Our local object is stale
        assert item.value == 10.0
        item.refresh()
        assert item.value == 99.0

    def test_refresh_unsaved_raises(self, alice_db):
        item = Item(name="unsaved", value=0.0)
        with pytest.raises(ValueError, match="no entity_id"):
            item.refresh()


# ═════════════════════════════════════════════════════════════════════════════
# Active Record: share / unshare (RLS)
# ═════════════════════════════════════════════════════════════════════════════

class TestShareUnshare:

    def test_share_read(self, conn_info, _provision_users):
        # Alice creates, shares with Bob
        alice = connect(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="alice", password="alice_pw",
        )
        item = Item(name="shared_item", value=42.0)
        entity_id = item.save()

        # Bob can't see it yet
        bob = connect(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bob", password="bob_pw",
        )
        assert Item.find(entity_id) is None

        # Alice shares read access
        alice.activate()
        item.share("bob", mode="read")

        # Bob can see it now
        bob.activate()
        found = Item.find(entity_id)
        assert found is not None
        assert found.name == "shared_item"

        bob.close()
        alice.activate()
        alice.close()

    def test_unshare_read(self, conn_info, _provision_users):
        alice = connect(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="alice", password="alice_pw",
        )
        item = Item(name="unshare_test", value=1.0)
        entity_id = item.save()
        item.share("bob", mode="read")

        bob = connect(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bob", password="bob_pw",
        )
        assert Item.find(entity_id) is not None

        alice.activate()
        item.unshare("bob", mode="read")

        bob.activate()
        assert Item.find(entity_id) is None

        bob.close()
        alice.activate()
        alice.close()

    def test_share_write(self, conn_info, _provision_users):
        alice = connect(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="alice", password="alice_pw",
        )
        item = Item(name="write_shared", value=1.0)
        entity_id = item.save()
        item.share("bob", mode="write")

        bob = connect(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bob", password="bob_pw",
        )
        found = Item.find(entity_id)
        assert found is not None
        found.value = 999.0
        found.save()
        assert found.version == 2

        bob.close()
        alice.activate()
        alice.close()


# ═════════════════════════════════════════════════════════════════════════════
# No-connection guard on instance methods
# ═════════════════════════════════════════════════════════════════════════════

class TestNoConnectionGuard:

    def test_save_without_connection_raises(self):
        _set_active(None)
        item = Item(name="orphan", value=0.0)
        with pytest.raises(RuntimeError, match="No active connection"):
            item.save()

    def test_find_without_connection_raises(self):
        _set_active(None)
        with pytest.raises(RuntimeError, match="No active connection"):
            Item.find("some-id")

    def test_delete_without_connection_raises(self):
        _set_active(None)
        item = Item(name="orphan", value=0.0)
        item._store_entity_id = "fake-id"
        with pytest.raises(RuntimeError, match="No active connection"):
            item.delete()
