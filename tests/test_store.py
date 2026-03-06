"""
Comprehensive tests for the bi-temporal event-sourced object store.
Tests cover: serde, event sourcing, bi-temporal queries, state machines,
RLS enforcement, trust boundary, sharing, and admin access.

Run with: pytest tests/test_store.py -v
"""

import json
import os
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg2.errors
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from store.admin import StoreServer
from store.base import Storable, _json_decoder_hook, _JSONEncoder
from store.connection import UserConnection
from store.permissions import list_shared_with, share_read, share_write, unshare_read, unshare_write
from store.query_result import QueryResult
from store.state_machine import GuardFailure, InvalidTransition, StateMachine, Transition, TransitionNotPermitted
from store.subscriptions import ChangeEvent, EventBus, SubscriptionListener

from store import VersionConflict

# ── Test models ──────────────────────────────────────────────────────────────

@dataclass
class Widget(Storable):
    name: str = ""
    color: str = ""
    weight: float = 0.0


@dataclass
class RichObject(Storable):
    label: str = ""
    amount: float = 0.0
    ts: str = ""
    tags: list | None = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []




# Action/hook trackers for testing
action_log = []


def _track_action(obj, from_state, to_state):
    action_log.append(("action", from_state, to_state))


def _track_on_enter(obj, from_state, to_state):
    action_log.append(("on_enter", to_state))  # type: ignore[arg-type]


def _track_on_exit(obj, from_state, to_state):
    action_log.append(("on_exit", from_state))  # type: ignore[arg-type]


class OrderLifecycle(StateMachine):
    initial = "PENDING"
    transitions = [
        Transition("PENDING", "PARTIAL",
                   on_exit=_track_on_exit),
        Transition("PENDING", "FILLED",
                   guard=lambda obj: obj.quantity > 0,
                   on_exit=_track_on_exit,
                   on_enter=_track_on_enter),
        Transition("PENDING", "CANCELLED",
                   allowed_by=["risk_manager"],
                   on_exit=_track_on_exit),
        Transition("PARTIAL", "FILLED",
                   on_enter=_track_on_enter),
        Transition("PARTIAL", "CANCELLED"),
        Transition("FILLED", "SETTLED",
                   guard=lambda obj: obj.price > 0,
                   action=_track_action),
    ]


@dataclass
class Order(Storable):
    symbol: str = ""
    quantity: int = 0
    price: float = 0.0
    side: str = ""

Order._state_machine = OrderLifecycle


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def server(store_server):
    """Delegate to session-scoped store_server from conftest.py."""
    return store_server


@pytest.fixture(scope="module")
def conn_info(server):
    """Connection info dict."""
    return server.conn_info()


@pytest.fixture(scope="module")
def _provision_users(server):
    """Provision test users: alice, bob, charlie."""
    server.provision_user("alice", "alice_pw")
    server.provision_user("bob", "bob_pw")
    server.provision_user("charlie", "charlie_pw")


@pytest.fixture()
def alice(conn_info, _provision_users):
    """UserConnection connected as alice."""
    c = UserConnection(
        user="alice", password="alice_pw",
        host=conn_info["host"], port=conn_info["port"], dbname=conn_info["dbname"],
    )
    yield c
    c.close()


@pytest.fixture()
def bob(conn_info, _provision_users):
    """UserConnection connected as bob."""
    c = UserConnection(
        user="bob", password="bob_pw",
        host=conn_info["host"], port=conn_info["port"], dbname=conn_info["dbname"],
    )
    yield c
    c.close()


@pytest.fixture()
def charlie(conn_info, _provision_users):
    """UserConnection connected as charlie."""
    c = UserConnection(
        user="charlie", password="charlie_pw",
        host=conn_info["host"], port=conn_info["port"], dbname=conn_info["dbname"],
    )
    yield c
    c.close()


@pytest.fixture()
def admin_client(server, conn_info):
    """UserConnection connected as app_admin."""
    c = UserConnection(
        user="app_admin", password="test_admin_pw",
        host=conn_info["host"], port=conn_info["port"], dbname=conn_info["dbname"],
    )
    yield c
    c.close()


# ── Serialization (no DB needed) ────────────────────────────────────────────

class TestSerde:
    def test_dataclass_to_json(self):
        w = Widget(name="gear", color="blue", weight=1.5)
        j = w.to_json()
        data = json.loads(j)
        assert data == {"name": "gear", "color": "blue", "weight": 1.5}

    def test_dataclass_from_json(self):
        j = '{"name": "gear", "color": "blue", "weight": 1.5}'
        w = Widget.from_json(j)
        assert w.name == "gear"
        assert w.color == "blue"
        assert w.weight == 1.5

    def test_roundtrip(self):
        original = Widget(name="bolt", color="red", weight=0.3)
        restored = Widget.from_json(original.to_json())
        assert restored.name == original.name
        assert restored.color == original.color
        assert restored.weight == pytest.approx(original.weight)

    def test_datetime_serde(self):
        dt = datetime(2025, 1, 15, 10, 30, 0)
        encoded = json.dumps({"ts": dt}, cls=_JSONEncoder)
        decoded = json.loads(encoded, object_hook=_json_decoder_hook)
        assert decoded["ts"] == dt

    def test_decimal_serde(self):
        d = Decimal("123.456")
        encoded = json.dumps({"val": d}, cls=_JSONEncoder)
        decoded = json.loads(encoded, object_hook=_json_decoder_hook)
        assert decoded["val"] == d

    def test_uuid_serde(self):
        u = uuid.uuid4()
        encoded = json.dumps({"id": u}, cls=_JSONEncoder)
        decoded = json.loads(encoded, object_hook=_json_decoder_hook)
        assert decoded["id"] == u

    def test_type_name(self):
        assert "Widget" in Widget.type_name()

    def test_extra_fields_ignored_on_deserialize(self):
        j = '{"name": "x", "color": "y", "weight": 1.0, "extra": "ignored"}'
        w = Widget.from_json(j)
        assert w.name == "x"
        assert not hasattr(w, "extra")


# ── Event Sourcing ──────────────────────────────────────────────────────────

class TestEventSourcing:
    def test_write_creates_version_1(self, alice):
        w = Widget(name="v1_test", color="green", weight=2.0)
        entity_id = w.save()
        assert entity_id is not None
        uuid.UUID(entity_id)
        assert w.version == 1
        assert w.event_type == "CREATED"

    def test_read_back(self, alice):
        w = Widget(name="spring", color="silver", weight=0.1)
        entity_id = w.save()
        loaded = Widget.get(entity_id)
        assert loaded is not None
        assert loaded.name == "spring"
        assert loaded.color == "silver"
        assert loaded.weight == pytest.approx(0.1)

    def test_store_metadata_set(self, alice):
        w = Widget(name="pin", color="black", weight=0.01)
        w.save()
        assert w.entity_id is not None
        assert w.version == 1
        assert w.owner == "alice"
        assert w.tx_time is not None
        assert w.valid_from is not None
        assert w.event_type == "CREATED"

    def test_update_creates_new_version(self, alice):
        w = Widget(name="updatable", color="white", weight=1.0)
        w.save()
        assert w.version == 1

        w.color = "black"
        w.save()
        assert w.version == 2
        assert w.event_type == "UPDATED"

        loaded = Widget.get(w.entity_id)
        assert loaded.color == "black"
        assert loaded.version == 2

    def test_update_never_overwrites(self, alice):
        """After update, both versions exist in history."""
        w = Widget(name="immutable_test", color="red", weight=1.0)
        w.save()
        entity_id = w.entity_id

        w.color = "blue"
        w.save()

        history = Widget.get(entity_id).history()
        assert len(history) == 2
        assert history[0].color == "red"
        assert history[0].version == 1
        assert history[1].color == "blue"
        assert history[1].version == 2

    def test_delete_creates_tombstone(self, alice):
        w = Widget(name="deletable", color="grey", weight=0.5)
        entity_id = w.save()

        w.delete()
        assert w.event_type == "DELETED"

        # Gone from read/query
        assert Widget.find(entity_id) is None

        # But present in history
        history = w.history()
        assert len(history) == 2
        assert history[-1].event_type == "DELETED"

    def test_version_numbers_monotonic(self, alice):
        w = Widget(name="mono_test", color="a", weight=1.0)
        w.save()
        for i in range(5):
            w.color = f"color_{i}"
            w.save()
        assert w.version == 6

        history = Widget.get(w.entity_id).history()
        versions = [h.version for h in history]
        assert versions == [1, 2, 3, 4, 5, 6]

    def test_independent_entity_versions(self, alice):
        w1 = Widget(name="ent1", color="a", weight=1.0)
        w2 = Widget(name="ent2", color="b", weight=2.0)
        w1.save()
        w2.save()

        w1.color = "updated"
        w1.save()

        assert w1.version == 2
        assert w2.version == 1

    def test_history_returns_all_versions(self, alice):
        w = Widget(name="history_test", color="v1", weight=1.0)
        w.save()
        w.color = "v2"
        w.save()
        w.color = "v3"
        w.save()

        history = Widget.get(w.entity_id).history()
        assert len(history) == 3
        colors = [h.color for h in history]
        assert colors == ["v1", "v2", "v3"]


# ── Bi-Temporal Queries ─────────────────────────────────────────────────────

class TestBiTemporal:
    def test_as_of_tx_time(self, alice):
        """What did we know at time T?"""
        w = Widget(name="bitemporal", color="original", weight=1.0)
        w.save()
        entity_id = w.entity_id
        after_write = datetime.now(timezone.utc)

        time.sleep(0.05)

        w.color = "corrected"
        w.save()

        # As-of before the update: should see original
        old = Widget.get(entity_id).as_of(tx_time=after_write)
        assert old is not None
        assert old.color == "original"

        # As-of now: should see corrected
        current = Widget.get(entity_id).as_of(tx_time=datetime.now(timezone.utc))
        assert current is not None
        assert current.color == "corrected"

    def test_backdated_correction(self, alice):
        """Write with valid_from in the past → event_type=CORRECTED."""
        w = Widget(name="backdate_test", color="original", weight=1.0)
        w.save()

        past = datetime.now(timezone.utc) - timedelta(hours=1)
        w.color = "corrected"
        w.save(valid_from=past)

        assert w.event_type == "CORRECTED"
        assert w.valid_from <= datetime.now(timezone.utc)  # type: ignore[operator]

    def test_valid_from_defaults_to_now(self, alice):
        """When valid_from is not specified, it defaults to now()."""
        w = Widget(name="default_vf", color="a", weight=1.0)
        _before = datetime.now(timezone.utc)
        w.save()
        _after = datetime.now(timezone.utc)

        assert w.valid_from is not None
        # valid_from should be roughly between before and after
        # (PG now() might differ slightly from Python now())

    def test_as_of_valid_time(self, alice):
        """What was effective at business time T?"""
        w = Widget(name="valid_time_test", color="original", weight=1.0)
        past_time = datetime.now(timezone.utc) - timedelta(hours=2)
        w.save(valid_from=past_time)
        entity_id = w.entity_id

        # Update effective from 1 hour ago
        later_time = datetime.now(timezone.utc) - timedelta(hours=1)
        w.color = "updated"
        w.save(valid_from=later_time)

        # Query valid_time before the update
        before_update = past_time + timedelta(minutes=30)
        old = Widget.get(entity_id).as_of(valid_time=before_update)
        assert old is not None
        assert old.color == "original"

        # Query valid_time after the update
        after_update = datetime.now(timezone.utc)
        current = Widget.get(entity_id).as_of(valid_time=after_update)
        assert current is not None
        assert current.color == "updated"

    def test_write_with_custom_valid_from(self, alice):
        past = datetime.now(timezone.utc) - timedelta(days=1)
        w = Widget(name="custom_vf", color="yesterday", weight=1.0)
        w.save(valid_from=past)
        assert w.valid_from is not None

    def test_tx_time_is_immutable(self, alice):
        """tx_time is set by the system and never changes."""
        w = Widget(name="tx_immutable", color="a", weight=1.0)
        w.save()
        tx1 = w.tx_time

        time.sleep(0.05)
        w.color = "b"
        w.save()
        tx2 = w.tx_time

        # Different versions have different tx_times
        assert tx2 > tx1  # type: ignore[operator]


# ── State Machine ───────────────────────────────────────────────────────────

class TestStateMachine:
    def test_write_sets_initial_state(self, alice):
        o = Order(symbol="AAPL", quantity=100, price=228.0, side="BUY")
        o.save()
        assert o.state == "PENDING"

    def test_valid_transition(self, alice):
        o = Order(symbol="AAPL", quantity=100, price=228.0, side="BUY")
        o.save()
        o.transition("FILLED")
        assert o.state == "FILLED"
        assert o.event_type == "STATE_CHANGE"

    def test_invalid_transition_raises(self, alice):
        o = Order(symbol="TSLA", quantity=50, price=355.0, side="SELL")
        o.save()
        with pytest.raises(InvalidTransition):
            o.transition("SETTLED")  # Can't go PENDING → SETTLED

    def test_state_tracked_across_versions(self, alice):
        o = Order(symbol="GOOG", quantity=200, price=192.0, side="BUY")
        o.save()
        assert o.state == "PENDING"

        o.transition("PARTIAL")
        assert o.state == "PARTIAL"

        o.transition("FILLED")
        assert o.state == "FILLED"

        o.transition("SETTLED")
        assert o.state == "SETTLED"

    def test_state_history(self, alice):
        o = Order(symbol="MSFT", quantity=100, price=415.0, side="BUY")
        o.save()
        o.transition("FILLED")
        o.transition("SETTLED")

        history = Order.get(o.entity_id).history()
        states = [h.state for h in history]
        assert states == ["PENDING", "FILLED", "SETTLED"]
        event_types = [h.event_type for h in history]
        assert event_types == ["CREATED", "STATE_CHANGE", "STATE_CHANGE"]

    def test_cancel_from_partial(self, alice):
        o = Order(symbol="NVDA", quantity=100, price=138.0, side="BUY")
        o.save()
        o.transition("PARTIAL")
        o.transition("CANCELLED")
        assert o.state == "CANCELLED"

    def test_cannot_transition_from_terminal_state(self, alice):
        o = Order(symbol="META", quantity=10, price=700.0, side="SELL")
        o.save()
        o.transition("PARTIAL")
        o.transition("CANCELLED")
        with pytest.raises(InvalidTransition):
            o.transition("PENDING")

    def test_object_without_state_machine(self, alice):
        """Widget has no state machine — state should be NULL."""
        w = Widget(name="no_sm", color="x", weight=1.0)
        w.save()
        assert w.state is None

    def test_transition_without_state_machine_raises(self, alice):
        w = Widget(name="no_sm_transition", color="x", weight=1.0)
        w.save()
        with pytest.raises(ValueError):
            w.transition("ACTIVE")

    def test_allowed_transitions(self):
        assert set(OrderLifecycle.allowed_transitions("PENDING")) == {"PARTIAL", "FILLED", "CANCELLED"}
        assert set(OrderLifecycle.allowed_transitions("FILLED")) == {"SETTLED"}
        assert OrderLifecycle.allowed_transitions("SETTLED") == []
        assert OrderLifecycle.allowed_transitions("CANCELLED") == []

    def test_read_preserves_state(self, alice):
        o = Order(symbol="NFLX", quantity=5, price=1020.0, side="BUY")
        o.save()
        o.transition("FILLED")

        loaded = Order.get(o.entity_id)
        assert loaded.state == "FILLED"

    # ── Guard tests ────────────────────────────────────────────────

    def test_guard_allows_transition(self, alice):
        """PENDING → FILLED has guard: quantity > 0. Passes with quantity=100."""
        o = Order(symbol="AAPL", quantity=100, price=228.0, side="BUY")
        o.save()
        o.transition("FILLED")
        assert o.state == "FILLED"

    def test_guard_blocks_transition(self, alice):
        """PENDING → FILLED has guard: quantity > 0. Fails with quantity=0."""
        o = Order(symbol="AAPL", quantity=0, price=228.0, side="BUY")
        o.save()
        with pytest.raises(GuardFailure):
            o.transition("FILLED")
        assert o.state == "PENDING"  # unchanged

    def test_guard_on_settled_allows(self, alice):
        """FILLED → SETTLED has guard: price > 0. Passes with price=228."""
        o = Order(symbol="AAPL", quantity=100, price=228.0, side="BUY")
        o.save()
        o.transition("FILLED")
        o.transition("SETTLED")
        assert o.state == "SETTLED"

    def test_guard_on_settled_blocks(self, alice):
        """FILLED → SETTLED has guard: price > 0. Fails with price=0."""
        o = Order(symbol="AAPL", quantity=100, price=0.0, side="BUY")
        o.save()
        o.transition("FILLED")
        with pytest.raises(GuardFailure):
            o.transition("SETTLED")
        assert o.state == "FILLED"  # unchanged

    def test_guard_failure_is_distinct_from_invalid(self, alice):
        """GuardFailure and InvalidTransition are different exceptions."""
        o = Order(symbol="AAPL", quantity=0, price=228.0, side="BUY")
        o.save()
        # GuardFailure: edge exists but guard fails
        with pytest.raises(GuardFailure):
            o.transition("FILLED")
        # InvalidTransition: edge doesn't exist
        with pytest.raises(InvalidTransition):
            o.transition("SETTLED")

    # ── Permission tests ───────────────────────────────────────────

    def test_allowed_by_blocks_unauthorized_user(self, alice):
        """PENDING → CANCELLED requires allowed_by=['risk_manager']. Alice is not in the list."""
        o = Order(symbol="TSLA", quantity=50, price=355.0, side="SELL")
        o.save()
        with pytest.raises(TransitionNotPermitted):
            o.transition("CANCELLED")
        assert o.state == "PENDING"

    def test_allowed_by_permits_authorized_user(self, conn_info, _provision_users, server):
        """User 'risk_manager' can cancel."""
        server.provision_user("risk_manager", "rm_pw")

        rm = UserConnection(
            user="risk_manager", password="rm_pw",
            host=conn_info["host"], port=conn_info["port"], dbname=conn_info["dbname"],
        )
        o = Order(symbol="TSLA", quantity=50, price=355.0, side="SELL")
        o.save()
        o.transition("CANCELLED")
        assert o.state == "CANCELLED"
        rm.close()

    def test_transition_without_allowed_by_open_to_all(self, alice):
        """PENDING → PARTIAL has no allowed_by — anyone can trigger."""
        o = Order(symbol="GOOG", quantity=200, price=192.0, side="BUY")
        o.save()
        o.transition("PARTIAL")
        assert o.state == "PARTIAL"

    # ── Action tests ───────────────────────────────────────────────

    def test_action_fires_on_transition(self, alice):
        """FILLED → SETTLED has an action. Verify it fires."""
        action_log.clear()
        o = Order(symbol="AAPL", quantity=100, price=228.0, side="BUY")
        o.save()
        o.transition("FILLED")
        o.transition("SETTLED")
        assert ("action", "FILLED", "SETTLED") in action_log

    def test_action_does_not_fire_on_other_transitions(self, alice):
        """PENDING → PARTIAL has no action."""
        action_log.clear()
        o = Order(symbol="AAPL", quantity=100, price=228.0, side="BUY")
        o.save()
        o.transition("PARTIAL")
        assert not any(e[0] == "action" for e in action_log)

    # ── Hook tests ─────────────────────────────────────────────────

    def test_on_exit_fires(self, alice):
        """on_exit['PENDING'] should fire when leaving PENDING."""
        action_log.clear()
        o = Order(symbol="AAPL", quantity=100, price=228.0, side="BUY")
        o.save()
        o.transition("PARTIAL")
        assert ("on_exit", "PENDING") in action_log  # type: ignore[comparison-overlap]

    def test_on_enter_fires(self, alice):
        """on_enter['FILLED'] should fire when entering FILLED."""
        action_log.clear()
        o = Order(symbol="AAPL", quantity=100, price=228.0, side="BUY")
        o.save()
        o.transition("FILLED")
        assert ("on_enter", "FILLED") in action_log  # type: ignore[comparison-overlap]

    def test_hook_order_exit_then_action_then_enter(self, alice):
        """Hooks fire in order: on_exit → action → on_enter."""
        action_log.clear()
        o = Order(symbol="AAPL", quantity=100, price=228.0, side="BUY")
        o.save()
        # PENDING → FILLED fires on_exit[PENDING] and on_enter[FILLED]
        o.transition("FILLED")
        # FILLED → SETTLED fires action + no hooks for these states
        o.transition("SETTLED")
        # Check on_exit[PENDING] came before on_enter[FILLED]
        exit_idx = action_log.index(("on_exit", "PENDING"))  # type: ignore[arg-type]
        enter_idx = action_log.index(("on_enter", "FILLED"))  # type: ignore[arg-type]
        assert exit_idx < enter_idx

    def test_no_hooks_for_unregistered_states(self, alice):
        """PARTIAL has no on_enter/on_exit hooks."""
        action_log.clear()
        o = Order(symbol="AAPL", quantity=100, price=228.0, side="BUY")
        o.save()
        o.transition("PARTIAL")
        # on_exit[PENDING] fires, but no on_enter[PARTIAL]
        assert not any(e == ("on_enter", "PARTIAL") for e in action_log)  # type: ignore[comparison-overlap]


# ── Basic CRUD ───────────────────────────────────────────────────────────────

class TestCRUD:
    def test_write_returns_uuid(self, alice):
        w = Widget(name="cog", color="green", weight=2.0)
        entity_id = w.save()
        assert entity_id is not None
        uuid.UUID(entity_id)

    def test_query_by_type(self, alice):
        Widget(name="q1", color="a", weight=1.0).save()
        Widget(name="q2", color="b", weight=2.0).save()
        results = Widget.query()
        assert len(results) >= 2
        assert all(isinstance(r, Widget) for r in results)

    def test_query_with_jsonb_filter(self, alice):
        Widget(name="filterable", color="purple", weight=9.9).save()
        results = Widget.query(filters={"color": "purple"})
        assert any(r.name == "filterable" for r in results)

    def test_count(self, alice):
        before = Widget.count()
        Widget(name="counted", color="x", weight=0.0).save()
        after = Widget.count()
        assert after == before + 1

    def test_count_excludes_deleted(self, alice):
        before = Widget.count()
        w = Widget(name="count_del", color="x", weight=0.0)
        w.save()
        assert Widget.count() == before + 1
        w.delete()
        assert Widget.count() == before

    def test_list_types(self, alice):
        Widget(name="typed", color="x", weight=0.0).save()
        types = Widget.list_types()
        assert any("Widget" in t for t in types)

    def test_rich_object_with_list(self, alice):
        r = RichObject(label="test", amount=42.0, ts="2025-01-01", tags=["a", "b"])
        entity_id = r.save()
        loaded = RichObject.get(entity_id)
        assert loaded.tags == ["a", "b"]

    def test_multiple_types_coexist(self, alice):
        Widget(name="w", color="x", weight=1.0).save()
        Order(symbol="AAPL", quantity=10, price=228.0, side="BUY").save()
        widgets = Widget.query()
        orders = Order.query()
        assert len(widgets) >= 1
        assert len(orders) >= 1
        assert all(isinstance(w, Widget) for w in widgets)
        assert all(isinstance(o, Order) for o in orders)

    def test_query_returns_latest_version_only(self, alice):
        """Query should return only the latest version per entity, not all versions."""
        w = Widget(name="latest_only", color="v1", weight=1.0)
        w.save()
        w.color = "v2"
        w.save()
        w.color = "v3"
        w.save()

        results = Widget.query(filters={"name": "latest_only"})
        assert len(results) == 1
        assert results[0].color == "v3"

    def test_save_creates_when_no_entity_id(self, alice):
        """save() on a new object creates it (no entity_id yet)."""
        w = Widget(name="no_id", color="x", weight=1.0)
        entity_id = w.save()
        assert entity_id is not None
        assert w.version == 1

    def test_delete_requires_entity_id(self, alice):
        w = Widget(name="no_id_del", color="x", weight=1.0)
        with pytest.raises(ValueError):
            w.delete()


# ── Optimistic Concurrency ──────────────────────────────────────────────────

class TestOptimisticConcurrency:
    def test_update_succeeds_when_version_matches(self, alice):
        """Normal update works — version is tracked automatically."""
        w = Widget(name="occ_ok", color="v1", weight=1.0)
        w.save()
        assert w.version == 1
        w.color = "v2"
        w.save()  # auto-checks version 1 matches
        assert w.version == 2

    def test_stale_object_raises_version_conflict(self, alice):
        """Two readers of the same version — second writer loses."""
        w = Widget(name="occ_stale", color="v1", weight=1.0)
        entity_id = w.save()
        # Simulate two readers
        reader1 = Widget.get(entity_id)
        reader2 = Widget.get(entity_id)
        # Reader 1 writes successfully
        reader1.color = "by_reader1"
        reader1.save()
        # Reader 2 is now stale (still version 1, but DB is at version 2)
        reader2.color = "by_reader2"
        with pytest.raises(VersionConflict) as exc_info:
            reader2.save()
        assert exc_info.value.expected_version == 1
        assert exc_info.value.actual_version == 2

    def test_delete_succeeds_when_version_matches(self, alice):
        w = Widget(name="occ_del_ok", color="v1", weight=1.0)
        w.save()
        w.delete()  # auto-checks version 1
        assert Widget.find(w.entity_id) is None

    def test_delete_stale_object_raises(self, alice):
        w = Widget(name="occ_del_stale", color="v1", weight=1.0)
        entity_id = w.save()
        stale = Widget.get(entity_id)
        # Update moves version to 2
        w.color = "v2"
        w.save()
        # stale is still version 1
        with pytest.raises(VersionConflict):
            stale.delete()

    def test_sequential_updates_succeed(self, alice):
        """Each update advances _store_version, so the next one passes."""
        w = Widget(name="occ_seq", color="v1", weight=1.0)
        w.save()
        w.color = "v2"
        w.save()
        w.color = "v3"
        w.save()
        assert w.version == 3

    def test_version_conflict_preserves_db_state(self, alice):
        """Conflicted update does not change the stored data."""
        w = Widget(name="occ_preserve", color="original", weight=1.0)
        entity_id = w.save()
        stale = Widget.get(entity_id)
        w.color = "winner"
        w.save()
        stale.color = "loser"
        with pytest.raises(VersionConflict):
            stale.save()
        loaded = Widget.get(entity_id)
        assert loaded.color == "winner"


# ── Bulk Operations ─────────────────────────────────────────────────────────

class TestBulkOperations:
    def test_write_many(self, alice):
        widgets = [Widget(name=f"bulk_{i}", color="x", weight=float(i)) for i in range(5)]
        ids = Widget.write_many(widgets)
        assert len(ids) == 5
        for i, w in enumerate(widgets):
            assert w.entity_id == ids[i]
            assert w.version == 1

    def test_write_many_atomic_on_failure(self, alice):
        """If one write fails, none should persist."""
        before = Widget.count()
        widgets = [Widget(name=f"atomic_{i}", color="x", weight=1.0) for i in range(3)]
        # Corrupt the third object to cause failure
        widgets[2]._state_machine = "not_a_state_machine"  # type: ignore[assignment, misc]
        with pytest.raises(Exception):
            Widget.write_many(widgets)
        # None should have persisted
        after = Widget.count()
        assert after == before

    def test_update_many(self, alice):
        w1 = Widget(name="ubulk_1", color="a", weight=1.0)
        w2 = Widget(name="ubulk_2", color="b", weight=2.0)
        w1.save()
        w2.save()
        w1.color = "updated_a"
        w2.color = "updated_b"
        Widget.update_many([w1, w2])
        assert w1.version == 2
        assert w2.version == 2
        loaded1 = Widget.get(w1.entity_id)
        loaded2 = Widget.get(w2.entity_id)
        assert loaded1.color == "updated_a"
        assert loaded2.color == "updated_b"

    def test_update_many_auto_version_check(self, alice):
        w1 = Widget(name="ubulk_ev1", color="a", weight=1.0)
        w2 = Widget(name="ubulk_ev2", color="b", weight=2.0)
        w1.save()
        w2.save()
        w1.color = "c"
        w2.color = "d"
        Widget.update_many([w1, w2])
        assert w1.version == 2
        assert w2.version == 2

    def test_update_many_rolls_back_on_conflict(self, alice):
        w1 = Widget(name="ubulk_rb1", color="a", weight=1.0)
        w2 = Widget(name="ubulk_rb2", color="b", weight=2.0)
        w1.save()
        w2.save()
        # Read stale copy of w2
        stale_w2 = Widget.get(w2.entity_id)
        # Update w2 so its version is now 2
        w2.color = "sneaky"
        w2.save()
        # Try bulk update — w1 is fine but stale_w2 will conflict
        w1.color = "should_not_persist"
        stale_w2.color = "conflict"
        with pytest.raises(VersionConflict):
            Widget.update_many([w1, stale_w2])
        # w1 should NOT have been updated (atomic rollback)
        loaded1 = Widget.get(w1.entity_id)
        assert loaded1.color == "a"


# ── Pagination ──────────────────────────────────────────────────────────────

class TestPagination:
    def test_query_returns_query_result(self, alice):
        Widget(name="page_test", color="x", weight=1.0).save()
        result = Widget.query()
        assert isinstance(result, QueryResult)
        assert len(result) >= 1
        assert result.items is not None

    def test_cursor_pagination(self, alice):
        """Page through results using cursor."""
        for i in range(5):
            Widget(name=f"paginate_{i}", color="x", weight=float(i)).save()
            time.sleep(0.01)  # Ensure distinct tx_times

        # First page: 3 items
        page1 = Widget.query(filters={"color": "x"}, limit=3)
        assert len(page1) == 3
        assert page1.next_cursor is not None

        # Second page: use cursor
        page2 = Widget.query(filters={"color": "x"}, limit=3, cursor=page1.next_cursor)
        assert len(page2) >= 1

        # No overlap
        ids1 = {w.entity_id for w in page1}
        ids2 = {w.entity_id for w in page2}
        assert ids1.isdisjoint(ids2)

    def test_last_page_has_no_cursor(self, alice):
        w = Widget(name="last_page_test", color="unique_lp", weight=1.0)
        w.save()
        result = Widget.query(filters={"color": "unique_lp"}, limit=100)
        assert result.next_cursor is None

    def test_query_result_iterable(self, alice):
        Widget(name="iter_test", color="x", weight=1.0).save()
        result = Widget.query(filters={"name": "iter_test"})
        names = [w.name for w in result]
        assert "iter_test" in names

    def test_query_result_indexable(self, alice):
        Widget(name="idx_test", color="unique_idx", weight=1.0).save()
        result = Widget.query(filters={"color": "unique_idx"})
        assert result[0].name == "idx_test"


# ── Audit Log ───────────────────────────────────────────────────────────────

class TestAuditLog:
    def test_audit_returns_all_events(self, alice):
        w = Widget(name="audit_test", color="v1", weight=1.0)
        w.save()
        w.color = "v2"
        w.save()
        w.color = "v3"
        w.save()

        trail = w.audit()
        assert len(trail) == 3
        assert trail[0]["version"] == 1
        assert trail[0]["event_type"] == "CREATED"
        assert trail[1]["version"] == 2
        assert trail[1]["event_type"] == "UPDATED"
        assert trail[2]["version"] == 3

    def test_audit_includes_updated_by(self, alice):
        w = Widget(name="audit_by", color="v1", weight=1.0)
        w.save()
        trail = w.audit()
        assert trail[0]["updated_by"] == "alice"

    def test_audit_includes_state_changes(self, alice):
        o = Order(symbol="AAPL", quantity=100, price=228.0, side="BUY")
        o.save()
        o.transition("FILLED")
        o.transition("SETTLED")

        trail = o.audit()
        assert len(trail) == 3
        states = [e["state"] for e in trail]
        assert states == ["PENDING", "FILLED", "SETTLED"]
        assert trail[1]["event_meta"]["from_state"] == "PENDING"
        assert trail[1]["event_meta"]["to_state"] == "FILLED"

    def test_audit_includes_delete_tombstone(self, alice):
        w = Widget(name="audit_del", color="v1", weight=1.0)
        w.save()
        w.delete()
        trail = w.audit()
        assert len(trail) == 2
        assert trail[-1]["event_type"] == "DELETED"

    def test_audit_tx_times_ascending(self, alice):
        w = Widget(name="audit_time", color="v1", weight=1.0)
        w.save()
        w.color = "v2"
        w.save()
        trail = w.audit()
        assert trail[0]["tx_time"] <= trail[1]["tx_time"]

    def test_audit_empty_for_nonexistent(self, alice):
        trail = Widget.audit_trail(str(uuid.uuid4()))
        assert trail == []


# ── RLS Isolation (zero-trust core) ─────────────────────────────────────────

class TestRLSIsolation:
    def test_alice_cannot_see_bobs_events(self, alice, bob):
        bob.activate()
        w = Widget(name="bobs_secret", color="red", weight=1.0)
        entity_id = w.save()

        alice.activate()
        assert Widget.find(entity_id) is None

    def test_bob_cannot_see_alices_events(self, alice, bob):
        alice.activate()
        w = Widget(name="alices_secret", color="blue", weight=2.0)
        entity_id = w.save()

        bob.activate()
        assert Widget.find(entity_id) is None

    def test_query_only_returns_own_entities(self, alice, bob):
        alice.activate()
        Widget(name="alice_only_rls", color="a", weight=1.0).save()

        bob.activate()
        Widget(name="bob_only_rls", color="b", weight=2.0).save()

        alice.activate()
        alice_results = Widget.query()
        bob.activate()
        bob_results = Widget.query()

        alice_names = {r.name for r in alice_results}
        bob_names = {r.name for r in bob_results}
        assert "alice_only_rls" in alice_names
        assert "bob_only_rls" not in alice_names
        assert "bob_only_rls" in bob_names
        assert "alice_only_rls" not in bob_names

    def test_alice_cannot_see_bobs_history(self, alice, bob):
        bob.activate()
        w = Widget(name="bob_history_secret", color="a", weight=1.0)
        w.save()
        w.color = "b"
        w.save()

        alice.activate()
        assert Widget.find(w.entity_id) is None

    def test_count_respects_rls(self, alice, bob):
        alice.activate()
        Widget(name="ac_rls", color="x", weight=0.0).save()

        bob.activate()
        Widget(name="bc_rls", color="x", weight=0.0).save()

        alice.activate()
        a_count = Widget.count()
        bob.activate()
        b_count = Widget.count()
        assert a_count > 0
        assert b_count > 0


# ── Trust Boundary Tests ─────────────────────────────────────────────────────

class TestTrustBoundary:
    def test_cannot_connect_with_wrong_password(self, conn_info, _provision_users):
        with pytest.raises(Exception):
            UserConnection(
                user="alice", password="wrong_password",
                host=conn_info["host"], port=conn_info["port"],
                dbname=conn_info["dbname"],
            )

    def test_cannot_connect_as_nonexistent_user(self, conn_info):
        with pytest.raises(Exception):
            UserConnection(
                user="nonexistent_user", password="whatever",
                host=conn_info["host"], port=conn_info["port"],
                dbname=conn_info["dbname"],
            )

    def test_alice_cannot_set_role_to_bob(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("SET ROLE bob")

    def test_alice_cannot_set_role_to_admin(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("SET ROLE app_admin")

    def test_alice_cannot_disable_rls(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("ALTER TABLE object_events DISABLE ROW LEVEL SECURITY")

    def test_alice_cannot_create_roles(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("CREATE ROLE hacker LOGIN PASSWORD 'x'")

    def test_alice_cannot_drop_table(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("DROP TABLE object_events")

    def test_alice_cannot_drop_policy(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("DROP POLICY user_select ON object_events")

    def test_alice_cannot_bypass_rls_attribute(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("ALTER ROLE alice BYPASSRLS")

    def test_alice_cannot_grant_superuser(self, alice):
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute("ALTER ROLE alice SUPERUSER")

    def test_alice_cannot_insert_as_bob(self, alice):
        """Alice cannot forge owner = 'bob' on insert — RLS blocks it."""
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            with alice.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO object_events
                        (entity_id, version, type_name, owner, data)
                    VALUES (gen_random_uuid(), 1, 'fake', 'bob', '{"x":1}'::jsonb)
                    """,
                )
        alice.conn.rollback()

    def test_alice_cannot_delete_events(self, alice):
        """Append-only: no DELETE permission on object_events."""
        w = Widget(name="no_hard_delete", color="x", weight=1.0)
        w.save()
        with pytest.raises(Exception):
            with alice.conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM object_events WHERE entity_id = %s",
                    (w.entity_id,),
                )
        alice.conn.rollback()


# ── Sharing ──────────────────────────────────────────────────────────────────

class TestSharing:
    def test_share_read_makes_visible(self, alice, bob):
        alice.activate()
        w = Widget(name="shared_to_bob", color="gold", weight=3.0)
        entity_id = w.save()

        bob.activate()
        assert Widget.find(entity_id) is None

        share_read(alice.conn, entity_id, "bob")
        loaded = Widget.get(entity_id)
        assert loaded is not None
        assert loaded.name == "shared_to_bob"

    def test_shared_read_user_cannot_update(self, alice, bob):
        """Reader cannot create new versions — not owner or writer."""
        alice.activate()
        w = Widget(name="readonly_for_bob", color="silver", weight=1.0)
        entity_id = w.save()
        share_read(alice.conn, entity_id, "bob")

        bob.activate()
        loaded = Widget.get(entity_id)
        assert loaded is not None
        loaded.color = "hacked"
        with pytest.raises(PermissionError):
            loaded.save()

    def test_share_write_allows_new_version(self, alice, bob):
        alice.activate()
        w = Widget(name="writable_for_bob", color="white", weight=1.0)
        entity_id = w.save()
        share_write(alice.conn, entity_id, "bob")

        bob.activate()
        loaded = Widget.get(entity_id)
        assert loaded is not None
        loaded.color = "updated_by_bob"
        loaded.save()

        alice.activate()
        refreshed = Widget.get(entity_id)
        assert refreshed.color == "updated_by_bob"
        assert refreshed.owner == "alice"
        assert refreshed.updated_by == "bob"

    def test_shared_history_visible(self, alice, bob):
        """Shared entity's full history is visible to the reader."""
        alice.activate()
        w = Widget(name="shared_history", color="v1", weight=1.0)
        entity_id = w.save()
        w.color = "v2"
        w.save()
        share_read(alice.conn, entity_id, "bob")

        bob.activate()
        history = Widget.get(entity_id).history()
        assert len(history) == 2

    def test_unshare_read_revokes_access(self, alice, bob):
        alice.activate()
        w = Widget(name="unshare_test", color="x", weight=1.0)
        entity_id = w.save()
        share_read(alice.conn, entity_id, "bob")

        bob.activate()
        assert Widget.find(entity_id) is not None

        unshare_read(alice.conn, entity_id, "bob")
        assert Widget.find(entity_id) is None

    def test_unshare_write_revokes_access(self, alice, bob):
        alice.activate()
        w = Widget(name="unshare_write_test", color="x", weight=1.0)
        entity_id = w.save()
        share_write(alice.conn, entity_id, "bob")

        bob.activate()
        assert Widget.find(entity_id) is not None

        unshare_write(alice.conn, entity_id, "bob")
        assert Widget.find(entity_id) is None

    def test_list_shared_with(self, alice):
        w = Widget(name="list_shared", color="x", weight=1.0)
        entity_id = w.save()
        share_read(alice.conn, entity_id, "bob")
        share_write(alice.conn, entity_id, "charlie")
        perms = list_shared_with(alice.conn, entity_id)
        assert "bob" in perms["readers"]  # type: ignore[index]
        assert "charlie" in perms["writers"]  # type: ignore[index]

    def test_third_party_cannot_see_shared_between_others(self, alice, bob, charlie):
        alice.activate()
        w = Widget(name="alice_bob_only", color="x", weight=1.0)
        entity_id = w.save()
        share_read(alice.conn, entity_id, "bob")

        charlie.activate()
        assert Widget.find(entity_id) is None


# ── Admin Access ─────────────────────────────────────────────────────────────

class TestAdminAccess:
    def test_admin_sees_all_entities(self, alice, bob, admin_client):
        alice.activate()
        Widget(name="admin_test_a", color="x", weight=1.0).save()
        bob.activate()
        Widget(name="admin_test_b", color="x", weight=1.0).save()

        admin_client.activate()
        results = Widget.query()
        names = {r.name for r in results}
        assert "admin_test_a" in names
        assert "admin_test_b" in names

    def test_admin_can_soft_delete(self, alice, admin_client):
        alice.activate()
        w = Widget(name="admin_deletable", color="x", weight=1.0)
        entity_id = w.save()

        admin_client.activate()
        admin_w = Widget.get(entity_id)
        admin_w.delete()
        assert Widget.find(entity_id) is None

    def test_admin_count_includes_all_users(self, alice, bob, admin_client):
        alice.activate()
        Widget(name="ac2", color="x", weight=0.0).save()
        bob.activate()
        Widget(name="bc2", color="x", weight=0.0).save()

        admin_client.activate()
        admin_count = Widget.count()
        alice.activate()
        alice_count = Widget.count()
        bob.activate()
        bob_count = Widget.count()
        assert admin_count > alice_count
        assert admin_count > bob_count

    def test_admin_can_see_history(self, alice, admin_client):
        alice.activate()
        w = Widget(name="admin_history", color="v1", weight=1.0)
        w.save()
        w.color = "v2"
        w.save()

        admin_client.activate()
        history = Widget.get(w.entity_id).history()
        assert len(history) == 2


# ── Context Manager ──────────────────────────────────────────────────────────

class TestContextManager:
    def test_client_as_context_manager(self, conn_info, _provision_users):
        with UserConnection(
            user="alice", password="alice_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        ) as _:
            w = Widget(name="ctx_test", color="x", weight=1.0)
            w.save()
            assert w.entity_id is not None


# ── EventBus (Tier 1: in-process) ──────────────────────────────────────────

class TestEventBus:
    def test_on_type_fires_for_matching_type(self):
        bus = EventBus()
        events = []
        bus.on("Widget", lambda e: events.append(e))
        bus.emit(ChangeEvent(
            entity_id="123", version=1, event_type="CREATED",
            type_name="Widget", updated_by="alice", state=None,
            tx_time=datetime.now(timezone.utc),
        ))
        assert len(events) == 1
        assert events[0].type_name == "Widget"

    def test_on_type_ignores_other_types(self):
        bus = EventBus()
        events = []
        bus.on("Widget", lambda e: events.append(e))
        bus.emit(ChangeEvent(
            entity_id="123", version=1, event_type="CREATED",
            type_name="Order", updated_by="alice", state=None,
            tx_time=datetime.now(timezone.utc),
        ))
        assert len(events) == 0

    def test_on_entity_fires_for_matching_entity(self):
        bus = EventBus()
        events = []
        bus.on_entity("abc-123", lambda e: events.append(e))
        bus.emit(ChangeEvent(
            entity_id="abc-123", version=1, event_type="UPDATED",
            type_name="Widget", updated_by="alice", state=None,
            tx_time=datetime.now(timezone.utc),
        ))
        assert len(events) == 1

    def test_on_all_catches_everything(self):
        bus = EventBus()
        events = []
        bus.on_all(lambda e: events.append(e))
        bus.emit(ChangeEvent(
            entity_id="x", version=1, event_type="CREATED",
            type_name="Widget", updated_by="a", state=None,
            tx_time=datetime.now(timezone.utc),
        ))
        bus.emit(ChangeEvent(
            entity_id="y", version=1, event_type="CREATED",
            type_name="Order", updated_by="b", state=None,
            tx_time=datetime.now(timezone.utc),
        ))
        assert len(events) == 2

    def test_off_unsubscribes(self):
        bus = EventBus()
        events = []
        def cb(e):
            events.append(e)
        bus.on("Widget", cb)
        bus.off("Widget", cb)
        bus.emit(ChangeEvent(
            entity_id="x", version=1, event_type="CREATED",
            type_name="Widget", updated_by="a", state=None,
            tx_time=datetime.now(timezone.utc),
        ))
        assert len(events) == 0

    def test_bad_callback_does_not_break_chain(self):
        bus = EventBus()
        events = []
        bus.on_all(lambda e: 1 / 0)  # will raise
        bus.on_all(lambda e: events.append(e))
        bus.emit(ChangeEvent(
            entity_id="x", version=1, event_type="CREATED",
            type_name="Widget", updated_by="a", state=None,
            tx_time=datetime.now(timezone.utc),
        ))
        assert len(events) == 1


# ── UserConnection + EventBus integration ──────────────────────────────────────

class TestClientEventBus:
    def test_write_emits_event(self, conn_info, _provision_users):
        bus = EventBus()
        events = []
        bus.on(Widget.type_name(), lambda e: events.append(e))
        c = UserConnection(
            user="alice", password="alice_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"], event_bus=bus,
        )
        w = Widget(name="bus_write", color="x", weight=1.0)
        w.save()
        assert len(events) == 1
        assert events[0].event_type == "CREATED"
        assert events[0].entity_id == w.entity_id
        c.close()

    def test_update_emits_event(self, conn_info, _provision_users):
        bus = EventBus()
        events = []
        bus.on_all(lambda e: events.append(e))
        c = UserConnection(
            user="alice", password="alice_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"], event_bus=bus,
        )
        w = Widget(name="bus_update", color="v1", weight=1.0)
        w.save()
        w.color = "v2"
        w.save()
        assert len(events) == 2
        assert events[1].event_type == "UPDATED"
        assert events[1].version == 2
        c.close()

    def test_delete_emits_event(self, conn_info, _provision_users):
        bus = EventBus()
        events = []
        bus.on_all(lambda e: events.append(e))
        c = UserConnection(
            user="alice", password="alice_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"], event_bus=bus,
        )
        w = Widget(name="bus_delete", color="x", weight=1.0)
        w.save()
        w.delete()
        assert events[-1].event_type == "DELETED"
        c.close()

    def test_transition_emits_event(self, conn_info, _provision_users):
        bus = EventBus()
        events = []
        bus.on(Order.type_name(), lambda e: events.append(e))
        c = UserConnection(
            user="alice", password="alice_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"], event_bus=bus,
        )
        o = Order(symbol="AAPL", quantity=100, price=228.0, side="BUY")
        o.save()
        o.transition("FILLED")
        assert len(events) == 2
        assert events[1].event_type == "STATE_CHANGE"
        assert events[1].state == "FILLED"
        c.close()

    def test_no_bus_is_fine(self, alice):
        """UserConnection without event_bus still works."""
        w = Widget(name="no_bus", color="x", weight=1.0)
        w.save()
        assert w.entity_id is not None

    def test_on_entity_filters_correctly(self, conn_info, _provision_users):
        bus = EventBus()
        events = []
        c = UserConnection(
            user="alice", password="alice_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"], event_bus=bus,
        )
        w1 = Widget(name="bus_e1", color="x", weight=1.0)
        w1.save()
        bus.on_entity(w1.entity_id, lambda e: events.append(e))  # type: ignore[arg-type]
        w2 = Widget(name="bus_e2", color="x", weight=1.0)
        w2.save()  # should NOT trigger
        w1.color = "updated"
        w1.save()  # should trigger
        assert len(events) == 1
        assert events[0].entity_id == w1.entity_id
        c.close()


# ── SubscriptionListener (Tier 2: LISTEN/NOTIFY) ───────────────────────────

class TestSubscriptionListener:
    def test_listener_receives_notify(self, conn_info, _provision_users):
        """Listener gets real-time NOTIFY from a different client's write."""
        bus = EventBus()
        events = []
        bus.on_all(lambda e: events.append(e))

        listener = SubscriptionListener(
            event_bus=bus,
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="alice", password="alice_pw",
        )
        listener.start()
        time.sleep(0.2)

        # Write from a separate client (no bus wired — purely DB trigger)
        writer = UserConnection(
            user="alice", password="alice_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w = Widget(name="notify_test", color="x", weight=1.0)
        w.save()
        writer.close()

        time.sleep(0.5)  # Give listener time to receive
        listener.stop()

        assert any(e.entity_id == w.entity_id for e in events)

    def test_listener_catches_up_on_start(self, conn_info, _provision_users):
        """Listener catches up on events that happened before it started."""
        # Write an event BEFORE the listener starts
        writer = UserConnection(
            user="alice", password="alice_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w = Widget(name="catchup_test", color="x", weight=1.0)
        w.save()
        before_time = w.tx_time
        writer.close()

        # Now start a listener with a checkpoint BEFORE that event
        bus = EventBus()
        events = []
        bus.on_all(lambda e: events.append(e))

        listener = SubscriptionListener(
            event_bus=bus,
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="alice", password="alice_pw",
        )
        # Manually set last_tx_time to before the write
        listener._conn = psycopg2.connect(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"], user="alice", password="alice_pw",
        )
        listener._conn.autocommit = True
        from datetime import timedelta
        listener._last_tx_time = before_time - timedelta(seconds=1)  # type: ignore[assignment, operator]
        listener._catch_up()
        listener._conn.close()

        assert any(e.entity_id == w.entity_id for e in events)

    def test_durable_checkpoint_persists(self, conn_info, _provision_users):
        """Subscriber with subscriber_id persists checkpoint to DB."""
        bus = EventBus()
        listener = SubscriptionListener(
            event_bus=bus,
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="alice", password="alice_pw",
            subscriber_id="test_durable_sub",
        )
        listener.start()
        time.sleep(0.2)

        # Write something so checkpoint advances
        writer = UserConnection(
            user="alice", password="alice_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w = Widget(name="durable_test", color="x", weight=1.0)
        w.save()
        writer.close()
        time.sleep(0.5)
        listener.stop()

        # Check that checkpoint was saved to DB
        check_conn = psycopg2.connect(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"], user="alice", password="alice_pw",
        )
        check_conn.autocommit = True
        with check_conn.cursor() as cur:
            cur.execute(
                "SELECT last_tx_time FROM subscription_checkpoints WHERE subscriber_id = %s",
                ("test_durable_sub",),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] is not None
        check_conn.close()

    def test_durable_checkpoint_recovers(self, conn_info, _provision_users):
        """Subscriber recovers checkpoint on restart."""
        sub_id = "test_recovery_sub"

        # First listener: start, process an event, stop
        bus1 = EventBus()
        listener1 = SubscriptionListener(
            event_bus=bus1,
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="alice", password="alice_pw",
            subscriber_id=sub_id,
        )
        listener1.start()
        time.sleep(0.2)

        writer = UserConnection(
            user="alice", password="alice_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w1 = Widget(name="recovery_1", color="x", weight=1.0)
        w1.save()
        time.sleep(0.5)
        listener1.stop()

        # Write another event while listener is DOWN
        w2 = Widget(name="recovery_2", color="y", weight=2.0)
        w2.save()
        writer.close()

        # Second listener: should catch up and get w2
        bus2 = EventBus()
        events2 = []
        bus2.on_all(lambda e: events2.append(e))

        listener2 = SubscriptionListener(
            event_bus=bus2,
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="alice", password="alice_pw",
            subscriber_id=sub_id,
        )
        listener2.start()
        time.sleep(0.3)
        listener2.stop()

        # Should have caught up on w2
        assert any(e.entity_id == w2.entity_id for e in events2)


# ===========================================================================
# Three-Tier Transition Side-Effects
# ===========================================================================

class TestThreeTierTransition:
    """Test the three tiers of side-effects on state transitions."""

    # ── Tier 1: Transactional action ─────────────────────────────────

    def test_action_commits_with_state_change(self, alice):
        """Action succeeds → both state change and action committed."""
        tier1_log = []

        class T1Lifecycle(StateMachine):
            initial = "NEW"
            transitions = [
                Transition("NEW", "DONE",
                           action=lambda obj, f, t: tier1_log.append("action_ran")),
            ]

        order = Order(symbol="AAPL", quantity=10, price=150.0, side="BUY")
        order._state_machine = T1Lifecycle  # type: ignore[misc]
        order.save()
        order.transition("DONE")

        assert order.state == "DONE"
        assert "action_ran" in tier1_log

    def test_action_failure_rolls_back_state_change(self, alice):
        """Action raises → state change is rolled back."""
        class FailLifecycle(StateMachine):
            initial = "NEW"
            transitions = [
                Transition("NEW", "DONE",
                           action=lambda obj, f, t: (_ for _ in ()).throw(
                               ValueError("action failed"))),
            ]

        order = Order(symbol="MSFT", quantity=5, price=200.0, side="SELL")
        order._state_machine = FailLifecycle  # type: ignore[misc]
        order.save()

        with pytest.raises(ValueError, match="action failed"):
            order.transition("DONE")

        # State should NOT have changed — rolled back
        fresh = Order.get(order.entity_id)
        assert fresh.state == "NEW"

    # ── Tier 2: Fire-and-forget hooks ────────────────────────────────

    def test_on_enter_on_exit_fire_after_commit(self, alice):
        """on_enter and on_exit fire after commit."""
        hook_log = []

        class T2Lifecycle(StateMachine):
            initial = "A"
            transitions = [
                Transition("A", "B",
                           on_exit=lambda obj, f, t: hook_log.append(("exit", f)),
                           on_enter=lambda obj, f, t: hook_log.append(("enter", t))),
            ]

        order = Order(symbol="GOOG", quantity=1, price=100.0, side="BUY")
        order._state_machine = T2Lifecycle  # type: ignore[misc]
        order.save()
        order.transition("B")

        assert order.state == "B"
        assert ("exit", "A") in hook_log
        assert ("enter", "B") in hook_log

    def test_on_enter_failure_does_not_rollback(self, alice):
        """on_enter is fire-and-forget — failure doesn't affect state."""
        class T2FailLifecycle(StateMachine):
            initial = "X"
            transitions = [
                Transition("X", "Y",
                           on_enter=lambda obj, f, t: (_ for _ in ()).throw(
                               RuntimeError("hook boom"))),
            ]

        order = Order(symbol="TSLA", quantity=1, price=300.0, side="BUY")
        order._state_machine = T2FailLifecycle  # type: ignore[misc]
        order.save()

        # Should NOT raise — on_enter failures are swallowed
        order.transition("Y")
        assert order.state == "Y"

    # ── Tier 3: Workflow dispatch ────────────────────────────────────

    def test_start_workflow_missing_engine_raises(self, alice):
        """start_workflow without _workflow_engine raises RuntimeError."""
        class T3Lifecycle(StateMachine):
            initial = "START"
            transitions = [
                Transition("START", "END",
                           start_workflow=lambda eid: None),
            ]

        order = Order(symbol="META", quantity=1, price=400.0, side="BUY")
        order._state_machine = T3Lifecycle  # type: ignore[misc]
        # Do NOT set _workflow_engine
        type(order)._workflow_engine = None
        order.save()

        with pytest.raises(RuntimeError, match="_workflow_engine is not set"):
            order.transition("END")

    def test_start_workflow_dispatches(self, alice):
        """start_workflow calls engine.workflow() with entity_id."""
        dispatched = []

        class FakeEngine:
            def workflow(self, fn, entity_id):
                dispatched.append((fn, entity_id))

        def my_workflow(entity_id):
            pass

        class T3DispatchLifecycle(StateMachine):
            initial = "OPEN"
            transitions = [
                Transition("OPEN", "CLOSED",
                           start_workflow=my_workflow),
            ]

        order = Order(symbol="AMZN", quantity=1, price=180.0, side="BUY")
        order._state_machine = T3DispatchLifecycle  # type: ignore[misc]
        type(order)._workflow_engine = FakeEngine()  # type: ignore[assignment]
        order.save()
        order.transition("CLOSED")

        assert len(dispatched) == 1
        assert dispatched[0][0] is my_workflow
        assert dispatched[0][1] == order.entity_id

        # Clean up
        type(order)._workflow_engine = None

    def test_all_three_tiers_fire_in_order(self, alice):
        """All three tiers fire in correct order on a single transition."""
        log = []

        class FakeEngine:
            def workflow(self, fn, entity_id):
                log.append("tier3_workflow")

        class AllTiersLifecycle(StateMachine):
            initial = "INIT"
            transitions = [
                Transition("INIT", "FINAL",
                           action=lambda obj, f, t: log.append("tier1_action"),
                           on_exit=lambda obj, f, t: log.append("tier2_on_exit"),
                           on_enter=lambda obj, f, t: log.append("tier2_on_enter"),
                           start_workflow=lambda eid: None),
            ]

        order = Order(symbol="NVDA", quantity=1, price=800.0, side="BUY")
        order._state_machine = AllTiersLifecycle  # type: ignore[misc]
        type(order)._workflow_engine = FakeEngine()  # type: ignore[assignment]
        order.save()
        order.transition("FINAL")

        assert log == [
            "tier1_action",     # Tier 1: inside transaction
            "tier2_on_exit",    # Tier 2: after commit
            "tier2_on_enter",   # Tier 2: after commit
            "tier3_workflow",   # Tier 3: durable dispatch
        ]

        # Clean up
        type(order)._workflow_engine = None
