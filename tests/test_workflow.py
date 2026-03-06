"""
Tests for the workflow orchestration layer.

Covers:
- WorkflowEngine ABC contract
- DBOSEngine: lifecycle, workflows, steps, queues, send/recv, status
- Integration with UserConnection (durable multi-step mutations)
"""

import tempfile
import time
from dataclasses import dataclass

import pytest
from store.admin import StoreServer
from store.base import Storable
from store.connection import UserConnection
from store.state_machine import StateMachine, Transition
from workflow.engine import WorkflowEngine, WorkflowHandle, WorkflowStatus
from workflow.factory import create_engine

# ---------------------------------------------------------------------------
# Module-level references (used by workflow functions via closures)
# ---------------------------------------------------------------------------

_engine: WorkflowEngine | None = None
_client: UserConnection | None = None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def server():
    tmp_dir = tempfile.mkdtemp(prefix="test_workflow_")
    srv = StoreServer(data_dir=tmp_dir, admin_password="test_admin_pw")
    srv.start()
    yield srv
    srv.stop()


@pytest.fixture(scope="module")
def engine(server):
    global _engine
    eng = create_engine(server.pg_url(), name="test-workflow")
    eng.launch()
    _engine = eng
    yield eng
    eng.destroy()
    _engine = None


@pytest.fixture(scope="module")
def conn_info(server):
    return server.conn_info()


@pytest.fixture(scope="module")
def _provision_users(server):
    server.provision_user("wf_alice", "pass_alice")


@pytest.fixture(scope="module")
def client(conn_info, _provision_users):
    global _client
    c = UserConnection(
        user="wf_alice", password="pass_alice",
        host=conn_info["host"], port=conn_info["port"], dbname=conn_info["dbname"],
    )
    _client = c
    yield c
    c.close()
    _client = None


# ---------------------------------------------------------------------------
# Test models
# ---------------------------------------------------------------------------

@dataclass
class Counter(Storable):
    name: str = ""
    value: float = 0


class TicketLifecycle(StateMachine):
    initial = "ACTIVE"
    transitions = [
        Transition("ACTIVE", "IN_PROGRESS"),
        Transition("ACTIVE", "CLOSED"),
        Transition("IN_PROGRESS", "CLOSED"),
    ]


@dataclass
class Ticket(Storable):
    title: str = ""
    status: str = "OPEN"

Ticket._state_machine = TicketLifecycle


# ---------------------------------------------------------------------------
# WorkflowEngine ABC
# ---------------------------------------------------------------------------

class TestWorkflowEngineABC:
    """WorkflowEngine cannot be instantiated directly."""

    def test_abc_not_instantiable(self):
        with pytest.raises(TypeError):
            WorkflowEngine()  # type: ignore[abstract]

    def test_handle_dataclass(self):
        class DummyEngine(WorkflowEngine):
            def workflow(self, fn, *a, **kw): ...
            def step(self, fn, *a, **kw): ...
            def queue(self, name, fn, *a, **kw): ...  # type: ignore[override]
            def sleep(self, s): ...
            def send(self, wid, topic, val): ...
            def recv(self, topic, timeout=None): ...
            def get_workflow_status(self, wid): return WorkflowStatus.SUCCESS
            def get_workflow_result(self, wid, *, timeout=None): return 42

        eng = DummyEngine()
        h = WorkflowHandle(workflow_id="test-123", _engine=eng)
        assert h.workflow_id == "test-123"
        assert h.get_status() == WorkflowStatus.SUCCESS
        assert h.get_result() == 42


# ---------------------------------------------------------------------------
# DBOSEngine lifecycle
# ---------------------------------------------------------------------------

class TestDBOSEngineLifecycle:
    """Engine launch, destroy, context manager."""

    def test_engine_is_workflow_engine(self, engine):
        assert isinstance(engine, WorkflowEngine)

    def test_engine_launched(self, engine):
        assert engine._launched is True


# ---------------------------------------------------------------------------
# Workflows and steps (using engine.run for sync execution)
# ---------------------------------------------------------------------------

class TestWorkflowsAndSteps:
    """Durable workflows with checkpointed steps."""

    def test_simple_workflow(self, engine):
        def add_one(x):
            return x + 1

        def double(x):
            return x * 2

        def my_workflow(val):
            a = _engine.step(add_one, val)  # type: ignore[union-attr]
            b = _engine.step(double, a)  # type: ignore[union-attr]
            return b

        result = engine.run(my_workflow, 5)
        assert result == 12  # (5+1)*2

    def test_workflow_with_serializable_args(self, engine):
        """workflow() with only serializable args returns a handle."""
        def noop():
            return "done"

        handle = engine.workflow(noop)
        result = handle.get_result()
        assert result == "done"

    def test_workflow_handle_status(self, engine):
        def noop():
            return "ok"

        handle = engine.workflow(noop)
        handle.get_result()
        status = handle.get_status()
        assert status == WorkflowStatus.SUCCESS

    def test_step_return_values(self, engine):
        def step_a():
            return "hello"

        def step_b(greeting):
            return f"{greeting} world"

        def pipeline():
            a = _engine.step(step_a)  # type: ignore[union-attr]
            b = _engine.step(step_b, a)  # type: ignore[union-attr]
            return b

        result = engine.run(pipeline)
        assert result == "hello world"

    def test_workflow_with_dict_result(self, engine):
        def build_report():
            count = _engine.step(lambda: 42)  # type: ignore[union-attr]
            label = _engine.step(lambda: "items")  # type: ignore[union-attr]
            return {"count": count, "label": label}

        result = engine.run(build_report)
        assert result == {"count": 42, "label": "items"}

    def test_multiple_steps_ordered(self, engine):
        log = []

        def append_step(val):
            log.append(val)
            return val

        def ordered_workflow():
            _engine.step(append_step, "first")  # type: ignore[union-attr]
            _engine.step(append_step, "second")  # type: ignore[union-attr]
            _engine.step(append_step, "third")  # type: ignore[union-attr]
            return log.copy()

        result = engine.run(ordered_workflow)
        assert result == ["first", "second", "third"]


# ---------------------------------------------------------------------------
# Queues
# ---------------------------------------------------------------------------

class TestQueues:
    """Durable queues with concurrency control."""

    def test_queue_single_task(self, engine):
        def compute(x):
            return x ** 2

        handle = engine.queue("test-queue", compute, 7)
        assert handle.get_result() == 49

    def test_queue_multiple_tasks(self, engine):
        def square(x):
            return x * x

        handles = [engine.queue("batch-queue", square, i) for i in range(5)]
        results = sorted([h.get_result() for h in handles])
        assert results == [0, 1, 4, 9, 16]


# ---------------------------------------------------------------------------
# Send / Recv
# ---------------------------------------------------------------------------

class TestSendRecv:
    """Inter-workflow notification messaging."""

    def test_send_recv(self, engine):
        def waiter_workflow():
            msg = _engine.recv("test-topic", timeout=10)  # type: ignore[union-attr]
            return msg

        handle = engine.workflow(waiter_workflow)
        # Give the workflow a moment to start and begin waiting
        time.sleep(0.3)

        engine.send(handle.workflow_id, "test-topic", {"key": "value"})
        result = handle.get_result(timeout=10)
        assert result == {"key": "value"}

    def test_recv_timeout_returns_none(self, engine):
        def timeout_workflow():
            return _engine.recv("no-one-sends", timeout=1)  # type: ignore[union-attr]

        handle = engine.workflow(timeout_workflow)
        result = handle.get_result(timeout=5)
        assert result is None


# ---------------------------------------------------------------------------
# UserConnection integration
# ---------------------------------------------------------------------------

class TestStoreIntegration:
    """Workflows that perform durable store mutations."""

    def test_multi_step_store_workflow(self, engine, client):
        def create_counter(name, initial):
            c = Counter(name=name, value=initial)
            c.save()  # type: ignore[union-attr]
            return c.entity_id

        def increment_counter(entity_id, amount):
            c = Counter.find(entity_id)  # type: ignore[union-attr]
            c.value += amount  # type: ignore[union-attr]
            c.save()  # type: ignore[union-attr]
            return c.value  # type: ignore[union-attr]

        def create_and_increment():
            eid = _engine.step(create_counter, "hits", 0)  # type: ignore[union-attr]
            _v1 = _engine.step(increment_counter, eid, 10)  # type: ignore[union-attr]
            v2 = _engine.step(increment_counter, eid, 5)  # type: ignore[union-attr]
            return {"entity_id": eid, "final_value": v2}

        result = engine.run(create_and_increment)
        assert result["final_value"] == 15

        # Verify in store
        obj = Counter.get(result["entity_id"])
        assert obj.value == 15
        assert obj.name == "hits"

    def test_workflow_with_error_in_step(self, engine, client):
        def create_ticket():
            t = Ticket(title="Bug report")
            t.save()  # type: ignore[union-attr]
            return t.entity_id

        def failing_step():
            raise ValueError("intentional failure")

        def flawed_workflow():
            eid = _engine.step(create_ticket)  # type: ignore[union-attr]
            _engine.step(failing_step)  # type: ignore[union-attr]  # this will fail
            return eid

        with pytest.raises(Exception):
            engine.run(flawed_workflow)


# ---------------------------------------------------------------------------
# Durable transition (engine convenience + WorkflowDispatcher)
# ---------------------------------------------------------------------------

class TestDurableTransition:
    """engine.durable_transition() and WorkflowDispatcher.durable_transition()."""

    def test_engine_durable_transition(self, engine, client):
        """engine.durable_transition() transitions state via checkpointed step."""
        t = Ticket(title="durable_ticket")
        t.save()
        t = Ticket.get(t.entity_id)  # type: ignore[arg-type]
        assert t.state == "ACTIVE"

        def transition_workflow():
            _engine.durable_transition(t, "CLOSED")  # type: ignore[union-attr]

        engine.run(transition_workflow)
        refreshed = Ticket.get(t.entity_id)  # type: ignore[arg-type]
        assert refreshed.state == "CLOSED"

    def test_dispatcher_durable_transition(self, engine, client):
        """WorkflowDispatcher wraps transition in a checkpointed step."""
        from workflow.dispatcher import WorkflowDispatcher

        dispatcher = WorkflowDispatcher(engine)
        t = Ticket(title="dispatch_ticket")
        t.save()
        t = Ticket.get(t.entity_id)  # type: ignore[arg-type]
        assert t.state == "ACTIVE"

        def dispatch_workflow():
            dispatcher.durable_transition(t, "CLOSED")

        engine.run(dispatch_workflow)
        refreshed = Ticket.get(t.entity_id)  # type: ignore[arg-type]
        assert refreshed.state == "CLOSED"
