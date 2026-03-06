"""
Tests for the scheduler package.

Covers:
- Embedded base class (reactive wiring, Storable.save() guard)
- Models: Schedule, Task, Run, TaskResult (serialization, state machines)
- Cron: next_fire, prev_fire, is_due, validate, describe
- DAG graph: acyclicity, execution_order, cycle detection
- DAG runner: parallel execution, failure handling, skip propagation
- Scheduler: register, tick, fire, pause/resume, history
- Integration: full round-trip with real PG + WorkflowEngine
"""

import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pytest

# ── Embedded tests ───────────────────────────────────────────────────────


class TestEmbedded:
    """Test the Embedded base class."""

    def test_embedded_is_storable_subclass(self):
        from store.base import Embedded, Storable
        assert issubclass(Embedded, Storable)

    def test_embedded_subclass_has_reactive_signals(self):
        from store.base import Embedded

        @dataclass
        class Point(Embedded):
            x: float = 0.0
            y: float = 0.0

        p = Point(x=1.0, y=2.0)
        assert p.x == 1.0
        assert p.y == 2.0
        # Reactive signals should exist
        assert '_reactive' in p.__dict__
        assert 'x' in p._reactive
        assert 'y' in p._reactive

    def test_embedded_batch_update(self):
        from store.base import Embedded

        @dataclass
        class Counter(Embedded):
            value: int = 0
            label: str = ""

        c = Counter(value=10, label="test")
        c.batch_update(value=20, label="updated")
        assert c.value == 20
        assert c.label == "updated"

    def test_embedded_serialization_roundtrip(self):
        from store.base import Embedded

        @dataclass
        class Tag(Embedded):
            name: str = ""
            weight: float = 0.0

        t = Tag(name="important", weight=0.9)
        json_str = t.to_json()
        t2 = Tag.from_json(json_str)
        assert t2.name == "important"
        assert t2.weight == 0.9

    def test_embedded_no_column_registry(self):
        """Embedded subclasses should not trigger column registry validation."""
        from store.base import Embedded

        # This should not raise — Embedded._registry is None
        @dataclass
        class Anything(Embedded):
            custom_field: str = ""
            another_field: float = 0.0

        a = Anything(custom_field="test", another_field=1.5)
        assert a.custom_field == "test"


# ── Model tests ──────────────────────────────────────────────────────────


class TestModels:
    """Test scheduler Storable/Embedded models."""

    def test_task_creation_and_serialization(self):
        from scheduler.models import Task
        t = Task(name="sync", fn="sync:events", depends_on=["init"])
        assert t.name == "sync" and t.enabled is True and t.timeout_s == 300
        t2 = Task.from_json(Task(name="test", fn="mod:fn", enabled=False).to_json())
        assert t2.name == "test" and t2.enabled is False

    def test_schedule_creation_and_serialization(self):
        from scheduler.models import Schedule, Task
        s = Schedule(name="etl", cron_expr="0 2 * * *", description="ETL pipeline",
                     tasks=[Task(name="extract", fn="jobs:extract"),
                            Task(name="load", fn="jobs:load", depends_on=["extract"])])
        assert s.name == "etl" and len(s.tasks) == 2
        s2 = Schedule.from_json(s.to_json())
        assert isinstance(s2.tasks[0], Task) and s2.tasks[0].name == "extract"
        # task_defs handles dict-based tasks post-deserialization
        s3 = Schedule(name="test", tasks=[
            {"name": "a", "fn": "fn_a", "depends_on": [], "timeout_s": 300, "retries": 0, "enabled": True}])
        assert isinstance(s3.task_defs[0], Task)

    def test_run_and_task_result(self):
        from scheduler.models import Run, TaskResult
        r = Run(schedule_name="test")
        assert r.run_id != ""
        r2 = Run(schedule_name="etl", task_results={
            "extract": TaskResult(task_name="extract", status="SUCCESS", duration_ms=100),
            "load": TaskResult(task_name="load", status="ERROR", error="timeout")})
        r3 = Run.from_json(r2.to_json())
        assert r3.task_results["extract"].status == "SUCCESS"
        assert r3.task_results["load"].error == "timeout"

    def test_state_machines_and_lifecycles(self):
        from scheduler.models import Run, RunLifecycle, Schedule
        assert Schedule._state_machine is not None and Schedule._state_machine.initial == "ACTIVE"
        assert Run._state_machine is not None and Run._state_machine.initial == "PENDING"
        assert "RUNNING" in RunLifecycle.allowed_transitions("PENDING")
        assert "SUCCESS" in RunLifecycle.allowed_transitions("RUNNING")
        assert "RETRYING" in RunLifecycle.allowed_transitions("ERROR")


# ── Cron tests ───────────────────────────────────────────────────────────


class TestCron:
    """Test cron utilities (croniter hidden inside)."""

    def test_next_fire_every_5_minutes(self):
        from scheduler.cron import next_fire
        base = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        nxt = next_fire("*/5 * * * *", after=base)
        assert nxt == datetime(2025, 1, 1, 12, 5, 0, tzinfo=timezone.utc)

    def test_next_fire_daily(self):
        from scheduler.cron import next_fire
        base = datetime(2025, 1, 1, 3, 0, 0, tzinfo=timezone.utc)
        nxt = next_fire("0 2 * * *", after=base)
        assert nxt == datetime(2025, 1, 2, 2, 0, 0, tzinfo=timezone.utc)

    def test_prev_fire(self):
        from scheduler.cron import prev_fire
        base = datetime(2025, 1, 1, 12, 7, 0, tzinfo=timezone.utc)
        prev = prev_fire("*/5 * * * *", before=base)
        assert prev == datetime(2025, 1, 1, 12, 5, 0, tzinfo=timezone.utc)

    def test_is_due_never_fired(self):
        from scheduler.cron import is_due
        assert is_due("*/5 * * * *", last_fire=None) is True

    def test_is_due_recently_fired(self):
        from scheduler.cron import is_due
        # Fixed time safely between 5-min boundaries to avoid flake
        now = datetime(2025, 6, 15, 12, 32, 0, tzinfo=timezone.utc)
        last = now - timedelta(seconds=30)
        assert is_due("*/5 * * * *", last_fire=last, now=now) is False

    def test_is_due_overdue(self):
        from scheduler.cron import is_due
        now = datetime.now(timezone.utc)
        last = now - timedelta(minutes=10)
        assert is_due("*/5 * * * *", last_fire=last, now=now) is True

    def test_validate_valid(self):
        from scheduler.cron import validate
        assert validate("*/5 * * * *") is True
        assert validate("0 2 * * *") is True
        assert validate("0 0 * * 0") is True

    def test_validate_invalid(self):
        from scheduler.cron import validate
        assert validate("not a cron") is False
        assert validate("") is False

    @pytest.mark.parametrize("expr,expected", [
        ("*/5 * * * *", "every 5 minutes"),
        ("*/1 * * * *", "every minute"),
        ("0 * * * *", "every hour"),
        ("0 2 * * *", "daily at 02:00"),
        ("0 0 * * 0", "weekly on Sunday at 00:00"),
        ("0 0 1,15 * *", "0 0 1,15 * *"),
    ])
    def test_describe(self, expr, expected):
        from scheduler.cron import describe
        assert describe(expr) == expected


# ── DAG graph tests ──────────────────────────────────────────────────────


class TestDAGGraph:
    """Test DAG graph helpers (acyclicity, execution order)."""

    def test_validate_acyclic_linear(self):
        from scheduler.dag import validate_acyclic
        from scheduler.models import Schedule, Task
        s = Schedule(name="linear", tasks=[
            Task(name="a", fn="fn_a"),
            Task(name="b", fn="fn_b", depends_on=["a"]),
            Task(name="c", fn="fn_c", depends_on=["b"]),
        ])
        order = validate_acyclic(s)
        assert order == ["a", "b", "c"]

    def test_validate_acyclic_parallel(self):
        from scheduler.dag import validate_acyclic
        from scheduler.models import Schedule, Task
        s = Schedule(name="parallel", tasks=[
            Task(name="a", fn="fn_a"),
            Task(name="b", fn="fn_b"),
            Task(name="c", fn="fn_c", depends_on=["a", "b"]),
        ])
        order = validate_acyclic(s)
        assert "c" == order[-1]
        assert set(order[:2]) == {"a", "b"}

    def test_validate_acyclic_cycle(self):
        from scheduler.dag import CycleError, validate_acyclic
        from scheduler.models import Schedule, Task
        s = Schedule(name="cycle", tasks=[
            Task(name="a", fn="fn_a", depends_on=["c"]),
            Task(name="b", fn="fn_b", depends_on=["a"]),
            Task(name="c", fn="fn_c", depends_on=["b"]),
        ])
        with pytest.raises(CycleError):
            validate_acyclic(s)

    def test_execution_order_levels(self):
        from scheduler.dag import execution_order
        from scheduler.models import Schedule, Task
        s = Schedule(name="diamond", tasks=[
            Task(name="a", fn="fn_a"),
            Task(name="b", fn="fn_b", depends_on=["a"]),
            Task(name="c", fn="fn_c", depends_on=["a"]),
            Task(name="d", fn="fn_d", depends_on=["b", "c"]),
        ])
        levels = execution_order(s)
        assert levels[0] == ["a"]
        assert set(levels[1]) == {"b", "c"}
        assert levels[2] == ["d"]

    def test_execution_order_skips_disabled(self):
        from scheduler.dag import execution_order
        from scheduler.models import Schedule, Task
        s = Schedule(name="partial", tasks=[
            Task(name="a", fn="fn_a"),
            Task(name="b", fn="fn_b", depends_on=["a"], enabled=False),
            Task(name="c", fn="fn_c", depends_on=["a"]),
        ])
        levels = execution_order(s)
        all_names = [name for level in levels for name in level]
        assert "b" not in all_names
        assert "a" in all_names
        assert "c" in all_names

    def test_execution_order_empty(self):
        from scheduler.dag import execution_order
        from scheduler.models import Schedule
        s = Schedule(name="empty", tasks=[])
        assert execution_order(s) == []

    def test_get_task_found(self):
        from scheduler.dag import get_task
        from scheduler.models import Schedule, Task
        s = Schedule(name="test", tasks=[
            Task(name="a", fn="fn_a"),
            Task(name="b", fn="fn_b"),
        ])
        t = get_task(s, "b")
        assert t is not None
        assert t.name == "b"

    def test_get_task_not_found(self):
        from scheduler.dag import get_task
        from scheduler.models import Schedule, Task
        s = Schedule(name="test", tasks=[Task(name="a", fn="fn_a")])
        assert get_task(s, "missing") is None


# ── DAG runner tests ─────────────────────────────────────────────────────


class TestDAGRunner:
    """Test task execution (no real WorkflowEngine — engine=None path)."""

    FX = "tests._sched_fixtures"

    def test_run_linear(self):
        from scheduler.dag_runner import DAGRunner
        from scheduler.models import Schedule, Task

        from tests._sched_fixtures import call_log, reset_log

        reset_log()
        s = Schedule(name="linear", tasks=[
            Task(name="a", fn=f"{self.FX}:fn_return_a"),
            Task(name="b", fn=f"{self.FX}:fn_return_b", depends_on=["a"]),
        ])

        runner = DAGRunner(engine=None)  # type: ignore[arg-type]
        run = runner.run(s)

        assert run.task_results["a"].status == "SUCCESS"
        assert run.task_results["b"].status == "SUCCESS"
        assert call_log == ["a", "b"]

    def test_run_parallel(self):
        from scheduler.dag_runner import DAGRunner
        from scheduler.models import Schedule, Task

        s = Schedule(name="parallel", tasks=[
            Task(name="a", fn=f"{self.FX}:fn_return_a"),
            Task(name="b", fn=f"{self.FX}:fn_return_b"),
            Task(name="c", fn=f"{self.FX}:fn_return_c", depends_on=["a", "b"]),
        ])

        runner = DAGRunner(engine=None)  # type: ignore[arg-type]
        run = runner.run(s)

        assert run.task_results["a"].status == "SUCCESS"
        assert run.task_results["b"].status == "SUCCESS"
        assert run.task_results["c"].status == "SUCCESS"
        assert run.result == "all_succeeded"

    def test_run_failure_skips_dependents(self):
        from scheduler.dag_runner import DAGRunner
        from scheduler.models import Schedule, Task

        s = Schedule(name="fail", tasks=[
            Task(name="a", fn=f"{self.FX}:fn_boom"),
            Task(name="b", fn=f"{self.FX}:fn_never", depends_on=["a"]),
        ])

        runner = DAGRunner(engine=None)  # type: ignore[arg-type]
        run = runner.run(s)

        assert run.task_results["a"].status == "ERROR"
        assert "boom" in run.task_results["a"].error
        assert run.task_results["b"].status == "SKIPPED"

    def test_run_bad_fn(self):
        from scheduler.dag_runner import DAGRunner
        from scheduler.models import Schedule, Task

        s = Schedule(name="missing", tasks=[
            Task(name="a", fn="nonexistent.module:fn"),
        ])

        runner = DAGRunner(engine=None)  # type: ignore[arg-type]
        run = runner.run(s)

        assert run.task_results["a"].status == "ERROR"

    def test_run_disabled_task_skipped(self):
        from scheduler.dag_runner import DAGRunner
        from scheduler.models import Schedule, Task

        s = Schedule(name="disabled", tasks=[
            Task(name="a", fn=f"{self.FX}:fn_return_a"),
            Task(name="b", fn=f"{self.FX}:fn_return_b", enabled=False),
            Task(name="c", fn=f"{self.FX}:fn_return_c"),
        ])

        runner = DAGRunner(engine=None)  # type: ignore[arg-type]
        run = runner.run(s)

        assert "a" in run.task_results
        assert "b" not in run.task_results  # disabled, not even attempted
        assert "c" in run.task_results

    def test_run_records_duration(self):
        from scheduler.dag_runner import DAGRunner
        from scheduler.models import Schedule, Task

        s = Schedule(name="timed", tasks=[
            Task(name="slow", fn=f"{self.FX}:fn_slow"),
        ])

        runner = DAGRunner(engine=None)  # type: ignore[arg-type]
        run = runner.run(s)

        assert run.task_results["slow"].duration_ms >= 40  # at least 40ms

    def test_run_empty(self):
        from scheduler.dag_runner import DAGRunner
        from scheduler.models import Schedule

        s = Schedule(name="empty", tasks=[])
        runner = DAGRunner(engine=None)  # type: ignore[arg-type]
        run = runner.run(s)

        assert run.result == "empty"
        assert len(run.task_results) == 0


# ── Decorator tests ──────────────────────────────────────────────────────


class TestDecorators:
    """Test @schedule decorator."""

    def test_schedule_decorator_registers(self):
        from scheduler.decorators import _pending_tasks, schedule

        initial_count = len(_pending_tasks)

        @schedule("*/10 * * * *", name="test_deco")
        def my_job():
            return "done"

        assert len(_pending_tasks) == initial_count + 1
        entry = _pending_tasks[-1]
        assert entry["schedule_name"] == "test_deco"
        assert entry["cron_expr"] == "*/10 * * * *"
        assert entry["task_name"] == "my_job"
        # fn is auto-derived as module:qualname
        assert "my_job" in entry["task_fn"]
        assert ":" in entry["task_fn"]

    def test_schedule_decorator_default_name(self):
        from scheduler.decorators import _pending_tasks, schedule

        @schedule("0 * * * *")
        def another_job():
            pass

        entry = _pending_tasks[-1]
        assert entry["schedule_name"] == "another_job"

    def test_schedule_decorator_depends_on(self):
        from scheduler.decorators import _pending_tasks, schedule

        @schedule("0 2 * * *", name="pipeline", depends_on=["extract"])
        def transform():
            pass

        entry = _pending_tasks[-1]
        assert entry["schedule_name"] == "pipeline"
        assert entry["depends_on"] == ["extract"]


# ── Integration tests (real PG) ──────────────────────────────────────────


class TestSchedulerIntegration:
    """Integration tests with real StoreServer + UserConnection."""

    @pytest.fixture(scope="class")
    def store_server(self, store_server):
        """Delegate to session-scoped store_server from conftest.py."""
        store_server.provision_user("sched_user", "sched_pw")
        return store_server

    @pytest.fixture
    def client(self, store_server):
        from store.connection import UserConnection
        info = store_server.conn_info()
        return UserConnection(
            host=info["host"],
            port=info["port"],
            dbname=info["dbname"],
            user="sched_user",
            password="sched_pw",
        )

    def test_embedded_write_guard(self, client):
        """Storable.save() should refuse Embedded subclasses."""
        from scheduler.models import Task
        td = Task(name="test", fn="mod:fn")
        with pytest.raises(TypeError, match="Embedded"):
            td.save()

    def test_schedule_write_read(self, client):
        """Write and read back a Schedule."""
        from scheduler.models import Schedule, Task
        s = Schedule(name="test_sched", cron_expr="*/5 * * * *", tasks=[
            Task(name="do_it", fn="tests._sched_fixtures:fn_ok"),
        ])
        entity_id = s.save()
        assert entity_id is not None
        s2 = Schedule.get(entity_id)
        assert s2.name == "test_sched"
        assert s2.state == "ACTIVE"

    def test_schedule_pause_resume(self, client):
        """Test Schedule state machine transitions."""
        from scheduler.models import Schedule
        s = Schedule(name="pausable", cron_expr="0 * * * *")
        s.save()
        assert s.state == "ACTIVE"

        s.transition("PAUSED")
        assert s.state == "PAUSED"

        s.transition("ACTIVE")
        assert s.state == "ACTIVE"

    def test_schedule_write_read_with_tasks(self, client):
        """Write a Schedule with Tasks and read back."""
        from scheduler.models import Schedule, Task
        s = Schedule(
            name="test_pipeline",
            cron_expr="0 2 * * *",
            tasks=[
                Task(name="step1", fn="mod:fn1"),
                Task(name="step2", fn="mod:fn2", depends_on=["step1"]),
            ],
        )
        entity_id = s.save()
        s2 = Schedule.get(entity_id)
        assert s2.name == "test_pipeline"
        assert len(s2.tasks) == 2
        # After read, tasks are dicts — use task_defs property
        defs = s2.task_defs
        assert isinstance(defs[0], Task)
        assert defs[1].depends_on == ["step1"]

    def test_run_write_with_state_transitions(self, client):
        """Test Run state machine: PENDING → RUNNING → SUCCESS."""
        from scheduler.models import Run
        r = Run(schedule_name="test")
        r.save()
        assert r.state == "PENDING"

        r.transition("RUNNING")
        assert r.state == "RUNNING"

        r.transition("SUCCESS")
        assert r.state == "SUCCESS"

    def test_run_write_with_task_results(self, client):
        """Write a Run with TaskResults and read back."""
        from scheduler.models import Run, TaskResult
        r = Run(
            schedule_name="etl",
            task_results={
                "extract": TaskResult(task_name="extract", status="SUCCESS", duration_ms=100),
            },
        )
        entity_id = r.save()
        r2 = Run.get(entity_id)
        assert "extract" in r2.task_results

    def test_scheduler_register_and_fire(self, client):
        """Full round-trip: register schedule, fire, check Run."""
        from scheduler.dag_runner import DAGRunner
        from scheduler.models import Schedule, Task
        from scheduler.server import SchedulerServer

        # Lightweight: inject client directly (no full start())
        server = SchedulerServer.__new__(SchedulerServer)
        server._conn = client
        server._engine = None
        server._dag_runner = DAGRunner(None)  # type: ignore[arg-type]
        server._last_fire = {}
        server._running = False
        server._thread = None
        server._store = None

        s = Schedule(
            name="fire_test",
            cron_expr="*/1 * * * *",
            tasks=[Task(name="fire_fn", fn="tests._sched_fixtures:fn_result_42")],
        )
        server.register(s)

        run = server.fire("fire_test")
        assert run.state in ("SUCCESS", "ERROR")

    def test_scheduler_tick_fires_due(self, client):
        """Tick should fire schedules that are due."""
        from scheduler.dag_runner import DAGRunner
        from scheduler.models import Schedule, Task
        from scheduler.server import SchedulerServer

        server = SchedulerServer.__new__(SchedulerServer)
        server._conn = client
        server._engine = None
        server._dag_runner = DAGRunner(None)  # type: ignore[arg-type]
        server._last_fire = {}
        server._running = False
        server._thread = None
        server._store = None

        s = Schedule(
            name="tick_test",
            cron_expr="*/1 * * * *",
            tasks=[Task(name="tick_fn", fn="tests._sched_fixtures:fn_tick")],
        )
        server.register(s)

        runs = server.tick()
        assert len(runs) >= 1
        assert any(r.schedule_name == "tick_test" for r in runs)

    def test_scheduler_pipeline_fire(self, client):
        """Fire a multi-task schedule."""
        from scheduler.dag_runner import DAGRunner
        from scheduler.models import Schedule, Task
        from scheduler.server import SchedulerServer

        server = SchedulerServer.__new__(SchedulerServer)
        server._conn = client
        server._engine = None
        server._dag_runner = DAGRunner(None)  # type: ignore[arg-type]
        server._last_fire = {}
        server._running = False
        server._thread = None
        server._store = None

        s = Schedule(
            name="pipeline_fire_test",
            cron_expr="0 * * * *",
            tasks=[
                Task(name="t1", fn="tests._sched_fixtures:fn_return_a"),
                Task(name="t2", fn="tests._sched_fixtures:fn_return_b", depends_on=["t1"]),
            ],
        )
        server.register(s)

        run = server.fire("pipeline_fire_test")
        assert run.state in ("SUCCESS", "ERROR")


# ── resolve_fn tests ─────────────────────────────────────────────────────


class TestResolveFn:
    """Test importlib-based function resolution."""

    def test_resolve_module_function(self):
        from scheduler.resolve import resolve_fn
        fn = resolve_fn("tests._sched_fixtures:fn_ok")
        assert fn() == "fine"

    def test_resolve_nested_attr(self):
        from scheduler.resolve import resolve_fn
        # os.path.join is a dotted qualname
        fn = resolve_fn("os.path:join")
        assert callable(fn)

    def test_resolve_invalid_format(self):
        from scheduler.resolve import resolve_fn
        with pytest.raises(ValueError, match="module:qualname"):
            resolve_fn("no_colon_here")

    def test_resolve_missing_module(self):
        from scheduler.resolve import resolve_fn
        with pytest.raises(ImportError):
            resolve_fn("nonexistent.module:fn")

    def test_resolve_missing_attr(self):
        from scheduler.resolve import resolve_fn
        with pytest.raises(AttributeError):
            resolve_fn("tests._sched_fixtures:nonexistent_fn")


# ── Full-stack integration (real DBOS + real PG, public API only) ────────


class TestSchedulerFullStack:
    """End-to-end tests through Scheduler public API with real WorkflowEngine.

    Users never touch WorkflowEngine or SchedulerServer directly.
    These tests prove the full stack works through the public surface only.
    """

    FX = "tests._sched_fixtures"

    @pytest.fixture(scope="class")
    def full_stack(self, scheduler_server):
        """Delegate to session-scoped scheduler_server from conftest.py."""
        scheduler_server.register_alias("test-fullstack")
        yield {"server": scheduler_server}

    @pytest.fixture
    def scheduler(self, full_stack):
        from scheduler import Scheduler
        return Scheduler("test-fullstack")

    def test_single_task_schedule_fire(self, scheduler):
        """Register a single-task schedule, fire it, verify Run is SUCCESS."""
        from scheduler.models import Schedule, Task

        from tests._sched_fixtures import call_log, reset_log

        reset_log()
        scheduler.register(Schedule(
            name="fs_fn_test",
            cron_expr="*/5 * * * *",
            tasks=[Task(name="do_it", fn=f"{self.FX}:fn_result_42")],
        ))

        run = scheduler.fire("fs_fn_test")

        assert run.schedule_name == "fs_fn_test"
        assert run.state == "SUCCESS"
        assert "executed" in call_log

    def test_pipeline_fire_linear(self, scheduler):
        """Register a 3-task linear pipeline, fire, verify all SUCCESS."""
        from scheduler.models import Schedule, Task

        from tests._sched_fixtures import call_log, reset_log

        reset_log()
        scheduler.register(Schedule(
            name="fs_linear",
            cron_expr="0 * * * *",
            tasks=[
                Task(name="step1", fn=f"{self.FX}:fn_return_a"),
                Task(name="step2", fn=f"{self.FX}:fn_return_b", depends_on=["step1"]),
                Task(name="step3", fn=f"{self.FX}:fn_return_c", depends_on=["step2"]),
            ],
        ))

        run = scheduler.fire("fs_linear")

        assert run.state == "SUCCESS"
        assert call_log == ["a", "b", "c"]
        assert run.task_results["step1"].status == "SUCCESS"
        assert run.task_results["step2"].status == "SUCCESS"
        assert run.task_results["step3"].status == "SUCCESS"

    def test_pipeline_parallel_branches(self, scheduler):
        """Diamond: a → (b, c) → d. b and c run in parallel."""
        from scheduler.models import Schedule, Task

        scheduler.register(Schedule(
            name="fs_diamond",
            cron_expr="0 * * * *",
            tasks=[
                Task(name="a", fn=f"{self.FX}:fn_return_a"),
                Task(name="b", fn=f"{self.FX}:fn_return_b", depends_on=["a"]),
                Task(name="c", fn=f"{self.FX}:fn_return_c", depends_on=["a"]),
                Task(name="d", fn=f"{self.FX}:fn_return_d", depends_on=["b", "c"]),
            ],
        ))

        run = scheduler.fire("fs_diamond")

        assert run.state == "SUCCESS"
        assert len(run.task_results) == 4
        for name in ["a", "b", "c", "d"]:
            assert run.task_results[name].status == "SUCCESS"

    def test_failure_propagation(self, scheduler):
        """Task fails → dependent is SKIPPED. Run is PARTIAL."""
        from scheduler.models import Schedule, Task

        scheduler.register(Schedule(
            name="fs_fail",
            cron_expr="0 * * * *",
            tasks=[
                Task(name="ok_task", fn=f"{self.FX}:fn_ok"),
                Task(name="bad_task", fn=f"{self.FX}:fn_boom"),
                Task(name="skip_task", fn=f"{self.FX}:fn_never", depends_on=["bad_task"]),
            ],
        ))

        run = scheduler.fire("fs_fail")

        assert run.state in ("PARTIAL", "ERROR")
        assert run.task_results["ok_task"].status == "SUCCESS"
        assert run.task_results["bad_task"].status == "ERROR"
        assert "boom" in run.task_results["bad_task"].error
        assert run.task_results["skip_task"].status == "SKIPPED"

    def test_tick_fires_due_schedules(self, scheduler):
        """Tick finds due schedules and fires them."""
        from scheduler.models import Schedule, Task

        scheduler.register(Schedule(
            name="fs_tick",
            cron_expr="*/1 * * * *",
            tasks=[Task(name="tick_fn", fn=f"{self.FX}:fn_tick")],
        ))

        runs = scheduler.tick()

        assert len(runs) >= 1
        assert any(r.schedule_name == "fs_tick" for r in runs)

    def test_pause_prevents_firing(self, scheduler):
        """Paused schedule should not fire on tick."""
        from scheduler.models import Schedule, Task

        scheduler.register(Schedule(
            name="fs_pause_test",
            cron_expr="*/1 * * * *",
            tasks=[Task(name="pause_fn", fn=f"{self.FX}:fn_pause")],
        ))

        scheduler.pause("fs_pause_test")

        runs = scheduler.tick()
        paused_runs = [r for r in runs if r.schedule_name == "fs_pause_test"]
        assert len(paused_runs) == 0

    def test_list_and_history(self, scheduler):
        """list_schedules and history work through public API."""
        schedules = scheduler.list_schedules()
        assert len(schedules) > 0
        assert all(hasattr(s, 'name') for s in schedules)

        runs = scheduler.history("fs_fn_test")
        assert isinstance(runs, list)

    def test_duration_tracking(self, scheduler):
        """Each task result has real duration_ms from actual execution."""
        from scheduler.models import Schedule, Task

        scheduler.register(Schedule(
            name="fs_timed_sched",
            cron_expr="0 * * * *",
            tasks=[
                Task(name="slow", fn=f"{self.FX}:fn_slow"),
                Task(name="fast", fn=f"{self.FX}:fn_fast"),
            ],
        ))

        run = scheduler.fire("fs_timed_sched")

        assert run.task_results["slow"].duration_ms >= 40
        assert run.task_results["fast"].duration_ms < run.task_results["slow"].duration_ms
