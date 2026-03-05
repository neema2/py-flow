"""
Scheduler models — Storables and Embeddeds for scheduling.

Storables (persisted independently):
  - Schedule: cron config + task list — what and when to run
  - Run: execution record — what happened

Embeddeds (live inside a parent Storable, reactive-capable):
  - Task: task blueprint (inside Schedule.tasks)
  - TaskResult: execution outcome (inside Run.task_results)

A single-function schedule is just a Schedule with one Task (degenerate case).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import ClassVar

from store import Embedded, Storable, StateMachine, Transition

# ── Task (Embedded — reactive, not independently persisted) ──────────────


@dataclass
class Task(Embedded):
    """A single task in a Schedule. Lives inside Schedule.tasks.

    Not persisted independently — Schedule is the unit of persistence.
    Extends Embedded for reactive wiring (Signals via __post_init__).
    """
    name: str = ""
    fn: str = ""
    depends_on: list = field(default_factory=list)
    timeout_s: int = 300
    retries: int = 0
    enabled: bool = True


# ── Schedule (Storable — what and when to run) ───────────────────────────


@dataclass
class Schedule(Storable):
    """Cron-based schedule with embedded task list.

    A Schedule always has a tasks list. A single-function schedule is
    just a one-element task list — no special case.
    """
    name: str = ""
    cron_expr: str = ""
    tasks: list = field(default_factory=list)
    description: str = ""
    max_retries: int = 0
    timeout_s: int = 300

    @property
    def task_defs(self) -> list[Task]:
        """Return tasks as typed Task instances (handles dict→Task after deserialization)."""
        return [
            Task(**t) if isinstance(t, dict) else t
            for t in self.tasks
        ]

    @classmethod
    def from_json(cls, json_str: str) -> Schedule:
        """Deserialize and reconstruct embedded Task instances."""
        sched = super().from_json(json_str)
        sched.tasks = [
            Task(**t) if isinstance(t, dict) else t
            for t in sched.tasks
        ]
        return sched  # type: ignore[return-value]


class ScheduleLifecycle(StateMachine):
    """Schedule config lifecycle."""
    initial = "ACTIVE"
    transitions: ClassVar[list] = [
        Transition("ACTIVE", "PAUSED"),
        Transition("PAUSED", "ACTIVE"),
        Transition("ACTIVE", "DELETED"),
        Transition("PAUSED", "DELETED"),
    ]


Schedule._state_machine = ScheduleLifecycle


# ── TaskResult (Embedded — execution outcome) ────────────────────────────


@dataclass
class TaskResult(Embedded):
    """Result of a single task execution. Lives inside Run.task_results.

    Not persisted independently — Run is the unit of persistence.
    Created once with final status after task completes.
    """
    task_name: str = ""
    status: str = ""
    result: str = ""
    error: str = ""
    duration_ms: float = 0


# ── Run (Storable — execution record) ────────────────────────────────────


@dataclass
class Run(Storable):
    """Execution record for a Schedule run.

    task_results maps task_name → TaskResult.
    """
    run_id: str = ""
    schedule_name: str = ""
    started_at: str = ""
    finished_at: str = ""
    result: str = ""
    error: str = ""
    workflow_id: str = ""
    task_results: dict = field(default_factory=dict)
    retries_left: int = 0

    def __post_init__(self) -> None:
        if not self.run_id:
            self.run_id = str(uuid.uuid4())
        super().__post_init__()

    @classmethod
    def from_json(cls, json_str: str) -> Run:
        """Deserialize and reconstruct embedded TaskResult instances."""
        run = super().from_json(json_str)
        run.task_results = {
            k: TaskResult(**v) if isinstance(v, dict) else v
            for k, v in run.task_results.items()
        }
        return run  # type: ignore[return-value]


class RunLifecycle(StateMachine):
    """Execution lifecycle — the important state machine."""
    initial = "PENDING"
    transitions: ClassVar[list] = [
        Transition("PENDING", "RUNNING"),
        Transition("RUNNING", "SUCCESS"),
        Transition("RUNNING", "PARTIAL"),
        Transition("RUNNING", "ERROR"),
        Transition("ERROR", "RETRYING",
                   guard=lambda obj: obj.retries_left > 0),
        Transition("RETRYING", "RUNNING"),
        Transition("RUNNING", "CANCELLED"),
        Transition("PENDING", "CANCELLED"),
    ]


Run._state_machine = RunLifecycle
