"""
SchedulerServer — self-contained scheduler with embedded PG.

Manages its own StoreServer (embedded PG) and WorkflowEngine internally.
Users never import this directly.

Public import via ``scheduler.admin``::

    from scheduler.admin import SchedulerServer

    server = SchedulerServer(data_dir="data/scheduler")
    server.start()
    server.register_alias("demo")
    server.collect_schedules()
"""

from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from store.admin import StoreServer
from workflow.dbos_engine import WorkflowEngine

from scheduler.cron import is_due
from scheduler.dag_runner import DAGRunner
from scheduler.models import Run, Schedule
from store import UserConnection

logger = logging.getLogger(__name__)

_SVC_USER = "_sched_svc"
_SVC_PASSWORD = "sched_" + uuid.uuid4().hex[:12]


class SchedulerServer:
    """Self-contained scheduler: embedded PG, workflow engine, tick loop.

    Manages its own ``StoreServer`` and ``WorkflowEngine`` internally —
    callers never touch workflow or store infrastructure directly.

    Usage::

        server = SchedulerServer(data_dir="data/scheduler")
        server.start()
        server.register_alias("demo")
        server.collect_schedules()
        # ...
        server.stop()
    """

    def __init__(self, data_dir: str = "data/scheduler") -> None:
        """
        Args:
            data_dir: Directory for the embedded PG data files.
        """
        self._data_dir = os.path.abspath(data_dir)
        self._store: StoreServer | None = None
        self._conn: UserConnection | None = None
        self._engine: WorkflowEngine | None = None
        self._dag_runner: DAGRunner | None = None
        self._last_fire: dict[str, datetime] = {}
        self._poll_interval = 10.0
        self._running = False
        self._thread: threading.Thread | None = None

    # ── Require helpers ──────────────────────────────────────────────

    def _require_connection(self) -> UserConnection:
        assert self._conn is not None, "SchedulerServer not started"
        return self._conn

    def _require_dag_runner(self) -> DAGRunner:
        assert self._dag_runner is not None, "SchedulerServer not started"
        return self._dag_runner

    # ── Lifecycle ──────────────────────────────────────────────────────

    def start(self, poll_interval: float = 10.0) -> SchedulerServer:
        """Start embedded PG, workflow engine, and optionally the tick loop.

        Args:
            poll_interval: Seconds between cron tick checks. Set to 0 to
                skip starting the background tick loop.

        Returns:
            self (for chaining).
        """
        from store.admin import StoreServer
        from workflow.factory import create_engine

        from store import connect

        # 1. Embedded PG
        self._store = StoreServer(data_dir=self._data_dir)
        self._store.start()
        self._store.provision_user(_SVC_USER, _SVC_PASSWORD)

        # 2. UserConnection
        info = self._store.conn_info()
        self._conn = connect(
            host=info["host"], port=info["port"],
            dbname=info["dbname"],
            user=_SVC_USER, password=_SVC_PASSWORD,
        )

        # 3. WorkflowEngine (shares same PG — no separate WorkflowServer)
        self._engine = create_engine(self._store.pg_url(), name="scheduler")
        self._engine.launch()

        # 4. DAGRunner
        self._dag_runner = DAGRunner(self._engine)

        # 5. Background tick loop (optional)
        self._poll_interval = poll_interval
        if poll_interval > 0:
            self._running = True
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()

        logger.info("SchedulerServer started (data_dir=%s)", self._data_dir)
        return self

    def stop(self) -> None:
        """Stop tick loop, workflow engine, and embedded PG."""
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=self._poll_interval + 1)
            self._thread = None

        if self._engine is not None:
            try:
                self._engine.destroy()
            except Exception:
                logger.debug("Engine destroy error (ignored)", exc_info=True)
            self._engine = None

        if self._conn is not None:
            self._conn.close()
            self._conn = None

        if self._store is not None:
            self._store.stop()
            self._store = None

        logger.info("SchedulerServer stopped")

    def register_alias(self, name: str) -> None:
        """Register this server under an alias name."""
        from scheduler._registry import register_alias
        register_alias(name, server=self)

    def __enter__(self) -> SchedulerServer:
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()

    # ── Registration ──────────────────────────────────────────────────

    def register(self, schedule: Schedule) -> Schedule:
        """Register a schedule (persists to PG).

        Returns the schedule with entity_id populated.
        """
        self._require_connection()  # ensure active connection
        entity_id = schedule.save()
        logger.info("Registered schedule '%s' (%s) → %s",
                     schedule.name, schedule.cron_expr, entity_id)
        return schedule

    def collect_schedules(self) -> int:
        """Flush all @schedule-decorated functions to PG.

        Groups tasks by schedule name — multiple tasks with the same name
        become a single Schedule with multiple Tasks.

        Idempotent — safe to call on every startup.
        Returns the number of schedules registered.
        """
        from scheduler.decorators import _pending_tasks
        from scheduler.models import Task

        groups: dict[str, list[dict]] = defaultdict(list)
        for entry in _pending_tasks:
            groups[entry["schedule_name"]].append(entry)

        count = 0
        for sched_name, entries in groups.items():
            tasks = [
                Task(
                    name=e["task_name"],
                    fn=e["task_fn"],
                    depends_on=e["depends_on"],
                )
                for e in entries
            ]
            first = entries[0]
            sched = Schedule(
                name=sched_name,
                cron_expr=first["cron_expr"],
                tasks=tasks,
                **first["kwargs"],
            )
            self.register(sched)
            count += 1

        return count

    # ── Trigger ───────────────────────────────────────────────────────

    def fire(self, name: str) -> Run:
        """Manually trigger a schedule by name (ad-hoc run).

        Raises ValueError if schedule not found.
        """
        sched = self._find_schedule(name)
        if sched is None:
            raise ValueError(f"Schedule '{name}' not found")
        return self.execute_run(sched)

    # ── Management ────────────────────────────────────────────────────

    def pause(self, name: str) -> Schedule:
        """Pause a schedule (ACTIVE → PAUSED)."""
        sched = self._find_schedule(name)
        if sched is None:
            raise ValueError(f"Schedule '{name}' not found")
        self._require_connection()  # ensure active connection
        sched.transition("PAUSED")
        return sched

    def resume(self, name: str) -> Schedule:
        """Resume a schedule (PAUSED → ACTIVE)."""
        sched = self._find_schedule(name)
        if sched is None:
            raise ValueError(f"Schedule '{name}' not found")
        self._require_connection()  # ensure active connection
        sched.transition("ACTIVE")
        return sched

    def delete(self, name: str) -> Schedule:
        """Soft-delete a schedule (→ DELETED)."""
        sched = self._find_schedule(name)
        if sched is None:
            raise ValueError(f"Schedule '{name}' not found")
        self._require_connection()  # ensure active connection
        sched.transition("DELETED")
        return sched

    # ── Query ─────────────────────────────────────────────────────────

    def list_schedules(self) -> list[Schedule]:
        """Return all non-deleted schedules."""
        self._require_connection()  # ensure active connection
        all_schedules = Schedule.query()
        return [s for s in all_schedules if s.state != "DELETED"]

    def history(self, name: str, limit: int = 20) -> list[Run]:
        """Return past runs for a schedule, most recent first."""
        self._require_connection()  # ensure active connection
        all_runs = Run.query(filters={"schedule_name": name})
        matching = list(all_runs)
        matching.sort(key=lambda r: r.started_at, reverse=True)
        return matching[:limit]

    # ── Tick ───────────────────────────────────────────────────────────

    def tick(self, now: datetime | None = None) -> list[Run]:
        """Check all active schedules, fire any that are due.

        Returns list of Runs that were started.
        """
        now = now or datetime.now(timezone.utc)
        runs = []

        schedules = self._load_active_schedules()
        for sched in schedules:
            last = self._last_fire.get(sched.name)
            if is_due(sched.cron_expr, last, now=now):
                try:
                    run = self.execute_run(sched, now)
                    runs.append(run)
                except Exception:
                    logger.exception("Failed to fire schedule '%s'", sched.name)

        return runs

    # ── Execution ──────────────────────────────────────────────────────

    def execute_run(self, sched: Schedule, now: datetime | None = None) -> Run:
        """Execute a schedule and return the completed Run.

        Creates a Run, transitions PENDING → RUNNING → final,
        runs all tasks via DAGRunner.
        """
        now = now or datetime.now(timezone.utc)
        self._last_fire[sched.name] = now

        run = Run(
            schedule_name=sched.name,
            started_at=now.isoformat(),
            retries_left=sched.max_retries,
        )
        self._require_connection()  # ensure active connection
        run.save()
        run.transition("RUNNING")

        try:
            dag_run = self._require_dag_runner().run(sched)
            run.task_results = dag_run.task_results
            run.result = dag_run.result

            # Determine final state from task results
            if run.task_results:
                statuses = {tr.status if hasattr(tr, 'status') else tr.get('status', '')
                            for tr in run.task_results.values()}
                if "ERROR" in statuses:
                    final_state = "PARTIAL" if "SUCCESS" in statuses else "ERROR"
                else:
                    final_state = "SUCCESS"
            else:
                final_state = "SUCCESS"

            run.finished_at = datetime.now(timezone.utc).isoformat()
            run.save()
            run.transition(final_state)

        except Exception as e:
            run.error = str(e)
            run.finished_at = datetime.now(timezone.utc).isoformat()
            run.save()
            run.transition("ERROR")
            logger.exception("Schedule '%s' run failed", sched.name)

        return run

    # ── Internal ───────────────────────────────────────────────────────

    def _find_schedule(self, name: str) -> Schedule | None:
        """Find a schedule by name."""
        self._require_connection()  # ensure active connection
        results = Schedule.query(filters={"name": name})
        items = list(results)
        return items[0] if items else None

    def _load_active_schedules(self) -> list[Schedule]:
        """Load all schedules in ACTIVE state."""
        self._require_connection()  # ensure active connection
        all_schedules = Schedule.query()
        return [s for s in all_schedules if s.state == "ACTIVE"]

    def _run_loop(self) -> None:
        """Internal blocking loop — runs in background thread."""
        while self._running:
            try:
                runs = self.tick()
                if runs:
                    logger.info("Fired %d schedule(s): %s",
                                len(runs), [r.schedule_name for r in runs])
            except Exception:
                logger.exception("Tick failed")
            time.sleep(self._poll_interval)
