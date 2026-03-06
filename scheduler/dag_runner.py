"""
DAG runner — parallel execution of DAG tasks with checkpointed steps.

Uses WorkflowEngine.step() for each task (exactly-once semantics).
Parallel branches within a level use concurrent.futures.ThreadPoolExecutor.
"""

from __future__ import annotations

import logging
import time as _time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from workflow.engine import WorkflowEngine

from scheduler.dag import execution_order, get_task
from scheduler.models import Run, Schedule, Task, TaskResult
from scheduler.resolve import resolve_fn

logger = logging.getLogger(__name__)


class DAGRunner:
    """Execute a DAG with parallel branches and checkpointed tasks.

    Each task is executed via engine.step() for crash recovery.
    Tasks within the same execution level run in parallel.
    If a task fails, its dependents are marked SKIPPED.
    """

    def __init__(self, engine: WorkflowEngine, max_workers: int = 4) -> None:
        """
        Args:
            engine: WorkflowEngine instance.
            max_workers: Max parallel tasks per level.
        """
        self._engine = engine
        self._max_workers = max_workers

    def run(
        self,
        sched: Schedule,
    ) -> Run:
        """Execute a Schedule's tasks. Returns a Run.

        Each task's fn is resolved via importlib at execution time.

        Args:
            sched: The Schedule to execute.

        Returns:
            Run with task_results populated.
        """
        now = datetime.now(timezone.utc)

        run = Run(
            schedule_name=sched.name,
            started_at=now.isoformat(),
        )

        levels = execution_order(sched)
        failed_tasks: set[str] = set()

        for level_idx, level_names in enumerate(levels):
            logger.info("Schedule '%s' level %d: %s", sched.name, level_idx, level_names)

            # Filter out tasks whose deps have failed
            runnable = []
            for name in level_names:
                task = get_task(sched, name)
                if task is None:
                    continue
                # Check if any dependency failed
                failed_deps = [d for d in task.depends_on if d in failed_tasks]
                if failed_deps:
                    run.task_results[name] = TaskResult(
                        task_name=name,
                        status="SKIPPED",
                        error=f"Skipped: dependencies failed: {failed_deps}",
                    )
                    failed_tasks.add(name)
                    continue
                runnable.append(task)

            if not runnable:
                continue

            # Execute tasks in parallel within this level
            if len(runnable) == 1:
                # Single task — no thread pool overhead
                task = runnable[0]
                result = self._execute_task(task)
                run.task_results[task.name] = result
                if result.status == "ERROR":
                    failed_tasks.add(task.name)
            else:
                with ThreadPoolExecutor(max_workers=min(self._max_workers, len(runnable))) as pool:
                    futures = {
                        pool.submit(self._execute_task, task): task
                        for task in runnable
                    }
                    for future in as_completed(futures):
                        task = futures[future]
                        try:
                            result = future.result()
                        except Exception as e:
                            result = TaskResult(
                                task_name=task.name,
                                status="ERROR",
                                error=str(e),
                            )
                        run.task_results[task.name] = result
                        if result.status == "ERROR":
                            failed_tasks.add(task.name)

        # Determine final state
        run.finished_at = datetime.now(timezone.utc).isoformat()
        statuses = {r.status for r in run.task_results.values()}
        if not statuses:
            run.result = "empty"
        elif statuses == {"SUCCESS"}:
            run.result = "all_succeeded"
        elif "ERROR" in statuses and "SUCCESS" in statuses:
            run.result = "partial"
        elif "ERROR" in statuses:
            run.result = "all_failed"
        else:
            run.result = "completed"

        return run

    def _execute_task(
        self,
        task: Task,
    ) -> TaskResult:
        """Execute a single task, resolving target_fn via importlib.

        Returns a TaskResult with status, result, error, and duration.
        """
        start = _time.monotonic()
        try:
            fn = resolve_fn(task.fn)

            # Execute via workflow engine step for checkpointing
            if self._engine is not None:
                result = self._engine.step(fn)
            else:
                result = fn()  # type: ignore[unreachable]

            elapsed = (_time.monotonic() - start) * 1000
            return TaskResult(
                task_name=task.name,
                status="SUCCESS",
                result=str(result) if result is not None else "",
                duration_ms=round(elapsed, 2),
            )
        except Exception as e:
            elapsed = (_time.monotonic() - start) * 1000
            logger.error("Task '%s' failed: %s", task.name, e)
            return TaskResult(
                task_name=task.name,
                status="ERROR",
                error=str(e),
                duration_ms=round(elapsed, 2),
            )
