"""
WorkflowEngine ABC — the only interface users interact with.

Concrete implementations (DBOS, Temporal, etc.) live in separate modules
and are never imported directly by application code.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional


class WorkflowStatus(str, Enum):
    """Possible states of a workflow execution."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"


@dataclass
class WorkflowHandle:
    """Opaque handle to a running or completed workflow.

    Backends populate this; users read workflow_id, get_status(), get_result().
    """
    workflow_id: str
    _engine: "WorkflowEngine"

    def get_status(self) -> WorkflowStatus:
        """Return the current status of this workflow."""
        return self._engine.get_workflow_status(self.workflow_id)

    def get_result(self, timeout: Optional[float] = None) -> Any:
        """Block until the workflow completes and return its result.

        Raises TimeoutError if *timeout* seconds elapse first.
        Raises RuntimeError if the workflow ended in ERROR.
        """
        return self._engine.get_workflow_result(self.workflow_id, timeout=timeout)


class WorkflowEngine(ABC):
    """Backend-swappable durable workflow orchestration.

    Implementations must override every abstract method.  Application code
    should only depend on this interface — never on a concrete backend.
    """

    # ── Running workflows / steps ────────────────────────────────────

    @abstractmethod
    def workflow(self, fn: Callable, *args: Any, **kwargs: Any) -> WorkflowHandle:
        """Execute *fn* as a durable workflow.

        The function and all its arguments must be serialisable.
        Returns a handle that can be used to poll status or await the result.
        """

    def run(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute *fn* as a durable workflow **synchronously** in-process.

        Unlike :meth:`workflow`, arguments need not be serialisable.
        The workflow still benefits from step-level checkpointing but
        cannot be recovered across process restarts.

        Default implementation delegates to :meth:`workflow` and blocks.
        Backends may override for a more efficient in-process path.
        """
        handle = self.workflow(fn, *args, **kwargs)
        return handle.get_result()

    @abstractmethod
    def step(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute *fn* as a checkpointed step inside the current workflow.

        On recovery the step is replayed from its recorded output rather than
        re-executed, guaranteeing exactly-once semantics.
        """

    # ── Queues ───────────────────────────────────────────────────────

    @abstractmethod
    def queue(
        self,
        queue_name: str,
        fn: Callable,
        *args: Any,
        **kwargs: Any,
    ) -> WorkflowHandle:
        """Enqueue *fn* for background execution on the named queue.

        The queue provides concurrency control — at most *N* functions from
        the same queue run concurrently (configured per-queue on the engine).
        """

    # ── Durable timers ───────────────────────────────────────────────

    @abstractmethod
    def sleep(self, seconds: float) -> None:
        """Durable sleep — survives process restarts.

        May only be called inside a workflow.
        """

    # ── Inter-workflow messaging ─────────────────────────────────────

    @abstractmethod
    def send(self, workflow_id: str, topic: str, value: Any) -> None:
        """Send a notification to the workflow identified by *workflow_id*."""

    @abstractmethod
    def recv(self, topic: str, timeout: Optional[float] = None) -> Any:
        """Wait for a notification on *topic* inside the current workflow.

        Returns ``None`` if *timeout* seconds elapse without a message.
        """

    # ── Workflow management ──────────────────────────────────────────

    @abstractmethod
    def get_workflow_status(self, workflow_id: str) -> WorkflowStatus:
        """Return the current status of a workflow."""

    @abstractmethod
    def get_workflow_result(
        self, workflow_id: str, *, timeout: Optional[float] = None
    ) -> Any:
        """Block until the workflow completes and return its output.

        Raises TimeoutError on timeout, RuntimeError on workflow error.
        """

    # ── Convenience ──────────────────────────────────────────────────

    def durable_transition(self, obj, new_state, **kwargs) -> Any:
        """Execute a state transition as a checkpointed workflow step.

        Wraps ``obj.transition()`` in ``self.step()`` so the transition
        is recorded — on crash recovery the step replays from its
        checkpoint rather than re-executing the DB write.

        Must be called inside an active workflow.
        Uses the active store connection.
        """
        from store.connection import get_connection
        client = get_connection()._client
        return self.step(
            lambda: client.transition(obj, new_state, **kwargs)
        )
