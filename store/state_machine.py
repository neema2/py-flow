"""Declarative state machines for Storable lifecycle management.

Three tiers of side-effects on each Transition:

  Tier 1 — action:         Runs inside DB transaction, atomic with state change.
  Tier 2 — on_enter/on_exit: Fire-and-forget after commit.
  Tier 3 — start_workflow: Durable workflow via WorkflowEngine after commit.

    from store.state_machine import StateMachine, Transition

    class OrderLifecycle(StateMachine):
        initial = "PENDING"
        transitions = [
            Transition("PENDING", "PARTIAL"),
            Transition("PENDING", "FILLED",
                       guard=lambda obj: obj.quantity > 0,
                       action=lambda obj, f, t: create_settlement(obj),
                       on_enter=lambda obj, f, t: log.info("Filled!"),
                       start_workflow=settlement_workflow),
            Transition("PENDING", "CANCELLED",
                       allowed_by=["risk_manager"]),
        ]

    Order._state_machine = OrderLifecycle
    Order._workflow_engine = engine  # enables start_workflow=
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, ClassVar


@dataclass
class Transition:
    """
    A single state machine edge with three tiers of side-effects.

    - guard: Callable(obj) → bool, or legacy Expr with .eval(dict) method.
    - action: Tier 1 — callable(obj, from_state, to_state) runs inside the DB
      transaction, atomic with the state change. If it raises, state rolls back.
    - on_exit: Tier 2 — callable(obj, from_state, to_state) fires after commit,
      best-effort. Exceptions are swallowed.
    - on_enter: Tier 2 — callable(obj, from_state, to_state) fires after commit,
      best-effort. Exceptions are swallowed.
    - start_workflow: Tier 3 — callable reference dispatched via WorkflowEngine
      after commit. Durable, survives crashes. Requires _workflow_engine on class.
    - allowed_by: list of usernames who can trigger this transition.
      If None, anyone with write access can trigger. Owner is always allowed.
    """
    from_state: str
    to_state: str
    guard: Any = None               # Expr | Callable — avoid circular import
    action: Callable | None = None
    on_exit: Callable | None = None
    on_enter: Callable | None = None
    start_workflow: Callable | None = None
    allowed_by: list[str] | None = None


class InvalidTransition(Exception):
    """Raised when the transition edge does not exist."""

    def __init__(self, from_state: str, to_state: str, allowed: list[str]) -> None:
        self.from_state = from_state
        self.to_state = to_state
        self.allowed = allowed
        super().__init__(
            f"Cannot transition from '{from_state}' to '{to_state}'. "
            f"Allowed: {allowed}"
        )


class GuardFailure(Exception):
    """Raised when a transition edge exists but the guard evaluates to False."""

    def __init__(self, from_state: str, to_state: str, guard: Any) -> None:
        self.from_state = from_state
        self.to_state = to_state
        self.guard = guard
        super().__init__(
            f"Guard failed for transition '{from_state}' → '{to_state}'"
        )


class TransitionNotPermitted(Exception):
    """Raised when the user is not authorized to trigger a transition."""

    def __init__(self, from_state: str, to_state: str, user: str, allowed_by: list[str]) -> None:
        self.from_state = from_state
        self.to_state = to_state
        self.user = user
        self.allowed_by = allowed_by
        super().__init__(
            f"User '{user}' not permitted for transition "
            f"'{from_state}' → '{to_state}'. Allowed: {allowed_by}"
        )


class StateMachine:
    """
    Base class for declarative state machines.

    Subclass and define:
        initial: str                    — the starting state
        transitions: list[Transition]   — list of Transition edges

    Side-effects are declared per-Transition (action, on_enter, on_exit,
    start_workflow), not on the StateMachine class.
    """

    initial: str | None = None
    transitions: ClassVar[list[Transition]] = []

    @classmethod
    def get_transition(cls, from_state: str, to_state: str) -> Transition | None:
        """Return the Transition object for this edge, or None."""
        for t in cls.transitions:
            if t.from_state == from_state and t.to_state == to_state:
                return t
        return None

    @classmethod
    def validate_transition(cls, from_state: str, to_state: str, context: dict | None = None, user: str | None = None, obj: Any = None) -> Transition:
        """
        Validate and return the Transition object.

        Args:
            from_state: Current state.
            to_state: Desired next state.
            context: Dict representation of the object (for legacy Expr guards).
            user: Username for permission checks.
            obj: The actual Storable instance (for callable guards).

        Raises:
            InvalidTransition — edge doesn't exist
            GuardFailure — guard evaluated to False
            TransitionNotPermitted — user not authorized
        """
        t = cls.get_transition(from_state, to_state)
        if t is None:
            allowed = cls.allowed_transitions(from_state)
            raise InvalidTransition(from_state, to_state, allowed)

        # Check guard — callable (lambda) or legacy Expr (.eval)
        if t.guard is not None:
            if hasattr(t.guard, 'eval'):
                # Legacy Expr guard: evaluate against dict context
                if context is not None and not t.guard.eval(context):
                    raise GuardFailure(from_state, to_state, t.guard)
            elif obj is not None:
                # Callable guard: pass the actual object
                if not t.guard(obj):
                    raise GuardFailure(from_state, to_state, t.guard)
            elif context is not None:
                # Callable guard fallback: pass dict context
                if not t.guard(context):
                    raise GuardFailure(from_state, to_state, t.guard)

        # Check permissions
        if t.allowed_by is not None and user is not None:
            if user not in t.allowed_by:
                raise TransitionNotPermitted(from_state, to_state, user, t.allowed_by)

        return t

    @classmethod
    def allowed_transitions(cls, from_state: str) -> list[str]:
        """Return list of valid next state names from from_state."""
        return [t.to_state for t in cls.transitions if t.from_state == from_state]

