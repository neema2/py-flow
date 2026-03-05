"""
Zero-trust Python object store backed by PostgreSQL JSONB + Row-Level Security.
"""

from store.base import Embedded, Storable
from store._client import VersionConflict
from store.columns import REGISTRY
from store.connection import active_connection, connect
from store.registry import ColumnDef
from store.state_machine import (
    GuardFailure,
    InvalidTransition,
    StateMachine,
    Transition,
    TransitionNotPermitted,
)
from store.subscriptions import ChangeEvent, EventListener

__all__ = [
    "ChangeEvent",
    "ColumnDef",
    "Embedded",
    "EventListener",
    "GuardFailure",
    "InvalidTransition",
    "REGISTRY",
    "StateMachine",
    "Storable",
    "Transition",
    "TransitionNotPermitted",
    "VersionConflict",
    "active_connection",
    "connect",
]
