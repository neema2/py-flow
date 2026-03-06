"""
Zero-trust Python object store backed by PostgreSQL JSONB + Row-Level Security.
"""

from store._active_record import VersionConflict
from store.base import Embedded, Storable
from store.columns import REGISTRY
from store.connection import UserConnection, active_connection, connect
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
    "REGISTRY",
    "ChangeEvent",
    "ColumnDef",
    "Embedded",
    "EventListener",
    "GuardFailure",
    "InvalidTransition",
    "StateMachine",
    "Storable",
    "Transition",
    "TransitionNotPermitted",
    "UserConnection",
    "VersionConflict",
    "active_connection",
    "connect",
]
