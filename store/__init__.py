"""
Zero-trust Python object store backed by PostgreSQL JSONB + Row-Level Security.
"""

from store.base import Storable
from store.connection import connect
from store.state_machine import (
    StateMachine, Transition,
    InvalidTransition, GuardFailure, TransitionNotPermitted,
)
from store.subscriptions import EventListener, ChangeEvent
from store.client import VersionConflict

__all__ = [
    "Storable", "connect",
    "StateMachine", "Transition",
    "EventListener", "ChangeEvent",
    "VersionConflict", "InvalidTransition", "GuardFailure", "TransitionNotPermitted",
]
