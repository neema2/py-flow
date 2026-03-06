"""
Bridge between reactive objects and the object store.

Provides an effect factory that auto-persists objects to the store
whenever @computed values change. Works with self-reactive Storable
objects — no ReactiveGraph needed.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from reaktiv import Effect
from reaktiv.signal import ComputeSignal as _ComputeSignal

from store import UserConnection

logger = logging.getLogger(__name__)


def auto_persist_effect(obj: Any, store_conn: UserConnection | None = None) -> list:
    """
    Create effects that write `obj` back to the store whenever
    any @computed value changes.

    Args:
        obj: A Storable instance with @computed properties
        store_conn: Optional UserConnection instance. If None, uses the
                      active UserConnection from ``store.connect()``.

    Returns:
        List of Effect instances created (one per @computed on this object).
    """
    if store_conn is None:
        from store import active_connection
        store_conn = active_connection()

    reactive = object.__getattribute__(obj, '_reactive')
    effects = []

    for name, node in reactive.items():
        if not isinstance(node.read, _ComputeSignal):
            continue

        def make_effect(computed_name: str, comp: _ComputeSignal) -> Callable[[], None]:
            def effect_fn() -> None:
                _value = comp()
                try:
                    obj.save()
                except Exception:
                    logger.exception(
                        f"auto_persist for {computed_name} failed"
                    )
            return effect_fn

        eff = Effect(make_effect(name, node.read))
        effects.append(eff)

    return effects
