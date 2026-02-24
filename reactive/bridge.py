"""
Bridge between reactive objects and the object store.

Provides an effect factory that auto-persists objects to the store
whenever @computed values change. Works with self-reactive Storable
objects — no ReactiveGraph needed.
"""

import logging
from reaktiv import Effect

logger = logging.getLogger(__name__)


def auto_persist_effect(obj, store_client=None):
    """
    Create effects that write `obj` back to the store whenever
    any @computed value changes.

    Args:
        obj: A Storable instance with @computed properties
        store_client: Optional StoreClient instance. If None, uses the
                      active UserConnection from ``store.connect()``.

    Returns:
        List of Effect instances created (one per @computed on this object).
    """
    if store_client is None:
        from store.connection import get_connection
        store_client = get_connection()._client

    computeds = object.__getattribute__(obj, '_computeds')
    effects = []

    for name, comp_signal in computeds.items():
        def make_effect(computed_name, comp_sig):
            def effect_fn():
                value = comp_sig()
                try:
                    store_client.update(obj)
                except Exception:
                    logger.exception(
                        f"auto_persist for {computed_name} failed"
                    )
            return effect_fn

        eff = Effect(make_effect(name, comp_signal))
        effects.append(eff)

    return effects
