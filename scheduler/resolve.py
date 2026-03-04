"""
Function resolution — resolve importable Python paths to callables.

Used by SchedulerServer and DAGRunner to resolve target_fn strings
like 'my_jobs:ingest_events' into actual Python callables via importlib.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any


def resolve_fn(target_fn: str) -> Callable[..., Any]:
    """Resolve 'module.path:qualname' to a callable via importlib.

    Args:
        target_fn: Importable path in 'module:qualname' format.
            Examples: 'my_jobs:ingest_events', 'lakehouse.sync:SyncEngine.sync_ticks'

    Returns:
        The resolved callable.

    Raises:
        ValueError: If target_fn format is invalid.
        ImportError: If the module cannot be imported.
        AttributeError: If the qualname cannot be resolved.
    """
    if ":" not in target_fn:
        raise ValueError(
            f"target_fn must be in 'module:qualname' format, got: {target_fn!r}"
        )
    module_path, qualname = target_fn.rsplit(":", 1)
    module = importlib.import_module(module_path)
    obj: Any = module
    for attr in qualname.split("."):
        obj = getattr(obj, attr)
    result: Callable[..., Any] = obj
    return result
