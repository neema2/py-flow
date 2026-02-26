"""
Backend Factory
===============
Selects and instantiates a TSDBBackend by name.
The only module that imports from timeseries/backends/.
"""

from __future__ import annotations

import os

from timeseries.base import TSDBBackend


def create_backend(name: str | None = None, **kwargs) -> TSDBBackend:
    """Create a TSDBBackend instance by name.

    Args:
        name: Backend name ("questdb"). Defaults to TSDB_BACKEND env var
              or "questdb" if unset.
        **kwargs: Backend-specific configuration passed to the constructor.

    Returns:
        A concrete TSDBBackend instance.

    Raises:
        ValueError: If the backend name is unknown.
    """
    if name is None:
        name = os.environ.get("TSDB_BACKEND", "questdb")

    if name == "questdb":
        from timeseries.backends.questdb import QuestDBBackend
        return QuestDBBackend(**kwargs)

    if name == "memory":
        from timeseries.backends.memory import MemoryBackend
        return MemoryBackend(**kwargs)

    raise ValueError(
        f"Unknown TSDB backend: {name!r}. Available: 'questdb', 'memory'"
    )
