"""
Timeseries alias registry — maps alias names to TSDB connection info.
"""

from __future__ import annotations

import threading
from typing import Dict, Optional

_aliases: Dict[str, dict] = {}   # name → {"host", "http_port", "ilp_port", "pg_port", "data_dir"}
_lock = threading.Lock()


def register_alias(name: str, **kwargs):
    """Register a TSDB server alias."""
    with _lock:
        _aliases[name] = kwargs


def resolve_alias(name: str) -> Optional[dict]:
    """Resolve a TSDB alias to connection info."""
    with _lock:
        return _aliases.get(name)
