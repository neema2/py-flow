"""
Market data alias registry — maps alias names to server URLs.
"""

from __future__ import annotations

import threading
from typing import Dict, Optional

_aliases: Dict[str, dict] = {}   # name → {"url": ..., "port": ...}
_lock = threading.Lock()


def register_alias(name: str, **kwargs):
    """Register a market data server alias."""
    with _lock:
        _aliases[name] = kwargs


def resolve_alias(name: str) -> Optional[dict]:
    """Resolve a market data alias."""
    with _lock:
        return _aliases.get(name)
