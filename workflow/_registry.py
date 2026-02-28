"""
Workflow alias registry — maps alias names to PG URLs.
"""

from __future__ import annotations

import threading
from typing import Dict, Optional

_aliases: Dict[str, dict] = {}   # name → {"pg_url": ...}
_lock = threading.Lock()


def register_alias(name: str, pg_url: str):
    """Register a workflow server alias."""
    with _lock:
        _aliases[name] = {"pg_url": pg_url}


def resolve_alias(name: str) -> Optional[dict]:
    """Resolve a workflow alias to connection info."""
    with _lock:
        return _aliases.get(name)
