"""
Media alias registry — maps alias names to S3 connection info.
"""

from __future__ import annotations

import threading
from typing import Dict, Optional

_aliases: Dict[str, dict] = {}   # name → {"endpoint", "access_key", "secret_key", "bucket"}
_lock = threading.Lock()


def register_alias(name: str, endpoint: str, access_key: str, secret_key: str,
                   bucket: str = "media"):
    """Register a media server alias."""
    with _lock:
        _aliases[name] = {
            "endpoint": endpoint,
            "access_key": access_key,
            "secret_key": secret_key,
            "bucket": bucket,
        }


def resolve_alias(name: str) -> Optional[dict]:
    """Resolve a media alias to S3 connection info."""
    with _lock:
        return _aliases.get(name)
