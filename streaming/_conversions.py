"""
streaming._conversions — DH-specific value conversions (private).

Keeps Deephaven import isolated to the streaming package.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any


def to_streaming_value(value: Any) -> Any:
    """Convert a Python value to a streaming-engine-compatible value.

    Currently the only conversion needed is datetime → java.time.Instant
    (required by the DynamicTableWriter).  All other types pass through.

    On ARM64 / remote mode, datetimes pass through unchanged — pydeephaven
    handles the conversion natively.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        from streaming._utils import _is_remote
        if _is_remote():
            # Remote (Docker) mode: pydeephaven accepts Python datetimes directly
            return value
        from deephaven.time import to_j_instant
        return to_j_instant(value)
    if isinstance(value, Decimal):
        return float(value)
    return value

