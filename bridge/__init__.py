"""
Deephaven ↔ Store Bridge — streams object store events into ticking tables.

Users import StoreBridge. Type-mapping helpers are internal.
The bridge is a library, not a service — embed it wherever makes sense.
"""

from bridge.store_bridge import StoreBridge

__all__ = ["StoreBridge"]
