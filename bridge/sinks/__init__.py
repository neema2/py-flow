"""
EventSink — Pluggable destination for store ChangeEvents.

Sinks receive ChangeEvents and deliver them somewhere:
- DeephavenSink  → TickingTable (real-time streaming)
- LakehouseSink  → Lakehouse table via Lakehouse.ingest() (batch ETL)

Usage::

    from bridge.sinks.lakehouse import LakehouseSink
    from lakehouse import Lakehouse

    lh = Lakehouse(catalog_uri="http://...", s3_endpoint="http://...")
    sink = LakehouseSink(lh, table_name="events")
    sink.on_event(change_event)   # buffer
    sink.flush()                  # write via Lakehouse.ingest()
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from store import ChangeEvent


class EventSink(ABC):
    """Abstract base for pluggable event destinations."""

    @abstractmethod
    def on_event(self, event: ChangeEvent) -> None:
        """Called for each ChangeEvent. Implementations may buffer or write immediately."""
        ...

    def flush(self) -> int:
        """Flush buffered events, return count written. Default: no-op."""
        return 0

    def close(self) -> None:
        """Release resources."""


__all__ = ["EventSink"]
