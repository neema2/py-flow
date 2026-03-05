"""
LakehouseSink — Buffers ChangeEvents and flushes to the Lakehouse.

Uses the public ``Lakehouse.ingest()`` API so events go through the same
catalog, S3, batch-ID, and ingestion-mode machinery as everything else.

Usage::

    from bridge.sinks.lakehouse import LakehouseSink
    from lakehouse import Lakehouse

    lh = Lakehouse(catalog_uri="http://...", s3_endpoint="http://...")
    sink = LakehouseSink(lh, table_name="events")

    # Wire into StoreBridge:
    bridge.add_sink(sink)
    bridge.start()

    # Periodically flush:
    sink.flush()
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from bridge.sinks import EventSink
from store import ChangeEvent

if TYPE_CHECKING:
    from lakehouse import Lakehouse

logger = logging.getLogger(__name__)


class LakehouseSink(EventSink):
    """Buffers ChangeEvents and batch-appends via Lakehouse.ingest()."""

    def __init__(
        self,
        lakehouse: Lakehouse,
        table_name: str = "store_events",
    ) -> None:
        self._lakehouse = lakehouse
        self._table_name = table_name
        self._buffer: list[ChangeEvent] = []

    def on_event(self, event: ChangeEvent) -> None:
        """Buffer a ChangeEvent for later flush."""
        self._buffer.append(event)

    def flush(self) -> int:
        """Convert buffered events to dicts and ingest via Lakehouse.

        Returns:
            Number of events flushed.
        """
        if not self._buffer:
            return 0

        rows = [_event_to_dict(e) for e in self._buffer]
        self._lakehouse.ingest(self._table_name, rows, mode="append")

        count = len(self._buffer)
        self._buffer.clear()
        logger.info("LakehouseSink: flushed %d events", count)
        return count

    @property
    def buffered(self) -> int:
        """Number of events currently buffered."""
        return len(self._buffer)

    def close(self) -> None:
        """Flush remaining events before closing."""
        if self._buffer:
            self.flush()


def _event_to_dict(event: ChangeEvent) -> dict:
    """Convert a ChangeEvent to a dict matching the full EVENTS_SCHEMA.

    The events Iceberg table has 13 columns (see lakehouse/tables.py).
    ChangeEvent only carries 7 fields from PG LISTEN/NOTIFY — the rest
    are set to None (nullable in the schema).
    """
    return {
        "event_id": None,        # Not in ChangeEvent (PG trigger doesn't send it)
        "entity_id": event.entity_id,
        "version": event.version,
        "type_name": event.type_name,
        "owner": None,           # Not in ChangeEvent
        "updated_by": event.updated_by,
        "data": None,            # Not in ChangeEvent (would need read-back)
        "state": event.state,
        "event_type": event.event_type,
        "event_meta": None,      # Not in ChangeEvent
        "tx_time": event.tx_time.isoformat() if event.tx_time else None,
        "valid_from": event.tx_time.isoformat() if event.tx_time else None,  # default to tx_time
        "valid_to": None,
    }
