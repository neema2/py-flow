"""
QuestDB Writer — ILP Ingestion
===============================
Writes ticks to QuestDB via the InfluxDB Line Protocol (ILP).
Uses the ``questdb`` pip package for high-throughput, non-blocking writes.
"""

from __future__ import annotations

import logging
import time
from datetime import timezone

from marketdata.models import CurveTick, FXTick, Tick, SwapTick, JacobianTick
from questdb.ingress import Sender

logger = logging.getLogger(__name__)

# Table name routing
_TABLE_MAP = {
    "equity": "equity_ticks",
    "fx": "fx_ticks",
    "curve": "curve_ticks",
    "swap": "swap_ticks",
    "jacobian": "jacobian_ticks",
}

# Auto-flush thresholds
_FLUSH_INTERVAL_MS = 100   # flush every 100ms
_FLUSH_ROW_COUNT = 100     # or every 100 rows


class QuestDBWriter:
    """Writes market data ticks to QuestDB via ILP."""

    def __init__(self, host: str = "localhost", ilp_port: int = 9009) -> None:
        self._host = host
        self._ilp_port = ilp_port
        self._sender: Sender | None = None
        self._row_count = 0
        self._last_flush: float = 0.0
        self._total_written: int = 0

    def connect(self) -> None:
        """Open the ILP sender connection."""
        from questdb.ingress import Protocol, Sender

        self._sender = Sender(Protocol.Tcp, self._host, self._ilp_port)
        self._sender.establish()
        self._last_flush = time.monotonic()
        logger.info("QuestDBWriter connected (ILP %s:%d)", self._host, self._ilp_port)

    def close(self) -> None:
        """Close the ILP sender."""
        if self._sender:
            try:
                self._sender.close()
            except Exception:
                pass
            self._sender = None
            logger.info(
                "QuestDBWriter closed (total written: %d)", self._total_written
            )

    async def write_tick(self, msg: Tick | FXTick | CurveTick | SwapTick | JacobianTick) -> None:
        """Buffer a tick for writing. Auto-flushes based on count/time."""
        if self._sender is None:
            raise RuntimeError("QuestDBWriter not connected")

        table = _TABLE_MAP.get(msg.type)
        if table is None:
            logger.warning("Unknown message type: %s", msg.type)
            return

        self._write_row(table, msg)
        self._row_count += 1

        # Auto-flush on row count or time
        elapsed_ms = (time.monotonic() - self._last_flush) * 1000
        if self._row_count >= _FLUSH_ROW_COUNT or elapsed_ms >= _FLUSH_INTERVAL_MS:
            await self.flush()

    async def flush(self) -> None:
        """Flush buffered rows to QuestDB."""
        if self._sender is None or self._row_count == 0:
            return

        try:
            self._sender.flush()
            self._total_written += self._row_count
            self._row_count = 0
            self._last_flush = time.monotonic()
        except Exception:
            logger.exception("QuestDBWriter flush error")

    def _require_sender(self) -> Sender:
        """Return the active sender or raise."""
        from questdb.ingress import Sender as _Sender
        s = self._sender
        assert isinstance(s, _Sender), "QuestDBWriter not connected"
        return s

    def _write_row(self, table: str, msg: Tick | FXTick | CurveTick | SwapTick | JacobianTick) -> None:
        """Write a single row to the ILP buffer."""
        from questdb.ingress import TimestampNanos

        ts = msg.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        ts_nanos = TimestampNanos(int(ts.timestamp() * 1_000_000_000))

        sender = self._require_sender()

        if isinstance(msg, Tick):
            sender.row(
                table,
                symbols={"symbol": msg.symbol},
                columns={
                    "price": msg.price,
                    "bid": msg.bid,
                    "ask": msg.ask,
                    "volume": msg.volume,
                    "change": msg.change,
                    "change_pct": msg.change_pct,
                },
                at=ts_nanos,
            )
        elif isinstance(msg, FXTick):
            sender.row(
                table,
                symbols={"pair": msg.pair, "currency": msg.currency},
                columns={
                    "bid": msg.bid,
                    "ask": msg.ask,
                    "mid": msg.mid,
                    "spread_pips": msg.spread_pips,
                },
                at=ts_nanos,
            )
        elif isinstance(msg, CurveTick):
            sender.row(
                table,
                symbols={"label": msg.label, "currency": msg.currency},
                columns={
                    "tenor_years": msg.tenor_years,
                    "rate": msg.rate,
                    "discount_factor": msg.discount_factor,
                },
                at=ts_nanos,
            )
        elif isinstance(msg, SwapTick):
            sender.row(
                table,
                symbols={"symbol": msg.symbol, "currency": msg.currency},
                columns={
                    "tenor": msg.tenor,
                    "bid": msg.bid,
                    "ask": msg.ask,
                    "rate": msg.rate,
                },
                at=ts_nanos,
            )
        elif isinstance(msg, JacobianTick):
            sender.row(
                table,
                symbols={"symbol": msg.symbol},
                columns={
                    "output_tenor": msg.output_tenor,
                    "input_tenor": msg.input_tenor,
                    "value": msg.value,
                },
                at=ts_nanos,
            )
