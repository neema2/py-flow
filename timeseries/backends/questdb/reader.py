"""
QuestDB Reader — PGWire Queries
================================
Reads historical data from QuestDB via its PostgreSQL wire protocol.
Uses psycopg2 (already a core dependency).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras

from timeseries.models import Bar

logger = logging.getLogger(__name__)


def _to_naive_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Strip tzinfo for QuestDB (it rejects ::timestamptz casts from psycopg2)."""
    if dt is None:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt

# Table name routing (same as writer)
_TABLE_MAP = {
    "equity": "equity_ticks",
    "fx": "fx_ticks",
    "curve": "curve_ticks",
}

# Symbol column name per table
_SYMBOL_COL = {
    "equity": "symbol",
    "fx": "pair",
    "curve": "label",
}

# Price column used for OHLCV (open/high/low/close)
_PRICE_COL = {
    "equity": "price",
    "fx": "mid",
    "curve": "rate",
}

# Volume column (only equity has volume)
_VOLUME_COL = {
    "equity": "volume",
}

# Valid bar intervals
_VALID_INTERVALS = {"1s", "5s", "15s", "30s", "1m", "5m", "15m", "30m", "1h", "4h", "1d"}


class QuestDBReader:
    """Reads historical data from QuestDB via PGWire (psycopg2)."""

    def __init__(self, host: str = "localhost", pg_port: int = 8812):
        self._host = host
        self._pg_port = pg_port
        self._conn = None

    @property
    def connection(self):
        """Raw psycopg2 connection (used by schema.create_tables)."""
        return self._conn

    def connect(self) -> None:
        """Open the PGWire connection."""
        self._conn = psycopg2.connect(
            host=self._host,
            port=self._pg_port,
            user="admin",
            password="quest",
            database="qdb",
        )
        # QuestDB requires autocommit for certain operations
        self._conn.autocommit = True
        logger.info("QuestDBReader connected (PGWire %s:%d)", self._host, self._pg_port)

    def close(self) -> None:
        """Close the PGWire connection."""
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
            logger.info("QuestDBReader closed")

    def get_ticks(
        self,
        msg_type: str,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> list[dict]:
        """Raw tick history for a symbol within a time range."""
        table = _TABLE_MAP.get(msg_type)
        sym_col = _SYMBOL_COL.get(msg_type)
        if table is None or sym_col is None:
            raise ValueError(f"Unknown message type: {msg_type!r}")

        query = f"""
            SELECT *
            FROM {table}
            WHERE {sym_col} = %s
              AND timestamp >= %s
              AND timestamp <= %s
            ORDER BY timestamp ASC
            LIMIT %s
        """
        return self._execute_dict(query, (symbol, _to_naive_utc(start), _to_naive_utc(end), limit))

    def get_bars(
        self,
        msg_type: str,
        symbol: str,
        interval: str = "1m",
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> list[Bar]:
        """OHLCV bars using QuestDB's SAMPLE BY."""
        table = _TABLE_MAP.get(msg_type)
        sym_col = _SYMBOL_COL.get(msg_type)
        price_col = _PRICE_COL.get(msg_type)
        vol_col = _VOLUME_COL.get(msg_type)

        if table is None or sym_col is None or price_col is None:
            raise ValueError(f"Unknown message type: {msg_type!r}")

        if interval not in _VALID_INTERVALS:
            raise ValueError(
                f"Invalid interval: {interval!r}. Valid: {sorted(_VALID_INTERVALS)}"
            )

        # Build volume expression
        vol_expr = f"sum({vol_col}) AS volume," if vol_col else ""

        # Build WHERE clause
        where_parts = [f"{sym_col} = %s"]
        params: list = [symbol]

        if start:
            where_parts.append("timestamp >= %s")
            params.append(_to_naive_utc(start))
        if end:
            where_parts.append("timestamp <= %s")
            params.append(_to_naive_utc(end))

        where_clause = " AND ".join(where_parts)

        query = f"""
            SELECT
                {sym_col} AS symbol,
                first({price_col}) AS open,
                max({price_col}) AS high,
                min({price_col}) AS low,
                last({price_col}) AS close,
                {vol_expr}
                count() AS trade_count,
                timestamp
            FROM {table}
            WHERE {where_clause}
            SAMPLE BY {interval}
            ORDER BY timestamp ASC
        """

        rows = self._execute_dict(query, tuple(params))
        bars = []
        for row in rows:
            bars.append(Bar(
                symbol=row["symbol"],
                interval=interval,
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=row.get("volume"),
                trade_count=row["trade_count"],
                timestamp=row["timestamp"],
            ))
        return bars

    def get_latest(
        self,
        msg_type: str,
        symbol: Optional[str] = None,
    ) -> list[dict]:
        """Latest tick(s) per symbol using LATEST ON."""
        table = _TABLE_MAP.get(msg_type)
        sym_col = _SYMBOL_COL.get(msg_type)
        if table is None or sym_col is None:
            raise ValueError(f"Unknown message type: {msg_type!r}")

        if symbol:
            query = f"""
                SELECT * FROM {table}
                WHERE {sym_col} = %s
                LATEST ON timestamp PARTITION BY {sym_col}
            """
            return self._execute_dict(query, (symbol,))
        else:
            query = f"""
                SELECT * FROM {table}
                LATEST ON timestamp PARTITION BY {sym_col}
            """
            return self._execute_dict(query)

    def _execute_dict(self, query: str, params=None) -> list[dict]:
        """Execute a query and return results as a list of dicts."""
        if self._conn is None:
            raise RuntimeError("QuestDBReader not connected")

        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]
