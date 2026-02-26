"""
QuestDB Schema
==============
DDL for tick tables. Executed via PGWire (psycopg2) on startup.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Table DDL per asset type.
# QuestDB uses designated timestamp, PARTITION BY, and TTL (deferred DROP).
# SYMBOL type = indexed, low-cardinality string — ideal for ticker symbols.

EQUITY_TICKS_DDL = """
CREATE TABLE IF NOT EXISTS equity_ticks (
    symbol SYMBOL,
    price DOUBLE,
    bid DOUBLE,
    ask DOUBLE,
    volume LONG,
    change DOUBLE,
    change_pct DOUBLE,
    timestamp TIMESTAMP
) timestamp(timestamp)
PARTITION BY DAY;
"""

FX_TICKS_DDL = """
CREATE TABLE IF NOT EXISTS fx_ticks (
    pair SYMBOL,
    bid DOUBLE,
    ask DOUBLE,
    mid DOUBLE,
    spread_pips DOUBLE,
    currency SYMBOL,
    timestamp TIMESTAMP
) timestamp(timestamp)
PARTITION BY DAY;
"""

CURVE_TICKS_DDL = """
CREATE TABLE IF NOT EXISTS curve_ticks (
    label SYMBOL,
    tenor_years DOUBLE,
    rate DOUBLE,
    discount_factor DOUBLE,
    currency SYMBOL,
    timestamp TIMESTAMP
) timestamp(timestamp)
PARTITION BY DAY;
"""

ALL_DDL = [EQUITY_TICKS_DDL, FX_TICKS_DDL, CURVE_TICKS_DDL]


def create_tables(conn, ttl_days: int = 90) -> None:
    """Execute table DDL on a psycopg2 connection.

    Args:
        conn: A psycopg2 connection to QuestDB's PGWire interface.
        ttl_days: Data retention period (informational — QuestDB TTL
                  is applied via ALTER TABLE after creation).
    """
    with conn.cursor() as cur:
        for ddl in ALL_DDL:
            try:
                cur.execute(ddl.strip())
            except Exception as e:
                # QuestDB may return errors for already-existing tables
                # depending on version; log and continue
                logger.debug("DDL note: %s", e)
    conn.commit()
    logger.info("TSDB schema ready (equity_ticks, fx_ticks, curve_ticks)")
