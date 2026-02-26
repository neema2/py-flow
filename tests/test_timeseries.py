"""
Tests for timeseries package — Pure Python (no QuestDB needed)
==============================================================
Tests the ABC contract, factory, models, consumer routing, and
table/column mapping logic without requiring a running QuestDB instance.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from marketdata.models import Tick, FXTick, CurveTick
from timeseries.base import TSDBBackend
from timeseries.models import Bar, HistoryQuery, BarQuery
from timeseries.factory import create_backend
from timeseries.consumer import TSDBConsumer


# ── ABC Contract Tests ────────────────────────────────────────────────────────


class _IncompleteBackend(TSDBBackend):
    """Should fail to instantiate — missing abstract methods."""
    pass


class _CompleteBackend(TSDBBackend):
    """Minimal concrete backend for testing."""

    def __init__(self):
        self.started = False
        self.stopped = False
        self.ticks_written: list = []
        self.flushed = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def write_tick(self, msg) -> None:
        self.ticks_written.append(msg)

    async def flush(self) -> None:
        self.flushed = True

    def get_ticks(self, msg_type, symbol, start, end, limit=1000):
        return []

    def get_bars(self, msg_type, symbol, interval="1m", start=None, end=None):
        return []

    def get_latest(self, msg_type, symbol=None):
        return []


def test_abc_cannot_instantiate_incomplete():
    """TSDBBackend subclass without all abstract methods cannot be instantiated."""
    with pytest.raises(TypeError):
        _IncompleteBackend()


def test_abc_can_instantiate_complete():
    """TSDBBackend subclass with all abstract methods can be instantiated."""
    backend = _CompleteBackend()
    assert isinstance(backend, TSDBBackend)


@pytest.mark.asyncio
async def test_abc_lifecycle():
    """start/stop flow works on a complete backend."""
    backend = _CompleteBackend()
    await backend.start()
    assert backend.started
    await backend.stop()
    assert backend.stopped


@pytest.mark.asyncio
async def test_abc_write_tick():
    """write_tick accepts a Tick and stores it."""
    backend = _CompleteBackend()
    tick = Tick(
        symbol="AAPL", price=150.0, bid=149.9, ask=150.1,
        volume=1000, change=0.5, change_pct=0.33,
        timestamp=datetime.now(timezone.utc),
    )
    await backend.write_tick(tick)
    assert len(backend.ticks_written) == 1
    assert backend.ticks_written[0] is tick


@pytest.mark.asyncio
async def test_abc_flush():
    """flush() can be called."""
    backend = _CompleteBackend()
    await backend.flush()
    assert backend.flushed


# ── Factory Tests ─────────────────────────────────────────────────────────────


def test_factory_unknown_backend():
    """create_backend with unknown name raises ValueError."""
    with pytest.raises(ValueError, match="Unknown TSDB backend"):
        create_backend("nonexistent")


def test_factory_questdb_import():
    """create_backend('questdb') imports QuestDBBackend (may fail if questdb not installed)."""
    try:
        backend = create_backend("questdb")
        assert type(backend).__name__ == "QuestDBBackend"
    except ImportError:
        pytest.skip("questdb package not installed")


def test_factory_env_var():
    """create_backend() reads TSDB_BACKEND env var."""
    with patch.dict("os.environ", {"TSDB_BACKEND": "nonexistent"}):
        with pytest.raises(ValueError, match="Unknown TSDB backend"):
            create_backend()


def test_factory_default():
    """create_backend() defaults to 'questdb'."""
    with patch.dict("os.environ", {}, clear=True):
        try:
            backend = create_backend()
            assert type(backend).__name__ == "QuestDBBackend"
        except ImportError:
            pytest.skip("questdb package not installed")


# ── Model Tests ───────────────────────────────────────────────────────────────


def test_bar_model():
    """Bar model validates correctly."""
    bar = Bar(
        symbol="AAPL",
        interval="1m",
        open=150.0,
        high=151.0,
        low=149.5,
        close=150.5,
        volume=10000,
        trade_count=42,
        timestamp=datetime.now(timezone.utc),
    )
    assert bar.symbol == "AAPL"
    assert bar.interval == "1m"
    assert bar.volume == 10000


def test_bar_model_no_volume():
    """Bar model accepts None volume (for FX/curve bars)."""
    bar = Bar(
        symbol="EUR/USD",
        interval="5m",
        open=1.085,
        high=1.086,
        low=1.084,
        close=1.0855,
        volume=None,
        trade_count=100,
        timestamp=datetime.now(timezone.utc),
    )
    assert bar.volume is None


def test_history_query_defaults():
    """HistoryQuery has sensible defaults."""
    q = HistoryQuery(type="equity", symbol="AAPL")
    assert q.limit == 1000
    assert q.start is None
    assert q.end is None


def test_bar_query_defaults():
    """BarQuery has sensible defaults."""
    q = BarQuery(type="fx", symbol="EUR/USD")
    assert q.interval == "1m"
    assert q.start is None
    assert q.end is None


def test_bar_model_dump():
    """Bar.model_dump() returns a serializable dict."""
    ts = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    bar = Bar(
        symbol="MSFT", interval="1h",
        open=400.0, high=405.0, low=399.0, close=403.0,
        volume=50000, trade_count=200, timestamp=ts,
    )
    d = bar.model_dump()
    assert d["symbol"] == "MSFT"
    assert d["high"] == 405.0
    assert d["timestamp"] == ts


# ── Consumer Tests ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_consumer_routes_ticks_to_backend():
    """TSDBConsumer subscribes to TickBus and calls backend.write_tick()."""
    from marketdata.bus import TickBus

    bus = TickBus()
    backend = _CompleteBackend()
    consumer = TSDBConsumer(bus, backend)

    await consumer.start()

    # Publish some ticks
    now = datetime.now(timezone.utc)
    tick = Tick(
        symbol="AAPL", price=150.0, bid=149.9, ask=150.1,
        volume=1000, change=0.5, change_pct=0.33, timestamp=now,
    )
    fx_tick = FXTick(
        pair="EUR/USD", bid=1.084, ask=1.086, mid=1.085,
        spread_pips=2.0, currency="USD", timestamp=now,
    )
    curve_tick = CurveTick(
        label="USD_5Y", tenor_years=5.0, rate=0.04,
        discount_factor=0.82, currency="USD", timestamp=now,
    )

    await bus.publish(tick)
    await bus.publish(fx_tick)
    await bus.publish(curve_tick)

    # Give consumer a moment to process
    await asyncio.sleep(0.2)

    await consumer.stop()

    assert len(backend.ticks_written) == 3
    assert isinstance(backend.ticks_written[0], Tick)
    assert isinstance(backend.ticks_written[1], FXTick)
    assert isinstance(backend.ticks_written[2], CurveTick)


@pytest.mark.asyncio
async def test_consumer_stop_flushes():
    """TSDBConsumer.stop() calls backend.flush()."""
    from marketdata.bus import TickBus

    bus = TickBus()
    backend = _CompleteBackend()
    consumer = TSDBConsumer(bus, backend)

    await consumer.start()
    await consumer.stop()

    assert backend.flushed


# ── QuestDB Writer Table Mapping Tests ────────────────────────────────────────


def test_writer_table_map():
    """Writer maps message types to correct table names."""
    from timeseries.backends.questdb.writer import _TABLE_MAP

    assert _TABLE_MAP["equity"] == "equity_ticks"
    assert _TABLE_MAP["fx"] == "fx_ticks"
    assert _TABLE_MAP["curve"] == "curve_ticks"


def test_reader_table_map():
    """Reader maps message types to correct table names."""
    from timeseries.backends.questdb.reader import _TABLE_MAP

    assert _TABLE_MAP["equity"] == "equity_ticks"
    assert _TABLE_MAP["fx"] == "fx_ticks"
    assert _TABLE_MAP["curve"] == "curve_ticks"


def test_reader_symbol_col_map():
    """Reader maps message types to correct symbol column names."""
    from timeseries.backends.questdb.reader import _SYMBOL_COL

    assert _SYMBOL_COL["equity"] == "symbol"
    assert _SYMBOL_COL["fx"] == "pair"
    assert _SYMBOL_COL["curve"] == "label"


def test_reader_price_col_map():
    """Reader maps message types to correct price column names."""
    from timeseries.backends.questdb.reader import _PRICE_COL

    assert _PRICE_COL["equity"] == "price"
    assert _PRICE_COL["fx"] == "mid"
    assert _PRICE_COL["curve"] == "rate"


def test_reader_valid_intervals():
    """Reader validates bar intervals."""
    from timeseries.backends.questdb.reader import _VALID_INTERVALS

    assert "1m" in _VALID_INTERVALS
    assert "5m" in _VALID_INTERVALS
    assert "1h" in _VALID_INTERVALS
    assert "1d" in _VALID_INTERVALS
    assert "2m" not in _VALID_INTERVALS


# ── Schema DDL Tests ──────────────────────────────────────────────────────────


def test_schema_ddl_contains_tables():
    """Schema DDL contains all three tick tables."""
    from timeseries.backends.questdb.schema import ALL_DDL

    ddl_text = " ".join(ALL_DDL)
    assert "equity_ticks" in ddl_text
    assert "fx_ticks" in ddl_text
    assert "curve_ticks" in ddl_text


def test_schema_ddl_uses_symbol_type():
    """Schema DDL uses SYMBOL type for low-cardinality columns."""
    from timeseries.backends.questdb.schema import EQUITY_TICKS_DDL, FX_TICKS_DDL

    assert "symbol SYMBOL" in EQUITY_TICKS_DDL
    assert "pair SYMBOL" in FX_TICKS_DDL


def test_schema_ddl_has_timestamp():
    """All DDL statements use designated timestamp and PARTITION BY DAY."""
    from timeseries.backends.questdb.schema import ALL_DDL

    for ddl in ALL_DDL:
        assert "timestamp(timestamp)" in ddl
        assert "PARTITION BY DAY" in ddl
