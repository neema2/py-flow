"""
Tests for timeseries package — Pure Python (no QuestDB needed)
==============================================================
Tests the ABC contract, factory, models, consumer routing, and
table/column mapping logic without requiring a running QuestDB instance.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from marketdata.models import CurveTick, FXTick, Tick
from timeseries.base import TSDBBackend
from timeseries.consumer import TSDBConsumer
from timeseries.factory import create_backend
from timeseries.models import Bar, BarQuery, HistoryQuery

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

    def get_all_ticks(self, msg_type, since=None):
        return []

    def get_ticks(self, msg_type, symbol, start, end, limit=1000):
        return []

    def get_bars(self, msg_type, symbol, interval="1m", start=None, end=None):
        return []

    def get_latest(self, msg_type, symbol=None):
        return []


def test_abc_cannot_instantiate_incomplete():
    """TSDBBackend subclass without all abstract methods cannot be instantiated."""
    with pytest.raises(TypeError):
        _IncompleteBackend()  # type: ignore[abstract]


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


def test_bar_model_and_queries():
    """Bar model validates, accepts None volume, and serializes correctly."""
    bar = Bar(symbol="AAPL", interval="1m", open=150.0, high=151.0, low=149.5,
             close=150.5, volume=10000, trade_count=42, timestamp=datetime.now(timezone.utc))
    assert bar.symbol == "AAPL" and bar.volume == 10000
    fx_bar = Bar(symbol="EUR/USD", interval="5m", open=1.085, high=1.086, low=1.084,
                close=1.0855, volume=None, trade_count=100, timestamp=datetime.now(timezone.utc))
    assert fx_bar.volume is None
    ts = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    d = Bar(symbol="MSFT", interval="1h", open=400.0, high=405.0, low=399.0,
           close=403.0, volume=50000, trade_count=200, timestamp=ts).model_dump()
    assert d["symbol"] == "MSFT" and d["high"] == 405.0


def test_query_defaults():
    """HistoryQuery and BarQuery have sensible defaults."""
    q = HistoryQuery(type="equity", symbol="AAPL")
    assert q.limit == 1000 and q.start is None
    bq = BarQuery(type="fx", symbol="EUR/USD")
    assert bq.interval == "1m" and bq.start is None


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
        symbol="EUR/USD", pair="EUR/USD", bid=1.084, ask=1.086, mid=1.085,
        spread_pips=2.0, currency="USD", timestamp=now,
    )
    curve_tick = CurveTick(
        symbol="USD_5Y", label="USD_5Y", tenor_years=5.0, rate=0.04,
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


def test_questdb_table_and_column_maps():
    """Writer/Reader map message types to correct tables and columns."""
    from timeseries.backends.questdb.reader import _PRICE_COL, _SYMBOL_COL
    from timeseries.backends.questdb.reader import _TABLE_MAP as R_TABLE
    from timeseries.backends.questdb.writer import _TABLE_MAP as W_TABLE

    for tmap in (W_TABLE, R_TABLE):
        assert tmap["equity"] == "equity_ticks"
        assert tmap["fx"] == "fx_ticks"
        assert tmap["curve"] == "curve_ticks"
    assert _SYMBOL_COL == {"equity": "symbol", "fx": "pair", "curve": "label"}
    assert _PRICE_COL == {"equity": "price", "fx": "mid", "curve": "rate"}


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
