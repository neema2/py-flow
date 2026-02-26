"""
Integration Tests for timeseries — Real write → read round-trips
=================================================================
Uses MemoryBackend to prove the full pipeline works:
  write_tick() → get_ticks() / get_bars() / get_latest()

Also tests the full server pipeline via FastAPI TestClient:
  POST /md/publish → TSDBConsumer → MemoryBackend → GET /md/history
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone, timedelta

import pytest
import pytest_asyncio

from marketdata.models import Tick, FXTick, CurveTick
from timeseries.backends.memory import MemoryBackend
from timeseries.consumer import TSDBConsumer
from timeseries.factory import create_backend
from timeseries.models import Bar


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ts(minutes: int = 0, seconds: int = 0) -> datetime:
    """Generate a UTC timestamp offset from a fixed base."""
    base = datetime(2024, 6, 15, 10, 0, 0, tzinfo=timezone.utc)
    return base + timedelta(minutes=minutes, seconds=seconds)


def _equity_tick(symbol: str, price: float, ts: datetime) -> Tick:
    return Tick(
        symbol=symbol, price=price, bid=price - 0.01, ask=price + 0.01,
        volume=1000, change=0.0, change_pct=0.0, timestamp=ts,
    )


def _fx_tick(pair: str, mid: float, ts: datetime) -> FXTick:
    return FXTick(
        pair=pair, bid=mid - 0.0003, ask=mid + 0.0003, mid=mid,
        spread_pips=0.6, currency="USD", timestamp=ts,
    )


def _curve_tick(label: str, rate: float, ts: datetime) -> CurveTick:
    return CurveTick(
        label=label, tenor_years=5.0, rate=rate,
        discount_factor=1.0 / (1.0 + rate) ** 5.0,
        currency="USD", timestamp=ts,
    )


# ── Factory Test ──────────────────────────────────────────────────────────────

def test_factory_creates_memory_backend():
    """create_backend('memory') returns a MemoryBackend."""
    backend = create_backend("memory")
    assert isinstance(backend, MemoryBackend)


def test_factory_env_var_memory():
    """TSDB_BACKEND=memory selects MemoryBackend."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("TSDB_BACKEND", "memory")
        backend = create_backend()
        assert isinstance(backend, MemoryBackend)


# ── Write → Read Round-Trip Tests ────────────────────────────────────────────

class TestWriteReadRoundTrip:
    """Tests that write ticks and read them back."""

    @pytest_asyncio.fixture
    async def backend(self):
        b = MemoryBackend()
        await b.start()
        yield b
        await b.stop()

    @pytest.mark.asyncio
    async def test_write_equity_tick_then_read(self, backend: MemoryBackend):
        """Write an equity tick, read it back via get_ticks."""
        tick = _equity_tick("AAPL", 150.0, _ts())
        await backend.write_tick(tick)

        rows = backend.get_ticks("equity", "AAPL", _ts(-1), _ts(1))
        assert len(rows) == 1
        assert rows[0]["symbol"] == "AAPL"
        assert rows[0]["price"] == 150.0

    @pytest.mark.asyncio
    async def test_write_fx_tick_then_read(self, backend: MemoryBackend):
        """Write an FX tick, read it back via get_ticks."""
        tick = _fx_tick("EUR/USD", 1.085, _ts())
        await backend.write_tick(tick)

        rows = backend.get_ticks("fx", "EUR/USD", _ts(-1), _ts(1))
        assert len(rows) == 1
        assert rows[0]["pair"] == "EUR/USD"
        assert rows[0]["mid"] == 1.085

    @pytest.mark.asyncio
    async def test_write_curve_tick_then_read(self, backend: MemoryBackend):
        """Write a curve tick, read it back via get_ticks."""
        tick = _curve_tick("USD_5Y", 0.041, _ts())
        await backend.write_tick(tick)

        rows = backend.get_ticks("curve", "USD_5Y", _ts(-1), _ts(1))
        assert len(rows) == 1
        assert rows[0]["label"] == "USD_5Y"
        assert rows[0]["rate"] == 0.041

    @pytest.mark.asyncio
    async def test_time_range_filter(self, backend: MemoryBackend):
        """get_ticks respects start/end time range."""
        for i in range(5):
            await backend.write_tick(_equity_tick("MSFT", 400 + i, _ts(minutes=i)))

        # Query only minutes 1-3
        rows = backend.get_ticks("equity", "MSFT", _ts(minutes=1), _ts(minutes=3))
        assert len(rows) == 3
        assert rows[0]["price"] == 401.0
        assert rows[-1]["price"] == 403.0

    @pytest.mark.asyncio
    async def test_limit(self, backend: MemoryBackend):
        """get_ticks respects limit parameter."""
        for i in range(20):
            await backend.write_tick(_equity_tick("TSLA", 300 + i, _ts(seconds=i)))

        rows = backend.get_ticks("equity", "TSLA", _ts(-1), _ts(minutes=1), limit=5)
        assert len(rows) == 5

    @pytest.mark.asyncio
    async def test_symbol_isolation(self, backend: MemoryBackend):
        """Ticks for different symbols don't mix."""
        await backend.write_tick(_equity_tick("AAPL", 150, _ts()))
        await backend.write_tick(_equity_tick("MSFT", 400, _ts()))
        await backend.write_tick(_equity_tick("AAPL", 151, _ts(seconds=1)))

        aapl = backend.get_ticks("equity", "AAPL", _ts(-1), _ts(minutes=1))
        msft = backend.get_ticks("equity", "MSFT", _ts(-1), _ts(minutes=1))
        assert len(aapl) == 2
        assert len(msft) == 1

    @pytest.mark.asyncio
    async def test_type_isolation(self, backend: MemoryBackend):
        """Ticks for different types don't mix."""
        await backend.write_tick(_equity_tick("AAPL", 150, _ts()))
        await backend.write_tick(_fx_tick("EUR/USD", 1.085, _ts()))

        equity = backend.get_ticks("equity", "AAPL", _ts(-1), _ts(1))
        fx = backend.get_ticks("fx", "EUR/USD", _ts(-1), _ts(1))
        assert len(equity) == 1
        assert len(fx) == 1

    @pytest.mark.asyncio
    async def test_tick_count(self, backend: MemoryBackend):
        """tick_count property tracks total stored ticks."""
        assert backend.tick_count == 0
        await backend.write_tick(_equity_tick("AAPL", 150, _ts()))
        await backend.write_tick(_fx_tick("EUR/USD", 1.085, _ts()))
        assert backend.tick_count == 2


# ── Bar Aggregation Tests ────────────────────────────────────────────────────

class TestBarAggregation:
    """Tests that bars are aggregated correctly from raw ticks."""

    @pytest_asyncio.fixture
    async def backend(self):
        b = MemoryBackend()
        await b.start()
        yield b
        await b.stop()

    @pytest.mark.asyncio
    async def test_single_bar(self, backend: MemoryBackend):
        """Multiple ticks in same interval produce one bar."""
        for i in range(10):
            price = 100.0 + i
            await backend.write_tick(_equity_tick("AAPL", price, _ts(seconds=i)))

        bars = backend.get_bars("equity", "AAPL", interval="1m")
        assert len(bars) == 1
        bar = bars[0]
        assert bar.symbol == "AAPL"
        assert bar.interval == "1m"
        assert bar.open == 100.0
        assert bar.high == 109.0
        assert bar.low == 100.0
        assert bar.close == 109.0
        assert bar.trade_count == 10

    @pytest.mark.asyncio
    async def test_multiple_bars(self, backend: MemoryBackend):
        """Ticks spanning multiple intervals produce multiple bars."""
        # 5 ticks per minute for 3 minutes
        for minute in range(3):
            for sec in range(5):
                price = 200.0 + minute * 10 + sec
                await backend.write_tick(
                    _equity_tick("MSFT", price, _ts(minutes=minute, seconds=sec * 10))
                )

        bars = backend.get_bars("equity", "MSFT", interval="1m")
        assert len(bars) == 3
        # First bar: prices 200-204
        assert bars[0].open == 200.0
        assert bars[0].close == 204.0
        assert bars[0].trade_count == 5
        # Last bar: prices 220-224
        assert bars[2].open == 220.0
        assert bars[2].close == 224.0

    @pytest.mark.asyncio
    async def test_bar_ohlcv_correctness(self, backend: MemoryBackend):
        """OHLCV values are computed correctly (not just first/last)."""
        prices = [100.0, 105.0, 95.0, 102.0, 110.0, 98.0]
        for i, price in enumerate(prices):
            await backend.write_tick(_equity_tick("TSLA", price, _ts(seconds=i)))

        bars = backend.get_bars("equity", "TSLA", interval="1m")
        assert len(bars) == 1
        bar = bars[0]
        assert bar.open == 100.0
        assert bar.high == 110.0
        assert bar.low == 95.0
        assert bar.close == 98.0

    @pytest.mark.asyncio
    async def test_fx_bars(self, backend: MemoryBackend):
        """FX bars use 'mid' as the price column."""
        mids = [1.085, 1.086, 1.084, 1.0855]
        for i, mid in enumerate(mids):
            await backend.write_tick(_fx_tick("EUR/USD", mid, _ts(seconds=i)))

        bars = backend.get_bars("fx", "EUR/USD", interval="1m")
        assert len(bars) == 1
        assert bars[0].open == 1.085
        assert bars[0].high == 1.086
        assert bars[0].low == 1.084
        assert bars[0].close == 1.0855
        assert bars[0].volume is None  # FX has no volume

    @pytest.mark.asyncio
    async def test_curve_bars(self, backend: MemoryBackend):
        """Curve bars use 'rate' as the price column."""
        rates = [0.040, 0.041, 0.039, 0.0405]
        for i, rate in enumerate(rates):
            await backend.write_tick(_curve_tick("USD_5Y", rate, _ts(seconds=i)))

        bars = backend.get_bars("curve", "USD_5Y", interval="1m")
        assert len(bars) == 1
        assert bars[0].open == 0.040
        assert bars[0].high == 0.041
        assert bars[0].low == 0.039
        assert bars[0].close == 0.0405

    @pytest.mark.asyncio
    async def test_bar_interval_5s(self, backend: MemoryBackend):
        """5s interval buckets ticks correctly."""
        # 10 ticks, 1 per second → should produce 2 bars at 5s interval
        for i in range(10):
            await backend.write_tick(_equity_tick("NVDA", 800 + i, _ts(seconds=i)))

        bars = backend.get_bars("equity", "NVDA", interval="5s")
        assert len(bars) == 2
        assert bars[0].trade_count == 5
        assert bars[1].trade_count == 5

    @pytest.mark.asyncio
    async def test_bar_returns_bar_model(self, backend: MemoryBackend):
        """get_bars returns actual Bar model instances."""
        await backend.write_tick(_equity_tick("META", 700, _ts()))
        bars = backend.get_bars("equity", "META", interval="1m")
        assert len(bars) == 1
        assert isinstance(bars[0], Bar)
        assert bars[0].model_dump()["symbol"] == "META"

    @pytest.mark.asyncio
    async def test_invalid_interval_raises(self, backend: MemoryBackend):
        """Invalid interval raises ValueError."""
        with pytest.raises(ValueError, match="Invalid interval"):
            backend.get_bars("equity", "AAPL", interval="2m")

    @pytest.mark.asyncio
    async def test_unknown_type_raises(self, backend: MemoryBackend):
        """Unknown message type raises ValueError."""
        with pytest.raises(ValueError, match="Unknown message type"):
            backend.get_bars("crypto", "BTC", interval="1m")


# ── get_latest Tests ──────────────────────────────────────────────────────────

class TestGetLatest:
    """Tests for the get_latest endpoint."""

    @pytest_asyncio.fixture
    async def backend(self):
        b = MemoryBackend()
        await b.start()
        yield b
        await b.stop()

    @pytest.mark.asyncio
    async def test_latest_per_symbol(self, backend: MemoryBackend):
        """get_latest returns the most recent tick per symbol."""
        await backend.write_tick(_equity_tick("AAPL", 150, _ts(seconds=0)))
        await backend.write_tick(_equity_tick("AAPL", 155, _ts(seconds=1)))
        await backend.write_tick(_equity_tick("MSFT", 400, _ts(seconds=0)))

        latest = backend.get_latest("equity")
        assert len(latest) == 2
        aapl = [r for r in latest if r["symbol"] == "AAPL"][0]
        assert aapl["price"] == 155  # latest, not first

    @pytest.mark.asyncio
    async def test_latest_single_symbol(self, backend: MemoryBackend):
        """get_latest with symbol filter returns only that symbol."""
        await backend.write_tick(_equity_tick("AAPL", 150, _ts()))
        await backend.write_tick(_equity_tick("MSFT", 400, _ts()))

        latest = backend.get_latest("equity", symbol="AAPL")
        assert len(latest) == 1
        assert latest[0]["symbol"] == "AAPL"

    @pytest.mark.asyncio
    async def test_latest_unknown_symbol(self, backend: MemoryBackend):
        """get_latest for non-existent symbol returns empty."""
        await backend.write_tick(_equity_tick("AAPL", 150, _ts()))
        assert backend.get_latest("equity", symbol="NOPE") == []

    @pytest.mark.asyncio
    async def test_latest_empty(self, backend: MemoryBackend):
        """get_latest on empty store returns empty."""
        assert backend.get_latest("equity") == []


# ── TSDBConsumer → MemoryBackend Integration ──────────────────────────────────

class TestConsumerIntegration:
    """Tests that TSDBConsumer routes real ticks through to a MemoryBackend."""

    @pytest.mark.asyncio
    async def test_bus_to_backend_round_trip(self):
        """Publish ticks to TickBus → TSDBConsumer → MemoryBackend → query back."""
        from marketdata.bus import TickBus

        bus = TickBus()
        backend = MemoryBackend()
        await backend.start()
        consumer = TSDBConsumer(bus, backend)
        await consumer.start()

        # Publish ticks
        now = _ts()
        await bus.publish(_equity_tick("AAPL", 150, now))
        await bus.publish(_equity_tick("AAPL", 151, _ts(seconds=1)))
        await bus.publish(_equity_tick("AAPL", 149, _ts(seconds=2)))
        await bus.publish(_fx_tick("EUR/USD", 1.085, now))
        await bus.publish(_curve_tick("USD_5Y", 0.041, now))

        # Give consumer time to process
        await asyncio.sleep(0.3)
        await consumer.stop()

        # Verify equity ticks arrived
        rows = backend.get_ticks("equity", "AAPL", _ts(-1), _ts(minutes=1))
        assert len(rows) == 3
        assert rows[0]["price"] == 150
        assert rows[2]["price"] == 149

        # Verify FX tick arrived
        fx = backend.get_ticks("fx", "EUR/USD", _ts(-1), _ts(1))
        assert len(fx) == 1

        # Verify curve tick arrived
        curve = backend.get_ticks("curve", "USD_5Y", _ts(-1), _ts(1))
        assert len(curve) == 1

        # Verify bars work on the ingested data
        bars = backend.get_bars("equity", "AAPL", interval="1m")
        assert len(bars) == 1
        assert bars[0].open == 150
        assert bars[0].high == 151
        assert bars[0].low == 149
        assert bars[0].close == 149
        assert bars[0].trade_count == 3

        # Verify latest
        latest = backend.get_latest("equity")
        assert len(latest) == 1
        assert latest[0]["symbol"] == "AAPL"
        assert latest[0]["price"] == 149  # last written

        await backend.stop()


# ── Server Integration Tests (FastAPI TestClient) ─────────────────────────────

class TestServerHistoricalEndpoints:
    """Full server integration: publish ticks → query via REST endpoints."""

    @pytest.fixture
    def client(self):
        """Create a TestClient with TSDB_BACKEND=memory."""
        os.environ["TSDB_BACKEND"] = "memory"
        os.environ["TSDB_ENABLED"] = "1"
        try:
            from fastapi.testclient import TestClient
            from marketdata.server import app
            with TestClient(app) as c:
                yield c
        finally:
            os.environ.pop("TSDB_BACKEND", None)
            os.environ.pop("TSDB_ENABLED", None)

    def test_health_shows_tsdb(self, client):
        """Health endpoint should work with TSDB enabled."""
        resp = client.get("/md/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_history_returns_ticks(self, client):
        """GET /md/history returns ticks that flowed through the bus."""
        import time
        # Give the simulator a moment to produce ticks
        time.sleep(1.5)

        # The simulator produces equity ticks for AAPL
        resp = client.get("/md/history/equity/AAPL")
        assert resp.status_code == 200
        ticks = resp.json()
        assert len(ticks) > 0
        assert ticks[0]["symbol"] == "AAPL"
        assert "price" in ticks[0]

    def test_bars_returns_ohlcv(self, client):
        """GET /md/bars returns OHLCV bars from ingested ticks."""
        import time
        time.sleep(1.5)

        resp = client.get("/md/bars/equity/AAPL", params={"interval": "1s"})
        assert resp.status_code == 200
        bars = resp.json()
        assert len(bars) > 0
        bar = bars[0]
        assert "open" in bar
        assert "high" in bar
        assert "low" in bar
        assert "close" in bar
        assert "trade_count" in bar

    def test_latest_returns_data(self, client):
        """GET /md/latest returns latest ticks from TSDB."""
        import time
        time.sleep(1.5)

        resp = client.get("/md/latest/equity")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) > 0
        # Simulator produces 8 equity symbols
        symbols = {row["symbol"] for row in data}
        assert "AAPL" in symbols

    def test_fx_history(self, client):
        """FX ticks are also captured in TSDB."""
        import time
        time.sleep(1.5)

        resp = client.get("/md/history/fx/EUR/USD")
        assert resp.status_code == 200
        ticks = resp.json()
        assert len(ticks) > 0
        assert ticks[0]["pair"] == "EUR/USD"

    def test_bars_by_type(self, client):
        """GET /md/bars/{type} returns bars for all symbols."""
        import time
        time.sleep(1.5)

        resp = client.get("/md/bars/equity", params={"interval": "1s"})
        assert resp.status_code == 200
        data = resp.json()
        # Should have bars keyed by symbol
        assert isinstance(data, dict)
        assert len(data) > 0

    def test_history_with_limit(self, client):
        """Limit parameter is respected."""
        import time
        time.sleep(1.5)

        resp = client.get("/md/history/equity/AAPL", params={"limit": 2})
        assert resp.status_code == 200
        ticks = resp.json()
        assert len(ticks) <= 2
