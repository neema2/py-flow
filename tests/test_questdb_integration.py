"""
Real QuestDB Integration Tests
===============================
These tests hit a live QuestDB instance — ILP writes and PGWire reads.
No mocks. No memory backend. No skips. The real thing.

QuestDB is auto-started via create_backend('questdb'). Requires Java 17-21.
Hard fails if QuestDB cannot start.

Run:
  pytest tests/test_questdb_integration.py -v -s
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest
from marketdata.models import CurveTick, FXTick, Tick
from timeseries.factory import create_backend

# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _unique_symbol(prefix: str) -> str:
    """Generate a unique symbol to avoid collisions between test runs."""
    return f"{prefix}_{int(time.time() * 1000) % 1_000_000}"


# ── Real QuestDB Backend Tests ───────────────────────────────────────────────

class TestQuestDBBackendRoundTrip:
    """Write ticks via ILP, read them back via PGWire. The real pipeline."""

    @pytest.fixture(scope="class")
    def backend(self, tsdb_server):
        """Create backend client connecting to session-scoped tsdb_server's QuestDB."""
        b = create_backend(
            "questdb",
            host=tsdb_server.host,
            http_port=tsdb_server.http_port,
            ilp_port=tsdb_server.ilp_port,
            pg_port=tsdb_server.pg_port,
            auto_start=False,  # don't start new QuestDB — connect to existing
        )
        asyncio.run(b.start())  # opens reader/writer connections only
        yield b
        asyncio.run(b.stop())

    def test_write_and_read_equity_tick(self, backend):
        """Write an equity tick via ILP, read it back via PGWire."""
        sym = _unique_symbol("TEST_EQ")
        now = _now()
        tick = Tick(
            symbol=sym, price=150.50, bid=150.49, ask=150.51,
            volume=1000, change=0.5, change_pct=0.33, timestamp=now,
        )

        asyncio.run(backend.write_tick(tick))
        asyncio.run(backend.flush())

        # QuestDB needs a moment to commit WAL writes
        time.sleep(1.5)

        rows = backend.get_ticks("equity", sym, now - timedelta(seconds=5), now + timedelta(seconds=5))
        assert len(rows) >= 1, f"Expected at least 1 equity tick for {sym}, got {len(rows)}"
        row = rows[0]
        assert row["symbol"] == sym
        assert abs(row["price"] - 150.50) < 0.01

    def test_write_and_read_fx_tick(self, backend):
        """Write an FX tick via ILP, read it back via PGWire."""
        pair = _unique_symbol("TEST/FX")
        now = _now()
        tick = FXTick(
            pair=pair, bid=1.0849, ask=1.0855, mid=1.0852,
            spread_pips=0.6, currency="USD", timestamp=now,
        )

        asyncio.run(backend.write_tick(tick))
        asyncio.run(backend.flush())
        time.sleep(1.5)

        rows = backend.get_ticks("fx", pair, now - timedelta(seconds=5), now + timedelta(seconds=5))
        assert len(rows) >= 1, f"Expected at least 1 FX tick for {pair}, got {len(rows)}"
        assert abs(rows[0]["mid"] - 1.0852) < 0.0001

    def test_write_and_read_curve_tick(self, backend):
        """Write a curve tick via ILP, read it back via PGWire."""
        label = _unique_symbol("TEST_CRV")
        now = _now()
        tick = CurveTick(
            label=label, tenor_years=5.0, rate=0.041,
            discount_factor=0.8171, currency="USD", timestamp=now,
        )

        asyncio.run(backend.write_tick(tick))
        asyncio.run(backend.flush())
        time.sleep(1.5)

        rows = backend.get_ticks("curve", label, now - timedelta(seconds=5), now + timedelta(seconds=5))
        assert len(rows) >= 1, f"Expected at least 1 curve tick for {label}, got {len(rows)}"
        assert abs(rows[0]["rate"] - 0.041) < 0.001

    def test_bars_from_real_ticks(self, backend):
        """Write multiple equity ticks, then query SAMPLE BY bars."""
        sym = _unique_symbol("TEST_BAR")
        base_time = _now()
        prices = [100.0, 105.0, 95.0, 102.0, 110.0, 98.0, 103.0, 107.0]

        for i, price in enumerate(prices):
            tick = Tick(
                symbol=sym, price=price, bid=price - 0.01, ask=price + 0.01,
                volume=100 * (i + 1), change=0.0, change_pct=0.0,
                timestamp=base_time + timedelta(seconds=i),
            )
            asyncio.run(backend.write_tick(tick))

        asyncio.run(backend.flush())
        time.sleep(1.5)

        bars = backend.get_bars(
            "equity", sym, interval="5s",
            start=base_time - timedelta(seconds=1),
            end=base_time + timedelta(seconds=30),
        )
        assert len(bars) >= 1, f"Expected at least 1 bar for {sym}, got {len(bars)}"

        # Verify OHLC invariants across all bars
        all_highs = [b.high for b in bars]
        all_lows = [b.low for b in bars]
        total_trades = sum(b.trade_count for b in bars)
        assert bars[0].symbol == sym
        assert bars[0].open == 100.0  # first tick price
        assert max(all_highs) == 110.0  # global max across bars
        assert min(all_lows) == 95.0  # global min across bars
        assert total_trades == len(prices)

    def test_latest_returns_most_recent(self, backend):
        """Write two ticks for same symbol, latest returns the newer one."""
        sym = _unique_symbol("TEST_LAT")
        now = _now()

        tick1 = Tick(
            symbol=sym, price=100.0, bid=99.99, ask=100.01,
            volume=500, change=0.0, change_pct=0.0, timestamp=now,
        )
        tick2 = Tick(
            symbol=sym, price=105.0, bid=104.99, ask=105.01,
            volume=600, change=5.0, change_pct=5.0,
            timestamp=now + timedelta(seconds=1),
        )

        asyncio.run(backend.write_tick(tick1))
        asyncio.run(backend.write_tick(tick2))
        asyncio.run(backend.flush())
        time.sleep(1.5)

        latest = backend.get_latest("equity", symbol=sym)
        assert len(latest) == 1
        assert abs(latest[0]["price"] - 105.0) < 0.01


# ── Server REST → QuestDB Round-Trip ─────────────────────────────────────────


class TestServerQuestDBRoundTrip:
    """Full server integration: SimulatorFeed ticks → QuestDB → REST query.

    Uses the session-scoped market_data_server from conftest.py.
    """

    @pytest.fixture(scope="class")
    def server_url(self, market_data_server):
        """Get the URL from the session-scoped market data server."""
        return market_data_server.url

    def test_history_has_real_ticks(self, server_url):
        """GET /md/history/equity/AAPL returns real ticks from QuestDB."""
        import httpx
        resp = httpx.get(f"{server_url}/md/history/equity/AAPL", params={"limit": 10}, timeout=5)
        assert resp.status_code == 200
        ticks = resp.json()
        assert len(ticks) > 0, "No AAPL ticks in QuestDB"
        assert "price" in ticks[0]
        assert "timestamp" in ticks[0]
        assert ticks[0]["symbol"] == "AAPL"

    def test_bars_have_ohlcv(self, server_url):
        """GET /md/bars/equity/AAPL returns real OHLCV bars."""
        import httpx
        resp = httpx.get(f"{server_url}/md/bars/equity/AAPL", params={"interval": "5s"}, timeout=5)
        assert resp.status_code == 200
        bars = resp.json()
        assert len(bars) > 0, "No AAPL bars in QuestDB"
        bar = bars[0]
        for field in ("open", "high", "low", "close", "volume", "trade_count"):
            assert field in bar, f"Bar missing field: {field}"
        assert bar["high"] >= bar["low"]
        assert bar["high"] >= bar["open"]
        assert bar["high"] >= bar["close"]

    def test_fx_ticks_stored(self, server_url):
        """FX ticks from simulator are stored in QuestDB."""
        import httpx
        resp = httpx.get(f"{server_url}/md/history/fx/EUR/USD", params={"limit": 5}, timeout=5)
        assert resp.status_code == 200
        ticks = resp.json()
        assert len(ticks) > 0, "No EUR/USD ticks in QuestDB"
        assert ticks[0]["pair"] == "EUR/USD"
        assert "mid" in ticks[0]

    def test_latest_all_equity_symbols(self, server_url):
        """LATEST ON returns one row per equity symbol."""
        import httpx
        resp = httpx.get(f"{server_url}/md/latest/equity", timeout=5)
        assert resp.status_code == 200
        data = resp.json()
        symbols = {row["symbol"] for row in data}
        assert len(symbols) >= 8, f"Expected 8 equity symbols, got {symbols}"
        for expected in ("AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "NFLX"):
            assert expected in symbols

    def test_multiple_intervals(self, server_url):
        """Different bar intervals produce different bar counts."""
        import httpx
        resp_5s = httpx.get(f"{server_url}/md/bars/equity/AAPL", params={"interval": "5s"}, timeout=5)
        resp_15s = httpx.get(f"{server_url}/md/bars/equity/AAPL", params={"interval": "15s"}, timeout=5)
        bars_5s = resp_5s.json()
        bars_15s = resp_15s.json()
        # 5s bars should be at least as many as 15s bars
        assert len(bars_5s) >= len(bars_15s)
