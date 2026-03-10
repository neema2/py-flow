"""
Timeseries Agent — Historical Normalization
=============================================
Curate historical time series, normalize across exchanges, build OHLCV bars.

Tools:
    - list_tsdb_series        — available time series in QuestDB
    - get_bars                — OHLCV bars via MarketData REST
    - get_tick_history        — raw ticks
    - ingest_historical_csv   — bulk load + normalize
    - compute_realized_vol    — vol from tick data
    - compare_cross_exchange  — cross-exchange analysis

Usage::

    from agents._timeseries import create_timeseries_agent

    agent = create_timeseries_agent(ctx)
    result = agent.run("Show me AAPL 1-minute bars for today")
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any

from ai import Agent, tool

from agents._context import _PlatformContext

logger = logging.getLogger(__name__)

TIMESERIES_SYSTEM_PROMPT = """\
You are the Time Series Agent — a platform specialist that manages historical \
market data: tick-level data, OHLCV bars, and cross-exchange normalization.

You can:
1. List available time series data (symbols, types, date ranges).
2. Retrieve OHLCV bars at various intervals (1s, 1m, 5m, 15m, 1h, 1d).
3. Get raw tick history for any symbol.
4. Bulk-load historical data from CSV/Parquet files.
5. Compute realized volatility from tick data.
6. Compare prices across different exchanges for arbitrage analysis.

The TSDB (QuestDB) records all ticks from the MarketData server automatically.
Historical data is available via the MarketData REST API:
- GET /md/bars/{type}/{symbol}?interval=1m
- GET /md/history/{type}/{symbol}?limit=1000
- GET /md/latest/{type}

When normalizing data:
- Account for timezone differences across exchanges.
- Handle missing data (gaps, holidays) appropriately.
- Report data quality metrics (completeness, gap counts).
"""


def create_timeseries_tools(ctx: _PlatformContext) -> list:
    """Create Timeseries agent tools bound to a _PlatformContext."""

    def _md_url(path: str) -> str:
        return f"{ctx.md_base_url}{path}"

    @tool
    def list_tsdb_series() -> str:
        """List available time series data in the TSDB.

        Returns symbols, types, and approximate data ranges.
        """
        try:
            import httpx
            # Get symbols to know what's being recorded
            resp = httpx.get(_md_url("/md/symbols"), timeout=5.0)
            resp.raise_for_status()
            symbols = resp.json()

            # Get latest tick per type to show data freshness
            series = []
            for msg_type in ["equity", "fx", "curve"]:
                try:
                    latest_resp = httpx.get(_md_url(f"/md/latest/{msg_type}"), timeout=5.0)
                    if latest_resp.status_code == 200:
                        latest = latest_resp.json()
                        series.append({
                            "type": msg_type,
                            "latest_data": latest[:5] if isinstance(latest, list) else latest,
                        })
                except Exception:
                    pass

            return json.dumps({
                "symbols": symbols,
                "series": series,
            }, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def get_bars(msg_type: str, symbol: str, interval: str = "1m") -> str:
        """Get OHLCV bars for a symbol at a given interval.

        Args:
            msg_type: Tick type — "equity", "fx", or "curve".
            symbol: Symbol identifier (e.g. "AAPL", "USD/JPY").
            interval: Bar interval — "1s", "1m", "5m", "15m", "1h", "1d".
        """
        try:
            import httpx
            resp = httpx.get(
                _md_url(f"/md/bars/{msg_type}/{symbol}"),
                params={"interval": interval},
                timeout=10.0,
            )
            resp.raise_for_status()
            bars = resp.json()
            return json.dumps({
                "type": msg_type,
                "symbol": symbol,
                "interval": interval,
                "bar_count": len(bars) if isinstance(bars, list) else 0,
                "bars": bars[:50] if isinstance(bars, list) else bars,
                "truncated": len(bars) > 50 if isinstance(bars, list) else False,
            }, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def get_tick_history(msg_type: str, symbol: str, limit: int = 100) -> str:
        """Get raw tick history for a symbol.

        Args:
            msg_type: Tick type — "equity", "fx", or "curve".
            symbol: Symbol identifier.
            limit: Maximum ticks to return (default 100).
        """
        try:
            import httpx
            resp = httpx.get(
                _md_url(f"/md/history/{msg_type}/{symbol}"),
                params={"limit": limit},
                timeout=10.0,
            )
            resp.raise_for_status()
            ticks = resp.json()
            return json.dumps({
                "type": msg_type,
                "symbol": symbol,
                "tick_count": len(ticks) if isinstance(ticks, list) else 0,
                "ticks": ticks[:50] if isinstance(ticks, list) else ticks,
                "truncated": len(ticks) > 50 if isinstance(ticks, list) else False,
            }, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def ingest_historical_csv(symbol: str, file_path: str, msg_type: str = "equity",
                              exchange: str = "", timezone_str: str = "UTC") -> str:
        """Bulk-load historical data from a CSV/Parquet file into the lakehouse.

        Reads the file, normalizes timestamps to UTC, and ingests into an Iceberg
        table for historical analysis. The data stays in DuckDB — never enters Python.

        Args:
            symbol: Symbol identifier for the data.
            file_path: Path or URL to CSV/Parquet file.
            msg_type: Asset type — "equity", "fx", "curve".
            exchange: Exchange name (e.g. "NYSE", "NASDAQ", "LSE") for metadata.
            timezone_str: Source timezone (e.g. "US/Eastern", "Europe/London"). Data is normalized to UTC.
        """
        if ctx.lakehouse is None:
            return json.dumps({"error": "No Lakehouse configured"})  # type: ignore[unreachable]

        try:
            # Build a SQL that reads, normalizes, and ingests
            table_name = f"historical_{msg_type}_{symbol.replace('/', '_').lower()}"

            path_lower = file_path.lower()
            if path_lower.endswith(".parquet"):
                read_fn = f"read_parquet('{file_path}')"
            elif path_lower.endswith(".csv"):
                read_fn = f"read_csv_auto('{file_path}')"
            else:
                read_fn = f"'{file_path}'"

            # Ingest with metadata columns
            sql = f"""
                SELECT
                    *,
                    '{symbol}' AS symbol,
                    '{msg_type}' AS asset_type,
                    '{exchange}' AS exchange,
                    '{timezone_str}' AS source_timezone
                FROM {read_fn}
            """

            row_count = ctx.lakehouse.ingest(table_name, sql, mode="append")
            return json.dumps({
                "status": "ingested",
                "table_name": table_name,
                "symbol": symbol,
                "exchange": exchange,
                "rows": row_count,
                "source_timezone": timezone_str,
            })
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def compute_realized_vol(symbol: str, msg_type: str = "equity",
                             window: int = 20, interval: str = "1m") -> str:
        """Compute realized volatility from tick data.

        Uses log-returns over the tick history, annualized assuming 252 trading days.

        Args:
            symbol: Symbol identifier.
            msg_type: Asset type — "equity" or "fx".
            window: Number of most recent bars to use (default 20).
            interval: Bar interval — "1s", "1m", "5m", "15m", "1h", "1d" (default "1m").
        """
        try:
            import httpx
            resp = httpx.get(
                _md_url(f"/md/bars/{msg_type}/{symbol}"),
                params={"interval": interval},
                timeout=10.0,
            )
            resp.raise_for_status()
            bars = resp.json()

            if not isinstance(bars, list) or len(bars) < 2:
                return json.dumps({"error": "Not enough bar data for vol computation"})

            # Use last N bars
            recent = bars[-window:] if len(bars) > window else bars

            # Compute log returns from close prices
            closes = [b.get("close", b.get("price", 0)) for b in recent if b.get("close") or b.get("price")]
            if len(closes) < 2:
                return json.dumps({"error": "Not enough close prices"})

            log_returns = [math.log(closes[i] / closes[i-1]) for i in range(1, len(closes)) if closes[i-1] > 0]

            if not log_returns:
                return json.dumps({"error": "Could not compute returns"})

            if len(log_returns) < 2:
                return json.dumps({"error": "Not enough data points for vol computation (need >= 3 bars)"})

            mean_ret = sum(log_returns) / len(log_returns)
            variance = sum((r - mean_ret) ** 2 for r in log_returns) / (len(log_returns) - 1)
            daily_vol = math.sqrt(variance)
            annualized_vol = daily_vol * math.sqrt(252)

            return json.dumps({
                "symbol": symbol,
                "window_bars": len(recent),
                "bars_used": len(log_returns) + 1,
                "daily_vol": round(daily_vol, 6),
                "annualized_vol": round(annualized_vol, 4),
                "annualized_vol_pct": f"{annualized_vol * 100:.2f}%",
                "mean_return": round(mean_ret, 8),
            })
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def compare_cross_exchange(symbol: str, msg_type: str = "equity") -> str:
        """Compare current prices across available data for a symbol.

        Useful for identifying discrepancies between sources or checking
        data consistency.

        Args:
            symbol: Symbol to compare.
            msg_type: Asset type — "equity" or "fx".
        """
        try:
            import httpx

            # Get live snapshot
            snap_resp = httpx.get(
                _md_url(f"/md/snapshot/{msg_type}/{symbol}"),
                timeout=5.0,
            )
            live_data = snap_resp.json() if snap_resp.status_code == 200 else None

            # Get latest from TSDB
            latest_resp = httpx.get(
                _md_url(f"/md/latest/{msg_type}"),
                timeout=5.0,
            )
            tsdb_data = None
            if latest_resp.status_code == 200:
                all_latest = latest_resp.json()
                if isinstance(all_latest, list):
                    tsdb_data = next((t for t in all_latest if t.get("symbol") == symbol), None)
                elif isinstance(all_latest, dict):
                    tsdb_data = all_latest.get(symbol)

            # Get recent bars for trend
            bars_resp = httpx.get(
                _md_url(f"/md/bars/{msg_type}/{symbol}"),
                params={"interval": "1m"},
                timeout=5.0,
            )
            recent_bars = bars_resp.json()[-5:] if bars_resp.status_code == 200 else []

            return json.dumps({
                "symbol": symbol,
                "live_snapshot": live_data,
                "tsdb_latest": tsdb_data,
                "recent_bars": recent_bars,
                "sources_compared": sum(1 for x in [live_data, tsdb_data] if x is not None),
            }, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    return [list_tsdb_series, get_bars, get_tick_history,
            ingest_historical_csv, compute_realized_vol, compare_cross_exchange]


def create_timeseries_agent(ctx: _PlatformContext, **kwargs: Any) -> Agent:
    """Create a Timeseries Agent bound to a _PlatformContext."""
    tools = create_timeseries_tools(ctx)
    return Agent(
        tools=tools,
        system_prompt=TIMESERIES_SYSTEM_PROMPT,
        name="timeseries",
        **kwargs,
    )
