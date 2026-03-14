"""
Market Data Server — FastAPI Application
=========================================
Standalone market data service providing REST snapshots and WebSocket streaming.

Run:  python -m marketdata.server
      uvicorn marketdata.server:app --port 8000
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import RedirectResponse
from timeseries import TSDBBackend
from timeseries.admin import TSDBConsumer, create_backend

from marketdata.bus import TickBus
from marketdata.consumers.ws_publisher import WebSocketPublisher
from marketdata.feeds.simulator import FX_PAIRS, SWAP_INSTRUMENTS, SYMBOLS, SimulatorFeed
from marketdata.models import (
    MarketDataMessage,
    Subscription,
    get_symbol_key,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Startup: create bus, start feed + WS publisher + TSDB. Shutdown: stop all."""
    bus = TickBus()
    feed = SimulatorFeed()
    ws_pub = WebSocketPublisher(bus)

    # Start TSDB backend + consumer (pluggable via TSDB_BACKEND env)
    tsdb: TSDBBackend | None = None
    tsdb_consumer: TSDBConsumer | None = None
    tsdb_enabled = os.environ.get("TSDB_ENABLED", "1").lower() in ("1", "true", "yes")
    if tsdb_enabled:
        try:
            tsdb = create_backend()
            await tsdb.start()
            tsdb_consumer = TSDBConsumer(bus, tsdb)
            await tsdb_consumer.start()
            logger.info("TSDB backend started: %s", type(tsdb).__name__)
        except Exception as exc:
            logger.warning("TSDB backend failed to start — historical endpoints disabled: %s", exc)
            tsdb = None
            tsdb_consumer = None

    feed_task = asyncio.create_task(feed.start(bus))
    await ws_pub.start()

    app.state.bus = bus
    app.state.feed = feed
    app.state.ws_publisher = ws_pub
    app.state.tsdb = tsdb
    app.state.tsdb_consumer = tsdb_consumer

    logger.info("Market Data Server started on all interfaces")

    yield

    if tsdb_consumer:
        await tsdb_consumer.stop()
    await feed.stop()
    await ws_pub.stop()
    if tsdb:
        await tsdb.stop()
    feed_task.cancel()
    try:
        await feed_task
    except asyncio.CancelledError:
        pass

    logger.info("Market Data Server stopped")


app = FastAPI(
    title="Market Data Server",
    description="Real-time market data via REST and WebSocket",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    """Redirect to API documentation."""
    return RedirectResponse(url="/docs")


@app.get("/md/health")
async def health() -> dict:
    """Server health check with per-type counts."""
    bus: TickBus = app.state.bus
    ws_pub: WebSocketPublisher = app.state.ws_publisher
    type_counts: dict[str, int] = {}
    for (msg_type, _key) in bus.latest:
        type_counts[msg_type] = type_counts.get(msg_type, 0) + 1
    return {
        "status": "ok",
        "feed": app.state.feed.name,
        "asset_types": type_counts,
        "subscribers": bus.subscriber_count,
        "ws_clients": ws_pub.client_count,
        "latest_count": len(bus.latest),
    }


@app.get("/md/symbols")
async def get_symbols() -> dict:
    """Return the symbol universe grouped by type."""
    return {
        "equity": list(SYMBOLS),
        "fx": list(FX_PAIRS),
        "swap": list(SWAP_INSTRUMENTS),
        "jacobian": ["IR_USD_YC_JACOBIAN.*"],
    }


@app.get("/md/snapshot")
async def get_all_snapshots() -> dict:
    """Get latest messages for all types, keyed by 'type:symbol'."""
    bus: TickBus = app.state.bus
    return {
        f"{msg_type}:{key}": msg.model_dump()
        for (msg_type, key), msg in bus.latest.items()
    }


@app.get("/md/snapshot/{msg_type}")
async def get_snapshots_by_type(msg_type: str) -> dict:
    """Get latest messages filtered by type (equity, fx, curve)."""
    bus: TickBus = app.state.bus
    return {
        key: msg.model_dump()
        for (t, key), msg in bus.latest.items()
        if t == msg_type
    }


@app.get("/md/snapshot/{msg_type}/{symbol}")
async def get_snapshot_by_type_symbol(msg_type: str, symbol: str) -> dict:
    """Get the latest message for a specific type and symbol."""
    bus: TickBus = app.state.bus
    msg = bus.latest.get((msg_type, symbol))
    if msg is None:
        return {"symbol": symbol, "type": msg_type, "data": None}
    return msg.model_dump()


# ── Historical Data Endpoints ────────────────────────────────────────────────


def _require_tsdb() -> TSDBBackend:
    """Return the TSDB backend or raise 503 if unavailable."""
    tsdb: TSDBBackend | None = getattr(app.state, "tsdb", None)
    if tsdb is None:
        raise HTTPException(status_code=503, detail="TSDB backend not available")
    return tsdb


@app.get("/md/history/{msg_type}/{symbol:path}")
async def get_tick_history(
    msg_type: str,
    symbol: str,
    start: datetime | None = Query(None, description="Start time (ISO 8601)"),
    end: datetime | None = Query(None, description="End time (ISO 8601)"),
    limit: int = Query(1000, ge=1, le=10000, description="Max rows"),
) -> list[dict[str, Any]]:
    """Raw tick history for a symbol within a time range."""
    tsdb = _require_tsdb()
    if start is None:
        start = datetime(2000, 1, 1)
    if end is None:
        end = datetime(2099, 12, 31)
    return tsdb.get_ticks(msg_type, symbol, start, end, limit)


@app.get("/md/bars/{msg_type}/{symbol:path}")
async def get_bars(
    msg_type: str,
    symbol: str,
    interval: str = Query("1m", description="Bar interval: 1s,5s,1m,5m,15m,1h,4h,1d"),
    start: datetime | None = Query(None, description="Start time (ISO 8601)"),
    end: datetime | None = Query(None, description="End time (ISO 8601)"),
) -> list[dict[str, Any]]:
    """OHLCV bars for a symbol at the given interval."""
    tsdb = _require_tsdb()
    bars = tsdb.get_bars(msg_type, symbol, interval, start, end)
    return [bar.model_dump() for bar in bars]


@app.get("/md/bars/{msg_type}")
async def get_bars_by_type(
    msg_type: str,
    interval: str = Query("1h", description="Bar interval"),
    start: datetime | None = Query(None, description="Start time (ISO 8601)"),
    end: datetime | None = Query(None, description="End time (ISO 8601)"),
) -> dict[str, list[dict[str, Any]]]:
    """Latest bars for all symbols of a given type."""
    tsdb = _require_tsdb()
    latest = tsdb.get_latest(msg_type)
    # Get bars for each unique symbol found in latest
    raw_symbols = {row.get("symbol") or row.get("pair") or row.get("label") for row in latest}
    symbols: set[str] = {s for s in raw_symbols if isinstance(s, str)}
    result = {}
    for sym in sorted(symbols):
        bars = tsdb.get_bars(msg_type, sym, interval, start, end)
        result[sym] = [bar.model_dump() for bar in bars]
    return result


@app.get("/md/latest/{msg_type}")
async def get_latest_from_tsdb(
    msg_type: str,
    symbol: str | None = Query(None, description="Filter by symbol"),
) -> list[dict[str, Any]]:
    """Latest tick(s) per symbol from the time-series store."""
    tsdb = _require_tsdb()
    return tsdb.get_latest(msg_type, symbol)


# ── Publish Endpoint ─────────────────────────────────────────────────────────


@app.post("/md/publish")
async def publish_message(payload: dict) -> dict:
    """Publish a market data message to the bus.

    Accepts any MarketDataMessage JSON (must include a 'type' discriminator).
    """
    from pydantic import TypeAdapter
    adapter: TypeAdapter[MarketDataMessage] = TypeAdapter(MarketDataMessage)
    msg = adapter.validate_python(payload)
    bus: TickBus = app.state.bus
    await bus.publish(msg)
    return {"status": "published", "type": msg.type, "key": get_symbol_key(msg)}


# ── WebSocket Endpoint ────────────────────────────────────────────────────────


@app.websocket("/md/subscribe")
async def websocket_subscribe(ws: WebSocket) -> None:
    """Bidirectional WebSocket endpoint for market data.

    Protocol:
    1. Client connects
    2. Client sends JSON — either:
       a. Subscription: {"types": [...], "symbols": [...]} to set filter
       b. MarketDataMessage: {"type": "curve", ...} to publish to bus
    3. Server streams matching messages as JSON
    4. Client can send subscription updates or publish messages at any time
    """
    await ws.accept()
    bus: TickBus = app.state.bus
    ws_pub: WebSocketPublisher = app.state.ws_publisher

    # Default: subscribe to all until client sends a filter
    sub = Subscription(types=None, symbols=None)
    await ws_pub.register(ws, sub)

    try:
        while True:
            data = await ws.receive_json()
            # If message has a 'type' field with a known asset type → publish
            if "type" in data and data["type"] in ("equity", "fx", "curve", "swap", "jacobian"):
                from pydantic import TypeAdapter
                adapter: TypeAdapter[MarketDataMessage] = TypeAdapter(MarketDataMessage)
                msg = adapter.validate_python(data)
                await bus.publish(msg)
                logger.debug(
                    "WS client %s published %s:%s",
                    id(ws), msg.type, get_symbol_key(msg),
                )
            else:
                # Treat as subscription update
                new_sub = Subscription.model_validate(data)
                await ws_pub.update_subscription(ws, new_sub)
                logger.info(
                    "WS client %s updated subscription: types=%s symbols=%s",
                    id(ws), new_sub.types, new_sub.symbols,
                )
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug("WS client %s error: %s", id(ws), e)
    finally:
        await ws_pub.unregister(ws)


# ── CLI Entry Point ───────────────────────────────────────────────────────────


def main() -> None:
    """Run the market data server via uvicorn."""
    import argparse

    import uvicorn

    parser = argparse.ArgumentParser(description="Market Data Server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8000, help="Bind port")
    parser.add_argument("--log-level", default="info", help="Log level")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    uvicorn.run(
        "marketdata.server:app",
        host=args.host,
        port=args.port,
        log_level=args.log_level,
    )


if __name__ == "__main__":
    main()
