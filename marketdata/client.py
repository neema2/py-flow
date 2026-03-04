"""
MarketDataClient — User-facing market data client with alias support.

Connects to a running MarketDataServer via alias, or explicit URL.

Usage::

    from marketdata import MarketDataClient

    # Via alias (registered by MarketDataServer)
    md = MarketDataClient("demo")

    # Or explicit URL
    md = MarketDataClient(url="http://localhost:8000")

    symbols = md.symbols()
    snap = md.snapshot("AAPL")
    ok = md.health()
"""

from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)

DEFAULT_URL = "http://localhost:8000"


class MarketDataClient:
    """User-facing market data client.

    Resolves alias → base URL, then provides REST helpers for
    snapshots, symbols, health, and publishing.
    """

    def __init__(
        self,
        alias_or_url: str | None = None,
        *,
        url: str | None = None,
    ) -> None:
        resolved = self._resolve(alias_or_url, url)
        self._base_url = resolved.get("url", DEFAULT_URL)

    @staticmethod
    def _resolve(alias_or_url: str | None, url: str | None) -> dict:
        """Resolve alias or explicit URL."""
        if alias_or_url is not None:
            from marketdata._registry import resolve_alias
            resolved = resolve_alias(alias_or_url)
            if resolved is not None:
                return resolved
            # Not an alias — treat as explicit URL
            return {"url": alias_or_url}
        if url is not None:
            return {"url": url}
        return {}

    @property
    def base_url(self) -> str:
        """The resolved base URL for the market data server."""
        return str(self._base_url)

    def health(self) -> bool:
        """Check if the market data server is healthy."""
        try:
            resp = httpx.get(f"{self._base_url}/md/health", timeout=2.0)
            return resp.status_code == 200
        except Exception:
            return False

    def symbols(self) -> list[str]:
        """List all available symbols."""
        try:
            resp = httpx.get(f"{self._base_url}/md/symbols", timeout=5.0)
            resp.raise_for_status()
            return list(resp.json())
        except Exception as e:
            logger.error("Failed to list symbols: %s", e)
            return []

    def snapshot(self, symbol: str) -> dict:
        """Get the latest snapshot for a symbol."""
        try:
            resp = httpx.get(
                f"{self._base_url}/md/snapshot/{symbol}", timeout=5.0
            )
            resp.raise_for_status()
            return dict(resp.json())
        except Exception as e:
            logger.error("Failed to get snapshot for %s: %s", symbol, e)
            return {"error": str(e)}

    def publish(self, tick: dict) -> dict:
        """Publish a custom tick to the market data bus."""
        try:
            resp = httpx.post(
                f"{self._base_url}/md/publish",
                json=tick,
                timeout=5.0,
            )
            resp.raise_for_status()
            return dict(resp.json())
        except Exception as e:
            logger.error("Failed to publish tick: %s", e)
            return {"error": str(e)}
