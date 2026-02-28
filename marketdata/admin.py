"""
marketdata.admin — Platform API for Market Data Server
=======================================================
Start/stop the market data server programmatically, register aliases.

Platform usage::

    from marketdata.admin import MarketDataServer

    server = MarketDataServer(port=8000)
    await server.start()
    server.register_alias("demo")

User code accesses via REST/WS using the alias URL.
"""

from __future__ import annotations

import asyncio
import logging
import subprocess
import sys
from typing import Optional

import httpx

from marketdata._registry import register_alias as _register_alias

logger = logging.getLogger(__name__)


class MarketDataServer:
    """Manages the market data FastAPI server as a subprocess.

    Hides uvicorn + FastAPI as implementation details.
    """

    def __init__(
        self,
        port: int = 8000,
        host: str = "0.0.0.0",
    ):
        self._port = port
        self._host = host
        self._process: Optional[subprocess.Popen] = None

    async def start(self) -> "MarketDataServer":
        """Start the market data server in a subprocess."""
        if await self.health():
            logger.info("Market data server already running on port %d", self._port)
            return self

        cmd = [
            sys.executable, "-m", "uvicorn",
            "marketdata.server:app",
            "--host", self._host,
            "--port", str(self._port),
            "--log-level", "warning",
        ]

        logger.info("Starting market data server on port %d...", self._port)
        self._process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Wait for health
        for attempt in range(30):
            await asyncio.sleep(1)
            if await self.health():
                logger.info("Market data server started on port %d", self._port)
                return self
            if self._process.poll() is not None:
                stderr = self._process.stderr.read().decode() if self._process.stderr else ""
                raise RuntimeError(
                    f"Market data server exited during startup: {stderr[:500]}"
                )

        raise RuntimeError(
            f"Market data server failed to start within 30s (port {self._port})"
        )

    async def stop(self) -> None:
        """Stop the market data server."""
        if self._process and self._process.poll() is None:
            logger.info("Stopping market data server (pid=%d)", self._process.pid)
            self._process.terminate()
            try:
                self._process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                logger.warning("Market data server did not stop gracefully, killing")
                self._process.kill()
                self._process.wait(timeout=5)
            logger.info("Market data server stopped")
        self._process = None

    async def health(self) -> bool:
        """Check if the market data server is healthy."""
        url = f"http://localhost:{self._port}/md/health"
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                resp = await client.get(url)
                return resp.status_code == 200
        except Exception:
            return False

    @property
    def port(self) -> int:
        return self._port

    @property
    def url(self) -> str:
        return f"http://localhost:{self._port}"

    def register_alias(self, name: str):
        """Register this server under an alias name."""
        _register_alias(name, url=self.url, port=self._port)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.stop()


__all__ = ["MarketDataServer"]
