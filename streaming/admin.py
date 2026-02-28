"""
streaming.admin — Platform API for Real-Time Streaming
=======================================================
Start/stop the ticking table server, register aliases.

Platform usage::

    from streaming.admin import StreamingServer

    server = StreamingServer(port=10000)
    server.start()
    server.register_alias("demo")

Hides Deephaven as an implementation detail.
"""

from __future__ import annotations

import logging

from streaming._registry import register_alias as _register_alias

logger = logging.getLogger(__name__)


class StreamingServer:
    """Real-time ticking table server.

    Wraps the Deephaven JVM server — user never sees 'Deephaven'.
    """

    def __init__(
        self,
        port: int = 10000,
        max_heap: str = "1g",
        *,
        jvm_args: list[str] | None = None,
        default_jvm_args: list[str] | None = None,
    ):
        self._port = port
        self._max_heap = max_heap
        self._jvm_args = jvm_args or [
            f"-Xmx{max_heap}",
            "-Dprocess.info.system-info.enabled=false",
            "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
        ]
        self._default_jvm_args = default_jvm_args or [
            "-XX:+UseG1GC",
            "-XX:MaxGCPauseMillis=100",
            "-XX:+UseStringDeduplication",
        ]
        self._server = None

    def start(self) -> "StreamingServer":
        """Start the streaming server (Deephaven JVM)."""
        from deephaven_server import Server

        self._server = Server(
            port=self._port,
            jvm_args=self._jvm_args,
            default_jvm_args=self._default_jvm_args,
        )
        self._server.start()
        logger.info("StreamingServer started on port %d", self._port)
        return self

    def stop(self) -> None:
        """Stop the streaming server."""
        if self._server is not None:
            # Deephaven doesn't have a clean shutdown API in all versions
            # but we nil out the reference
            self._server = None
            logger.info("StreamingServer stopped")

    @property
    def port(self) -> int:
        return self._port

    @property
    def url(self) -> str:
        return f"http://localhost:{self._port}"

    def register_alias(self, name: str):
        """Register this server under an alias name."""
        _register_alias(name, port=self._port)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()


__all__ = ["StreamingServer"]
