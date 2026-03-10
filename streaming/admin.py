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
from typing import Any

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
    ) -> None:
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
        self._server: Any = None  # deephaven_server.Server, lazily imported
        self._container_name: str | None = None

    def start(self) -> StreamingServer:
        """Start the streaming server. On ARM64, we prefer an external Docker instance."""
        import platform
        import httpx
        import time

        # Architecture detection: if on Linux/ARM, 'deephaven_server' (the embedded JVM) won't work. 
        sys_name = platform.system()
        machine = platform.machine().lower()
        is_arm_linux = sys_name == "Linux" and any(arch in machine for arch in ("aarch64", "arm64", "armv8"))
        
        if not is_arm_linux and sys_name == "Linux":
            try:
                import deephaven_server
            except ImportError:
                is_arm_linux = True

        # 1. Check if a server is ALREADY running (e.g. in Docker)
        try:
            resp = httpx.get(f"{self.url}/io.deephaven.proto.backplane.grpc.ConfigService/GetConfiguration", timeout=1.0)
            if resp.status_code in (200, 404, 405): # Generic check 
                logger.info("StreamingServer found running on port %d (assuming Docker/External)", self._port)
                return self
        except Exception:
            pass

        if is_arm_linux:
            import subprocess
            import uuid
            
            # Check if Docker is actually available
            try:
                subprocess.run(["docker", "version"], check=True, capture_output=True)
            except Exception:
                logger.error("ARM64 detected but 'docker' command failed. Please install Docker.")
                raise RuntimeError("Docker is required for StreamingServer on ARM64.")

            container_name = f"deephaven-test-server" # Stable name for easier cleanup
            
            # Stop any existing container with same name (stale from previous crashes)
            subprocess.run(["docker", "stop", container_name], check=False, capture_output=True)
            subprocess.run(["docker", "rm", container_name], check=False, capture_output=True)

            logger.info(
                "ARM64 detected. Starting native Docker Deephaven Server (%s) on port %d...",
                container_name, self._port
            )
            
            try:
                subprocess.run([
                    "docker", "run", "-d", "--rm", "--name", container_name,
                    "-p", f"{self._port}:10000",
                    "-e", "START_OPTS=-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
                    "ghcr.io/deephaven/server:latest"
                ], check=True, capture_output=True)
            except subprocess.CalledProcessError as e:
                logger.error("Failed to start Deephaven Docker container: %s", e.stderr.decode())
                raise RuntimeError(f"Docker start failed: {e.stderr.decode()}")
            
            # Wait for ready status (increased timeout for slow ARM/WSL environments)
            logger.info("Waiting for Deephaven to become ready...")
            ready = False
            for i in range(45):
                try:
                    r = httpx.get(f"{self.url}/io.deephaven.proto.backplane.grpc.ConfigService/GetConfiguration", timeout=1.0)
                    if r.status_code in (200, 404, 405):
                        ready = True
                        break
                except Exception:
                    pass
                time.sleep(1)
            
            if not ready:
                logger.error("Deephaven Docker container started but failed health check within 45s.")
                raise RuntimeError("Deephaven Docker health check timed out.")

            self._container_name = container_name
            logger.info("Docker StreamingServer started and ready on port %d", self._port)
            return self

        # 2. Fallback to started the in-process server (x86 only)
        try:
            from deephaven_server import Server
            self._server = Server(
                port=self._port,
                jvm_args=self._jvm_args,
                default_jvm_args=self._default_jvm_args,
            )
            self._server.start()
            logger.info("StreamingServer started on port %d", self._port)
        except ImportError:
            logger.error("deephaven_server package not found. Run 'pip install deephaven-server'")
            raise

        return self

    def stop(self) -> None:
        """Stop the streaming server."""
        if getattr(self, "_container_name", None):
            import subprocess
            subprocess.run(["docker", "stop", self._container_name], check=False, capture_output=True)
            logger.info("Docker StreamingServer stopped")
            self._container_name = None

        if getattr(self, "_server", None) is not None:
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

    def register_alias(self, name: str) -> None:
        """Register this server under an alias name."""
        _register_alias(name, port=self._port)

    def __enter__(self) -> StreamingServer:
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()


__all__ = ["StreamingServer"]
