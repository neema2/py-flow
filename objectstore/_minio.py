"""
Private MinIO backend for the objectstore package.

This is the ONLY file in the codebase that knows about MinIO.
All other code uses the ObjectStore ABC via objectstore.configure().
"""

from __future__ import annotations

import asyncio
import logging
import os
import platform
import stat
import subprocess
from pathlib import Path

import httpx

from objectstore import ObjectStore

logger = logging.getLogger(__name__)

_MINIO_URLS = {
    ("darwin", "arm64"): "https://dl.min.io/server/minio/release/darwin-arm64/minio",
    ("darwin", "aarch64"): "https://dl.min.io/server/minio/release/darwin-arm64/minio",
    ("darwin", "x86_64"): "https://dl.min.io/server/minio/release/darwin-amd64/minio",
    ("linux", "x86_64"): "https://dl.min.io/server/minio/release/linux-amd64/minio",
    ("linux", "aarch64"): "https://dl.min.io/server/minio/release/linux-arm64/minio",
    ("linux", "arm64"): "https://dl.min.io/server/minio/release/linux-arm64/minio",
}


def _minio_download_url() -> str:
    """Detect the correct MinIO download URL for the current platform."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    key = (system, machine)
    if key not in _MINIO_URLS:
        raise RuntimeError(
            f"No MinIO binary for {system}/{machine}. "
            "Use Docker instead: docker run -p 9002:9000 minio/minio server /data"
        )
    return _MINIO_URLS[key]


class _MinIOBackend(ObjectStore):
    """ObjectStore implementation backed by a local MinIO subprocess.

    Do NOT instantiate directly — use ``objectstore.configure("minio", ...)``.
    """

    def __init__(
        self,
        data_dir: str = "data/objectstore",
        api_port: int = 9002,
        console_port: int = 9003,
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
    ) -> None:
        self._data_dir = Path(data_dir).resolve()
        self._api_port = api_port
        self._console_port = console_port
        self._access_key = access_key
        self._secret_key = secret_key
        self._process: subprocess.Popen | None = None

    @property
    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    @property
    def endpoint(self) -> str:
        return f"http://localhost:{self._api_port}"

    @property
    def access_key(self) -> str:
        return self._access_key

    @property
    def secret_key(self) -> str:
        return self._secret_key

    async def _start(self) -> None:
        """Download if needed and start the MinIO process.

        Called by ``objectstore.configure()`` — not part of the public API.
        """
        if await self.health():
            logger.info("Object store already running on port %d", self._api_port)
            return

        binary = self._ensure_binary()
        storage_dir = self._data_dir / "storage"
        storage_dir.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()
        env["MINIO_ROOT_USER"] = self._access_key
        env["MINIO_ROOT_PASSWORD"] = self._secret_key

        cmd = [
            str(binary), "server",
            str(storage_dir),
            "--address", f":{self._api_port}",
            "--console-address", f":{self._console_port}",
        ]

        logger.info("Starting object store on port %d...", self._api_port)
        self._process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        for _attempt in range(30):
            await asyncio.sleep(1)
            if await self.health():
                logger.info(
                    "Object store started (API=%d, Console=%d)",
                    self._api_port, self._console_port,
                )
                return
            if self._process.poll() is not None:
                stderr = self._process.stderr.read().decode() if self._process.stderr else ""
                raise RuntimeError(f"Object store exited during startup: {stderr[:500]}")

        raise RuntimeError(f"Object store failed to start within 20s (port {self._api_port})")

    def _stop(self) -> None:
        """Synchronously stop the MinIO process.

        Called by ``atexit`` — must be sync (no async in atexit handlers).
        """
        if self._process and self._process.poll() is None:
            logger.info("Stopping object store (pid=%d)", self._process.pid)
            self._process.terminate()
            try:
                self._process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                logger.warning("Object store did not stop gracefully, killing")
                self._process.kill()
                self._process.wait(timeout=5)
            logger.info("Object store stopped")
        self._process = None

    async def health(self) -> bool:
        """Check health via the /minio/health/live endpoint."""
        url = f"http://localhost:{self._api_port}/minio/health/live"
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                resp = await client.get(url)
                return resp.status_code == 200
        except Exception:
            return False

    async def ensure_bucket(self, bucket: str) -> None:
        """Create a bucket if it doesn't exist."""
        try:
            import urllib3
            from minio import Minio
            http_client = urllib3.PoolManager(
                timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
                retries=urllib3.Retry(
                    total=3,
                    backoff_factor=0.2,
                    status_forcelist=[500, 502, 503, 504],
                ),
            )
            http_client.connection_pool_kw['timeout'] = 5.0
            
            client = Minio(
                f"localhost:{self._api_port}",
                access_key=self._access_key,
                secret_key=self._secret_key,
                secure=False,
                http_client=http_client,
            )
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                logger.info("Created bucket '%s'", bucket)
            else:
                logger.info("Bucket '%s' already exists", bucket)
        except Exception as e:
            logger.warning("Failed to ensure bucket '%s': %s", bucket, e)

    def _ensure_binary(self) -> Path:
        """Ensure MinIO binary is available, downloading if needed."""
        # Use a shared cache directory to avoid re-downloading for every temp data_dir
        cache_dir = Path.home() / ".cache" / "py-flow" / "minio"
        binary = cache_dir / "minio"
        if binary.exists() and os.access(binary, os.X_OK):
            return binary

        logger.info("Object store binary not found, downloading...")
        url = _minio_download_url()

        cache_dir.mkdir(parents=True, exist_ok=True)

        with httpx.stream("GET", url, follow_redirects=True, timeout=120) as resp:
            resp.raise_for_status()
            with open(binary, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=65536):
                    f.write(chunk)

        binary.chmod(binary.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        logger.info("Object store binary installed to %s", binary)
        return binary
