"""
media.admin — Platform API for Media Storage
==============================================
Start/stop S3-compatible storage, register aliases.

Platform usage::

    from media.admin import MediaServer

    server = MediaServer(data_dir="data/media")
    await server.start()
    server.register_alias("demo")

User code uses ``MediaStore("demo", ai=ai)``.
"""

from __future__ import annotations

import logging

import objectstore
from media._registry import register_alias as _register_alias

logger = logging.getLogger(__name__)


class MediaServer:
    """Manages S3-compatible storage for media files."""

    def __init__(
        self,
        data_dir: str = "data/media",
        api_port: int = 9002,
        console_port: int = 9003,
        bucket: str = "media",
    ):
        self._data_dir = data_dir
        self._api_port = api_port
        self._console_port = console_port
        self._bucket = bucket
        self._store = None  # ObjectStore, set by start()

    async def start(self) -> "MediaServer":
        """Start the media storage backend and ensure the default bucket exists."""
        self._store = await objectstore.configure(
            "minio",
            data_dir=f"{self._data_dir}/objectstore",
            api_port=self._api_port,
            console_port=self._console_port,
        )
        await self._store.ensure_bucket(self._bucket)
        return self

    async def health(self) -> bool:
        """Check if the media storage backend is healthy."""
        if self._store is None:
            return False
        return await self._store.health()

    @property
    def endpoint(self) -> str:
        """S3 endpoint (internal — passed to MediaStore via alias)."""
        return self._store.endpoint

    @property
    def access_key(self) -> str:
        """S3 access key (internal)."""
        return self._store.access_key

    @property
    def secret_key(self) -> str:
        """S3 secret key (internal)."""
        return self._store.secret_key

    @property
    def bucket(self) -> str:
        """S3 bucket name."""
        return self._bucket

    def register_alias(self, name: str):
        """Register this server's connection info under an alias name."""
        _register_alias(
            name,
            endpoint=self._store.endpoint,
            access_key=self._store.access_key,
            secret_key=self._store.secret_key,
            bucket=self._bucket,
        )

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        pass  # atexit handles cleanup


__all__ = ["MediaServer"]
