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

from media._minio import MinIOManager
from media._registry import register_alias as _register_alias

logger = logging.getLogger(__name__)


class MediaServer:
    """Manages S3-compatible storage for media files.

    Hides the underlying MinIO implementation — user never sees "MinIO."
    """

    def __init__(
        self,
        data_dir: str = "data/media",
        api_port: int = 9002,
        console_port: int = 9003,
        bucket: str = "media",
    ):
        self._minio = MinIOManager(
            data_dir=f"{data_dir}/minio",
            api_port=api_port,
            console_port=console_port,
        )
        self._bucket = bucket

    async def start(self) -> "MediaServer":
        """Start the media storage server and ensure the default bucket exists."""
        await self._minio.start()
        await self._minio.ensure_bucket(self._bucket)
        return self

    async def stop(self) -> None:
        """Stop the media storage server."""
        await self._minio.stop()

    async def health(self) -> bool:
        """Check if the media storage server is healthy."""
        return await self._minio.health()

    @property
    def endpoint(self) -> str:
        """S3 endpoint (internal — passed to MediaStore via alias)."""
        return self._minio.endpoint

    @property
    def access_key(self) -> str:
        """S3 access key (internal)."""
        return self._minio.access_key

    @property
    def secret_key(self) -> str:
        """S3 secret key (internal)."""
        return self._minio.secret_key

    @property
    def bucket(self) -> str:
        """S3 bucket name."""
        return self._bucket

    def register_alias(self, name: str):
        """Register this server's connection info under an alias name."""
        _register_alias(
            name,
            endpoint=self._minio.endpoint,
            access_key=self._minio.access_key,
            secret_key=self._minio.secret_key,
            bucket=self._bucket,
        )

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.stop()


__all__ = ["MediaServer"]
