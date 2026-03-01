"""
objectstore — Pluggable S3-compatible object storage.

Provides an ``ObjectStore`` ABC and a ``configure()`` factory.
The only backend today is ``"minio"`` (local dev); real AWS S3 is a
future backend.

Usage::

    import objectstore

    store = await objectstore.configure("minio", data_dir="data/media", api_port=9002)
    await store.ensure_bucket("media")
    s3 = store.client("media")        # → S3Client
    s3.upload("key", b"data", "application/octet-stream")

The word "MinIO" never appears outside ``objectstore/_minio.py``.
"""

from __future__ import annotations

import atexit
import logging
from abc import ABC, abstractmethod

from objectstore.client import S3Client

logger = logging.getLogger(__name__)

__all__ = ["ObjectStore", "S3Client", "configure"]


class ObjectStore(ABC):
    """Abstract S3-compatible storage backend."""

    @abstractmethod
    async def health(self) -> bool:
        """Check if the storage backend is healthy."""
        ...

    @abstractmethod
    async def ensure_bucket(self, bucket: str) -> None:
        """Create a bucket if it doesn't exist."""
        ...

    @property
    @abstractmethod
    def endpoint(self) -> str:
        """S3-compatible endpoint URL."""
        ...

    @property
    @abstractmethod
    def access_key(self) -> str:
        """Access key for authentication."""
        ...

    @property
    @abstractmethod
    def secret_key(self) -> str:
        """Secret key for authentication."""
        ...

    def client(self, bucket: str, secure: bool = False) -> S3Client:
        """Create an S3Client bound to a specific bucket."""
        return S3Client(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            bucket=bucket,
            secure=secure,
        )


async def configure(backend: str = "minio", **kwargs) -> ObjectStore:
    """
    Configure and start an object store backend.

    Returns a ready-to-use ``ObjectStore``. Registers an ``atexit`` handler
    for automatic cleanup (no explicit stop/close needed).

    Args:
        backend: Backend type. Currently only ``"minio"`` is supported.
        **kwargs: Backend-specific configuration (e.g., ``data_dir``,
                  ``api_port``, ``console_port``).

    Returns:
        A running ``ObjectStore`` instance.
    """
    if backend == "minio":
        from objectstore._minio import _MinIOBackend
        store = _MinIOBackend(**kwargs)
        await store._start()
        atexit.register(store._stop)
        return store

    raise ValueError(f"Unknown objectstore backend: {backend!r}")
