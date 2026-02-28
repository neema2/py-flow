"""
S3 Client — Thin wrapper around the minio Python SDK.

Handles upload, download, delete, and presigned URL generation
for binary files stored in MinIO (S3-compatible).
"""

from __future__ import annotations

import io
import logging
from datetime import timedelta
from typing import Optional

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

DEFAULT_BUCKET = "media"


class S3Client:
    """
    MinIO/S3 client for binary file storage.

    Usage::

        s3 = S3Client(endpoint="localhost:9002", access_key="minioadmin", secret_key="minioadmin")
        s3.upload("docs/report.pdf", data, "application/pdf")
        data = s3.download("docs/report.pdf")
        url = s3.presign_url("docs/report.pdf")
        s3.delete("docs/report.pdf")
    """

    def __init__(
        self,
        endpoint: str = "localhost:9002",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
        bucket: str = DEFAULT_BUCKET,
        secure: bool = False,
    ):
        self._endpoint = endpoint.replace("http://", "").replace("https://", "")
        self._bucket = bucket
        self._client = Minio(
            self._endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def ensure_bucket(self) -> None:
        """Create the bucket if it doesn't exist."""
        if not self._client.bucket_exists(self._bucket):
            self._client.make_bucket(self._bucket)
            logger.info("Created S3 bucket '%s'", self._bucket)

    def upload(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        """
        Upload binary data to S3.

        Args:
            key: S3 object key (e.g., "media/{entity_id}/{filename}").
            data: Binary content.
            content_type: MIME type.

        Returns:
            The S3 key.
        """
        self._client.put_object(
            self._bucket,
            key,
            io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )
        logger.debug("Uploaded %d bytes to s3://%s/%s", len(data), self._bucket, key)
        return key

    def download(self, key: str) -> bytes:
        """
        Download binary data from S3.

        Args:
            key: S3 object key.

        Returns:
            Binary content.
        """
        response = self._client.get_object(self._bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def delete(self, key: str) -> None:
        """Delete an object from S3."""
        self._client.remove_object(self._bucket, key)
        logger.debug("Deleted s3://%s/%s", self._bucket, key)

    def exists(self, key: str) -> bool:
        """Check if an object exists in S3."""
        try:
            self._client.stat_object(self._bucket, key)
            return True
        except S3Error:
            return False

    def presign_url(self, key: str, expires: int = 3600) -> str:
        """
        Generate a presigned download URL.

        Args:
            key: S3 object key.
            expires: URL validity in seconds (default: 1 hour).

        Returns:
            Presigned URL string.
        """
        return self._client.presigned_get_object(
            self._bucket, key, expires=timedelta(seconds=expires),
        )

    def list_keys(self, prefix: str = "") -> list[str]:
        """List object keys with an optional prefix."""
        objects = self._client.list_objects(self._bucket, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]

    @property
    def bucket(self) -> str:
        return self._bucket
