"""
Lakehouse Catalog — PyIceberg REST Catalog via Lakekeeper
==========================================================
Connects to Lakekeeper REST catalog backed by PostgreSQL.
Uniform dev/prod — no SQLite fallback.
"""

from __future__ import annotations

import logging
import os

from pyiceberg.catalog import load_catalog

logger = logging.getLogger(__name__)

# Default configuration — all overridable via environment variables
DEFAULT_CATALOG_URI = "http://localhost:8181/catalog"
DEFAULT_WAREHOUSE = "lakehouse"
DEFAULT_S3_ENDPOINT = "http://localhost:9002"
DEFAULT_S3_ACCESS_KEY = "minioadmin"
DEFAULT_S3_SECRET_KEY = "minioadmin"
DEFAULT_S3_REGION = "us-east-1"


def create_catalog(
    name: str = "lakehouse",
    uri: str | None = None,
    warehouse: str | None = None,
    s3_endpoint: str | None = None,
    s3_access_key: str | None = None,
    s3_secret_key: str | None = None,
    s3_region: str | None = None,
):
    """
    Create a PyIceberg REST catalog connected to Lakekeeper + S3-compatible storage.

    All parameters can be overridden via environment variables:
        LAKEKEEPER_URI, LAKEHOUSE_WAREHOUSE, S3_ENDPOINT,
        S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION
    """
    catalog_uri = uri or os.environ.get("LAKEKEEPER_URI", DEFAULT_CATALOG_URI)
    wh = warehouse or os.environ.get("LAKEHOUSE_WAREHOUSE", DEFAULT_WAREHOUSE)
    endpoint = s3_endpoint or os.environ.get("S3_ENDPOINT", DEFAULT_S3_ENDPOINT)
    access = s3_access_key or os.environ.get("S3_ACCESS_KEY", DEFAULT_S3_ACCESS_KEY)
    secret = s3_secret_key or os.environ.get("S3_SECRET_KEY", DEFAULT_S3_SECRET_KEY)
    region = s3_region or os.environ.get("S3_REGION", DEFAULT_S3_REGION)

    logger.info("Creating Iceberg REST catalog: uri=%s, warehouse=%s", catalog_uri, wh)

    catalog = load_catalog(
        name,
        **{
            "type": "rest",
            "uri": catalog_uri,
            "warehouse": wh,
            "s3.endpoint": endpoint,
            "s3.access-key-id": access,
            "s3.secret-access-key": secret,
            "s3.region": region,
            "s3.path-style-access": "true",
        },
    )

    return catalog
