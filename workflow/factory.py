"""
Workflow Engine Factory
=======================
Creates a WorkflowEngine from an alias or PG URL.
The only module that imports concrete backend implementations.
"""

from __future__ import annotations

import os
import urllib.parse

from workflow.engine import WorkflowEngine


def _to_dbos_url(pg_url: str) -> str:
    """Convert a generic postgres:// URL to the SQLAlchemy+psycopg format DBOS requires."""
    parsed = urllib.parse.urlparse(pg_url)
    params = urllib.parse.parse_qs(parsed.query)

    # Extract host param (Unix socket path) if present
    host_param = params.get("host", [None])[0]
    user = parsed.username or "app_admin"
    password = parsed.password or ""
    port = parsed.port or 5432
    dbname = parsed.path.lstrip("/") or "postgres"

    if host_param:
        host_encoded = urllib.parse.quote(host_param, safe="")
        return (
            f"postgresql+psycopg://{user}:{password}@"
            f":{port}/{dbname}?host={host_encoded}"
        )
    else:
        hostname = parsed.hostname or "localhost"
        return (
            f"postgresql+psycopg://{user}:{password}@"
            f"{hostname}:{port}/{dbname}"
        )


def create_engine(alias_or_url: str, *, name: str = "workflow-app") -> WorkflowEngine:
    """Create a WorkflowEngine from an alias or PG URL.

    Args:
        alias_or_url: Either a registered alias name or a postgres:// URL.
        name: Application name for the workflow backend.

    Returns:
        A concrete WorkflowEngine instance.
    """
    from workflow._registry import resolve_alias

    # Try alias first
    resolved = resolve_alias(alias_or_url)
    if resolved is not None:
        pg_url = resolved["pg_url"]
    else:
        pg_url = alias_or_url

    backend = os.environ.get("WORKFLOW_BACKEND", "dbos")

    if backend == "dbos":
        from workflow.dbos_engine import DBOSEngine
        dbos_url = _to_dbos_url(pg_url)
        return DBOSEngine(dbos_url, name=name)

    raise ValueError(
        f"Unknown workflow backend: {backend!r}. Available: 'dbos'"
    )
