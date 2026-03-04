"""
Query Agent — "Query My Data"
===============================
Takes natural language questions and uses ANY platform API to return answers.
This is the universal read agent — it knows every data store and picks the right one.

Tools:
    - query_store             — Storable.query() for OLTP data
    - query_lakehouse         — DuckDB SQL over Iceberg tables
    - query_tsdb              — OHLCV bars + tick history
    - search_documents        — hybrid search over MediaStore
    - get_md_snapshot         — live market data
    - list_all_datasets       — unified catalog across all stores
    - describe_dataset        — schema/metadata for any dataset

Usage::

    from agents._query import create_query_agent

    agent = create_query_agent(ctx)
    result = agent.run("What was AAPL's average volume last week?")
"""

from __future__ import annotations

import dataclasses
import json
import logging
from typing import Any

from ai import Agent, tool

from agents._context import _PlatformContext

logger = logging.getLogger(__name__)

QUERY_SYSTEM_PROMPT = """\
You are the Query Agent — the universal data access layer for the platform. \
Users ask you questions in natural language and you figure out WHICH data store \
to query and HOW to query it.

Available data stores:
1. **Object Store (OLTP)** — PostgreSQL with Storable objects. For operational data \
   (trades, orders, positions, instruments). Use query_store().
2. **Lakehouse (Iceberg)** — Analytical tables via DuckDB SQL. For curated facts/dims, \
   historical snapshots, aggregated data. Use query_lakehouse().
3. **TSDB (QuestDB)** — Time-series data. For OHLCV bars, tick history, vol data. \
   Use query_tsdb().
4. **MediaStore** — Unstructured documents (PDFs, reports). For document search. \
   Use search_documents().
5. **Market Data (live)** — Real-time prices from the WebSocket hub. For current \
   market data. Use get_md_snapshot().

Strategy:
- First identify which store(s) have the answer.
- For price/market data: use get_md_snapshot() for current, query_tsdb() for historical.
- For business data (trades, positions): try query_lakehouse() first (faster aggregations), \
  fall back to query_store() for latest OLTP state.
- For documents/research: use search_documents().
- For cross-store questions: call multiple tools and synthesize.
- Use list_all_datasets() if you need to discover what's available.
- Use describe_dataset() to understand a dataset's schema before querying it.

Always return clear, formatted answers with relevant numbers and context.
"""


def create_query_tools(ctx: _PlatformContext) -> list:
    """Create Query agent tools bound to a _PlatformContext."""

    @tool
    def query_store(type_name: str, limit: int = 20) -> str:
        """Query records from an OLTP dataset (Storable objects in PostgreSQL).

        Best for: current operational state (latest trades, positions, orders).

        Args:
            type_name: Name of the Storable type to query.
            limit: Maximum records to return (default 20).
        """
        cls = ctx.get_storable_type(type_name)
        if cls is None:
            return json.dumps({"error": f"Type '{type_name}' not found in OLTP store."})

        try:
            from store.connection import get_connection
            get_connection()
        except RuntimeError:
            if ctx.store_alias:
                ctx.get_store_connection()
            else:
                return json.dumps({"error": "No store connection."})

        try:
            result = cls.query(limit=limit)
            rows = []
            for obj in result:
                row = {}
                if dataclasses.is_dataclass(obj):
                    for f in dataclasses.fields(obj):
                        if not f.name.startswith("_"):
                            row[f.name] = getattr(obj, f.name)
                if obj._store_entity_id:
                    row["_entity_id"] = obj._store_entity_id
                rows.append(row)
            return json.dumps({
                "source": "oltp",
                "type_name": type_name,
                "count": len(rows),
                "rows": rows,
            }, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def query_lakehouse(sql: str) -> str:
        """Execute SQL against the Iceberg lakehouse via DuckDB.

        Best for: analytical queries, aggregations, historical data, fact/dim tables.
        Tables are: lakehouse.default.<table_name>

        Args:
            sql: SQL query to execute.
        """
        if ctx.lakehouse is None:
            return json.dumps({"error": "No Lakehouse configured."})
        try:
            rows = ctx.lakehouse.query(sql)
            return json.dumps({
                "source": "lakehouse",
                "row_count": len(rows),
                "rows": rows[:100],
                "truncated": len(rows) > 100,
            }, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def query_tsdb(msg_type: str, symbol: str, interval: str = "1m",
                   limit: int = 100) -> str:
        """Query time-series data (OHLCV bars or raw ticks).

        Best for: price history, volume analysis, volatility, trend data.

        Args:
            msg_type: Asset type — "equity", "fx", or "curve".
            symbol: Symbol identifier (e.g. "AAPL", "USD/JPY").
            interval: Bar interval — "ticks" for raw ticks, or "1s", "1m", "5m", "15m", "1h", "1d".
            limit: Maximum records (default 100, applies to ticks only).
        """
        try:
            import httpx
            base = ctx.md_base_url

            if interval == "ticks":
                resp = httpx.get(
                    f"{base}/md/history/{msg_type}/{symbol}",
                    params={"limit": limit},
                    timeout=10.0,
                )
            else:
                resp = httpx.get(
                    f"{base}/md/bars/{msg_type}/{symbol}",
                    params={"interval": interval},
                    timeout=10.0,
                )
            resp.raise_for_status()
            data = resp.json()
            items = data if isinstance(data, list) else [data]
            return json.dumps({
                "source": "tsdb",
                "type": msg_type,
                "symbol": symbol,
                "interval": interval,
                "count": len(items),
                "data": items[:100],
                "truncated": len(items) > 100,
            }, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def search_documents(query: str, mode: str = "hybrid", limit: int = 10) -> str:
        """Search documents in the MediaStore.

        Best for: research reports, PDFs, policy documents, unstructured text.

        Args:
            query: Natural language search query.
            mode: "text" (keyword), "semantic" (meaning), or "hybrid" (both).
            limit: Maximum results (default 10).
        """
        if ctx.media_store is None:
            return json.dumps({"error": "No MediaStore configured."})
        try:
            ms = ctx.media_store
            if mode == "semantic":
                results = ms.semantic_search(query, limit=limit)
            elif mode == "text":
                results = ms.search(query, limit=limit)
            else:
                results = ms.hybrid_search(query, limit=limit)
            return json.dumps({
                "source": "media",
                "query": query,
                "mode": mode,
                "count": len(results),
                "results": results,
            }, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def get_md_snapshot(msg_type: str = "", symbol: str = "") -> str:
        """Get current live market data snapshot.

        Best for: current prices, real-time quotes, live market state.

        Args:
            msg_type: Type filter — "equity", "fx", "curve", or "" for all.
            symbol: Specific symbol, or "" for all symbols of the type.
        """
        try:
            import httpx
            base = ctx.md_base_url
            if msg_type and symbol:
                url = f"{base}/md/snapshot/{msg_type}/{symbol}"
            elif msg_type:
                url = f"{base}/md/snapshot/{msg_type}"
            else:
                url = f"{base}/md/snapshot"
            resp = httpx.get(url, timeout=5.0)
            resp.raise_for_status()
            data = resp.json()
            return json.dumps({"source": "marketdata_live", "data": data}, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def list_all_datasets() -> str:
        """List ALL datasets across ALL platform stores.

        Returns a unified catalog showing what data is available in:
        OLTP store, Lakehouse, TSDB, and MediaStore.
        """
        catalog: dict[str, Any] = {}

        # OLTP types
        oltp_types = ctx.list_storable_types()
        if oltp_types:
            catalog["oltp"] = []
            for name in oltp_types:
                cls = ctx.get_storable_type(name)
                fields: list[str] = []
                if cls and dataclasses.is_dataclass(cls):
                    fields = [f.name for f in dataclasses.fields(cls) if not f.name.startswith("_")]  # type: ignore[arg-type]
                catalog["oltp"].append({"name": name, "fields": fields})

        # Lakehouse tables
        if ctx.has_lakehouse():
            try:
                tables = ctx.lakehouse.tables()
                catalog["lakehouse"] = [{"name": t} for t in tables]
            except Exception:
                catalog["lakehouse"] = []

        # TSDB (via symbols)
        try:
            if not ctx.has_md():
                raise RuntimeError("no md")
            import httpx
            resp = httpx.get(f"{ctx.md_base_url}/md/symbols", timeout=3.0)
            if resp.status_code == 200:
                catalog["tsdb"] = resp.json()
        except Exception:
            pass

        # MediaStore
        if ctx.has_media():
            try:
                docs = ctx.media_store.list(limit=5)
                catalog["media"] = {
                    "document_count": len(docs),
                    "sample_titles": [d.title for d in docs[:5]],
                }
            except Exception:
                pass

        return json.dumps(catalog, default=str)

    @tool
    def describe_dataset(name: str) -> str:
        """Describe a dataset's schema regardless of which store it's in.

        Checks OLTP, Lakehouse, and TSDB to find the dataset.

        Args:
            name: Dataset name (Storable type name, Lakehouse table, or symbol).
        """
        # Try OLTP
        cls = ctx.get_storable_type(name)
        if cls and dataclasses.is_dataclass(cls):
            fields = [
                {"name": f.name, "type": f.type.__name__ if isinstance(f.type, type) else str(f.type)}
                for f in dataclasses.fields(cls) if not f.name.startswith("_")
            ]
            return json.dumps({"source": "oltp", "name": name, "fields": fields})

        # Try Lakehouse
        if ctx.lakehouse is not None:
            try:
                info = ctx.lakehouse.table_info(name)
                return json.dumps({
                    "source": "lakehouse",
                    "name": name,
                    "columns": info,
                    "row_count": ctx.lakehouse.row_count(name),
                }, default=str)
            except Exception:
                pass

        # Try TSDB (check if it's a symbol)
        try:
            import httpx
            resp = httpx.get(f"{ctx.md_base_url}/md/snapshot/equity/{name}", timeout=3.0)
            if resp.status_code == 200:
                return json.dumps({
                    "source": "tsdb",
                    "name": name,
                    "type": "equity_symbol",
                    "current_data": resp.json(),
                }, default=str)
        except Exception:
            pass

        return json.dumps({"error": f"Dataset '{name}' not found in any store."})

    return [query_store, query_lakehouse, query_tsdb, search_documents,
            get_md_snapshot, list_all_datasets, describe_dataset]


def create_query_agent(ctx: _PlatformContext, **kwargs: Any) -> Agent:
    """Create a Query Agent bound to a _PlatformContext."""
    tools = create_query_tools(ctx)
    return Agent(
        tools=tools,
        system_prompt=QUERY_SYSTEM_PROMPT,
        name="query",
        **kwargs,
    )
