# PlatformAgents — Multi-Agent Data Engineering Team

Eight specialist AI agents orchestrated by an LLM router, each wired to real platform services via a shared `_PlatformContext`. One import, one constructor, full-stack data engineering.

```python
from agents import PlatformAgents

team = PlatformAgents(alias="prod", user="alice", password="pw")
result = team.run("Create a trades dataset, load it into the lakehouse, and compute statistics")
```

---

## Architecture

```
                        ┌─────────────────┐
            prompt ───▶ │  AgentTeam       │
                        │  (LLM Router)    │
                        └────┬───┬───┬─────┘
                   ┌─────────┘   │   └─────────┐
                   ▼             ▼             ▼
              ┌────────┐   ┌────────┐   ┌────────┐   ...
              │  OLTP  │   │  Feed  │   │ Quant  │
              └───┬────┘   └───┬────┘   └───┬────┘
                  │            │            │
                  ▼            ▼            ▼
            _PlatformContext (shared, lazy clients)
                  │            │            │
           ┌──────┘   ┌───────┘   ┌────────┘
           ▼          ▼           ▼
      StoreServer  MarketData  Lakehouse  Media  Streaming
      (Postgres)   (FastAPI+   (Iceberg)  (S3)   (Deephaven)
                    QuestDB)
```

The router reads agent descriptions and delegates to the best specialist. Each agent has tools bound to `_PlatformContext`, which lazily creates service clients from registered aliases.

---

## Agents

### 1. OLTP Agent (`oltp`)
Creates and manages operational datasets in PostgreSQL.

| Tool | Description |
|------|-------------|
| `create_dataset` | Define a new Storable type with typed fields, auto-generates column definitions and model code |
| `insert_records` | Insert JSON records into a dataset |
| `query_dataset` | Query records with optional filters and limits |
| `list_storable_types` | List all registered Storable types |
| `describe_type` | Describe a type's schema and fields |
| `ingest_from_file` | Bulk-load from CSV/Parquet into a dataset |
| + codegen tools | `inspect_registry`, `define_module`, `execute_python` |

**Services**: StoreServer

### 2. Feed Agent (`feed`)
Manages real-time market data feeds.

| Tool | Description |
|------|-------------|
| `list_md_symbols` | List all available symbols on the feed |
| `get_md_snapshot` | Get latest prices for a message type |
| `get_feed_health` | Check feed server health and symbol counts |
| `publish_custom_tick` | Publish a custom tick to the market data bus |
| `describe_feed_setup` | Explain feed architecture |

**Services**: MarketDataServer

### 3. Timeseries Agent (`timeseries`)
Historical market data analysis via QuestDB.

| Tool | Description |
|------|-------------|
| `list_tsdb_series` | List all series in the time-series store |
| `get_bars` | Get OHLCV bars for a symbol at a given interval |
| `get_tick_history` | Get raw tick history with time range filters |
| `compute_realized_vol` | Compute annualized realized volatility from tick data |
| `compare_cross_exchange` | Compare prices across data sources |
| `ingest_historical_csv` | Load historical CSV data into the TSDB |

**Services**: MarketDataServer (embeds QuestDB)

### 4. Lakehouse Agent (`lakehouse`)
Transforms operational data into analytical Iceberg tables.

| Tool | Description |
|------|-------------|
| `list_lakehouse_tables` | List all Iceberg tables |
| `describe_lakehouse_table` | Describe table schema and row count |
| `design_star_schema` | Use AI to design a star schema from OLTP data |
| `create_lakehouse_table` | Create/populate a table from SQL |
| `ingest_to_lakehouse` | Ingest from CSV/Parquet/SQL |
| `build_datacube` | Configure a Datacube report |
| `query_lakehouse` | Execute ad-hoc SQL against Iceberg tables |

**Services**: LakehouseServer (Lakekeeper + MinIO + PG + DuckDB)

### 5. Quant Agent (`quant`)
Analytical and quantitative workflows.

| Tool | Description |
|------|-------------|
| `run_sql_analysis` | Execute SQL and compute descriptive stats |
| `compute_statistics` | Detailed stats: skewness, kurtosis, percentiles |
| `compute_correlation` | Pairwise Pearson correlation matrix |
| `detect_anomalies` | Z-score or IQR anomaly detection |
| `run_regression` | OLS linear regression with R², coefficients |
| `time_series_decompose` | Trend/seasonal/residual decomposition |
| `suggest_visualization` | AI-recommended chart types |

**Services**: LakehouseServer + MarketDataServer + AI

### 6. Document Agent (`document`)
Manages unstructured documents with semantic search.

| Tool | Description |
|------|-------------|
| `upload_document` | Upload a file to S3 with metadata and tags |
| `list_documents` | List documents with optional tag filter |
| `search_documents` | Text, semantic, or hybrid search |
| `extract_structured_data` | AI extraction of structured data from text |
| `bulk_upload` | Upload multiple files from a directory |
| `tag_document` | Add/update tags on a document |

**Services**: StoreServer + MediaServer

### 7. Dashboard Agent (`dashboard`)
Builds real-time streaming dashboards on Deephaven.

| Tool | Description |
|------|-------------|
| `list_ticking_tables` | List all ticking tables in the JVM |
| `create_ticking_table` | Create a new ticking table with a typed schema |
| `create_derived_table` | Create a derived table (joins, aggregations) |
| `setup_store_bridge` | Configure StoreBridge for event streaming from PG |
| `create_reactive_model` | Define a Storable with `@computed` fields |
| `publish_table` | Publish a table to the Deephaven web UI |
| + codegen tools | `inspect_registry`, `define_module`, `execute_python` |

**Services**: StreamingServer + StoreServer

### 8. Query Agent (`query`)
Universal data access across all platform stores.

| Tool | Description |
|------|-------------|
| `query_store` | Query OLTP datasets |
| `query_lakehouse` | Run SQL against Iceberg tables |
| `get_md_snapshot` | Get latest market data |
| `list_all_datasets` | Unified catalog across all stores |
| `describe_dataset` | Describe any dataset regardless of store |
| `search_documents` | Search documents via MediaStore |
| `cross_store_query` | Natural language query routed to the right store |

**Services**: All services

---

## Configuration

```python
# Minimal — single alias for everything
team = PlatformAgents(alias="prod", user="alice", password="pw")

# Per-service alias overrides
team = PlatformAgents(
    alias="default",
    user="alice",
    password="pw",
    store_alias="prod-pg",
    lakehouse_alias="prod-iceberg",
    md_alias="prod-md",
    media_alias="prod-s3",
    streaming_alias="prod-dh",
)

# Subset of agents
team = PlatformAgents(alias="demo", agents=["oltp", "lakehouse", "quant"])
```

### Service Aliases

Each service registers under an alias at startup:

| Service | Registration | Used By |
|---------|-------------|---------|
| `StoreServer` | `server.register_alias("name")` | OLTP, Query, Dashboard, Document |
| `MarketDataServer` | `server.register_alias("name")` | Feed, Timeseries, Query, Quant |
| `LakehouseServer` | `server.register_alias("name")` | Lakehouse, Quant, Query |
| `MediaServer` | `server.register_alias("name")` | Document, Query |
| `StreamingServer` | Deephaven JVM (session-global) | Dashboard |

---

## Direct Agent Access

```python
# Via typed properties
result = team.oltp.run("Create a trades table")
result = team.lakehouse.run("Design a star schema")
result = team.quant.run("Compute statistics on demo_positions")

# Via the router (auto-delegates)
result = team.run("What data is available across the platform?")
```

---

## Eval Framework

Built-in evaluation for measuring agent quality across multiple dimensions.

```python
from agents._eval.framework import AgentEval, EvalPhase, DEFAULT_DIMENSIONS
from agents._eval.datasets import ALL_EVAL_CASES

evaluator = AgentEval(agents=team._agents, max_phase=EvalPhase.TOOL_SELECTION)
results = evaluator.run(ALL_EVAL_CASES[:5])
```

### Scoring Dimensions
- **Tool Selection** — did the agent pick the right tools?
- **Output Contains** — does the output include expected keywords?
- **Schema Quality** — are field names/types well-designed?
- **Table Creation** — were the expected tables created?
- **Query Correctness** — does the query return the expected result?
- **Metadata Completeness** — are descriptions and tags present?
- **Naming Conventions** — PascalCase types, snake_case fields?

### Eval Datasets
- `OLTP_EVAL_CASES` — 5+ cases for dataset creation and querying
- `LAKEHOUSE_EVAL_CASES` — 4+ cases for star schema design and ETL
- `QUERY_EVAL_CASES` — 4+ cases for cross-store queries
- `DATASCIENCE_EVAL_CASES` — 3+ cases for analytics
- `ALL_EVAL_CASES` — 20+ total cases across all agents

---

## Codegen Integration

OLTP and Dashboard agents include codegen tools that generate persistent Python code:

1. **Column definitions** — `store/columns/agent_generated/<name>.py`
2. **Model definitions** — `store/models/agent_generated/<name>.py`
3. **Registry inspection** — check existing columns before creating duplicates

Generated code is importable and survives restarts. The column registry enforces uniqueness and type consistency.

---

## Testing

### Integration Tests (`tests/test_data_engineers.py`)

67 tests covering all 8 agents against real services:

| Agent | Tests | Services |
|-------|-------|----------|
| OLTP | create, insert, query, list types, describe | StoreServer |
| Feed | list symbols, snapshot, health, publish | MarketDataServer |
| Timeseries | list series, bars, tick history, vol, cross-exchange | MarketDataServer (+QuestDB) |
| Lakehouse | list tables, create table, query | LakehouseServer |
| Quant | SQL analysis, statistics, anomalies, regression, decompose, viz | LakehouseServer + MarketDataServer + AI |
| Document | upload, list, search | StoreServer + MediaServer |
| Dashboard | list ticking, create ticking, reactive model, store bridge | StreamingServer + StoreServer |
| Query | query store, list all, describe, md snapshot, query lakehouse | All services |

```bash
python3 -m pytest tests/test_data_engineers.py -v
```

### Demo

```bash
export GEMINI_API_KEY="your-key"
python3 demo_platform_agents.py
```

---

## File Structure

```
agents/
├── __init__.py          # Public API: PlatformAgents
├── _team.py             # PlatformAgents class, AgentTeam router
├── _context.py          # _PlatformContext (shared state, lazy clients)
├── _oltp.py             # OLTP agent + tools
├── _lakehouse.py        # Lakehouse agent + tools
├── _feed.py             # Feed agent + tools
├── _timeseries.py       # Timeseries agent + tools
├── _document.py         # Document agent + tools
├── _dashboard.py        # Dashboard agent + tools
├── _query.py            # Query agent + tools
├── _datascience.py      # Quant agent + tools
├── _codegen.py          # Shared codegen tools (OLTP, Dashboard)
└── _eval/
    ├── framework.py     # AgentEval, scoring dimensions
    ├── scorers.py       # Individual scoring functions
    ├── judges.py        # LLM judge rubrics
    └── datasets.py      # Eval test cases
```
