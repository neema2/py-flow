# Functional API Reference

Complete public API organized by feature area — **47 symbols** across 11 packages.

---

## 0. Platform (Admin)

**Start and manage infrastructure services.** Every server follows `start()` / `stop()` / `register_alias()`.

| Symbol | Package | Wraps | User API |
|--------|---------|-------|----------|
| `StoreServer` | `store.admin` | Embedded PG + RLS + pgvector | `connect("alias")` |
| `WorkflowServer` | `workflow.admin` | Embedded PG + DBOS | `create_engine("alias")` |
| `StreamingServer` | `streaming.admin` | Deephaven JVM | `DeephavenClient()` |
| `MarketDataServer` | `marketdata.admin` | FastAPI + uvicorn + QuestDB | REST / WS |
| `TsdbServer` | `timeseries.admin` | QuestDB binary | `Timeseries("alias")` |
| `MediaServer` | `media.admin` | MinIO S3 | `MediaStore("alias")` |
| `LakehouseServer` | `lakehouse.admin` | Lakekeeper + MinIO + PG | `Lakehouse("alias")` |

### Common interface

```python
from store.admin import StoreServer

srv = StoreServer(data_dir="data/myapp", admin_password="secret")
srv.start()
srv.register_alias("demo")
srv.provision_user("alice", "alice_pw")  # StoreServer-specific
# ...
srv.stop()
```

### Constructor signatures

| Server | Key params |
|--------|-----------|
| `StoreServer(data_dir, admin_password)` | PG data dir, admin password |
| `WorkflowServer(data_dir, admin_password)` | Own PG instance (decoupled from store) |
| `StreamingServer(port=10000, max_heap="4g")` | DH JVM port and heap size |
| `MarketDataServer(port=8000, host="0.0.0.0")` | FastAPI server port |
| `TsdbServer(data_dir, http_port, ilp_port, pg_port)` | QuestDB ports |
| `MediaServer(data_dir, api_port=9002, console_port=9003)` | MinIO ports |
| `LakehouseServer(data_dir)` | Wraps Lakekeeper + MinIO + PG |

### Properties

| Property | Description |
|----------|-------------|
| `.port` | Server port (where applicable) |
| `.url` | Server URL (where applicable) |
| `.conn_info()` | PG connection dict (StoreServer, WorkflowServer) |
| `.admin_conn()` | Admin PG connection (StoreServer) |
| `.pg_url()` | SQLAlchemy connection string (StoreServer, WorkflowServer) |

---

## 1. Connection

**How users connect to the store.**

```python
from store import connect
```

| Symbol | Kind | Description |
|--------|------|-------------|
| `connect()` | function | Open a connection and make it the active thread-local connection. |

```python
# Positional alias or keyword host
db = connect("trading", user="alice", password="alice_pw")
db = connect(host="/tmp/pg", port=5432, dbname="mydb", user="alice", password="pw")

# Context manager supported
with connect(host="/tmp/pg", port=5432, user="alice", password="pw") as db:
    ...  # all Storable ops use this connection
```

---

## 2. Object Store

**Define models, persist, query, time-travel.**

```python
from store import Storable
```

### Define a model

```python
from dataclasses import dataclass

@dataclass
class Trade(Storable):
    symbol: str = ""
    quantity: int = 0
    price: float = 0.0
```

### Instance methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `.save()` | `save(valid_from=None) → entity_id` | Create or update. Returns entity_id on first save. |
| `.delete()` | `delete()` | Soft-delete (DELETED tombstone). |
| `.transition()` | `transition(new_state, valid_from=None)` | Move to a new lifecycle state. Fires 3-tier side-effects. |
| `.refresh()` | `refresh()` | Reload from store (latest version). Updates reactive Signals. |
| `.history()` | `history() → list[Self]` | All versions of this entity. |
| `.audit()` | `audit() → list[dict]` | Full audit trail (event_type, who, when). |
| `.share()` | `share(user, mode="read")` | Grant read or write access to another user. |
| `.unshare()` | `unshare(user, mode="read")` | Revoke access. |
| `.batch_update()` | `batch_update(**kwargs)` | Set multiple fields in one reactive tick. |
| `.clear_override()` | `clear_override(name)` | Remove a @computed override, revert to formula. |
| `.to_json()` | `to_json() → str` | Serialize to JSON string. |

### Errors

| Error | Raised by | When |
|-------|-----------|------|
| `VersionConflict` | `.save()` | Optimistic concurrency check failed (stale version) |

### Class methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `.find()` | `find(entity_id) → Self or None` | Read latest non-deleted version by ID. |
| `.query()` | `query(filters=None, limit=100, cursor=None) → QueryResult` | Paginated query with optional filters. |
| `.count()` | `count() → int` | Count current entities of this type. |
| `.as_of()` | `as_of(entity_id, *, tx_time=None, valid_time=None) → Self` | Bi-temporal point-in-time query. |
| `.from_json()` | `from_json(json_str) → Self` | Deserialize from JSON string. |

### Store metadata (read-only, set automatically)

| Attribute | Type | Description |
|-----------|------|-------------|
| `_store_entity_id` | `str` | Stable identity across versions |
| `_store_version` | `int` | Monotonic version number |
| `_store_owner` | `str` | User who created the entity |
| `_store_updated_by` | `str` | User who wrote this version |
| `_store_tx_time` | `datetime` | When this version was recorded (immutable) |
| `_store_valid_from` | `datetime` | When this version is effective |
| `_store_valid_to` | `datetime` | When this version stops being effective |
| `_store_state` | `str` | Current lifecycle state |
| `_store_event_type` | `str` | CREATED / UPDATED / DELETED / STATE_CHANGE / CORRECTED |

---

## 3. Reactive Computation

**Declarative computed properties and side-effects on Storable objects.**

```python
from reactive import computed, effect
```

| Symbol | Kind | Description |
|--------|------|-------------|
| `@computed` | decorator | Declare a reactive computed property. Auto-recomputes when dependencies change. |
| `@effect` | decorator | Declare a side-effect that fires when a @computed value changes. |

```python
@dataclass
class Position(Storable):
    quantity: int = 0
    current_price: float = 0.0

    @computed
    def market_value(self):
        return self.quantity * self.current_price

    @effect("market_value")
    def _on_mv_change(self, value):
        print(f"MV changed to {value}")
```

### Features

- **Single-entity**: `self.x * self.y` — auto-parsed to Expr via AST, compilable to SQL/Pure
- **Cross-entity**: `self.child.field` — proxy-based reactive evaluation
- **Overrides**: `pos.market_value = 999` overrides formula; `pos.clear_override("market_value")` reverts
- **Batch**: `pos.batch_update(quantity=100, current_price=250)` — single recomputation

---

## 4. State Machine

**Declarative lifecycle management with three tiers of side-effects.**

```python
from store import StateMachine, Transition
```

| Symbol | Kind | Description |
|--------|------|-------------|
| `StateMachine` | base class | Subclass to define lifecycle. Set `initial` and `transitions`. |
| `Transition` | dataclass | One edge: from_state → to_state with guard, action, hooks, workflow. |

### Transition fields

| Field | Type | Description |
|-------|------|-------------|
| `from_state` | `str` | Source state (required) |
| `to_state` | `str` | Target state (required) |
| `guard` | `callable(obj) → bool` | Must return True for transition to proceed. |
| `action` | `callable(obj, from_state, to_state)` | **Tier 1**: Runs inside DB transaction. Atomic. |
| `on_exit` | `callable(obj, from_state, to_state)` | **Tier 2**: Fire-and-forget after commit. |
| `on_enter` | `callable(obj, from_state, to_state)` | **Tier 2**: Fire-and-forget after commit. |
| `start_workflow` | `callable(entity_id)` | **Tier 3**: Durable workflow dispatch after commit. |
| `allowed_by` | `list[str]` | Users permitted to trigger. None = anyone with write access. |

### StateMachine class methods

| Method | Description |
|--------|-------------|
| `.validate_transition(from_state, to_state, ...)` | Check edge, guard, permissions. |
| `.allowed_transitions(from_state)` | Valid next states from current state. |
| `.get_transition(from_state, to_state)` | Get the Transition object for an edge. |

```python
class OrderLifecycle(StateMachine):
    initial = "PENDING"
    transitions = [
        Transition("PENDING", "FILLED",
                   guard=lambda obj: obj.quantity > 0,
                   action=lambda obj, f, t: book_settlement(obj)),
        Transition("PENDING", "CANCELLED",
                   allowed_by=["risk_manager"]),
    ]

Order._state_machine = OrderLifecycle
order.transition("FILLED")
```

### Errors

| Error | Raised by | When |
|-------|-----------|------|
| `InvalidTransition` | `.transition()` | Edge doesn't exist in the state machine |
| `GuardFailure` | `.transition()` | Guard callable returned False |
| `TransitionNotPermitted` | `.transition()` | User not in `allowed_by` list |

---

## 5. Events

**React to store changes — in-process or cross-process.**

```python
from store import EventListener, ChangeEvent
```

| Symbol | Kind | Description |
|--------|------|-------------|
| `EventListener` | class | Unified event listener. Mode set by presence of `subscriber_id`. |
| `ChangeEvent` | dataclass | Event payload: entity_id, version, event_type, type_name, updated_by, state, tx_time. |

### EventListener

| Method | Description |
|--------|-------------|
| `EventListener(subscriber_id=None)` | No subscriber_id = in-process only. With subscriber_id = durable PG LISTEN/NOTIFY. |
| `.on(type_name, callback)` | Subscribe to all changes for a type. Lazy-starts PG listener on first call. |
| `.on_entity(entity_id, callback)` | Subscribe to one specific entity. |
| `.on_all(callback)` | Subscribe to all changes. |
| `.off(type_name, callback)` | Unsubscribe a type listener. |
| `.off_entity(entity_id, callback)` | Unsubscribe an entity listener. |
| `.off_all(callback)` | Unsubscribe a catch-all listener. |
| `.emit(event)` | Dispatch a ChangeEvent (used internally by StoreClient). |

```python
# In-process only
listener = EventListener()
listener.on("Order", lambda e: print(e.event_type))
db = connect(..., event_bus=listener)

# Durable cross-process (context manager for clean shutdown)
with EventListener(subscriber_id="risk-svc") as listener:
    listener.on("Order", handle_order)   # PG listener starts here
    ...
```

---

## 6. Workflows

**Durable, crash-recoverable workflow orchestration.**

```python
from workflow import WorkflowEngine, WorkflowStatus
```

| Symbol | Kind | Description |
|--------|------|-------------|
| `WorkflowEngine` | ABC | Abstract base — concrete backend (e.g. DBOS) is hidden. |
| `WorkflowStatus` | enum | `PENDING`, `RUNNING`, `SUCCESS`, `ERROR`, `CANCELLED` |

### WorkflowEngine methods

| Method | Description |
|--------|-------------|
| `.workflow(fn, *args, **kwargs)` | Execute fn as a durable workflow. Returns `WorkflowHandle`. |
| `.run(fn, *args, **kwargs)` | Execute workflow synchronously, block for result. |
| `.step(fn, *args, **kwargs)` | Checkpointed step — exactly-once on replay. |
| `.durable_transition(obj, new_state)` | State transition as a checkpointed step. Uses active connection. |
| `.queue(queue_name, fn, *args)` | Enqueue fn for background execution. |
| `.sleep(seconds)` | Durable sleep — survives process restarts. |
| `.send(workflow_id, topic, value)` | Send notification to a workflow. |
| `.recv(topic, timeout=None)` | Receive notification inside a workflow. |
| `.get_workflow_status(workflow_id)` | Returns `WorkflowStatus`. |
| `.get_workflow_result(workflow_id, timeout=None)` | Block until complete, return result. |

```python
def settlement_workflow(entity_id):
    order = engine.step(lambda: Order.find(entity_id))
    engine.step(lambda: call_clearing_house(order))
    engine.durable_transition(order, "SETTLED")   # checkpointed, exactly-once
```

Backend is swappable — implement `WorkflowEngine` for Temporal, AWS Step Functions, or custom.

---

## 7. Streaming

**Real-time ticking tables with auto-locked derivations.** See [STREAMING.md](STREAMING.md) for full docs.

```python
from streaming import TickingTable, LiveTable, StreamingClient, flush, agg, ticking, get_tables
```

| Symbol | Kind | Description |
|--------|------|-------------|
| `TickingTable` | class | Writable ticking table backed by DynamicTableWriter. Python-typed schema. |
| `LiveTable` | class | Read-only derived table. All operations auto-acquire UG shared lock. |
| `flush` | function | Flush the update graph so pending writes become visible. Thread-safe. |
| `agg` | module | Aggregation helpers: `sum`, `avg`, `count`, `min`, `max`, `first`, `last`, `std`, `var`, `median`, `pct`, `weighted_avg`. |
| `ticking` | decorator | Auto-creates TickingTable + LiveTable from a Storable dataclass. |
| `get_tables` | function | Returns dict of all `@ticking`-decorated tables. |
| `StreamingClient` | class | Lightweight remote client for querying a streaming server. Wraps pydeephaven. |

### TickingTable

```python
prices = TickingTable({"Symbol": str, "Price": float, "Volume": int})
```

| Method | Returns | Description |
|--------|---------|-------------|
| `.write_row(*values)` | — | Write a single row. Thread-safe. |
| `.flush()` | — | Flush the update graph. |
| `.close()` | — | Close the underlying writer. |
| `.last_by(by)` | `LiveTable` | Latest row per group key. |
| `.agg_by(aggs, by_columns)` | `LiveTable` | Aggregation. |
| `.sort_descending(by)` | `LiveTable` | Sort descending. |
| `.where(filters)` | `LiveTable` | Filter rows. |
| `.select(columns)` | `LiveTable` | Select/rename columns. |
| `.update(formulas)` | `LiveTable` | Add computed columns. |
| `.snapshot()` | `DataFrame` | Pandas snapshot. |
| `.publish(name)` | — | Publish to DH query scope. |
| `.size` | `int` | Current row count. |

### Supported types

| Python | Deephaven |
|--------|-----------|
| `str` | `string` |
| `int` | `int64` |
| `float` | `double` |
| `bool` | `bool_` |
| `datetime` | `Instant` |
| `Decimal` | `double` |

---

## 8. Deephaven Bridge

**Stream store events into Deephaven ticking tables.**

```python
from bridge import StoreBridge
```

| Symbol | Kind | Description |
|--------|------|-------------|
| `StoreBridge` | class | Streams PG NOTIFY events into `TickingTable` instances. |

### StoreBridge methods

| Method | Description |
|--------|-------------|
| `StoreBridge(host, port, dbname, user, password, subscriber_id)` | Create bridge with PG connection params. |
| `.register(storable_cls, *, filter=None, columns=None)` | Register a Storable type to be bridged. |
| `.table(storable_cls)` | Get the `TickingTable` for a type (all derivations auto-locked). |
| `.start()` | Begin listening and bridging events. |
| `.stop()` | Stop and clean up. |

---

## 9. Lakehouse

**Query, ingest, and transform data in Apache Iceberg tables via DuckDB SQL.**

```python
from lakehouse import Lakehouse
```

| Symbol | Kind | Description |
|--------|------|-------------|
| `Lakehouse` | class | User-facing Iceberg interface: query, ingest, and transform. |

### Constructor

```python
lh = Lakehouse(
    catalog_uri=None,      # Lakekeeper REST URL (default: env or http://localhost:8181/catalog)
    warehouse=None,        # Iceberg warehouse name (default: "lakehouse")
    s3_endpoint=None,      # MinIO endpoint (default: env or http://localhost:9002)
    s3_access_key=None,    # MinIO access key (default: "minioadmin")
    s3_secret_key=None,    # MinIO secret key (default: "minioadmin")
    s3_region=None,        # S3 region (default: "us-east-1")
    namespace="default",   # Iceberg namespace
)
```

### Query methods

| Method | Signature | Returns | Description |
|--------|-----------|---------|-------------|
| `.query()` | `query(sql, params=None)` | `list[dict]` | Execute SQL, return rows as dicts. |
| `.query_arrow()` | `query_arrow(sql, params=None)` | `pa.Table` | Execute SQL, return PyArrow Table. |
| `.query_df()` | `query_df(sql, params=None)` | `pd.DataFrame` | Execute SQL, return pandas DataFrame. |

### Write methods

| Method | Signature | Returns | Description |
|--------|-----------|---------|-------------|
| `.ingest()` | `ingest(table_name, data, mode="append", primary_key=None)` | `int` | Write data to Iceberg table. Returns row count. |
| `.transform()` | `transform(table_name, sql, mode="append", primary_key=None)` | `int` | Run SQL query, write results to Iceberg table. Returns row count. |

**`data`** accepts: `pa.Table`, `pd.DataFrame`, or `list[dict]`.

**`mode`** options:

| Mode | Metadata added | Requires `primary_key` |
|------|---------------|----------------------|
| `"append"` | `_batch_id`, `_batch_ts` | No |
| `"snapshot"` | `_batch_id`, `_batch_ts`, `_is_current` | No |
| `"incremental"` | `_batch_id`, `_batch_ts`, `_is_current`, `_updated_at` | Yes |
| `"bitemporal"` | `_batch_id`, `_batch_ts`, `_is_current`, `_tx_time`, `_valid_from`, `_valid_to` | Yes |

### Metadata methods

| Method | Signature | Returns | Description |
|--------|-----------|---------|-------------|
| `.tables()` | `tables()` | `list[str]` | List all tables in the catalog. |
| `.table_info()` | `table_info(table_name)` | `list[dict]` | Column info for a table. |
| `.row_count()` | `row_count(table_name)` | `int` | Row count for a table. |
| `.close()` | `close()` | `None` | Close the DuckDB connection. |

### Errors

| Error | Raised by | When |
|-------|-----------|------|
| `ValueError` | `.ingest()` | Invalid mode, missing primary_key, or primary_key not in data |
| `duckdb.CatalogException` | `.query()`, `.ingest()` | Table or namespace doesn't exist |

```python
lh = Lakehouse()

# Ingest
lh.ingest("signals", [{"symbol": "AAPL", "score": 0.95}], mode="append")
lh.ingest("trades", df, mode="incremental", primary_key="trade_id")

# Transform
lh.transform("daily_pnl", "SELECT ... GROUP BY ...", mode="snapshot")

# Query
results = lh.query("SELECT * FROM lakehouse.default.signals WHERE _is_current = true")
lh.close()
```

---

## 10. Media Store

**Unstructured data storage & search — full-text, semantic, and hybrid.**

```python
from media import MediaStore, Document
```

| Symbol | Kind | Description |
|--------|------|-------------|
| `MediaStore` | class | Upload, download, search, list, delete unstructured files. |
| `Document` | Storable | Metadata model for files stored in S3. Inherits all Storable features. |

### Constructor

```python
from ai import AI

ai = AI()  # optional — enables embeddings + semantic search
ms = MediaStore(
    s3_endpoint="localhost:9002",   # MinIO endpoint
    s3_access_key="minioadmin",     # MinIO access key
    s3_secret_key="minioadmin",     # MinIO secret key
    s3_bucket="media",              # S3 bucket name
    ai=ai,                          # enables auto-embed on upload
)
```

### Methods

| Method | Signature | Returns | Description |
|--------|-----------|---------|-------------|
| `.upload()` | `upload(source, *, filename, title, content_type, tags, metadata, extract)` | `Document` | Upload file, extract text, chunk + embed (if ai= set). |
| `.download()` | `download(doc_or_id)` | `bytes` | Download file content from S3. |
| `.download_to()` | `download_to(doc_or_id, path)` | `Path` | Download to local file. |
| `.search()` | `search(query, content_type=None, tags=None, limit=50)` | `list[dict]` | Full-text keyword search (tsvector). |
| `.semantic_search()` | `semantic_search(query, limit=10)` | `list[dict]` | Vector similarity search on chunks (requires ai=). |
| `.hybrid_search()` | `hybrid_search(query, limit=10)` | `list[dict]` | RRF fusion of text + semantic (requires ai=). |
| `.list()` | `list(content_type=None, tags=None, limit=100)` | `list[Document]` | List documents with optional filters. |
| `.delete()` | `delete(doc_or_id)` | `None` | Soft-delete (Storable semantics). S3 object retained. |
| `.close()` | `close()` | `None` | Clean up resources. |

**`source`** accepts: file path (`str`/`Path`) or `bytes`.

### Text Extraction

| Content type | Library | Status |
|-------------|---------|--------|
| `application/pdf` | pymupdf | ✅ |
| `text/plain`, `text/csv` | built-in | ✅ |
| `text/markdown` | built-in (regex) | ✅ |
| `text/html` | beautifulsoup4 | ✅ |
| `image/*`, `audio/*`, `video/*` | — | Stored, no extraction |

### Search Modes

| Mode | Method | How | Best for |
|------|--------|-----|----------|
| Full-text | `.search()` | PG tsvector weighted ranking | Exact keywords |
| Semantic | `.semantic_search()` | pgvector cosine on chunks | Meaning-based queries |
| Hybrid | `.hybrid_search()` | RRF fusion (k=60) | General queries |

```python
from ai import AI
from media import MediaStore

ai = AI()
ms = MediaStore(s3_endpoint="localhost:9002", ai=ai)

doc = ms.upload("report.pdf", title="Q1 Report", tags=["research"])
ms.search("interest rate swap")          # keywords
ms.semantic_search("risk transfer")      # meaning
ms.hybrid_search("credit derivatives")   # best of both

data = ms.download(doc)
ms.close()
```

---

## 11. AI

**Embeddings, LLM generation, RAG, extraction, and tool calling.** See [AI.md](AI.md) for full docs.

```python
from ai import AI, Message, LLMResponse, ToolCall, RAGResult, ExtractionResult, Tool
```

| Symbol | Kind | Description |
|--------|------|-------------|
| `AI` | class | Single entry point for all AI capabilities. |
| `Message` | dataclass | Conversation message (role + content). |
| `LLMResponse` | dataclass | Generated text + optional tool calls + usage stats. |
| `ToolCall` | dataclass | A tool call in `LLMResponse.tool_calls`. |
| `RAGResult` | dataclass | Answer + sources + usage from `ai.ask()`. |
| `ExtractionResult` | dataclass | Extracted data + raw response from `ai.extract()`. |
| `Tool` | dataclass | Custom tool definition (name, schema, function). |

### Constructor

```python
ai = AI(
    api_key=None,          # Falls back to GEMINI_API_KEY env var
    provider="gemini",     # Currently only "gemini" supported
    embedding_dim=768,     # Embedding vector dimension
    model=None,            # LLM model (default: gemini-3-flash-preview)
)
```

### Methods

| Method | Signature | Returns | Description |
|--------|-----------|---------|-------------|
| `.generate()` | `(messages, tools=, temperature=, max_tokens=)` | `LLMResponse` | Generate. `messages` = string or list[Message]. |
| `.stream()` | `(messages, tools=, temperature=, max_tokens=)` | `Generator[str]` | Stream response chunks. |
| `.ask()` | `(question, documents=, search_mode=, limit=, temperature=)` | `RAGResult` | RAG: retrieve from documents + generate answer. |
| `.extract()` | `(text, schema, model_class=, temperature=)` | `ExtractionResult` | Extract structured data matching JSON schema. |
| `.run_tool_loop()` | `(messages, tools=, execute_tool=, max_iterations=)` | `LLMResponse` | Generate → tool call → execute → respond loop. |
| `.search_tools()` | `(media_store)` | `list[dict]` | Get search tool declarations for a MediaStore. |

```python
ai = AI()

# Generate
response = ai.generate("Explain convexity.")

# RAG
result = ai.ask("What are CDS?", documents=ms)

# Extract
data = ai.extract("Revenue $12.7B, EPS $8.40", schema={...})

# Stream
for chunk in ai.stream("Explain gamma hedging"):
    print(chunk, end="")

# Tool calling
response = ai.run_tool_loop("Search for Basel docs", tools=ai.search_tools(ms))
```

---

## 12. Time-Series Database

**Backend-agnostic historical market data storage.**

```python
from timeseries import TSDBBackend, TSDBConsumer, create_backend, Bar, HistoryQuery, BarQuery
```

| Symbol | Kind | Description |
|--------|------|-------------|
| `TSDBBackend` | ABC | Abstract base class for TSDB backends. Implement `start`, `stop`, `write_tick`, `flush`, `get_ticks`, `get_bars`, `get_latest`. |
| `TSDBConsumer` | class | TickBus subscriber that routes every tick to a `TSDBBackend`. |
| `create_backend(name)` | function | Factory — instantiates a backend by name. Default: `"questdb"`. Reads `TSDB_BACKEND` env var. |
| `Bar` | Pydantic model | OHLCV bar: `symbol`, `interval`, `open`, `high`, `low`, `close`, `volume`, `trade_count`, `timestamp`. |
| `HistoryQuery` | Pydantic model | Query params for raw tick history: `type`, `symbol`, `start`, `end`, `limit`. |
| `BarQuery` | Pydantic model | Query params for OHLCV bars: `type`, `symbol`, `interval`, `start`, `end`. |

### REST Endpoints (on market data server)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/md/history/{type}/{symbol}` | Raw tick history with time range and limit |
| GET | `/md/bars/{type}/{symbol}` | OHLCV bars at a given interval |
| GET | `/md/bars/{type}` | Bars for all symbols of a type |
| GET | `/md/latest/{type}` | Latest tick(s) per symbol from TSDB |

See [TIMESERIES.md](TIMESERIES.md) for full details.

---

## 13. Agents (PlatformAgents)

**8 specialist AI agents orchestrated by an LLM router, each wired to real platform services.** See [PLATFORM_AGENTS.md](PLATFORM_AGENTS.md) for full docs.

```python
from agents import PlatformAgents
```

### Constructor

```python
PlatformAgents(
    alias="",              # Default alias for all services
    user="",               # Default user for authenticated services
    password="",           # Default password
    *,
    store_alias=None,      # Override: object store alias
    lakehouse_alias=None,  # Override: lakehouse alias
    tsdb_alias=None,       # Override: TSDB alias
    streaming_alias=None,  # Override: streaming server alias
    md_alias=None,         # Override: market data alias
    media_alias=None,      # Override: media store alias
    ai=None,               # Pre-built AI instance
    agents=None,           # Subset: ["oltp", "lakehouse", ...] (default: all 8)
    temperature=0.5,       # Router LLM temperature
    max_delegations=8,     # Max delegation rounds per run
)
```

### Methods & Properties

| Symbol | Kind | Description |
|--------|------|-------------|
| `PlatformAgents` | class | Multi-agent team — one constructor, 8 agents |
| `.run(prompt)` | method | LLM router delegates to specialist agents |
| `.oltp` | property | OLTP Dataset Agent |
| `.lakehouse` | property | Lakehouse Curation Agent |
| `.feed` | property | Market Data Feed Agent |
| `.timeseries` | property | Time Series Agent |
| `.document` | property | Document Agent |
| `.dashboard` | property | Dashboard Agent |
| `.query` | property | Query Agent (cross-store) |
| `.quant` | property | Quant/DataScience Agent |

### Per-Agent Tool Inventories

**OLTP** (`create_oltp_tools`): `create_dataset`, `insert_records`, `query_dataset`, `list_storable_types`, `describe_type`, `ingest_from_file` + codegen tools

**Feed** (`create_feed_tools`): `list_md_symbols`, `get_md_snapshot`, `get_feed_health`, `publish_custom_tick`, `describe_feed_setup`

**Timeseries** (`create_timeseries_tools`): `list_tsdb_series`, `get_bars`, `get_tick_history`, `compute_realized_vol`, `compare_cross_exchange`, `ingest_historical_csv`

**Lakehouse** (`create_lakehouse_tools`): `list_lakehouse_tables`, `describe_lakehouse_table`, `design_star_schema`, `create_lakehouse_table`, `ingest_to_lakehouse`, `build_datacube`, `query_lakehouse`

**Quant** (`create_datascience_tools`): `run_sql_analysis`, `compute_statistics`, `compute_correlation`, `detect_anomalies`, `run_regression`, `time_series_decompose`, `suggest_visualization`

**Document** (`create_document_tools`): `upload_document`, `list_documents`, `search_documents`, `extract_structured_data`, `bulk_upload`, `tag_document`

**Dashboard** (`create_dashboard_tools`): `list_ticking_tables`, `create_ticking_table`, `create_derived_table`, `setup_store_bridge`, `create_reactive_model`, `publish_table` + codegen tools

**Query** (`create_query_tools`): `query_store`, `query_lakehouse`, `get_md_snapshot`, `list_all_datasets`, `describe_dataset`, `search_documents`, `cross_store_query`

### Eval Symbols

| Symbol | Package | Description |
|--------|---------|-------------|
| `AgentEval` | `agents._eval.framework` | Multi-phase evaluator |
| `EvalPhase` | `agents._eval.framework` | Enum: TOOL_SELECTION, EXECUTION, OUTPUT_QUALITY |
| `AgentEvalCase` | `agents._eval.datasets` | Test case with expected tools and outputs |
| `ALL_EVAL_CASES` | `agents._eval.datasets` | 20+ eval cases across all agents |
| `OLTP_EVAL_CASES` | `agents._eval.datasets` | OLTP-specific eval cases |
| `LAKEHOUSE_EVAL_CASES` | `agents._eval.datasets` | Lakehouse eval cases |
| `QUERY_EVAL_CASES` | `agents._eval.datasets` | Query agent eval cases |
| `DATASCIENCE_EVAL_CASES` | `agents._eval.datasets` | Quant agent eval cases |

---

## Summary

| Package | Symbols | Count |
|---------|---------|-------|
| **platform** | `StoreServer`, `WorkflowServer`, `StreamingServer`, `MarketDataServer`, `TsdbServer`, `MediaServer`, `LakehouseServer` | 7 |
| **store** | `Storable`, `connect`, `StateMachine`, `Transition`, `EventListener`, `ChangeEvent`, `VersionConflict`, `InvalidTransition`, `GuardFailure`, `TransitionNotPermitted` | 10 |
| **reactive** | `computed`, `effect` | 2 |
| **workflow** | `WorkflowEngine`, `WorkflowStatus`, `create_engine` | 3 |
| **streaming** | `TickingTable`, `LiveTable`, `StreamingClient`, `flush`, `agg`, `ticking`, `get_tables` | 7 |
| **bridge** | `StoreBridge` | 1 |
| **lakehouse** | `Lakehouse` | 1 |
| **media** | `MediaStore`, `Document` | 2 |
| **ai** | `AI`, `Message`, `LLMResponse`, `ToolCall`, `RAGResult`, `ExtractionResult`, `Tool` | 7 |
| **timeseries** | `TSDBBackend`, `TSDBConsumer`, `Timeseries`, `create_backend`, `Bar`, `HistoryQuery`, `BarQuery` | 7 |
| **agents** | `PlatformAgents` + 8 agent properties + eval symbols | 10+ |
| **Total** | | **57+** |
