# Roadmap

A governance-first data+AI platform in pure Python. This roadmap tracks what's built and what's next.

---

## What's Built

| Package | What | Status |
|---------|------|--------|
| **store** | Bi-temporal event-sourced object store, embedded PG, RLS, column registry, state machines, permissions | ✅ Done |
| **reactive** | Expression tree (Python/SQL/Pure), `@computed` + `@effect` decorators, reactive graph | ✅ Done |
| **workflow** | Durable workflow engine (ABC + DBOS backend), checkpointed steps, queues, timers | ✅ Done |
| **bridge** | StoreBridge — PG LISTEN/NOTIFY → Deephaven ticking tables, `@dh_table` decorator | ✅ Done |
| **lakehouse** | Iceberg tables via DuckDB + Lakekeeper + MinIO, 4 write modes, SyncEngine (PG/QuestDB → Iceberg) | ✅ Done |
| **timeseries** | QuestDB time-series backend, tick storage, OHLCV bars, backtest engine | ✅ Done |
| **marketdata** | FastAPI market data hub, WebSocket streaming, multi-asset (equity/FX/curve), TickBus | ✅ Done |
| **media** | Unstructured data store, S3 + PG full-text search, PDF/text/markdown/HTML extraction | ✅ Done |
| **ai** | Embeddings (Gemini), LLM client (Gemini 3 Flash), RAG pipeline, tool calling, structured extraction | ✅ Done |

**Current totals:** ~36 public symbols across 9 packages, 7 demos, 350+ tests.

---

## What's Next

### Phase 1 — Vector Search & Embeddings
> Extend `media/` with pgvector semantic search and chunked embeddings.

| # | Step | Status |
|---|------|--------|
| 1 | pgvector extension + HNSW index on `document_search` | ✅ |
| 2 | Text chunking (`media/chunking.py`) — overlapping ~512-token chunks | ✅ |
| 3 | Embedding providers (`ai/embeddings.py`) — Gemini primary | ✅ |
| 4 | Embed-on-upload — `MediaStore(embedding_provider=...)` | ✅ |
| 5 | Semantic search — `MediaStore.semantic_search(query)` | ✅ |
| 6 | Hybrid search — RRF fusion of tsvector rank + vector distance | ✅ |

**New package:** `ai/`  
**Dependencies:** `pgvector`, `google-genai`

---

### Phase 2 — LLM Integration
> Provider-agnostic LLM client with tool-calling against the platform.

| # | Step | Status |
|---|------|--------|
| 7 | LLM client ABC — GeminiLLM (gemini-3-flash-preview) | ✅ |
| 8 | Platform tools — `search_documents`, `semantic_search`, `hybrid_search`, `list_documents` | ✅ |
| 9 | RAG pipeline — retrieve → augment → generate with citations | ✅ |
| 10 | Structured extraction — LLM → typed Python objects | ✅ |

**Dependencies:** `google-genai`

---

### Phase 3 — Agents & Evaluation
> Tool-calling agent framework + systematic eval harness for measuring agent quality.

| # | Step | Status |
|---|------|--------|
| 11 | Agent class — tool-calling loop with plan → act → observe cycle | ✅ |
| 12 | Tool registry — `@tool` decorator, `registry.from_platform()` auto-registers all built-in tools | ✅ |
| 13 | Agent memory — conversation, storable-backed, LLM-summarized. All RLS + auditable | ✅ |
| 14 | Multi-agent orchestration — `AgentTeam` with router + delegation | ✅ |
| 15 | Eval framework — `EvalRunner` with cases, expected outputs, tool call assertions | ✅ |
| 16 | Eval datasets & metrics — built-in QA/tool-use/SQL benchmarks, A/B model comparison | ✅ |

**New files:** `ai/agents/`, `ai/eval/`  
**Dependencies:** same as Phase 2 (no new deps)

---

### Phase 4 — Scheduling & DAG Execution
> Cron-based scheduling + dependency DAGs with durable execution via WorkflowEngine.

| # | Step | Status |
|---|------|--------|
| 17 | Schedule + ScheduleRun Storable models | 🔲 |
| 18 | Cron parser (croniter) | 🔲 |
| 19 | Scheduler engine — register, fire, track runs | 🔲 |
| 20 | `@schedule` decorator | 🔲 |
| 21 | Management API — list, pause, resume, trigger, history | 🔲 |
| 22 | DAG model — `.task(name, fn, depends_on=[...])`, acyclicity validation | 🔲 |
| 23 | DAG runner — parallel branches, checkpointed steps, per-task status | 🔲 |
| 24 | DAG + Schedule — `register_dag(name, cron, dag)` | 🔲 |
| 25 | Event-driven DAG trigger — `on_event(type_name, dag)` | 🔲 |
| 26 | Pre-built DAGs — `sync_dag()`, `media_embed_dag()`, `quality_dag()` | 🔲 |

**New package:** `scheduler/`  
**Dependencies:** `croniter`

---

### Phase 5 — Service Manager
> Pluggable process supervisor — same API in dev (subprocess) and prod (Docker).

| # | Step | Status |
|---|------|--------|
| 27 | ServiceManager ABC — start, stop, restart, status, health, logs | 🔲 |
| 28 | SubprocessManager — dev/demo backend, replaces ad-hoc process management | 🔲 |
| 29 | DockerComposeManager — generates + manages docker-compose.yml | 🔲 |
| 30 | Platform class — dependency-ordered startup, health check gates | 🔲 |

**New package:** `platform/`  
**Dependencies:** none (stdlib + docker CLI)

---

### Phase 6 — Data Lineage
> Track what produces what — transform-level and column-level lineage.

| # | Step | Status |
|---|------|--------|
| 31 | LineageNode + LineageEdge Storable models | 🔲 |
| 32 | Auto-capture on `Lakehouse.transform()` / `ingest()` | 🔲 |
| 33 | Auto-capture on `SyncEngine.sync_*()` | 🔲 |
| 34 | Lineage query API — upstream, downstream, column lineage | 🔲 |
| 35 | Impact analysis — "what breaks if I change this?" | 🔲 |

**New package:** `lineage/`  
**Dependencies:** `sqlglot` (opt)

---

### Phase 7 — Data Quality
> Schema validation, freshness checks, anomaly detection on lakehouse tables.

| # | Step | Status |
|---|------|--------|
| 36 | QualityCheck + QualityResult Storable models | 🔲 |
| 37 | Built-in checks — schema, freshness, row count, null rate, custom SQL | 🔲 |
| 38 | Quality runner — `run_all()`, `run_check()`, `register()` | 🔲 |
| 39 | Alerts — failed checks emit ChangeEvent (warn/error/critical) | 🔲 |
| 40 | Quality + Scheduler — `quality_check_dag()` on cron | 🔲 |

**New package:** `quality/`  
**Dependencies:** `croniter` (shared with scheduler)

---

### Phase 8 — Datacube Engine & Perspective
> Multi-dimensional pivot table with SQL pushdown to lakehouse. Inspired by FINOS Legend Studio.

| # | Step | Status |
|---|------|--------|
| 41 | DatacubeConfig model — rows, columns, values, filters, sort | 🔲 |
| 42 | SQL pushdown compiler — config → DuckDB GROUP BY / PIVOT / WHERE | 🔲 |
| 43 | Drill-down — hierarchical expand with server-side re-query | 🔲 |
| 44 | Extended columns — user-defined DuckDB SQL expressions | 🔲 |
| 45 | Perspective server — Arrow → Tornado WebSocket → WASM viewer | 🔲 |
| 46 | Datacube ↔ Perspective bridge — config change → re-query → update | 🔲 |
| 47 | Live ticking — EventBus → debounced `table.update()` | 🔲 |
| 48 | Bidirectional config — client pivot changes → server SQL recompile | 🔲 |

**New package:** `dashboard/`  
**Dependencies:** `perspective-python`, `tornado`

---

### Phase 9 — Dashboard Framework & Components
> Panel-based dashboards with custom Streamlit-quality theme.

| # | Step | Status |
|---|------|--------|
| 49 | Custom Panel theme — Inter font, dark/light mode, card layout | 🔲 |
| 50 | Dashboard class — `add_datacube()`, `add_chart()`, `add_table()`, `add_widget()` | 🔲 |
| 51 | Layout engine — grid pages, multi-page sidebar nav | 🔲 |
| 52 | Filter binding — widget ↔ datacube cross-filtering | 🔲 |
| 53 | Serve — `dashboard.serve(port=8050)` + Jupyter `show()` | 🔲 |
| 54 | Storable datacube — pivot any Storable type via object_events | 🔲 |
| 55 | Lakehouse explorer — browse Iceberg tables with inline datacube | 🔲 |
| 56 | Lineage dashboard — graph visualization + quality badges | 🔲 |
| 57 | Live market data — ticking datacube from TickBus | 🔲 |

**Dependencies:** `panel`, `plotly`, `perspective-python`

---

## Summary

| Metric | Current | After roadmap |
|--------|---------|---------------|
| **Packages** | 8 | 14 |
| **Public symbols** | ~24 | ~54 |
| **Phases** | — | 9 |
| **Steps** | — | 57 |

```
┌─────────────────────────────────────────────────────────────────┐
│                        py-flow Platform                          │
├──────────┬──────────┬───────────┬──────────┬───────────┬────────┤
│  store   │ reactive │ workflow  │  bridge  │ lakehouse │ media  │
│  ✅      │  ✅      │  ✅       │  ✅      │  ✅       │ ✅     │
├──────────┼──────────┼───────────┼──────────┼───────────┼────────┤
│ market   │ time     │    ai     │scheduler │ platform  │lineage │
│ data ✅  │series ✅ │   ✅      │  🔲      │   🔲      │  🔲    │
├──────────┴──────────┼───────────┼──────────┴───────────┼────────┤
│      quality 🔲     │           │    dashboard 🔲      │        │
└─────────────────────┴───────────┴──────────────────────┴────────┘
```

Track progress in this file. Update status: 🔲 → 🔧 (in progress) → ✅ (done).
