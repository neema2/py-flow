# py-flow

A governance-first reactive platform backed by **PostgreSQL** with **[Deephaven.io](https://deephaven.io)** for real-time streaming — featuring a **bi-temporal event-sourced object store**, a **reactive expression language** that compiles to Python, SQL, and Legend Pure, and **durable workflow orchestration** with zero external infrastructure.

Every service follows the same pattern: **`XxxServer`** (platform/admin) → **`connect()` / `Xxx()`** (user code). Implementation details (PG, MinIO, QuestDB, Deephaven JVM, uvicorn) are hidden.

```
┌──────────────────────────────────────────────────────────────────┐
│                        PLATFORM (admin)                          │
│                                                                  │
│  ┌─────────┐ ┌──────────┐ ┌───────┐ ┌─────────┐ ┌───────────┐  │
│  │  Store   │ │ Workflow │ │ Media │ │Lakehouse│ │ Timeseries│  │
│  │ Server   │ │  Server  │ │Server │ │ Server  │ │  Server   │  │
│  └────┬─────┘ └────┬─────┘ └───┬───┘ └────┬────┘ └─────┬─────┘  │
│       │            │           │          │            │          │
│  ┌────┴─────┐ ┌────┴─────┐ ┌──┴───┐ ┌────┴────┐ ┌─────┴─────┐  │
│  │Embedded  │ │Embedded  │ │MinIO │ │Lakekeeper│ │  QuestDB  │  │
│  │PG + RLS  │ │PG + DBOS │ │  S3  │ │+MinIO+PG│ │  binary   │  │
│  └──────────┘ └──────────┘ └──────┘ └─────────┘ └───────────┘  │
│                                                                  │
│  ┌────────────┐  ┌───────────────┐                               │
│  │ Streaming  │  │  Market Data  │                               │
│  │  Server    │  │    Server     │                               │
│  └─────┬──────┘  └──────┬────────┘                               │
│        │                │                                        │
│  ┌─────┴──────┐  ┌──────┴────────┐                               │
│  │ Deephaven  │  │FastAPI+uvicorn│                               │
│  │    JVM     │  │  +simulator   │                               │
│  └────────────┘  └───────────────┘                               │
│                                                                  │
│  Each: start() / stop() / register_alias("demo")                │
└────────────────────────────┬─────────────────────────────────────┘
                             │ alias
┌────────────────────────────▼─────────────────────────────────────┐
│                         USER CODE                                │
│                                                                  │
│  connect("demo")         ─── Object store (Storable API)         │
│  create_engine("demo")   ─── Workflow orchestration              │
│  MediaStore("demo", ai=) ─── Upload, search, RAG                │
│  Lakehouse("demo")       ─── Iceberg SQL via DuckDB              │
│  Timeseries("demo")      ─── Historical tick read/write          │
│  StoreBridge("demo")     ─── PG events → DH ticking tables      │
│  DeephavenClient()       ─── Ticking table queries               │
└────────────────────────────┬─────────────────────────────────────┘
                             │
┌────────────────────────────▼─────────────────────────────────────┐
│                     REACTIVE LAYER                               │
│                                                                  │
│  @computed + @effect   ─── pure OO reactivity on Storable objects│
│  Expression tree       ─── compiles to Python / SQL / Legend Pure│
└──────────────────────────────────────────────────────────────────┘
```

**942 tests**, zero skips, zero failures. All services auto-start in the test harness.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Platform Architecture](#platform-architecture) — XxxServer admin + alias-based user APIs
3. [Object Store](#object-store) — bi-temporal, event-sourced, RLS-secured
4. [Column Registry](#column-registry) — enforced schema governance with AI metadata
5. [State Machines](#state-machines) — declarative lifecycle with three-tier side-effects
6. [Reactive Expressions](#reactive-expressions) — one AST, three compilation targets
7. [Reactive Properties](#reactive-properties) — @computed, @effect, cross-entity aggregation
8. [Workflow Engine](#workflow-engine) — durable orchestration with checkpointed steps
9. [Event Subscriptions](#event-subscriptions) — in-process bus + cross-process PG NOTIFY
10. [Deephaven Bridge](#deephaven-bridge) — stream store events into ticking tables
11. [Streaming & Clients](#streaming--clients) — real-time ticking tables
12. [Market Data Server](#market-data-server) — real-time WebSocket + REST
13. [Historical Time-Series](#historical-time-series) — QuestDB tick archive
14. [Lakehouse](#lakehouse) — Iceberg analytical store
15. [Media Store](#media-store) — unstructured data storage & search
16. [AI](#ai) — embeddings, LLM, RAG, extraction, tool calling
17. [Project Structure](#project-structure)
18. [Demos](#demos)

---

## Quick Start

```bash
pip install -r requirements-store.txt
```

```python
from dataclasses import dataclass
from store import connect, Storable
from reactive import computed, effect

db = connect("trading", user="alice", password="alice_pw")

@dataclass
class Position(Storable):
    symbol: str = ""
    quantity: int = 0
    avg_cost: float = 0.0
    current_price: float = 0.0

    @computed
    def pnl(self):
        return (self.current_price - self.avg_cost) * self.quantity

    @computed
    def market_value(self):
        return self.current_price * self.quantity

    @effect("pnl")
    def on_pnl_change(self, value):
        if value < -5000:
            print(f"ALERT: {self.symbol} PnL = {value}")

# Reactive from creation — pnl and market_value auto-compute
pos = Position(symbol="AAPL", quantity=100, avg_cost=220.0, current_price=230.0)
print(pos.pnl)              # 1000.0
print(pos.market_value)     # 23000.0

pos.current_price = 235.0   # triggers recomputation + effect
print(pos.pnl)              # 1500.0

# Persist — same object, same class
pos.save()                   # CREATED event (version 1)
pos.current_price = 240.0
pos.save()                   # UPDATED event (version 2)

found = Position.find(pos._store_entity_id)
pos.share("bob")             # RLS: bob can now read this position
```

---

## Platform Architecture

Every infrastructure service follows the same three-step pattern:

### 1. Admin starts a server

```python
from store.admin import StoreServer

store = StoreServer(data_dir="data/myapp")
store.start()
store.register_alias("demo")          # publish under a name
store.provision_user("alice", "pw")   # create a user
```

### 2. Users connect by alias

```python
from store import connect

db = connect("demo", user="alice", password="pw")
```

### 3. All 7 servers follow this pattern

| Server | Package | Starts | User API |
|--------|---------|--------|----------|
| `StoreServer` | `store.admin` | Embedded PG + RLS + pgvector | `connect("alias")` |
| `WorkflowServer` | `workflow.admin` | Embedded PG + DBOS | `create_engine("alias")` |
| `StreamingServer` | `streaming.admin` | Deephaven JVM | `DeephavenClient()` |
| `MarketDataServer` | `marketdata.admin` | FastAPI + simulator + QuestDB | REST / WebSocket |
| `TsdbServer` | `timeseries.admin` | QuestDB binary | `Timeseries("alias")` |
| `MediaServer` | `media.admin` | MinIO S3 | `MediaStore("alias", ai=)` |
| `LakehouseServer` | `lakehouse.admin` | Lakekeeper + MinIO + PG | `Lakehouse("alias")` |

Each `XxxServer` has `start()`, `stop()`, and `register_alias()`. Users never see the implementation — no PG connection strings, no MinIO credentials, no JVM args.

---

## Object Store

An **append-only, bi-temporal, event-sourced** object store with embedded PostgreSQL. Nothing is ever overwritten or deleted — every mutation creates an immutable event.

### Core API

```python
from store import connect

db = connect("trading", user="alice", password="alice_pw")

pos.save()                   # CREATED (first time) or UPDATED (subsequent)
pos.delete()                 # DELETED tombstone (soft delete)
pos.transition("CLOSED")     # STATE_CHANGE event

found = Position.find(entity_id)               # read by ID
page  = Position.query(filters={"symbol": "AAPL"})  # paginated query
```

### Bi-Temporal Queries

Every event carries two time dimensions:

| Column | Meaning | Set by |
|--------|---------|--------|
| `tx_time` | When we recorded this fact | System (immutable) |
| `valid_from` | When this fact becomes effective | User (defaults to now) |

```python
trade = Trade.find(entity_id)                                           # current state
versions = trade.history()                                              # full history
old = Trade.as_of(entity_id, tx_time=noon)                              # "what did we know at noon?"
eff = Trade.as_of(entity_id, valid_time=ten_am)                         # "what was effective at 10am?"
snap = Trade.as_of(entity_id, tx_time=noon, valid_time=ten_am)          # both dimensions
```

### Backdated Corrections

```python
trade.price = 151.25
trade.save(valid_from=datetime(2026, 2, 22, 10, 0, tzinfo=timezone.utc))
# event_type → "CORRECTED"
```

### Optimistic Concurrency

```python
trade = Trade.find(entity_id)            # version=3
trade.price = 152.0
trade.save()                             # version 3→4, succeeds

stale = Trade.find(entity_id)            # version=4
# ... someone else updates to version 5 ...
stale.price = 999.0
stale.save()                             # raises VersionConflict
```

### Batch Operations & Pagination

```python
page = Trade.query(filters={"side": "BUY"}, limit=50)
if page.next_cursor:
    page2 = Trade.query(filters={"side": "BUY"}, limit=50, cursor=page.next_cursor)
```

### Row-Level Security & Permissions

Every entity has an `owner`, `readers[]`, and `writers[]`. PostgreSQL RLS policies enforce visibility — users only see entities they own or have been granted access to.

```python
pos.share("bob")                    # bob can read
pos.share("charlie", mode="write")  # charlie can read + write
pos.unshare("bob")                  # revoke
```

### Audit Trail

```python
trail = pos.audit()
for entry in trail:
    print(f"v{entry['version']} {entry['event_type']} by {entry['updated_by']} at {entry['tx_time']}")
# v1 CREATED by alice at 2026-02-22 10:00:00
# v2 STATE_CHANGE by alice at 2026-02-22 10:10:00
# v3 CORRECTED by bob at 2026-02-22 15:00:00
```

### Event Types

| Type | Meaning |
|------|---------|
| `CREATED` | Entity first written |
| `UPDATED` | Data changed |
| `DELETED` | Soft-delete tombstone |
| `STATE_CHANGE` | Lifecycle state transition |
| `CORRECTED` | Backdated correction |

---

## Column Registry

**Governance-first schema catalog** — every field on every `Storable` must be pre-approved. Enforced at class-definition time. No rogue columns.

```python
from store.columns import REGISTRY

REGISTRY.define("symbol", str,
    description="Financial instrument ticker symbol",
    semantic_type="identifier", role="dimension",
    synonyms=["ticker", "instrument", "security"],
    sample_values=["AAPL", "GOOGL", "MSFT"],
    max_length=12, pattern=r"^[A-Z0-9./]+$",
)

REGISTRY.define("price", float,
    description="Trade execution price",
    semantic_type="currency_amount", role="measure",
    unit="USD", min_value=0, format=",.2f",
)

# Prefixed columns — base column controls allowed prefixes
REGISTRY.define("name", str,
    description="Person name", role="dimension",
    allowed_prefixes=["trader", "salesperson", "client"],
)
# trader_name, client_name → valid.  random_name → rejected.
```

### Enforcement

```python
@dataclass
class Trade(Storable):
    symbol: str = ""         # ✅ registered, type matches
    trader_name: str = ""    # ✅ prefix "trader" approved on "name"
    price: float = 0.0       # ✅ registered

@dataclass
class Bad(Storable):
    foo: str = ""            # ❌ RegistryError: not in registry
    price: str = ""          # ❌ RegistryError: type str ≠ float
```

### Column Metadata (7 Categories)

| Category | Fields | Purpose |
|----------|--------|---------|
| **Core** | name, python_type, nullable, default | Type system |
| **Constraints** | enum, min/max, max_length, pattern | Validation |
| **AI / Semantic** | description, synonyms, sample_values, semantic_type | NL queries, LLM tools |
| **OLAP** | role (dim/measure/attr), aggregation, unit | Analytics classification |
| **Display** | display_name, format, category | UI rendering |
| **Governance** | sensitivity, deprecated, tags | Data governance |
| **Cross-Layer** | legend_type, dh_type_override | Legend / Deephaven hints |

Measures **require** `unit`. All columns require `role` and `description`.

### Introspection

```python
REGISTRY.resolve("trader_name")        # → (ColumnDef("name"), "trader")
REGISTRY.entities_with("symbol")       # → [Trade, Order, Signal]
REGISTRY.columns_for(Trade)            # → [ColumnDef("symbol"), ...]
REGISTRY.prefixed_columns("name")      # → ["trader_name", "salesperson_name", "client_name"]
REGISTRY.validate_instance(trade_obj)  # runtime constraint checks
```

### Column Catalog

```
store/columns/
  __init__.py    # REGISTRY global instance (49 columns)
  trading.py     # symbol, price, quantity, side, pnl, order_type, ...
  finance.py     # bid, ask, strike, volatility, notional, isin, ...
  general.py     # name, label, title, color, weight, status, ...
```

---

## State Machines

Declarative lifecycle management with **three tiers of side-effects**:

```python
from store import StateMachine, Transition

class OrderLifecycle(StateMachine):
    initial = "PENDING"
    transitions = [
        Transition("PENDING", "FILLED",
            guard=lambda obj: obj.quantity > 0,                   # callable guard
            action=lambda obj, f, t: create_settlement(obj),    # Tier 1: atomic
            on_exit=lambda obj, f, t: log("left PENDING"),      # Tier 2: fire-and-forget
            on_enter=lambda obj, f, t: notify_risk(obj),        # Tier 2: fire-and-forget
            start_workflow=settlement_workflow),                  # Tier 3: durable
        Transition("PENDING", "CANCELLED",
            allowed_by=["risk_manager"]),
        Transition("FILLED", "SETTLED",
            guard=lambda obj: obj.price > 0),
    ]

Order._state_machine = OrderLifecycle
Order._workflow_engine = engine   # enables start_workflow=
```

### Three-Tier Side-Effects

| Tier | Field | Runs | Guarantee |
|------|-------|------|-----------|
| **1** | `action=` | Inside DB transaction | **Atomic** — rolls back with state change |
| **2** | `on_enter=` / `on_exit=` | After commit | Best-effort, fire-and-forget |
| **3** | `start_workflow=` | After commit, via engine | **Durable** — survives crashes |

Everything declared on the `Transition` — one place, one DSL.

### Guards & Permissions

| Feature | Description |
|---------|------------|
| **guard** | Callable `(obj) → bool`. Raises `GuardFailure` if falsy. |
| **allowed_by** | Usernames permitted to trigger. Raises `TransitionNotPermitted`. |

```python
order.save()                             # state → "PENDING"
order.transition("FILLED")               # guard passes → Tier 1/2/3 fire
order.transition("SETTLED")              # guard passes
order.transition("PENDING")              # raises InvalidTransition
```

---

## Reactive Expressions

A typed expression tree that compiles to **three targets** from a single definition:

| Target | Method | Use case |
|--------|--------|----------|
| **Python** | `expr.eval(ctx)` | Powers @computed evaluation + standalone |
| **PostgreSQL** | `expr.to_sql(col)` | JSONB push-down queries |
| **Legend Pure** | `expr.to_pure(var)` | FINOS Legend Engine integration |

### Operations

| Category | Operations |
|----------|-----------|
| **Arithmetic** | `+`, `-`, `*`, `/`, `%`, `**`, negation, abs |
| **Comparison** | `>`, `<`, `>=`, `<=`, `==`, `!=` |
| **Logical** | `&` (AND), `\|` (OR), `~` (NOT) |
| **Conditionals** | `If(cond, then, else)` → `CASE WHEN` in SQL |
| **Null handling** | `Coalesce([...])`, `IsNull(expr)`, `.is_null()` |
| **Functions** | `sqrt`, `ceil`, `floor`, `round`, `log`, `exp`, `min`, `max` |
| **String** | `.length()`, `.upper()`, `.lower()`, `.contains()`, `.starts_with()`, `.concat()` |

Expressions are fully serializable via `to_json()` / `from_json()`.

### Examples

```python
from reactive.expr import Field, Const, If, Func

pnl = (Field("current_price") - Field("avg_cost")) * Field("quantity")
alert = If(pnl < Const(-5000), Const("STOP_LOSS"), Const("OK"))

intrinsic = If(
    Field("underlying_price") > Field("strike"),
    Field("underlying_price") - Field("strike"),
    Const(0),
)

# Evaluate
pnl.eval({"current_price": 235.0, "avg_cost": 220.0, "quantity": 100})  # 1500.0

# Compile to SQL (for JSONB push-down)
pnl.to_sql("data")   # ((data->>'current_price')::float - ...) * ...

# Compile to Legend Pure
pnl.to_pure("$pos")  # (($pos.current_price - $pos.avg_cost) * $pos.quantity)
```

---

## Reactive Properties

Pure object-oriented reactivity via `@computed` and `@effect` decorators. Objects are inherently reactive from creation — no external graph or wiring needed. Powered internally by [reaktiv](https://github.com/nichochar/reaktiv) (hidden from user code).

The same `Position` class from Quick Start gets all of this for free:

### `@computed` — Reactive Derived Values

```python
# Same Position from Quick Start — pnl and market_value auto-recompute
pos = Position(symbol="AAPL", quantity=100, avg_cost=220.0, current_price=230.0)

print(pos.pnl)              # 1000.0
print(pos.market_value)     # 23000.0

pos.current_price = 235.0   # triggers recomputation of pnl AND market_value
print(pos.pnl)              # 1500.0
print(pos.market_value)     # 23500.0
```

### `@effect` — Automatic Side-Effects

```python
# @effect methods fire whenever their target @computed changes
pos.current_price = 180.0   # pnl = (180 - 220) * 100 = -4000 → no alert
pos.current_price = 160.0   # pnl = (160 - 220) * 100 = -6000 → prints ALERT
```

### Cross-Entity Aggregation

Cross-entity `@computed` references other objects — just another class:

```python
@dataclass
class Portfolio(Storable):
    positions: list = field(default_factory=list)

    @computed
    def total_pnl(self):
        return sum(p.pnl for p in self.positions)

    @computed
    def total_mv(self):
        return sum(p.market_value for p in self.positions)

aapl = Position(symbol="AAPL", quantity=100, avg_cost=220.0, current_price=230.0)
goog = Position(symbol="GOOG", quantity=50, avg_cost=170.0, current_price=180.0)

book = Portfolio(positions=[aapl, goog])
print(book.total_pnl)       # 1500.0

aapl.current_price = 235.0  # propagates through to portfolio
print(book.total_pnl)       # 2000.0

book.positions = [aapl]     # dynamic membership change
```

### Computed Overrides (What-If Scenarios)

Override any `@computed` value — the override ripples through the graph just like a formula change:

```python
pos = Position(symbol="AAPL", quantity=100, avg_cost=220.0, current_price=230.0)
print(pos.pnl)              # 1000.0  (formula)

pos.pnl = 5000.0            # override — downstream dependents see 5000.0
print(pos.pnl)              # 5000.0  (override)

pos.clear_override("pnl")   # revert to formula
print(pos.pnl)              # 1000.0  (formula again)
```

### Batch Updates

```python
pos.batch_update(current_price=240.0, quantity=200)  # single recomputation
```

### SQL/Pure Compilation

Single-entity `@computed` methods are **auto-parsed** into `Expr` trees via AST analysis, enabling compilation to SQL and Legend Pure:

```python
expr = Position.pnl.expr
expr.to_sql("data")    # ((data->>'current_price')::float - (data->>'avg_cost')::float) * ...
expr.to_pure("$pos")   # (($pos.current_price - $pos.avg_cost) * $pos.quantity)
```

Cross-entity methods (referencing other objects) use proxy-based runtime evaluation — they work correctly but don't compile to SQL.

### Auto-Persist Bridge

```python
from reactive.bridge import auto_persist_effect

effects = auto_persist_effect(pos)
# Whenever any @computed value changes → auto-save back to the store
```

---

## Workflow Engine

Durable multi-step workflows with a **backend-swappable** engine — currently DBOS Transact (PostgreSQL-only, zero extra infrastructure). Users never import the backend directly.

```python
from workflow import WorkflowEngine

engine: WorkflowEngine = ...  # injected

def order_to_trade(symbol, qty, price, side):
    oid = engine.step(create_order, symbol, qty, price, side)   # checkpointed
    engine.step(fill_order, oid)                                 # checkpointed
    trade = Trade(symbol=symbol, quantity=qty, price=price, side=side)
    engine.step(trade.save)

handle = engine.workflow(order_to_trade, "AAPL", 100, 150.0, "BUY")
handle.get_status()   # PENDING | RUNNING | SUCCESS | ERROR
handle.get_result()   # blocks until done
```

### Interface

| Method | Description |
|--------|------------|
| `engine.workflow(fn, *args)` | Run as durable workflow (async) |
| `engine.run(fn, *args)` | Run as durable workflow (sync, any args) |
| `engine.step(fn, *args)` | Checkpointed step — exactly-once on recovery |
| `engine.queue(name, fn, *args)` | Enqueue for background execution |
| `engine.sleep(seconds)` | Durable sleep — survives restarts |
| `engine.send(wf_id, topic, value)` | Send notification to a workflow |
| `engine.recv(topic, timeout)` | Wait for notification inside a workflow |

Backend is swappable — implement `WorkflowEngine` for Temporal, AWS Step Functions, or custom.

### Durable Transitions

For checkpointed state transitions inside workflows — uses the active connection:

```python
def settlement_workflow(entity_id):
    order = engine.step(lambda: Order.find(entity_id))
    engine.step(lambda: call_clearing_house(order))
    engine.durable_transition(order, "SETTLED")   # checkpointed, exactly-once
```

---

## Event Subscriptions

Unified `EventListener` — mode is determined by a single parameter:

```python
from store import EventListener

# In-process only (no subscriber_id)
listener = EventListener()
listener.on("Order", lambda e: print(f"{e.event_type} on {e.entity_id}"))
listener.on_entity(entity_id, lambda e: recalc_risk(e))
listener.on_all(lambda e: audit_log(e))

db = connect("trading", user="alice", password="alice_pw", event_bus=listener)

# Durable cross-process (subscriber_id → PG LISTEN/NOTIFY, lazy-started)
with EventListener(subscriber_id="risk_engine") as listener:
    listener.on("Order", handle_order)   # PG listener starts here
    ...
```

With `subscriber_id`, missed events are replayed from the append-only log on reconnect. The PG listener thread starts lazily on the first `.on()` call and is cleaned up by the context manager.

---

## Deephaven Bridge

Streams object store events into Deephaven ticking tables in real time. A **library, not a service** — embed in any process.

```python
from bridge import StoreBridge

bridge = StoreBridge(host=host, port=port, dbname=dbname,
                     user="bridge_user", password="bridge_pw")

bridge.register(Order)
bridge.register(Trade, filter=Field("symbol") == Const("AAPL"))
bridge.start()

orders_raw  = bridge.table(Order)              # append-only event stream
orders_live = orders_raw.last_by("EntityId")   # latest state per entity
```

### Three Patterns: Computed Values → Deephaven

| Pattern | Flow | Use when |
|---------|------|-----------|
| **Persist → bridge** | @effect → `auto_persist_effect` → store → bridge → DH | Calc must be durable |
| **Calc in DH** | Bridge ships raw data → DH `.update(["RiskScore = ..."])` | Dashboards |
| **Direct push** | @effect → DH writer (same process, no PG hop) | Ultra-low-latency |

---

## Streaming & Clients

### Start the streaming server

```python
from streaming.admin import StreamingServer

streaming = StreamingServer(port=10000)
streaming.start()
# Web IDE at http://localhost:10000
```

### Connect from client code

```python
from base_client import DeephavenClient

with DeephavenClient() as client:
    tables = client.list_tables()
    df = client.open_table("prices_live").to_arrow().to_pandas()
    client.run_script('filtered = prices_live.where(["Symbol = `AAPL`"])')
```

Clients connect via `pydeephaven` (lightweight — **no Java needed** on client machines).

### Client Capabilities

| Feature | How |
|---------|-----|
| Read shared tables | `client.open_table("prices_live")` |
| List all tables | `client.list_tables()` |
| Run server-side scripts | `client.run_script("...")` |
| Export to pandas | `table.to_arrow().to_pandas()` |
| Filter / sort | DH table operations via `run_script` |

---

## Market Data Server

Real-time market data hub — WebSocket streaming + REST snapshots via FastAPI. See [MARKETDATA.md](MARKETDATA.md) for full docs.

```python
from marketdata.admin import MarketDataServer

md = MarketDataServer(port=8000)
await md.start()
# REST: http://localhost:8000/md/snapshot
# WS:   ws://localhost:8000/md/subscribe
```

---

## Historical Time-Series

Persists every tick from the TickBus into QuestDB. Backend-agnostic via `TSDBBackend` ABC. See [TIMESERIES.md](TIMESERIES.md) for full docs.

```bash
pip install -e ".[timeseries]"
# Ticks auto-stored when market data server runs with TSDB_ENABLED=1
# REST: GET /md/history/{type}/{symbol}, GET /md/bars/{type}/{symbol}
```

---

## Lakehouse

Iceberg analytical store — all reads and writes via DuckDB SQL (Iceberg extension + REST catalog). Lakekeeper + MinIO S3 storage. See [LAKEHOUSE.md](LAKEHOUSE.md) for full docs.

```bash
pip install -e ".[lakehouse]"
python3 demo_lakehouse.py   # auto-starts MinIO + Lakekeeper
```

```python
from lakehouse import Lakehouse

lh = Lakehouse()

# Query existing tables
lh.query("SELECT type_name, count(*) FROM lakehouse.default.events GROUP BY type_name")

# Ingest data (4 write modes)
lh.ingest("my_signals", df, mode="append")
lh.ingest("daily_snapshot", df, mode="snapshot")
lh.ingest("trades", df, mode="incremental", primary_key="trade_id")
lh.ingest("positions", df, mode="bitemporal", primary_key="entity_id")

# Transform: SQL → new Iceberg table
lh.transform("daily_pnl", "SELECT ... GROUP BY ...", mode="snapshot")
```

All write modes include `_batch_id` and `_batch_ts` for audit. Modes with versioning add `_is_current` for querying latest state.

---

## Media Store

Unstructured data storage & search — documents, images, audio, video. Three search modes: full-text, semantic, and hybrid. See [MEDIA.md](MEDIA.md) for full docs.

```bash
pip install -e ".[media,ai]"
python3 demo_media.py
```

```python
from ai import AI
from media import MediaStore

ai = AI()  # reads GEMINI_API_KEY env var
ms = MediaStore(s3_endpoint="localhost:9002", ai=ai)

# Upload (auto-chunks + embeds when ai= is set)
doc = ms.upload("reports/q1.pdf", title="Q1 Report", tags=["research"])

# Search — three modes
results = ms.search("interest rate swap")           # full-text (keywords)
results = ms.semantic_search("risk transfer")       # vector (meaning)
results = ms.hybrid_search("credit derivatives")    # RRF fusion (best)

data = ms.download(doc)
```

Text extraction: PDF (pymupdf), plain text, markdown, HTML. Documents inherit all Storable features — bi-temporal audit trail, RLS access control, event sourcing.

---

## AI

Embeddings, LLM generation, RAG, structured extraction, and tool calling — all through a single `AI` class. Provider details are internal. See [AI.md](AI.md) for full docs.

```bash
pip install -e ".[ai,media]"
export GEMINI_API_KEY="your-key"
python3 demo_rag.py
```

```python
from ai import AI, Message
from media import MediaStore

ai = AI()                                           # one key, zero provider names
ms = MediaStore(s3_endpoint="localhost:9002", ai=ai) # auto-embeds on upload

# RAG — document-grounded Q&A
result = ai.ask("What are credit default swaps?", documents=ms)
print(result.answer)

# Structured extraction
data = ai.extract("Revenue $12.7B, EPS $8.40", schema={...})

# Direct generation
response = ai.generate("Explain convexity in fixed income.")

# Streaming
for chunk in ai.stream("Explain gamma hedging"):
    print(chunk, end="")

# Tool calling — LLM searches documents autonomously
response = ai.run_tool_loop("Find Basel III docs", tools=ai.search_tools(ms))
```

7 public symbols: `AI`, `Message`, `LLMResponse`, `ToolCall`, `RAGResult`, `ExtractionResult`, `Tool`.

---

## Project Structure

```
py-flow/
├── store/
│   ├── admin.py            # StoreServer (alias: ObjectStoreServer)
│   ├── base.py             # Storable base class + bi-temporal metadata
│   ├── client.py           # StoreClient (event-sourced, bi-temporal)
│   ├── connection.py       # connect() + alias registry
│   ├── server.py           # Embedded PG bootstrap (used by admin.py)
│   ├── schema.py           # DDL: object_events table + RLS policies
│   ├── registry.py         # ColumnDef, ColumnRegistry, RegistryError
│   ├── columns/            # Column catalog (49 columns)
│   ├── state_machine.py    # StateMachine + 3-tier Transitions
│   ├── permissions.py      # Share/unshare entities between users
│   └── subscriptions.py    # EventListener + EventBus
├── reactive/
│   ├── expr.py             # Expression tree (eval / to_sql / to_pure)
│   ├── computed.py         # @computed + @effect decorators
│   └── bridge.py           # Auto-persist effect factory
├── workflow/
│   ├── admin.py            # WorkflowServer (own PG, decoupled)
│   ├── engine.py           # WorkflowEngine ABC + durable_transition()
│   ├── factory.py          # create_engine("alias") factory
│   └── dbos_engine.py      # DBOS-backed implementation (hidden)
├── streaming/
│   ├── admin.py            # StreamingServer (Deephaven JVM)
│   └── _registry.py        # Alias registry
├── bridge/
│   ├── store_bridge.py     # StoreBridge: PG NOTIFY → DH ticking tables
│   └── type_mapping.py     # @dataclass → DH schema + row extraction
├── marketdata/
│   ├── admin.py            # MarketDataServer (FastAPI + uvicorn)
│   ├── server.py           # FastAPI app — REST + WebSocket + TSDB
│   ├── bus.py              # TickBus — async pub/sub
│   ├── models.py           # Tick, FXTick, CurveTick, Subscription
│   └── feeds/simulator.py  # SimulatorFeed — GBM equities + FX
├── timeseries/
│   ├── admin.py            # TsdbServer (QuestDB lifecycle)
│   ├── base.py             # TSDBBackend ABC
│   ├── factory.py          # create_backend() — selects by env
│   ├── consumer.py         # TSDBConsumer — TickBus → TSDB writer
│   ├── models.py           # Bar, HistoryQuery, BarQuery
│   └── backends/questdb/   # QuestDB: manager, writer (ILP), reader (PGWire)
├── lakehouse/
│   ├── admin.py            # LakehouseServer (Lakekeeper + MinIO + PG)
│   ├── catalog.py          # PyIceberg REST catalog
│   ├── query.py            # Lakehouse class: query, ingest, transform
│   ├── sync.py             # Incremental ETL: PG + QuestDB → Iceberg
│   ├── services.py         # Binary lifecycle managers
│   └── models.py           # SyncState, TableInfo
├── media/
│   ├── admin.py            # MediaServer (MinIO lifecycle)
│   ├── store.py            # MediaStore: upload, download, search
│   ├── models.py           # Document Storable + search schema
│   ├── chunking.py         # Sentence-aware text chunking
│   ├── extraction.py       # Text extraction: PDF, text, markdown, HTML
│   └── _minio.py           # MinIO management (hidden)
├── ai/
│   ├── __init__.py         # 7 public symbols: AI, Message, LLMResponse, ...
│   ├── client.py           # AI class — single entry point
│   └── _*.py               # Private: embeddings, LLM, RAG, tools, extraction
├── client/
│   ├── base_client.py      # DeephavenClient wrapper (pydeephaven)
│   ├── quant_client.py     # Watchlists, top movers, volume leaders
│   ├── risk_client.py      # Exposure monitoring, risk scoring
│   └── pm_client.py        # Portfolio summary, P&L snapshots
├── server/
│   └── app.py              # Legacy standalone DH server (demo)
├── tests/
│   ├── conftest.py         # Session fixtures: StreamingServer + MarketDataServer
│   ├── test_store.py       # Bi-temporal + state machine + RLS (134)
│   ├── test_reactive.py    # Expr + @computed + @effect (159)
│   ├── test_bridge.py      # StoreBridge PG → DH round-trip (14)
│   ├── test_server_tables.py  # DH table publishing + ticking (21)
│   ├── test_multi_client.py   # Cross-session table visibility (10)
│   ├── test_client_ops.py     # Client run_script + table ops (20)
│   ├── test_workflow.py       # Durable workflows + steps (16)
│   ├── test_marketdata.py     # Multi-asset bus, WS, REST (59)
│   ├── test_questdb_integration.py  # QuestDB + MarketDataServer (10+)
│   ├── test_lakehouse.py             # Schemas, sync state (34)
│   ├── test_lakehouse_integration.py # Full round-trip PG → Iceberg → DuckDB (10)
│   ├── test_media.py                 # Extraction, Document model (41)
│   ├── test_media_integration.py     # Upload, search, download (18)
│   ├── test_vector_search.py         # pgvector cosine search (12)
│   ├── test_embed_upload.py          # Embed + upload + semantic search (12)
│   ├── test_ai_client.py             # AI generation, RAG, tools (11)
│   └── ...                           # 942 tests total, 0 skips
├── demo_irs.py             # IRS reactive grid → DH ticking tables
├── demo_bridge.py          # Store + @computed → DH ticking tables
├── demo_trading.py         # Trading server: prices + risk → DH tables
├── demo_backtest.py        # TSDB tick collection + MA crossover backtest
├── demo_lakehouse.py       # Iceberg lakehouse end-to-end
├── demo_lakehouse_ingest.py  # Lakehouse ingest/transform all 4 modes
├── demo_media.py           # Media store: upload, extract, search
├── demo_rag.py             # AI + RAG: upload, search, ask, extract, tools
├── demo_three_tiers.py     # Three-tier state machine side-effects
├── API.md                  # Functional API reference
├── AI.md                   # AI architecture docs
├── REACTIVE.md             # Reactive properties design
├── TIMESERIES.md           # Time-series architecture
├── LAKEHOUSE.md            # Lakehouse architecture
├── MEDIA.md                # Media store architecture
└── README.md
```

---

## Demos

### `demo_irs.py` — Interest Rate Swap Reactive Grid

A live IRS trading desk in Deephaven. Yield curve ticks cascade through the reactive graph: **FX spots → curve points → swap pricing → portfolio aggregates** — all recomputing automatically every 1.5 seconds.

```bash
python3 demo_irs.py
# Open http://localhost:10000
```

| Table | Description | Ticking? |
|-------|-------------|----------|
| `fx_live` | FX spot rates: mid, spread (USD/JPY, EUR/USD, GBP/USD) | ✅ |
| `curve_live` | Yield curve points: rate, discount factor (USD + JPY) | ✅ |
| `swap_live` | 6 IRS swaps: NPV, DV01, fixed/float leg PV, PnL status | ✅ |
| `swap_summary` | Aggregate: total NPV, total DV01, avg NPV | ✅ |
| `portfolio_live` | Portfolio breakdown: ALL / USD / JPY | ✅ |

Raw append-only tables also published: `fx_raw`, `curve_raw`, `swap_raw`, `portfolio_raw`.

Domain models use `@computed` with override support — set `swap.npv = 500_000` for what-if, then `swap.clear_override("npv")` to revert to formula.

### `demo_bridge.py` — Store + @computed → Deephaven

Starts embedded PG + Deephaven, bridges store events, and pushes in-memory @computed calcs directly to DH via @effect. Publishes **8 ticking tables**:

```bash
python3 demo_bridge.py
# Open http://localhost:10000
```

| Table | Source | Persisted? |
|-------|--------|------------|
| `orders_raw` / `orders_live` | Store events via bridge | ✅ |
| `trades_raw` / `trades_live` | Store events via bridge | ✅ |
| `portfolio` | DH aggregation on trades | ✅ |
| `risk_calcs` / `risk_live` | @effect → DH writer (Pattern 3) | ❌ |
| `risk_totals` | DH aggregation (total MV + risk) | ❌ |

### `demo_rag.py` — AI + RAG Document Q&A

Upload financial documents, search three ways, ask questions with RAG, extract structured data, and use tool calling — all through `from ai import AI`.

```bash
export GEMINI_API_KEY="your-key"
python3 demo_rag.py
```

| Feature | What it shows |
|---------|--------------|
| Upload + auto-embed | 4 finance docs chunked + embedded on upload |
| Search 3 modes | Full-text, semantic, hybrid (RRF) |
| RAG Q&A | 4 questions answered with document citations |
| Extraction | Earnings report → structured JSON |
| Streaming | Real-time token-by-token output |
| Tool calling | LLM autonomously searches documents |

### `demo_three_tiers.py` — Three-Tier Side-Effects

Exercises all three tiers of state machine side-effects:

```bash
python3 demo_three_tiers.py
```

- **Tier 1**: Action commits atomically with state change
- **Tier 1 rollback**: Action failure rolls back state change
- **Tier 2**: Hook failure is swallowed, state is safe
- **Tier 3**: Durable workflow dispatched and tracked to completion

---

## Roadmap

See **[ROADMAP.md](ROADMAP.md)** for the full platform roadmap — 9 phases, 57 steps across AI (vector search, LLM, agents/eval), scheduling/DAG, service management, data lineage, data quality, and dashboard/datacube.

Track progress there: 🔲 planned → 🔧 in progress → ✅ done.

---

## License

Apache 2.0 — see [LICENSE](LICENSE)
