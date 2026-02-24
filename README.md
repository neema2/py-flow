# Deephaven Real-Time Trading Platform

A governance-first real-time trading platform built on [Deephaven.io](https://deephaven.io), featuring a **bi-temporal event-sourced object store**, a **reactive expression language** that compiles to Python, SQL, and Legend Pure, and **durable workflow orchestration** — all backed by embedded PostgreSQL with zero external infrastructure.

```
┌──────────────────────────────────────────────────────────────┐
│  DEEPHAVEN SERVER  (server/)                    Port 10000   │
│  Embedded JVM • DynamicTableWriter • Web IDE • Market sim    │
└──────────────────────┬───────────────────────────────────────┘
                       │ gRPC
          ┌────────────┼────────────────┐
    ┌─────▼────┐ ┌─────▼─────┐  ┌──────▼─────┐
    │  Quant   │ │   Risk    │  │     PM     │
    │  Client  │ │  Client   │  │   Client   │
    └──────────┘ └───────────┘  └────────────┘

┌──────────────────────────────────────────────────────────────┐
│  OBJECT STORE  (store/)                                      │
│  Embedded PG • RLS • Bi-temporal event sourcing • Append-only│
│                                                              │
│  ┌─────────────────┐  ┌──────────────┐  ┌────────────────┐  │
│  │ Column Registry  │  │State Machines│  │   Permissions  │  │
│  │ Enforced schema  │  │ 3-tier hooks │  │ RLS + sharing  │  │
│  │ AI/OLAP metadata │  │ guard/action │  │ owner/readers  │  │
│  └─────────────────┘  └──────────────┘  └────────────────┘  │
└──────────────────────┬───────────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────────┐
│  REACTIVE LAYER  (reactive/)                                 │
│  Expression tree → Python eval / PG SQL / Legend Pure         │
│  @computed + @effect: pure OO reactivity on Storable objects  │
└──────────────────────┬───────────────────────────────────────┘
                       │
          ┌────────────┼────────────────┐
    ┌─────▼────┐ ┌─────▼─────┐  ┌──────▼─────┐
    │ Workflow  │ │   Store   │  │ Deephaven  │
    │  Engine   │ │  Bridge   │  │  Bridge    │
    │ (DBOS)    │ │ auto-save │  │ PG→DH tick │
    └──────────┘ └───────────┘  └────────────┘
```

**403+ tests** across 6 test suites. Zero external dependencies beyond Python + PostgreSQL.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Object Store](#object-store) — bi-temporal, event-sourced, RLS-secured
3. [Column Registry](#column-registry) — enforced schema governance with AI metadata
4. [State Machines](#state-machines) — declarative lifecycle with three-tier side-effects
5. [Reactive Expressions](#reactive-expressions) — one AST, three compilation targets
6. [Reactive Properties](#reactive-properties) — @computed, @effect, cross-entity aggregation
7. [Workflow Engine](#workflow-engine) — durable orchestration with checkpointed steps
8. [Event Subscriptions](#event-subscriptions) — in-process bus + cross-process PG NOTIFY
9. [Deephaven Bridge](#deephaven-bridge) — stream store events into ticking tables
10. [Deephaven Server & Clients](#deephaven-server--clients) — real-time market data
11. [Project Structure](#project-structure)
12. [Demos](#demos)

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
trade = client.read(Trade, entity_id)                                   # current state
versions = client.history(Trade, entity_id)                             # full history
old = client.as_of(Trade, entity_id, tx_time=noon)                      # "what did we know at noon?"
eff = client.as_of(Trade, entity_id, valid_time=ten_am)                 # "what was effective at 10am?"
snap = client.as_of(Trade, entity_id, tx_time=noon, valid_time=ten_am)  # both dimensions
```

### Backdated Corrections

```python
trade.price = 151.25
client.update(trade, valid_from=datetime(2026, 2, 22, 10, 0, tzinfo=timezone.utc))
# event_type → "CORRECTED"
```

### Optimistic Concurrency

```python
trade = client.read(Trade, entity_id)   # version=3
trade.price = 152.0
client.update(trade)                    # version 3→4, succeeds

stale = client.read(Trade, entity_id)   # version=4
# ... someone else updates to version 5 ...
stale.price = 999.0
client.update(stale)                    # raises VersionConflict
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
from store.state_machine import StateMachine, Transition
from reactive.expr import Field, Const

class OrderLifecycle(StateMachine):
    initial = "PENDING"
    transitions = [
        Transition("PENDING", "FILLED",
            guard=Field("quantity") > Const(0),
            action=lambda obj, f, t: create_settlement(obj),    # Tier 1: atomic
            on_exit=lambda obj, f, t: log("left PENDING"),      # Tier 2: fire-and-forget
            on_enter=lambda obj, f, t: notify_risk(obj),        # Tier 2: fire-and-forget
            start_workflow=settlement_workflow),                  # Tier 3: durable
        Transition("PENDING", "CANCELLED",
            allowed_by=["risk_manager"]),
        Transition("FILLED", "SETTLED",
            guard=Field("price") > Const(0)),
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
| **guard** | `Expr` evaluated against object data. Raises `GuardFailure` if falsy. |
| **allowed_by** | Usernames permitted to trigger. Raises `TransitionNotPermitted`. |

```python
client.write(order)                     # state → "PENDING"
client.transition(order, "FILLED")      # guard passes → Tier 1/2/3 fire
client.transition(order, "SETTLED")     # guard passes
client.transition(order, "PENDING")     # raises InvalidTransition
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
from reactive import Field, Const, If, Func

pnl = (Field("current_price") - Field("avg_cost")) * Field("quantity")
alert = If(pnl < Const(-5000), Const("STOP_LOSS"), Const("OK"))

intrinsic = If(
    Field("underlying_price") > Field("strike"),
    Field("underlying_price") - Field("strike"),
    Const(0),
)
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

effects = auto_persist_effect(pos, store_client)
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
    engine.step(client.write, Trade(symbol=symbol, quantity=qty, price=price, side=side))

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

### WorkflowDispatcher (Durable Transitions)

For multi-step state progressions inside workflows:

```python
from workflow.dispatcher import WorkflowDispatcher

dispatcher = WorkflowDispatcher(engine, client)

def settlement_workflow(entity_id):
    order = engine.step(lambda: client.read(Order, entity_id))
    engine.step(lambda: call_clearing_house(order))
    dispatcher.durable_transition(order, "SETTLED")   # checkpointed, exactly-once
```

---

## Event Subscriptions

Two-tier notification system — zero external infrastructure:

```python
from store.subscriptions import EventBus, SubscriptionListener

# Tier 1: In-process — synchronous callbacks after DB writes
bus = EventBus()
bus.on("Order", lambda e: print(f"{e.event_type} on {e.entity_id}"))
bus.on_entity(entity_id, lambda e: recalc_risk(e))
bus.on_all(lambda e: audit_log(e))

client = StoreClient(user="alice", ..., event_bus=bus)

# Tier 2: Cross-process — PG LISTEN/NOTIFY with durable catch-up
listener = SubscriptionListener(
    event_bus=bus,
    host=host, port=port, dbname=dbname,
    user="bob", password="bob_pw",
    subscriber_id="risk_engine",   # persists checkpoint for crash recovery
)
listener.start()
```

With `subscriber_id`, missed events are replayed from the append-only log on reconnect.

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

## Deephaven Server & Clients

### Server

```bash
pip install -r requirements-server.txt
cd server && python3 -i app.py
# Web IDE at http://localhost:10000
```

### Clients

```bash
pip install -r requirements-client.txt
cd client
python3 quant_client.py      # Watchlists, top movers, volume leaders
python3 risk_client.py       # Large exposures, risk scoring
python3 pm_client.py         # P&L snapshots, position sizing
```

Clients connect via `pydeephaven` (lightweight — **no Java needed** on client machines).

### Published Tables

| Table | Description |
|-------|-------------|
| `prices_raw` | Append-only price ticks |
| `prices_live` | Latest price per symbol (ticking) |
| `risk_raw` | Append-only risk ticks |
| `risk_live` | Latest risk per symbol (ticking) |
| `portfolio_summary` | Aggregated portfolio metrics |

### Client Capabilities

| Feature | How |
|---------|-----|
| Read shared tables | `session.open_table("prices_live")` |
| Filter / sort | `table.where(...)`, `table.sort(...)` |
| Create server-side views | `session.run_script("...")` |
| Publish tables | `session.bind_table(name, table)` |
| Export to pandas | `table.to_arrow().to_pandas()` |
| Subscribe to ticks | `pydeephaven-ticking` listener API |

---

## Project Structure

```
windsurf-project/
├── server/
│   ├── app.py              # Deephaven server + data engine
│   ├── market_data.py      # Market data simulation
│   ├── risk_engine.py      # Black-Scholes Greeks calculator
│   └── start_server.sh     # Launch script
├── client/
│   ├── base_client.py      # Reusable connection helper
│   ├── quant_client.py     # Quant: filtered views, derived tables
│   ├── risk_client.py      # Risk: exposure monitoring, alerts
│   └── pm_client.py        # PM: portfolio summary, P&L snapshots
├── store/
│   ├── base.py             # Storable base class + bi-temporal metadata
│   ├── registry.py         # ColumnDef, ColumnRegistry, RegistryError
│   ├── columns/            # Column catalog (49 columns)
│   │   ├── __init__.py     # REGISTRY global instance
│   │   ├── trading.py      # symbol, price, quantity, side, pnl, ...
│   │   ├── finance.py      # bid, ask, strike, volatility, notional, ...
│   │   └── general.py      # name, label, title, status, weight, ...
│   ├── models.py           # Domain models: Trade, Order, Signal
│   ├── server.py           # Embedded PG server bootstrap
│   ├── client.py           # StoreClient (event-sourced, bi-temporal)
│   ├── schema.py           # DDL: object_events table + RLS policies
│   ├── state_machine.py    # StateMachine + 3-tier Transitions
│   ├── permissions.py      # Share/unshare entities between users
│   └── subscriptions.py    # EventBus + SubscriptionListener + checkpoints
├── reactive/
│   ├── expr.py             # Expression tree (eval / to_sql / to_pure)
│   ├── computed.py         # @computed + @effect decorators, AST→Expr parser
│   └── bridge.py           # Auto-persist effect factory
├── workflow/
│   ├── engine.py           # WorkflowEngine ABC + WorkflowHandle
│   ├── dbos_engine.py      # DBOS-backed implementation (hidden)
│   └── dispatcher.py       # WorkflowDispatcher: durable transitions
├── bridge/
│   ├── store_bridge.py     # StoreBridge: PG NOTIFY → DH ticking tables
│   └── type_mapping.py     # @dataclass → DH schema + row extraction
├── tests/                  # 403+ tests
│   ├── test_store.py       # Bi-temporal + state machine + RLS + 3-tier (134)
│   ├── test_reactive.py    # Expr + @computed + @effect + cross-entity (131)
│   ├── test_reactive_finance.py  # Finance domain @computed tests (49)
│   ├── test_workflow.py    # Workflow engine (16)
│   ├── test_bridge.py      # DH ↔ Store bridge, real DH + PG (17)
│   └── test_registry.py    # Column registry enforcement (56)
├── demo_bridge.py          # End-to-end: store + @computed → DH ticking tables
├── REACTIVE.md             # Reactive properties design document
├── demo_three_tiers.py     # Three-tier state machine side-effects
├── requirements-server.txt
├── requirements-client.txt
├── requirements-store.txt  # reaktiv, psycopg2-binary, pgserver, dbos
└── README.md
```

---

## Demos

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

## License

MIT
