# Functional API Reference

Complete public API organized by feature area — **15 symbols** across 4 packages.

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

## 7. Deephaven Bridge

**Stream store events into Deephaven ticking tables.**

```python
from bridge import StoreBridge
```

| Symbol | Kind | Description |
|--------|------|-------------|
| `StoreBridge` | class | Streams PG NOTIFY events into Deephaven DynamicTableWriter tables. |

### StoreBridge methods

| Method | Description |
|--------|-------------|
| `StoreBridge(host, port, dbname, user, password, subscriber_id)` | Create bridge with PG connection params. |
| `.register(storable_cls, *, filter=None, columns=None, writer=None)` | Register a Storable type to be bridged. |
| `.table(storable_cls)` | Get the raw append-only DH table for a type. |
| `.start()` | Begin listening and bridging events. |
| `.stop()` | Stop and clean up. |

---

## 8. Time-Series Database

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

## Summary

| Package | Symbols | Count |
|---------|---------|-------|
| **store** | `Storable`, `connect`, `StateMachine`, `Transition`, `EventListener`, `ChangeEvent`, `VersionConflict`, `InvalidTransition`, `GuardFailure`, `TransitionNotPermitted` | 10 |
| **reactive** | `computed`, `effect` | 2 |
| **workflow** | `WorkflowEngine`, `WorkflowStatus` | 2 |
| **bridge** | `StoreBridge` | 1 |
| **timeseries** | `TSDBBackend`, `TSDBConsumer`, `create_backend`, `Bar`, `HistoryQuery`, `BarQuery` | 6 |
| **Total** | | **21** |
