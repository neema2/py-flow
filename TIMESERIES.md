# Time-Series Database (TSDB) Integration

Backend-agnostic historical market data storage for ticks and OHLCV bars.

---

## Architecture

```
TickBus ──► TSDBConsumer ──► TSDBBackend (ABC)
                                  │
                          ┌───────┴───────┐
                          │   QuestDB     │  ◄── first concrete backend
                          │  (ILP + SQL)  │
                          └───────────────┘
```

- **`TSDBBackend`** — abstract base class defining lifecycle, write, and read methods
- **`TSDBConsumer`** — TickBus subscriber that routes every tick to the backend
- **Factory** — `create_backend(name)` instantiates a backend by name (default: `questdb`)
- **No backend leakage** — server code only imports from `timeseries`, never from `timeseries.backends.*`

---

## Package Layout

```
timeseries/
├── __init__.py          # Public exports: TSDBBackend, TSDBConsumer, create_backend, Bar, ...
├── base.py              # TSDBBackend ABC
├── models.py            # Bar, HistoryQuery, BarQuery (Pydantic)
├── factory.py           # create_backend()
├── consumer.py          # TSDBConsumer (TickBus → backend)
└── backends/
    └── questdb/
        ├── __init__.py  # QuestDBBackend facade
        ├── manager.py   # Binary lifecycle (download, start, health, stop)
        ├── schema.py    # DDL for tick tables
        ├── writer.py    # ILP ingestion (high-throughput writes)
        └── reader.py    # PGWire queries (SQL reads)
```

---

## Quick Start

The TSDB is automatically started with the market data server:

```bash
# Default: QuestDB backend, enabled
python -m marketdata.server

# Disable TSDB (e.g. for tests)
TSDB_ENABLED=0 python -m marketdata.server

# Switch backend (when alternatives are implemented)
TSDB_BACKEND=questdb python -m marketdata.server
```

### Install dependencies

```bash
pip install -e ".[timeseries]"
```

---

## REST Endpoints

All historical endpoints return **503** if the TSDB backend is unavailable.

### Raw Tick History

```
GET /md/history/{type}/{symbol}?start=...&end=...&limit=1000
```

| Param    | Type     | Default    | Description                |
|----------|----------|------------|----------------------------|
| `type`   | path     | required   | `equity`, `fx`, or `curve` |
| `symbol` | path     | required   | e.g. `AAPL`, `EUR/USD`    |
| `start`  | query    | 2000-01-01 | ISO 8601 datetime          |
| `end`    | query    | 2099-12-31 | ISO 8601 datetime          |
| `limit`  | query    | 1000       | Max rows (1–10000)         |

### OHLCV Bars

```
GET /md/bars/{type}/{symbol}?interval=1m&start=...&end=...
```

| Param      | Type  | Default  | Description                              |
|------------|-------|----------|------------------------------------------|
| `interval` | query | `1m`     | `1s`, `5s`, `1m`, `5m`, `15m`, `1h`, `4h`, `1d` |

### Bars by Type (all symbols)

```
GET /md/bars/{type}?interval=1h&start=...&end=...
```

Returns a dict keyed by symbol, each containing an array of bars.

### Latest from TSDB

```
GET /md/latest/{type}?symbol=AAPL
```

Returns the most recent tick per symbol from the time-series store (not the in-memory bus snapshot).

---

## TSDBBackend ABC

```python
from timeseries import TSDBBackend

class MyBackend(TSDBBackend):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def write_tick(self, msg) -> None: ...
    async def flush(self) -> None: ...
    def get_ticks(self, msg_type, symbol, start, end, limit=1000) -> list[dict]: ...
    def get_bars(self, msg_type, symbol, interval, start, end) -> list[Bar]: ...
    def get_latest(self, msg_type, symbol=None) -> list[dict]: ...
```

### Adding a new backend

1. Create `timeseries/backends/<name>/` with an `__init__.py` exporting a class that subclasses `TSDBBackend`
2. Register it in `timeseries/factory.py`
3. Set `TSDB_BACKEND=<name>` at runtime

---

## QuestDB Backend Details

### Protocols

| Protocol | Port | Purpose              |
|----------|------|----------------------|
| HTTP     | 9000 | Health check, Web UI |
| ILP      | 9009 | High-throughput tick ingestion |
| PGWire   | 8812 | SQL queries (psycopg2) |

### Tables

| Table          | Symbol Column | Price Column | Partitioned |
|----------------|---------------|--------------|-------------|
| `equity_ticks` | `symbol`      | `price`      | By DAY      |
| `fx_ticks`     | `pair`        | `mid`        | By DAY      |
| `curve_ticks`  | `label`       | `rate`       | By DAY      |

### Writer

- Uses QuestDB ILP (InfluxDB Line Protocol) for nanosecond-precision ingestion
- Auto-flushes every 1000 rows or 5 seconds
- Maps `Tick`, `FXTick`, `CurveTick` to their respective tables

### Reader

- Uses PGWire (psycopg2) for standard SQL queries
- Generates `SAMPLE BY` queries for OHLCV bar aggregation
- Supports `LATEST ON` for per-symbol latest ticks

### Manager

- Auto-downloads QuestDB binary on first run
- Stores data in `data/questdb/`
- Graceful shutdown via `/exec?query=shutdown` HTTP endpoint

---

## Models

### Bar

```python
from timeseries import Bar

bar = Bar(
    symbol="AAPL",
    interval="1m",
    open=150.0, high=151.0, low=149.5, close=150.5,
    volume=10000,
    trade_count=42,
    timestamp=datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc),
)
```

### HistoryQuery / BarQuery

```python
from timeseries import HistoryQuery, BarQuery

hq = HistoryQuery(type="equity", symbol="AAPL", limit=500)
bq = BarQuery(type="fx", symbol="EUR/USD", interval="5m")
```

---

## Testing

```bash
# Pure Python tests (no QuestDB needed)
TSDB_ENABLED=0 python -m pytest tests/test_timeseries.py -v

# All tests
TSDB_ENABLED=0 python -m pytest tests/ -v
```

24 tests cover: ABC contract, factory, models, consumer routing, table/column mappings, and schema DDL.
