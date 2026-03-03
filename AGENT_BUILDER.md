# Multi-Agent Finance Team — Full Platform Demo

Three AI agents collaborate on live portfolio risk analysis, powered by the
full py-flow stack: **reactive graph → QuestDB TSDB → Lakehouse (Iceberg) → RAG**.

```
SimulatorFeed → TickBus → TSDBConsumer → QuestDB
                   ↓                        ↓ (MarketData REST)
             WebSocket consumer        Agent tools (bars, vol)
                   ↓
           Reactive Graph (Position + PortfolioRisk)
                   ↓
         @effect(portfolio_var_95)
                   ↓ (throttled, every 5s)
         Lakehouse.ingest() → Iceberg → DuckDB
                                          ↓
                                  Agent tools (SQL analytics)
```

## What It Does

1. **Market Data Agent** — reads live prices from the reactive `Position` graph,
   queries OHLCV bars from QuestDB via MarketData REST, computes realized vol
   from tick history and compares to implied vol.

2. **Risk Agent** — reads `@computed` VaR, HHI, stress tests from the reactive
   `PortfolioRisk` graph. Queries portfolio trajectory from the Lakehouse
   (Iceberg tables via DuckDB SQL) to show how VaR has changed over the session.

3. **Research Agent** — searches uploaded research documents via MediaStore
   hybrid search (pgvector + tsvector) and RAG Q&A with Gemini embeddings.

An `AgentTeam` routes a complex query across all three, then synthesizes
a final briefing with specific hedge recommendations.

## Services Started (all automatic)

| Service | What | Port(s) |
|---------|------|---------|
| **StoreServer** | Embedded PG (pgserver) for MediaStore + conversations | auto |
| **MediaServer** | MinIO for document blobs | 9072 |
| **TsdbServer** | QuestDB — records all ticks, serves OHLCV bars | 9000/9009/8812 |
| **MarketDataServer** | SimulatorFeed → TickBus → TSDBConsumer → QuestDB | 8765 |
| **LakehouseServer** | EmbeddedPG + Lakekeeper + MinIO → Iceberg + DuckDB | 5488/8181/9082 |

## Reactive Graph

Two `Storable` classes with `@computed` properties that auto-recompute when
any input changes:

- **`Position`** — `market_value`, `unrealized_pnl`, `var_1d_95`, `var_1d_99`
  (all recompute when `price` ticks via WebSocket)
- **`PortfolioRisk`** — cross-entity `@computed` reading all positions:
  `total_value`, `portfolio_var_95`, `portfolio_var_99`, `hhi`,
  `concentration_level`, `sector_weights`, `top_risk_contributors`

### @effect → Lakehouse

`PortfolioRisk` has an `@effect("portfolio_var_95")` that fires whenever VaR
changes. With a 5-second throttle, it calls `Lakehouse.ingest()` to append a
portfolio snapshot to the Iceberg `portfolio_snapshots` table:

```python
@effect("portfolio_var_95")
def on_var_change(self, value):
    now = _time.time()
    if now - PortfolioRisk._last_ingest > 5 and _lakehouse is not None:
        PortfolioRisk._last_ingest = now
        _lakehouse.ingest("portfolio_snapshots", [{
            "timestamp": ..., "total_value": ..., "var_95": ...,
            "hhi": ..., "concentration_level": ..., ...
        }], mode="append")
```

No background threads. No timers. The reactive graph drives it.

## Agent Tools

### Market Data (via reactive graph + MarketData REST)
- `get_portfolio_positions()` — all positions with live prices
- `get_live_quote(symbol)` — single position detail
- `get_fx_rates()` — live FX from MarketData server
- `query_price_history(symbol, interval)` — OHLCV bars from QuestDB via `GET /md/bars/equity/{symbol}`
- `get_realized_vol(symbol)` — realized vol from tick history via `GET /md/history/equity/{symbol}`

### Risk (via reactive graph + Lakehouse)
- `get_portfolio_risk()` — VaR, HHI, sector weights from `@computed`
- `run_stress_test(scenario)` — tech_crash, recession, rate_hike, inflation_spike
- `query_analytics(sql)` — DuckDB SQL over Iceberg `portfolio_snapshots` via `Lakehouse.query()`

### Research (via MediaStore RAG)
- `search_research(query)` — hybrid semantic + keyword search
- `ask_research(question)` — RAG Q&A with Gemini

## Data Flow — No Hacks

Every data path uses the real public API:

- **Ticks → QuestDB**: MarketData server's built-in `TSDBConsumer` writes every
  tick to QuestDB automatically. The demo doesn't manually write ticks.
- **Portfolio → Lakehouse**: `@effect` on the reactive graph calls
  `Lakehouse.ingest()` — the real Iceberg write path through Lakekeeper + MinIO.
- **Agent reads TSDB**: Tools call `GET /md/bars/` and `GET /md/history/` —
  the MarketData server's REST API backed by QuestDB.
- **Agent reads Lakehouse**: `Lakehouse("demo-agent").query()` — DuckDB SQL
  over Iceberg tables via the REST catalog.

## Running

```bash
export GEMINI_API_KEY="your-key"
python3 demo_agent_builder.py
```

Requires Java 17-21 for QuestDB (auto-detected on macOS via
`/usr/libexec/java_home`). First run downloads QuestDB + Lakekeeper binaries
(~30s each, cached for subsequent runs).

## Output

The demo runs three phases:

1. **Team Analysis** — all three agents collaborate on a full portfolio risk
   briefing with hedge recommendations
2. **RAG Deep-Dive** — research agent answers a detailed hedging question
3. **Eval** — 6 test cases verify correct tool selection (typically 5/6 pass)

Typical output shows:
- 5 services started automatically
- 6 positions with live prices from reactive graph
- ~24 Lakehouse snapshots ingested via `@effect`
- Full team analysis with delegation log
- 83% eval accuracy across market_data, risk, tsdb, lakehouse, and research tags
