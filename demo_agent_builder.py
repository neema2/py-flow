#!/usr/bin/env python3
"""
Demo: Agent Builder Framework — Custom Multi-Agent Finance Team
=================================================================

Shows how to BUILD your own agents using the Agent/AgentTeam framework.
Three custom specialist agents collaborate using REAL platform services:

  1. **Market Data Agent** — reads live prices from reactive Position graph
     (fed by MarketData server WebSocket → @computed market_value, unrealized_pnl),
     queries OHLCV bars from TSDB, computes realized vol from tick data
  2. **Risk Agent** — reads @computed VaR, stress tests, and concentration
     from reactive PortfolioRisk Storable (auto-recomputes when prices tick),
     queries portfolio trajectory from lakehouse analytics (DuckDB)
  3. **Research Agent** — searches uploaded documents via MediaStore + RAG

Platform services auto-started:
  - MarketData server (SimulatorFeed → live GBM equity ticks + FX)
  - StoreServer (embedded PG for MediaStore + conversations)
  - ObjectStore (MinIO for document blobs)
  - Reactive graph: Position + PortfolioRisk Storables with @computed
  - TsdbServer (QuestDB — records all ticks, OHLCV bars via MarketData REST)
  - LakehouseServer (Lakekeeper + Iceberg + MinIO + DuckDB analytics)
  - @effect on PortfolioRisk — throttled ingest to Lakehouse on VaR change

The reactive graph does ALL the math. Agents just observe the results.

Usage::

    export GEMINI_API_KEY="your-key"
    python3 demo_agent_builder.py
"""

import asyncio
import json
import logging
import math
import os
import tempfile
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from media import MediaStore as _MediaStoreType

import textwrap
import threading
import time as _time
from datetime import datetime, timezone

import httpx

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)-5s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)

from dataclasses import dataclass, field

from ai import AI, Agent, tool
from ai.eval import EvalCase, EvalRunner
from ai.team import AgentTeam
from reactive.agg import group_by, rank_by
from reactive.computed import computed, effect
from store import Storable


def section(title: str):
    print()
    print(f"{'─' * 70}")
    print(f"  {title}")
    print(f"{'─' * 70}")


# ── Reactive Domain Models ───────────────────────────────────────────────
#
# Position: one per holding, @computed market_value / unrealized_pnl / var
# PortfolioRisk: cross-entity @computed reading all positions — VaR, HHI,
#   sector weights, stress test PnL. Recomputes when ANY price ticks.
#

SECTORS = {
    "AAPL": "Technology", "NVDA": "Technology", "MSFT": "Technology",
    "GOOGL": "Technology", "AMZN": "Technology",
    "TSLA": "Consumer Discretionary",
}

IV = {
    "AAPL": 0.22, "NVDA": 0.55, "MSFT": 0.20,
    "TSLA": 0.62, "GOOGL": 0.25, "AMZN": 0.30,
}

BETA = {
    "AAPL": 1.18, "NVDA": 1.72, "MSFT": 1.10,
    "TSLA": 2.05, "GOOGL": 1.15, "AMZN": 1.25,
}

STRESS_SHOCKS = {
    "rate_hike":       {"AAPL": -0.08, "NVDA": -0.12, "MSFT": -0.07, "TSLA": -0.15, "GOOGL": -0.06, "AMZN": -0.09},
    "tech_crash":      {"AAPL": -0.25, "NVDA": -0.40, "MSFT": -0.22, "TSLA": -0.35, "GOOGL": -0.20, "AMZN": -0.28},
    "recession":       {"AAPL": -0.15, "NVDA": -0.20, "MSFT": -0.18, "TSLA": -0.30, "GOOGL": -0.15, "AMZN": -0.22},
    "inflation_spike": {"AAPL": -0.10, "NVDA": -0.15, "MSFT": -0.08, "TSLA": -0.18, "GOOGL": -0.07, "AMZN": -0.12},
}


@dataclass
class Position(Storable):
    """A single portfolio holding. Price is fed from MarketData WS.

    @computed properties recompute automatically when price changes.
    """
    __key__ = "symbol"

    symbol: str = ""
    quantity: int = 0
    avg_cost: float = 0.0
    price: float = 0.0
    sector: str = ""
    implied_vol: float = 0.25
    beta: float = 1.0

    @computed
    def market_value(self):
        return self.price * self.quantity

    @computed
    def unrealized_pnl(self):
        return (self.price - self.avg_cost) * self.quantity

    @computed
    def weight(self):
        """Weight within portfolio (set by PortfolioRisk after total is known)."""
        return 0.0  # Placeholder — real weight computed by portfolio

    @computed
    def var_1d_95(self):
        """Individual 1-day 95% parametric VaR."""
        daily_vol = self.implied_vol / math.sqrt(252)
        return self.market_value * daily_vol * 1.645

    @computed
    def var_1d_99(self):
        """Individual 1-day 99% parametric VaR."""
        daily_vol = self.implied_vol / math.sqrt(252)
        return self.market_value * daily_vol * 2.326


@dataclass
class PortfolioRisk(Storable):
    """Aggregate portfolio risk. Cross-entity @computed reads all positions.

    Recomputes VaR, stress tests, and concentration when ANY position ticks.
    """
    __key__ = "name"

    name: str = "main"
    positions: list = field(default_factory=list)

    @computed
    def total_value(self):
        return sum(p.market_value for p in self.positions)

    @computed
    def total_unrealized_pnl(self):
        return sum(p.unrealized_pnl for p in self.positions)

    @computed
    def portfolio_var_95(self):
        """Diversified 1-day 95% VaR (~15% diversification benefit)."""
        sum_sq = sum(p.var_1d_95 ** 2 for p in self.positions)
        return math.sqrt(sum_sq) * 0.85

    @computed
    def portfolio_var_99(self):
        """Diversified 1-day 99% VaR."""
        sum_sq = sum(p.var_1d_99 ** 2 for p in self.positions)
        return math.sqrt(sum_sq) * 0.85

    @computed
    def var_pct_95(self):
        tv = self.total_value
        return (self.portfolio_var_95 / tv * 100) if tv else 0

    @computed
    def var_pct_99(self):
        tv = self.total_value
        return (self.portfolio_var_99 / tv * 100) if tv else 0

    @computed
    def hhi(self):
        """Herfindahl-Hirschman Index for concentration."""
        tv = self.total_value
        if tv == 0:
            return 0
        return sum((p.market_value / tv) ** 2 for p in self.positions)

    @computed
    def concentration_level(self):
        if self.hhi > 0.25:
            return "concentrated"
        if self.hhi > 0.15:
            return "moderate"
        return "diversified"

    @computed
    def sector_weights(self):
        return group_by([(p.sector, p.market_value) for p in self.positions],
                        normalize=True)

    @computed
    def top_risk_contributors(self):
        return rank_by([(p.symbol, p.var_1d_95) for p in self.positions],
                       as_pct=True)

    _last_ingest = 0.0
    _snapshot_count = 0

    @effect("portfolio_var_95")
    def on_var_change(self, value):
        now = _time.time()
        if now - PortfolioRisk._last_ingest > 5 and _lakehouse is not None:
            PortfolioRisk._last_ingest = now
            try:
                _lakehouse.ingest("portfolio_snapshots", [{
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "total_value": round(self.total_value, 2),
                    "var_95": round(self.portfolio_var_95, 2),
                    "var_99": round(self.portfolio_var_99, 2),
                    "var_pct_95": round(self.var_pct_95, 4),
                    "hhi": round(self.hhi, 4),
                    "concentration_level": self.concentration_level,
                    "unrealized_pnl": round(self.total_unrealized_pnl, 2),
                    "position_count": len(self.positions),
                }], mode="append")
                PortfolioRisk._snapshot_count += 1
            except Exception:
                pass

    def stress_test(self, scenario: str):
        """Apply a stress scenario to current live positions."""
        shocks = STRESS_SHOCKS.get(scenario)
        if not shocks:
            return {"error": f"Unknown scenario: {scenario}"}
        total_pnl = 0
        details = []
        for p in self.positions:
            shock_pct = shocks.get(p.symbol, 0)
            loss = p.market_value * shock_pct
            total_pnl += loss
            details.append({
                "symbol": p.symbol,
                "live_price": round(p.price, 2),
                "market_value": round(p.market_value, 2),
                "shock_pct": round(shock_pct * 100, 1),
                "pnl": round(loss, 2),
            })
        tv = self.total_value
        return {
            "scenario": scenario,
            "total_pnl": round(total_pnl, 2),
            "total_pnl_pct": round(total_pnl / tv * 100, 2) if tv else 0,
            "details": sorted(details, key=lambda x: x["pnl"]),
        }


# ── Global reactive graph instances ─────────────────────────────────────

_positions: dict[str, Position] = {}
_portfolio_risk: PortfolioRisk = None  # type: ignore[assignment]
MD_BASE_URL = None
_media_store: _MediaStoreType | None = None
_lakehouse = None      # Lakehouse client for analytics


# ── Agent Tools (read from reactive graph) ───────────────────────────────


@tool
def get_portfolio_positions() -> str:
    """Get all portfolio positions with live prices and computed values from the reactive graph.

    Returns live price, market value, unrealized P&L, VaR, beta, and sector
    for each position — all reactively computed from the latest WS tick.
    """
    result = []
    for p in _positions.values():
        result.append({
            "symbol": p.symbol,
            "quantity": p.quantity,
            "avg_cost": p.avg_cost,
            "live_price": round(p.price, 2),
            "market_value": round(p.market_value, 2),
            "unrealized_pnl": round(p.unrealized_pnl, 2),
            "var_1d_95": round(p.var_1d_95, 2),
            "beta": p.beta,
            "sector": p.sector,
            "source": "reactive_graph_live_ws_prices",
        })
    return json.dumps(sorted(result, key=lambda x: -x["market_value"]))


@tool
def get_live_quote(symbol: str) -> str:
    """Get a single position's live data from the reactive graph.

    Args:
        symbol: Ticker symbol (e.g. AAPL, NVDA).
    """
    p = _positions.get(symbol)
    if not p:
        return json.dumps({"error": f"No position for {symbol}"})
    return json.dumps({
        "symbol": p.symbol,
        "live_price": round(p.price, 2),
        "quantity": p.quantity,
        "market_value": round(p.market_value, 2),
        "unrealized_pnl": round(p.unrealized_pnl, 2),
        "var_1d_95": round(p.var_1d_95, 2),
        "var_1d_99": round(p.var_1d_99, 2),
        "beta": p.beta,
        "implied_vol": p.implied_vol,
        "sector": p.sector,
        "source": "reactive_graph_live_ws_prices",
    })


@tool
def get_portfolio_risk() -> str:
    """Get aggregate portfolio risk metrics from the reactive PortfolioRisk graph.

    Returns total value, VaR (95%/99%), concentration (HHI), sector weights,
    and top risk contributors — all reactively computed from live prices.
    """
    pr = _portfolio_risk
    return json.dumps({
        "total_value": round(pr.total_value, 2),
        "total_unrealized_pnl": round(pr.total_unrealized_pnl, 2),
        "var_1d_95": round(pr.portfolio_var_95, 2),
        "var_1d_95_pct": round(pr.var_pct_95, 2),
        "var_1d_99": round(pr.portfolio_var_99, 2),
        "var_1d_99_pct": round(pr.var_pct_99, 2),
        "hhi": round(pr.hhi, 4),
        "concentration": pr.concentration_level,
        "sector_weights": pr.sector_weights,
        "top_risk_contributors": pr.top_risk_contributors,
        "position_count": len(pr.positions),
        "source": "reactive_portfolio_risk_graph",
    })


@tool
def run_stress_test(scenario: str) -> str:
    """Run a stress test on the portfolio using the reactive graph's live positions.

    Args:
        scenario: One of 'rate_hike', 'tech_crash', 'recession', 'inflation_spike'.
    """
    result = _portfolio_risk.stress_test(scenario)
    result["source"] = "reactive_graph_stress_test"
    return json.dumps(result)


@tool
def query_price_history(symbol: str, interval: str = "1m") -> str:
    """Get OHLCV price bars for a symbol from the TSDB via MarketData REST API.

    Args:
        symbol: Ticker symbol (e.g. AAPL, NVDA).
        interval: Bar interval — '1s', '5s', '15s', '1m', '5m', '15m', '1h'.
    """
    try:
        resp = httpx.get(
            f"{MD_BASE_URL}/md/bars/equity/{symbol}",
            params={"interval": interval},
            timeout=5.0,
        )
        if resp.status_code == 503:
            return json.dumps({"error": "TSDB not available on MarketData server"})
        bars = resp.json()
        if not bars:
            return json.dumps({"symbol": symbol, "bars": [], "message": "No bars yet"})
        return json.dumps({
            "symbol": symbol, "interval": interval,
            "bar_count": len(bars),
            "bars": bars[-20:],
            "source": "tsdb_questdb_via_marketdata_rest",
        }, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_realized_vol(symbol: str) -> str:
    """Compute realized volatility from TSDB tick data and compare to implied vol.

    Args:
        symbol: Ticker symbol (e.g. AAPL, NVDA).
    """
    try:
        resp = httpx.get(
            f"{MD_BASE_URL}/md/history/equity/{symbol}",
            params={"limit": 500},
            timeout=5.0,
        )
        if resp.status_code == 503:
            return json.dumps({"error": "TSDB not available on MarketData server"})
        ticks = resp.json()
        prices = [t["price"] for t in ticks if "price" in t]
        if len(prices) < 10:
            return json.dumps({"symbol": symbol, "error": "Not enough ticks for vol calc", "tick_count": len(prices)})
        # Log returns
        returns = [math.log(prices[i] / prices[i-1]) for i in range(1, len(prices))]
        realized_vol_tick = (sum(r**2 for r in returns) / len(returns)) ** 0.5
        # Annualize (assume ~1 tick/sec, 252 trading days, 6.5h/day)
        ticks_per_year = 252 * 6.5 * 3600
        realized_vol_ann = realized_vol_tick * math.sqrt(ticks_per_year)
        pos = _positions.get(symbol)
        implied = pos.implied_vol if pos else None
        return json.dumps({
            "symbol": symbol,
            "tick_count": len(prices),
            "realized_vol_annualized": round(realized_vol_ann, 4),
            "implied_vol": implied,
            "vol_spread": round(realized_vol_ann - implied, 4) if implied else None,
            "interpretation": (
                "realized > implied → market underpricing risk"
                if implied and realized_vol_ann > implied
                else "realized < implied → market overpricing risk"
            ),
            "source": "tsdb_questdb_via_marketdata_rest",
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def query_analytics(sql: str) -> str:
    """Run a SQL query on the Lakehouse (Iceberg + DuckDB) over portfolio snapshots.

    Available tables (use fully qualified names):
      - lakehouse.default.portfolio_snapshots: timestamp, total_value, var_95,
        var_99, var_pct_95, hhi, concentration_level, unrealized_pnl, position_count

    Example queries:
      - SELECT * FROM lakehouse.default.portfolio_snapshots ORDER BY timestamp
      - SELECT max(var_95) - min(var_95) as var_range FROM lakehouse.default.portfolio_snapshots
      - SELECT timestamp, total_value, var_pct_95 FROM lakehouse.default.portfolio_snapshots

    Args:
        sql: DuckDB SQL query string.
    """
    if not _lakehouse:
        return json.dumps({"error": "Lakehouse not available"})
    try:  # type: ignore[unreachable]
        data = _lakehouse.query(sql)
        return json.dumps({
            "row_count": len(data),
            "data": data[:50],
            "source": "lakehouse_iceberg_duckdb",
        }, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_fx_rates() -> str:
    """Get live FX rates from the MarketData server."""
    try:
        resp = httpx.get(f"{MD_BASE_URL}/md/snapshot/fx", timeout=5.0)
        if resp.status_code == 200:
            return json.dumps({"source": "live_market_data_server", "fx": resp.json()})
        return json.dumps({"error": f"HTTP {resp.status_code}"})
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def search_research(query: str) -> str:
    """Search internal research documents using hybrid semantic + keyword search.

    Args:
        query: Search query (e.g. 'NVDA earnings risk', 'Fed rate outlook').
    """
    try:
        assert _media_store is not None, "MediaStore not initialized"
        results = _media_store.hybrid_search(query, limit=3)
        return json.dumps(results, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def ask_research(question: str) -> str:
    """Ask a question answered by internal research documents using RAG.

    Args:
        question: Natural language question (e.g. 'What are the risks to NVDA?').
    """
    try:
        ai = AI()
        result = ai.ask(question, documents=_media_store)
        return json.dumps({
            "answer": result.answer,
            "sources": [s.get("title", s.get("filename", "")) for s in result.sources],
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


# ── Research Documents ───────────────────────────────────────────────────

RESEARCH_DOCS = [
    {
        "filename": "fed_outlook.txt",
        "title": "Fed Policy Outlook — March 2024",
        "tags": ["fed", "rates", "macro"],
        "content": textwrap.dedent("""\
            Federal Reserve Policy Outlook — March 2024

            The Federal Reserve is expected to hold rates steady at 5.25-5.50%
            through Q2 2024. Market pricing suggests 3 cuts in H2 2024, though
            persistent services inflation may delay the first cut to September.

            Key implications for portfolios:
            - Duration-sensitive assets remain vulnerable if cuts are delayed
            - Recommendation: underweight long-duration tech until rate path clarifies
            - Financials (JPM, GS) benefit from higher-for-longer via net interest income
            - VIX at 16.8 makes put protection relatively cheap

            The 10-year yield at 4.35% creates competition for equity risk premium.
            US Dollar Index (DXY) at 104.2 supports multinational earnings headwinds.
        """),
    },
    {
        "filename": "nvda_analysis.txt",
        "title": "NVIDIA — AI Semiconductor Cycle Analysis",
        "tags": ["nvda", "semiconductors", "ai", "tech"],
        "content": textwrap.dedent("""\
            NVIDIA Corporation (NVDA) — Deep Dive Analysis

            Revenue growth driven by data center demand (+265% YoY). The Blackwell
            architecture launch in H2 2024 positions NVDA for continued dominance
            in AI training infrastructure.

            Key metrics:
            - Forward P/E: 35x (stretched but justified by TAM expansion)
            - Data center revenue: $18.4B (84% of total)
            - Gross margin: 76% (expanding with software/services mix)
            - Customer concentration risk: top 4 hyperscalers = 40% of revenue

            Risks: export controls (China = 20% of pre-ban revenue), AMD MI300X
            competition, potential customer vertical integration (Google TPU,
            Amazon Trainium).

            Recommendation: Hold with trailing stop at -15%. Position size should
            not exceed 20% of portfolio due to concentration risk.
        """),
    },
    {
        "filename": "tsla_competitive.txt",
        "title": "Tesla — EV Market & Competitive Position",
        "tags": ["tsla", "ev", "auto"],
        "content": textwrap.dedent("""\
            Tesla Inc (TSLA) — Competitive Position Analysis

            Global EV penetration reached 18% in 2024. Tesla's US market share
            has declined from 62% to 51% as legacy OEMs ramp production.

            Margin pressure:
            - Auto gross margin: 17.6% (down from 25.9% in 2022)
            - Price cuts across Model 3/Y to defend volume
            - Chinese competitors (BYD, NIO) gaining share in Europe

            Optionality plays:
            - FSD (Full Self-Driving): regulatory path unclear, monetization TBD
            - Energy storage: Megapack revenue growing 40% YoY
            - Robotaxi: announced for 2025, execution risk very high

            Risk/reward unfavorable above $200. The stock trades on narrative
            momentum rather than fundamentals. Beta of 2.05 makes it a significant
            portfolio risk contributor.

            Recommendation: Trim to <5% of portfolio. Use options collar to
            protect remaining position (buy 180P, sell 250C).
        """),
    },
    {
        "filename": "hedging_strategies.txt",
        "title": "Portfolio Hedging Strategies — Current Environment",
        "tags": ["hedging", "options", "risk", "portfolio"],
        "content": textwrap.dedent("""\
            Portfolio Hedging Strategies — Q1 2024

            With VIX at 16.8, put protection is relatively cheap. Recommended hedges
            for a tech-heavy portfolio:

            1. Index Protection:
               - Buy SPY 500P 3-month puts for tail risk protection
               - Cost: ~1.5% of notional, provides -15% downside coverage
               - Alternative: QQQ puts for tech-specific hedge

            2. Single-Name Collars:
               - NVDA: Buy 800P / Sell 1000C — caps upside but protects large gains
               - TSLA: Buy 180P / Sell 250C — limits downside with margin risk

            3. Sector Rotation (no cost):
               - Reduce Technology from 60%+ to 40-45% of portfolio
               - Add Healthcare (XLV), Energy (XLE), or Utilities (XLU)
               - Reduces correlation risk without options premium

            4. Volatility Strategies:
               - VIX call spreads as crash insurance
               - Cost of carry: ~0.5% annualized for 15/25 call spread

            Implementation priority: Start with sector rotation (free), then
            add index puts, then single-name collars for largest positions.
        """),
    },
    {
        "filename": "banking_review.txt",
        "title": "US Banking Sector — Q1 2024 Review",
        "tags": ["banks", "financials", "jpm"],
        "content": textwrap.dedent("""\
            US Banking Sector Review — Q1 2024

            JPMorgan Chase (JPM) delivered a strong quarter with 15% ROE.
            Net interest income remains elevated due to higher-for-longer rates.

            Key sector themes:
            - Commercial real estate (CRE) exposure: 8% of JPM loan book
            - CRE vacancy rates at 18.6% (historic highs in office segment)
            - Consumer credit quality stable but subprime delinquencies ticking up
            - Regional bank stress has subsided post-SVB

            JPM-specific strengths:
            - Diversified revenue: investment banking recovery (+12% YoY)
            - Trading revenue: strong fixed income performance
            - Capital position: CET1 ratio at 15.0% (well above 11.5% minimum)

            Recommendation: Core holding. Add on dips below $190. Well-positioned
            for both soft landing and mild recession scenarios. Defensive within
            financials due to diversification and capital strength.
        """),
    },
]


# ── WebSocket Price Consumer ─────────────────────────────────────────────

def _start_ws_consumer(md_port: int):
    """Background thread: consume equity ticks from MarketData WS, update positions.

    Ticks are written to QuestDB automatically by the MarketData server's
    built-in TSDBConsumer — no manual writes needed here.
    """

    async def _consume():
        import websockets

        url = f"ws://localhost:{md_port}/md/subscribe"
        while True:
            try:
                async with websockets.connect(url) as ws:
                    syms = list(_positions.keys())
                    await ws.send(json.dumps({"types": ["equity"], "symbols": syms}))
                    async for msg_str in ws:
                        tick = json.loads(msg_str)
                        sym = tick.get("symbol")
                        pos = _positions.get(sym)
                        if pos and "price" in tick:
                            pos.batch_update(price=tick["price"])
            except Exception:
                await asyncio.sleep(1)

    asyncio.run(_consume())


# ── Build the Team ───────────────────────────────────────────────────────


def build_team():
    market_agent = Agent(
        tools=[get_portfolio_positions, get_live_quote, get_fx_rates,
               query_price_history, get_realized_vol],
        system_prompt=(
            "You are a Market Data Specialist. Your data comes from: "
            "(1) a reactive graph backed by live WebSocket prices, "
            "(2) QuestDB TSDB recording all ticks — query OHLCV bars and compute realized vol "
            "via MarketData REST API. Report positions, trends, and vol analysis."
        ),
        name="market_data",
    )

    risk_agent = Agent(
        tools=[get_portfolio_risk, run_stress_test, query_analytics],
        system_prompt=(
            "You are a Risk Analyst. Your tools read from: "
            "(1) reactive PortfolioRisk graph (@computed VaR, HHI, sector weights), "
            "(2) Lakehouse analytics (Iceberg + DuckDB SQL over portfolio_snapshots). "
            "Run stress tests, query how VaR has changed over the session, "
            "and recommend specific hedges. Use fully qualified table names: "
            "lakehouse.default.portfolio_snapshots (cols: timestamp, total_value, "
            "var_95, var_99, var_pct_95, hhi, concentration_level, unrealized_pnl, position_count)."
        ),
        name="risk_analyst",
    )

    research_agent = Agent(
        tools=[search_research, ask_research],
        system_prompt=(
            "You are a Research Analyst with access to internal research documents via "
            "hybrid semantic search (pgvector + tsvector) and RAG Q&A backed by real "
            "Gemini embeddings. Search for relevant research on portfolio holdings "
            "and macro themes. Synthesize findings into actionable insights."
        ),
        name="research",
    )

    team = AgentTeam(
        agents={"market_data": market_agent, "risk_analyst": risk_agent, "research": research_agent},
    )

    return team, market_agent, risk_agent, research_agent


# ── Main ─────────────────────────────────────────────────────────────────


HOLDINGS = {
    "AAPL":  {"qty": 500,  "avg_cost": 178.50},
    "NVDA":  {"qty": 200,  "avg_cost": 450.00},
    "MSFT":  {"qty": 300,  "avg_cost": 380.00},
    "TSLA":  {"qty": 150,  "avg_cost": 242.00},
    "GOOGL": {"qty": 250,  "avg_cost": 155.00},
    "AMZN":  {"qty": 100,  "avg_cost": 185.00},
}


def main():
    global MD_BASE_URL, _media_store, _portfolio_risk, _lakehouse

    print("=" * 70)
    print("  MULTI-AGENT FINANCE TEAM — Full Platform Integration")
    print("  Reactive Graph + MarketData + QuestDB + Lakehouse + RAG")
    print("=" * 70)

    if not os.environ.get("GEMINI_API_KEY"):
        print("\n  ERROR: Set GEMINI_API_KEY environment variable.")
        return

    # ── Start Infrastructure ─────────────────────────────────────────
    section("Starting Platform Infrastructure")

    # 1. StoreServer (PG)
    from store.admin import StoreServer
    store = StoreServer(data_dir=tempfile.mkdtemp(prefix="demo_agent_store_"))
    store.start()
    store.register_alias("demo-agent")
    store.provision_user("agent_user", "agent_pw")
    print("  ✓ StoreServer (embedded PG)")

    # Bootstrap media + conversation schemas
    from media.models import bootstrap_chunks_schema, bootstrap_search_schema
    admin = store.admin_conn()
    bootstrap_search_schema(admin, embedding_dim=768)
    admin.close()
    admin = store.admin_conn()
    bootstrap_chunks_schema(admin, embedding_dim=768)
    admin.close()
    # Conversations are now Storable — stored in object_events, no separate table needed
    print("  ✓ Schemas bootstrapped (media search, chunks)")

    # 2. ObjectStore (S3/MinIO)
    from media.admin import MediaServer
    media_srv = MediaServer(
        data_dir=tempfile.mkdtemp(prefix="demo_agent_s3_"),
        api_port=9072, console_port=9073,
    )
    asyncio.run(media_srv.start())
    media_srv.register_alias("demo-agent")
    print("  ✓ ObjectStore (MinIO on port 9072)")

    # 3. TsdbServer (QuestDB) — start BEFORE MarketData so it auto-connects
    from timeseries.admin import TsdbServer
    tmpdir_tsdb = tempfile.mkdtemp(prefix="demo_agent_tsdb_")
    tsdb_server = TsdbServer(data_dir=tmpdir_tsdb)
    asyncio.run(tsdb_server.start())
    tsdb_server.register_alias("demo-agent")
    print(f"  ✓ TsdbServer (QuestDB: HTTP={tsdb_server.http_port}, ILP={tsdb_server.ilp_port}, PG={tsdb_server.pg_port})")

    # 4. MarketData Server — detects running QuestDB, TSDBConsumer writes ticks automatically
    from marketdata.admin import MarketDataServer
    md_server = MarketDataServer(port=8765)
    asyncio.run(md_server.start())
    MD_BASE_URL = f"http://localhost:{md_server.port}"
    print(f"  ✓ MarketData Server on port {md_server.port} (SimulatorFeed → QuestDB)")

    # 5. LakehouseServer (EmbeddedPG + Lakekeeper + MinIO)
    from lakehouse.admin import LakehouseServer
    tmpdir_lh = tempfile.mkdtemp(prefix="lh_", dir="/tmp")
    lh_server = LakehouseServer(
        data_dir=tmpdir_lh,
        s3_api_port=9082, s3_console_port=9083,
    )
    asyncio.run(lh_server.start())
    lh_server.register_alias("demo-agent")
    print("  ✓ LakehouseServer (Lakekeeper + Iceberg + MinIO on 9082)")

    # Create Lakehouse client + portfolio_snapshots table
    from lakehouse import Lakehouse
    _lakehouse = Lakehouse("demo-agent")
    _lakehouse.ingest("portfolio_snapshots", [{
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_value": 0.0, "var_95": 0.0, "var_99": 0.0,
        "var_pct_95": 0.0, "hhi": 0.0, "concentration_level": "unknown",
        "unrealized_pnl": 0.0, "position_count": 0,
    }], mode="append")
    print("  ✓ Lakehouse client connected, portfolio_snapshots table created")

    # ── Build Reactive Graph ─────────────────────────────────────────
    section("Building reactive graph (Position + PortfolioRisk)")

    # Seed initial prices from REST snapshot
    _time.sleep(2)
    try:
        resp = httpx.get(f"{MD_BASE_URL}/md/snapshot/equity", timeout=5.0)
        live_prices = resp.json() if resp.status_code == 200 else {}
    except Exception:
        live_prices = {}

    for sym, h in HOLDINGS.items():
        initial_price = live_prices.get(sym, {}).get("price", h["avg_cost"])
        pos = Position(
            symbol=sym,
            quantity=h["qty"],  # type: ignore[arg-type]
            avg_cost=h["avg_cost"],
            price=initial_price,
            sector=SECTORS.get(sym, "Unknown"),
            implied_vol=IV.get(sym, 0.25),
            beta=BETA.get(sym, 1.0),
        )
        _positions[sym] = pos
        print(f"    {sym}: qty={h['qty']:>4d}  cost=${h['avg_cost']:>7.2f}  "
              f"live=${initial_price:>7.2f}  mv=${pos.market_value:>10,.2f}")

    _portfolio_risk = PortfolioRisk(
        name="main",
        positions=list(_positions.values()),
    )
    print(f"\n  Portfolio value: ${_portfolio_risk.total_value:,.2f}")
    print(f"  VaR (95% 1d):   ${_portfolio_risk.portfolio_var_95:,.2f} ({_portfolio_risk.var_pct_95:.2f}%)")
    print(f"  Concentration:  {_portfolio_risk.concentration_level} (HHI={_portfolio_risk.hhi:.4f})")
    print(f"  Sectors:        {_portfolio_risk.sector_weights}")

    # ── Start WS Consumer ───────────────────────────────────────────
    section("Starting live data pipeline")

    # WS consumer: prices → reactive graph (which triggers @effect → Lakehouse)
    # Ticks → QuestDB is handled automatically by MarketData server's TSDBConsumer
    ws_thread = threading.Thread(
        target=_start_ws_consumer, args=(md_server.port,),
        daemon=True, name="agent-ws-consumer",
    )
    ws_thread.start()
    print("  ✓ WebSocket consumer started (prices → reactive graph)")
    print("  ✓ @effect(portfolio_var_95) → throttled Lakehouse ingest on VaR change")

    # ── Connect user + create MediaStore ─────────────────────────────
    section("Setting up AI + MediaStore")

    from store import connect
    conn = connect("demo-agent", user="agent_user", password="agent_pw")

    ai = AI()
    from media import MediaStore
    _media_store = MediaStore("demo-agent", ai=ai)
    print("  ✓ AI (Gemini) + MediaStore with vector embeddings")

    # ── Upload Research Documents ────────────────────────────────────
    section("Uploading research documents (real MinIO + PG + Gemini embeddings)")

    for doc_info in RESEARCH_DOCS:
        doc = _media_store.upload(
            doc_info["content"].encode(),  # type: ignore[attr-defined]
            filename=doc_info["filename"],  # type: ignore[arg-type]
            title=doc_info["title"],  # type: ignore[arg-type]
            tags=doc_info["tags"],  # type: ignore[arg-type]
        )
        print(f"  ✓ {doc.title} ({doc.size} bytes)")

    print(f"\n  {len(RESEARCH_DOCS)} documents uploaded, chunked, and embedded")

    # Verify search
    results = _media_store.hybrid_search("NVDA risk", limit=2)
    print(f"  ✓ Hybrid search verified ({len(results)} results for 'NVDA risk')")

    # ── Let graph tick for a moment ──────────────────────────────────
    print("\n  ⏳ Letting prices tick for 10 seconds (building QuestDB history + Lakehouse snapshots)...")
    _time.sleep(10)
    print(f"  Portfolio value now: ${_portfolio_risk.total_value:,.2f}")
    print(f"  Lakehouse snapshots: {PortfolioRisk._snapshot_count}")

    # ── Part 1: Team Analysis ────────────────────────────────────────
    section("Running Multi-Agent Team Analysis")
    print("  3 agents: market_data → risk_analyst → research")
    print("  All reading from reactive graph + MediaStore RAG")
    print()

    team, _market_agent, _risk_agent, research_agent = build_team()

    result = team.run(
        "Analyze my portfolio's current risk exposure. "
        "Get all positions with live reactive prices, read the portfolio risk metrics "
        "(VaR, concentration, sector weights), run stress tests for tech_crash and recession, "
        "search our research docs for insights on the biggest risk contributors, "
        "and recommend specific hedges."
    )

    print("\n" + "=" * 70)
    print("  TEAM ANALYSIS RESULT")
    print("=" * 70)
    print(textwrap.fill(result.content, width=70))
    print("=" * 70)

    print(f"\n📊 Delegation Log ({len(result.delegation_log)} steps):")
    for i, step in enumerate(result.delegation_log, 1):
        print(f"  {i}. {step.agent_name:15s} ({step.duration_ms:.0f}ms)")
        print(f"     Task: {step.prompt[:65]}...")

    # ── Part 2: Research Deep-Dive (RAG) ─────────────────────────────
    section("Research Agent — RAG Deep-Dive")

    research_agent.reset()
    r = research_agent.run(
        "What hedging strategies does our research recommend for a tech-heavy portfolio? "
        "Include specific option structures and cost estimates."
    )
    print(f"\n{textwrap.fill(r.content, width=70)}")
    print(f"\n  Tools: {[s.action.name for s in r.steps]}")

    # ── Part 3: Eval ─────────────────────────────────────────────────
    section("Eval: Agent Quality Check (reactive graph + RAG)")

    eval_agent = Agent(
        tools=[
            get_portfolio_positions, get_live_quote, get_portfolio_risk,
            get_fx_rates, run_stress_test,
            search_research, ask_research,
            query_price_history, get_realized_vol, query_analytics,
        ],
        system_prompt=(
            "You are a financial analyst with access to: "
            "(1) reactive portfolio graph (live prices, VaR, stress tests), "
            "(2) QuestDB TSDB (OHLCV bars, realized vol via MarketData REST), "
            "(3) Lakehouse analytics (Iceberg + DuckDB SQL over lakehouse.default.portfolio_snapshots), "
            "(4) research RAG (hybrid search + Q&A)."
        ),
    )

    cases = [
        EvalCase(
            input="What are my current portfolio positions with live prices?",
            expected_tools=["get_portfolio_positions"],
            tags=["market_data"],
        ),
        EvalCase(
            input="What is the portfolio VaR and concentration risk?",
            expected_tools=["get_portfolio_risk"],
            tags=["risk"],
        ),
        EvalCase(
            input="Run a tech crash stress test",
            expected_tools=["run_stress_test"],
            tags=["risk"],
        ),
        EvalCase(
            input="Show me NVDA's recent price bars from the time-series database",
            expected_tools=["query_price_history"],
            tags=["tsdb"],
        ),
        EvalCase(
            input="How has portfolio VaR changed over this session? Query the analytics.",
            expected_tools=["query_analytics"],
            tags=["lakehouse"],
        ),
        EvalCase(
            input="What does our research say about Tesla's competitive position?",
            expected_tools=["search_research"],
            expected_output_contains=["Tesla"],
            tags=["research"],
        ),
    ]

    runner = EvalRunner(agent=eval_agent)
    runner.run(cases)
    runner.summary()

    # ── Cleanup ──────────────────────────────────────────────────────
    section("Cleanup")
    print(f"  Lakehouse snapshots: {PortfolioRisk._snapshot_count}")
    _media_store.close()
    if _lakehouse:
        _lakehouse.close()
    conn.close()
    asyncio.run(md_server.stop())
    asyncio.run(lh_server.stop())
    asyncio.run(tsdb_server.stop())
    store.stop()
    print("  ✓ All services stopped")
    print("\n  Done! 🎯")


if __name__ == "__main__":
    main()
