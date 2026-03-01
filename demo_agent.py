#!/usr/bin/env python3
"""
Demo: Multi-Agent Finance Team
===============================

Three specialist agents collaborate on a portfolio risk analysis:

  1. **Market Data Agent** — fetches live prices, vol, and greeks
  2. **Risk Analyst Agent** — computes VaR, stress tests, concentration risk
  3. **Research Agent** — searches docs and provides macro/regulatory context

An AgentTeam routes a complex query like "Analyze my portfolio's risk exposure
and recommend hedges" across all three, then synthesizes a final briefing.

Usage::

    export GEMINI_API_KEY="your-key"
    python3 demo_agent.py
"""

import json
import math
import random
import textwrap

from ai import Agent, tool
from ai.team import AgentTeam
from ai.eval import EvalRunner, EvalCase


# ── Market Data Tools ────────────────────────────────────────────────────

PORTFOLIO = {
    "AAPL":  {"qty": 500,  "avg_cost": 178.50},
    "NVDA":  {"qty": 200,  "avg_cost": 450.00},
    "JPM":   {"qty": 300,  "avg_cost": 185.00},
    "TSLA":  {"qty": 150,  "avg_cost": 242.00},
    "SPY":   {"qty": 100,  "avg_cost": 510.00},
}

# Simulated market data (realistic-ish)
MARKET = {
    "AAPL":  {"price": 227.50, "change_pct": 1.2,  "iv": 0.22, "beta": 1.18, "sector": "Technology"},
    "NVDA":  {"price": 875.30, "change_pct": 3.5,  "iv": 0.55, "beta": 1.72, "sector": "Technology"},
    "JPM":   {"price": 198.40, "change_pct": -0.3, "iv": 0.18, "beta": 1.05, "sector": "Financials"},
    "TSLA":  {"price": 195.60, "change_pct": -2.1, "iv": 0.62, "beta": 2.05, "sector": "Consumer Discretionary"},
    "SPY":   {"price": 525.80, "change_pct": 0.4,  "iv": 0.14, "beta": 1.00, "sector": "Index"},
}

MACRO = {
    "fed_rate": 5.50,
    "cpi_yoy": 3.1,
    "vix": 16.8,
    "us_10y": 4.35,
    "dxy": 104.2,
}


@tool
def get_portfolio() -> str:
    """Get the current portfolio holdings with quantities and average costs."""
    return json.dumps(PORTFOLIO)


@tool
def get_quote(symbol: str) -> str:
    """Get the current market quote for a stock.

    Args:
        symbol: Ticker symbol (e.g. AAPL, NVDA).
    """
    if symbol not in MARKET:
        return json.dumps({"error": f"Unknown symbol: {symbol}"})
    data = MARKET[symbol]
    pos = PORTFOLIO.get(symbol, {})
    result = {
        "symbol": symbol,
        "price": data["price"],
        "change_pct": data["change_pct"],
        "implied_vol": data["iv"],
        "beta": data["beta"],
        "sector": data["sector"],
    }
    if pos:
        result["position_qty"] = pos["qty"]
        result["avg_cost"] = pos["avg_cost"]
        result["market_value"] = round(data["price"] * pos["qty"], 2)
        result["unrealized_pnl"] = round((data["price"] - pos["avg_cost"]) * pos["qty"], 2)
    return json.dumps(result)


@tool
def get_macro_data() -> str:
    """Get current macroeconomic indicators (Fed rate, CPI, VIX, 10Y yield, DXY)."""
    return json.dumps(MACRO)


# ── Risk Analysis Tools ──────────────────────────────────────────────────


@tool
def compute_portfolio_var(confidence: float = 0.95, horizon_days: int = 1) -> str:
    """Compute portfolio Value-at-Risk using parametric method.

    Args:
        confidence: Confidence level (default: 0.95).
        horizon_days: Time horizon in days (default: 1).
    """
    from math import sqrt, log

    # Z-scores for common confidence levels
    z_map = {0.90: 1.282, 0.95: 1.645, 0.99: 2.326}
    z = z_map.get(confidence, 1.645)

    total_value = 0
    weighted_var = 0
    positions = []

    for sym, pos in PORTFOLIO.items():
        mkt = MARKET[sym]
        mv = mkt["price"] * pos["qty"]
        total_value += mv
        # Daily VaR = MV * IV * z / sqrt(252) * sqrt(horizon)
        daily_vol = mkt["iv"] / sqrt(252)
        var = mv * daily_vol * z * sqrt(horizon_days)
        weighted_var += var ** 2  # Sum of squares for diversified VaR
        positions.append({
            "symbol": sym,
            "market_value": round(mv, 2),
            "weight_pct": 0,  # filled below
            "individual_var": round(var, 2),
            "contribution_pct": 0,
        })

    portfolio_var = sqrt(weighted_var) * 0.85  # ~15% diversification benefit
    for p in positions:
        p["weight_pct"] = round(p["market_value"] / total_value * 100, 1)
        p["contribution_pct"] = round((p["individual_var"] ** 2 / weighted_var) * 100, 1)

    return json.dumps({
        "portfolio_value": round(total_value, 2),
        "var": round(portfolio_var, 2),
        "var_pct": round(portfolio_var / total_value * 100, 2),
        "confidence": confidence,
        "horizon_days": horizon_days,
        "positions": positions,
    })


@tool
def run_stress_test(scenario: str) -> str:
    """Run a stress test scenario on the portfolio.

    Args:
        scenario: One of 'rate_hike', 'tech_crash', 'recession', 'inflation_spike'.
    """
    shocks = {
        "rate_hike":       {"AAPL": -0.08, "NVDA": -0.12, "JPM": 0.05,  "TSLA": -0.15, "SPY": -0.05},
        "tech_crash":      {"AAPL": -0.25, "NVDA": -0.40, "JPM": -0.05, "TSLA": -0.35, "SPY": -0.15},
        "recession":       {"AAPL": -0.15, "NVDA": -0.20, "JPM": -0.25, "TSLA": -0.30, "SPY": -0.20},
        "inflation_spike": {"AAPL": -0.10, "NVDA": -0.15, "JPM": 0.02,  "TSLA": -0.18, "SPY": -0.08},
    }

    if scenario not in shocks:
        return json.dumps({"error": f"Unknown scenario. Choose from: {list(shocks.keys())}"})

    shock = shocks[scenario]
    total_pnl = 0
    details = []
    for sym, pct in shock.items():
        mv = MARKET[sym]["price"] * PORTFOLIO[sym]["qty"]
        loss = mv * pct
        total_pnl += loss
        details.append({
            "symbol": sym,
            "shock_pct": round(pct * 100, 1),
            "pnl": round(loss, 2),
        })

    total_mv = sum(MARKET[s]["price"] * PORTFOLIO[s]["qty"] for s in PORTFOLIO)
    return json.dumps({
        "scenario": scenario,
        "total_pnl": round(total_pnl, 2),
        "total_pnl_pct": round(total_pnl / total_mv * 100, 2),
        "details": details,
    })


@tool
def analyze_concentration() -> str:
    """Analyze portfolio concentration by sector and single-name exposure."""
    total_value = 0
    sector_exposure = {}
    positions = []

    for sym, pos in PORTFOLIO.items():
        mkt = MARKET[sym]
        mv = mkt["price"] * pos["qty"]
        total_value += mv
        sector = mkt["sector"]
        sector_exposure[sector] = sector_exposure.get(sector, 0) + mv
        positions.append({"symbol": sym, "market_value": round(mv, 2), "sector": sector})

    # HHI (Herfindahl-Hirschman Index) for concentration
    weights = [p["market_value"] / total_value for p in positions]
    hhi = sum(w ** 2 for w in weights)

    sector_breakdown = {
        s: {"value": round(v, 2), "weight_pct": round(v / total_value * 100, 1)}
        for s, v in sector_exposure.items()
    }

    # Flag risks
    risks = []
    for s, info in sector_breakdown.items():
        if info["weight_pct"] > 40:
            risks.append(f"High sector concentration: {s} at {info['weight_pct']}%")
    for p in positions:
        w = p["market_value"] / total_value * 100
        if w > 25:
            risks.append(f"Large single-name exposure: {p['symbol']} at {w:.1f}%")

    return json.dumps({
        "total_value": round(total_value, 2),
        "hhi": round(hhi, 4),
        "hhi_interpretation": "concentrated" if hhi > 0.25 else "moderate" if hhi > 0.15 else "diversified",
        "sector_breakdown": sector_breakdown,
        "positions": positions,
        "risk_flags": risks,
    })


# ── Research Tools ───────────────────────────────────────────────────────


@tool
def search_research(query: str) -> str:
    """Search internal research notes and market commentary.

    Args:
        query: Search query (e.g. 'Fed policy outlook', 'NVDA earnings').
    """
    # Simulated research database
    research = [
        {
            "title": "Fed Policy Outlook — March 2024",
            "summary": "The Federal Reserve is expected to hold rates steady at 5.25-5.50% through Q2 2024. "
                       "Market pricing suggests 3 cuts in H2 2024. Duration-sensitive assets remain vulnerable "
                       "if cuts are delayed. Recommendation: underweight long-duration tech.",
            "tags": ["fed", "rates", "monetary policy"],
        },
        {
            "title": "AI Semiconductor Cycle Analysis",
            "summary": "NVDA revenue growth driven by data center demand (+265% YoY). Supply constraints easing "
                       "in Q3. Valuation stretched at 35x forward P/E but justified by TAM expansion. Key risk: "
                       "customer concentration (top 4 hyperscalers = 40% of revenue). Hold with trailing stop.",
            "tags": ["nvda", "semiconductors", "ai"],
        },
        {
            "title": "Banking Sector Q1 Review",
            "summary": "JPM delivered 15% ROE with strong NII. Commercial real estate exposure remains a concern "
                       "(8% of loan book). Consumer credit quality stable but delinquencies ticking up in subprime. "
                       "Well-positioned for a soft landing. Add on dips below $190.",
            "tags": ["jpm", "banks", "financials"],
        },
        {
            "title": "EV Market & TSLA Competitive Position",
            "summary": "Global EV penetration at 18%. TSLA market share declining from 62% to 51% in US. "
                       "Price cuts compressing margins (auto gross margin: 17.6%, down from 25.9%). "
                       "FSD and energy storage are optionality plays. Risk/reward unfavorable above $200.",
            "tags": ["tsla", "ev", "auto"],
        },
        {
            "title": "Portfolio Hedging Strategies — Current Environment",
            "summary": "With VIX at 16.8, put protection is relatively cheap. Recommended hedges: "
                       "(1) SPY 500P 3-month for tail risk (-5% portfolio cost), "
                       "(2) NVDA collar 800P/1000C to cap upside but protect gains, "
                       "(3) Sector rotation: reduce Tech from 60% to 45%, add Healthcare/Energy. "
                       "Cost of carry for put spreads: ~1.2% annualized.",
            "tags": ["hedging", "options", "risk management"],
        },
    ]

    query_lower = query.lower()
    matches = []
    for doc in research:
        score = 0
        for tag in doc["tags"]:
            if tag in query_lower:
                score += 2
        for word in query_lower.split():
            if word in doc["title"].lower() or word in doc["summary"].lower():
                score += 1
        if score > 0:
            matches.append({**doc, "_score": score})

    matches.sort(key=lambda x: x["_score"], reverse=True)
    # Strip internal score
    for m in matches:
        del m["_score"]

    return json.dumps(matches[:3] if matches else [{"note": "No matching research found."}])


# ── Build the Team ───────────────────────────────────────────────────────


def build_team():
    """Build the multi-agent finance team."""

    market_agent = Agent(
        tools=[get_portfolio, get_quote, get_macro_data],
        system_prompt=(
            "You are a Market Data Specialist. You fetch and report market data accurately. "
            "When asked about a portfolio, get ALL positions and their current quotes. "
            "Present data in clear tables. Include unrealized P&L and key metrics."
        ),
        name="market_data",
    )

    risk_agent = Agent(
        tools=[compute_portfolio_var, run_stress_test, analyze_concentration],
        system_prompt=(
            "You are a Risk Analyst. You analyze portfolio risk using quantitative methods. "
            "Always run VaR, at least 2 stress tests, and concentration analysis. "
            "Highlight the top 3 risks and provide specific, actionable recommendations."
        ),
        name="risk_analyst",
    )

    research_agent = Agent(
        tools=[search_research],
        system_prompt=(
            "You are a Research Analyst. You search internal research for relevant context. "
            "Search for research on the key portfolio holdings and any macro themes. "
            "Synthesize findings into concise, actionable insights."
        ),
        name="research",
    )

    team = AgentTeam(
        agents={
            "market_data": market_agent,
            "risk_analyst": risk_agent,
            "research": research_agent,
        },
    )

    return team, market_agent, risk_agent, research_agent


# ── Main ─────────────────────────────────────────────────────────────────


def main():
    print("=" * 70)
    print("  MULTI-AGENT FINANCE TEAM")
    print("  Portfolio Risk Analysis & Hedge Recommendations")
    print("=" * 70)

    team, market_agent, risk_agent, research_agent = build_team()

    # ── Part 1: Team Analysis ────────────────────────────────────────────

    print("\n📋 Task: Full portfolio risk analysis with hedge recommendations\n")
    print("-" * 70)

    result = team.run(
        "Analyze my portfolio's current risk exposure. "
        "Get all positions and market data, run VaR and stress tests, "
        "check concentration risk, search for relevant research on our holdings, "
        "and recommend specific hedges."
    )

    print("\n🏦 TEAM ANALYSIS RESULT")
    print("=" * 70)
    print(textwrap.fill(result.content, width=70))
    print("=" * 70)

    print(f"\n📊 Delegation Log ({len(result.delegation_log)} steps):")
    for i, step in enumerate(result.delegation_log, 1):
        print(f"  {i}. {step.agent_name:15s} ({step.duration_ms:.0f}ms)")
        print(f"     Task: {step.prompt[:60]}...")

    # ── Part 2: Individual Agent Drill-Down ──────────────────────────────

    print("\n" + "=" * 70)
    print("  DRILL-DOWN: Risk Agent — Stress Testing")
    print("=" * 70)

    risk_result = risk_agent.run(
        "Run all four stress test scenarios (rate_hike, tech_crash, recession, inflation_spike) "
        "and rank them by portfolio impact. Which scenario is our biggest vulnerability?"
    )
    print(f"\n{risk_result.content}")
    print(f"\n  Tools called: {[s.action.name for s in risk_result.steps]}")
    print(f"  Iterations: {risk_result.iterations}")

    # ── Part 3: Eval ─────────────────────────────────────────────────────

    print("\n" + "=" * 70)
    print("  EVAL: Agent Quality Check")
    print("=" * 70)

    cases = [
        EvalCase(
            input="What is my portfolio's total market value?",
            expected_tools=["get_portfolio"],
            tags=["market_data"],
        ),
        EvalCase(
            input="What is the VaR at 99% confidence?",
            expected_tools=["compute_portfolio_var"],
            tags=["risk"],
        ),
        EvalCase(
            input="Run a tech crash stress test",
            expected_tools=["run_stress_test"],
            tags=["risk"],
        ),
        EvalCase(
            input="What does our research say about NVDA?",
            expected_tools=["search_research"],
            expected_output_contains=["NVDA"],
            tags=["research"],
        ),
    ]

    # Use a fresh agent with all tools for eval
    all_tools = [
        get_portfolio, get_quote, get_macro_data,
        compute_portfolio_var, run_stress_test, analyze_concentration,
        search_research,
    ]
    eval_agent = Agent(
        tools=all_tools,
        system_prompt="You are a financial analyst with access to market data, risk tools, and research.",
    )
    runner = EvalRunner(agent=eval_agent)
    runner.run(cases)
    runner.summary()

    print("\nDone! 🎯")


if __name__ == "__main__":
    main()
