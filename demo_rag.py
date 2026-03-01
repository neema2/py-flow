#!/usr/bin/env python3
"""
AI + RAG Demo — Document-Grounded Q&A with Structured Extraction
==================================================================
Upload financial documents, ask questions with RAG, extract structured
data, and use tool calling — all through the clean AI class API.

  1. Start object store + PG
  2. Upload financial documents (auto-chunk + embed)
  3. Search three ways: full-text, semantic, hybrid
  4. RAG: ask questions grounded in documents
  5. Structured extraction from text
  6. Tool calling: LLM uses search tools autonomously
  7. Streaming generation

Usage:
  export GEMINI_API_KEY="your-key"
  pip install -e ".[ai,media]"
  python3 demo_rag.py
"""

import asyncio
import logging
import os
import tempfile
import textwrap

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("demo_rag")


def section(title: str):
    print()
    print(f"{'─' * 70}")
    print(f"  {title}")
    print(f"{'─' * 70}")


def run_demo():
    print("=" * 70)
    print("  AI + RAG Demo — Document-Grounded Q&A")
    print("=" * 70)

    if not os.environ.get("GEMINI_API_KEY"):
        print("\n  ERROR: Set GEMINI_API_KEY environment variable.")
        return

    # ── Platform setup ──────────────────────────────────────────────
    section("Starting infrastructure")

    from store.admin import StoreServer
    from media.admin import MediaServer

    store = StoreServer(data_dir=tempfile.mkdtemp(prefix="demo_rag_store_"))
    store.start()
    store.register_alias("demo-rag")
    store.provision_user("demo_user", "demo_pw")

    from media.models import bootstrap_search_schema, bootstrap_chunks_schema
    admin_conn = store.admin_conn()
    bootstrap_search_schema(admin_conn, embedding_dim=768)
    admin_conn.close()
    admin_conn = store.admin_conn()
    bootstrap_chunks_schema(admin_conn, embedding_dim=768)
    admin_conn.close()

    media_srv = MediaServer(data_dir=tempfile.mkdtemp(prefix="demo_rag_s3_"), api_port=9062, console_port=9063)
    asyncio.run(media_srv.start())
    media_srv.register_alias("demo-rag")

    # ── User code ──────────────────────────────────────────────────
    from store import connect

    conn = connect("demo-rag", user="demo_user", password="demo_pw")
    info = store.conn_info()
    print(f"  PG:    {info['host']}:{info['port']}")
    print(f"  Media: {media_srv.endpoint}")

    # ── Create AI + MediaStore ────────────────────────────────────────
    section("Initializing AI + MediaStore")

    from ai import AI, Message
    from media import MediaStore

    ai = AI()  # reads GEMINI_API_KEY from env
    ms = MediaStore("demo-rag", ai=ai)

    print(f"  AI initialized (provider: gemini)")
    print(f"  MediaStore with AI-powered embeddings")

    try:
        # ── 1. Upload documents ───────────────────────────────────────
        section("1. Upload documents (auto-chunk + embed)")

        docs = []

        doc = ms.upload(
            textwrap.dedent("""\
            Credit Default Swaps (CDS) are financial derivatives that allow investors
            to transfer credit risk. The buyer of a CDS makes periodic payments to the
            seller and receives a payoff if a credit event occurs on the reference entity.

            CDS spreads reflect the market's perception of credit risk. A wider spread
            indicates higher perceived default probability. The CDS market grew rapidly
            before the 2008 financial crisis and played a significant role in the
            systemic risk that led to the collapse of major financial institutions.

            Key terms: reference entity, credit event, notional amount, premium leg,
            protection leg, recovery rate, ISDA documentation.
            """).encode(),
            filename="cds_overview.txt",
            title="Credit Default Swaps Overview",
            tags=["derivatives", "credit", "risk"],
        )
        docs.append(doc)
        print(f"  ✓ {doc.title} ({doc.size} bytes)")

        doc = ms.upload(
            textwrap.dedent("""\
            Basel III Capital Requirements

            The Basel III framework strengthens bank capital requirements through:

            1. Common Equity Tier 1 (CET1): Minimum 4.5% of risk-weighted assets
            2. Tier 1 Capital: Minimum 6% of risk-weighted assets
            3. Total Capital: Minimum 8% of risk-weighted assets
            4. Capital Conservation Buffer: Additional 2.5% CET1
            5. Countercyclical Buffer: 0-2.5% at national discretion

            The Leverage Ratio requires Tier 1 capital of at least 3% of total
            exposures. The Liquidity Coverage Ratio (LCR) requires sufficient
            high-quality liquid assets to cover 30-day net cash outflows.

            Banks must also report the Net Stable Funding Ratio (NSFR) to ensure
            long-term funding stability.
            """).encode(),
            filename="basel3_requirements.txt",
            title="Basel III Capital Requirements",
            tags=["regulatory", "capital", "banking"],
        )
        docs.append(doc)
        print(f"  ✓ {doc.title} ({doc.size} bytes)")

        doc = ms.upload(
            textwrap.dedent("""\
            Interest Rate Swap Valuation

            An interest rate swap (IRS) exchanges fixed-rate payments for floating-rate
            payments on a notional principal. Valuation involves:

            1. Discount factors derived from the yield curve
            2. Forward rates for projecting floating leg cash flows
            3. Present value of fixed leg minus present value of floating leg

            The swap rate is the fixed rate that makes the swap NPV equal to zero
            at inception. DV01 (dollar value of a basis point) measures sensitivity
            to a 1bp parallel shift in the yield curve.

            Common day count conventions: ACT/360 for floating, 30/360 for fixed.
            Payment frequencies: quarterly floating, semi-annual fixed.

            Mark-to-market P&L = current NPV minus inception NPV.
            """).encode(),
            filename="irs_valuation.txt",
            title="Interest Rate Swap Valuation",
            tags=["derivatives", "rates", "pricing"],
        )
        docs.append(doc)
        print(f"  ✓ {doc.title} ({doc.size} bytes)")

        doc = ms.upload(
            textwrap.dedent("""\
            Portfolio Risk Metrics

            Value at Risk (VaR) estimates the maximum expected loss over a given
            time horizon at a specified confidence level. A 1-day 99% VaR of $5M
            means there is a 1% chance of losing more than $5M in one day.

            Expected Shortfall (ES), also called CVaR, measures the average loss
            in the worst X% of cases. ES is considered a more coherent risk measure
            than VaR because it satisfies subadditivity.

            Stress testing involves hypothetical or historical scenarios:
            - 2008 Financial Crisis replay
            - +200bp parallel rate shock
            - Equity market crash (-30%)
            - Credit spread widening (+500bp)

            Greeks (Delta, Gamma, Vega, Theta, Rho) measure option sensitivities.
            """).encode(),
            filename="risk_metrics.txt",
            title="Portfolio Risk Metrics",
            tags=["risk", "portfolio", "var"],
        )
        docs.append(doc)
        print(f"  ✓ {doc.title} ({doc.size} bytes)")

        print(f"\n  Total: {len(docs)} documents uploaded and embedded")

        # ── 2. Search three ways ──────────────────────────────────────
        section("2. Search — three modes")

        query = "credit risk derivatives"

        print(f'\n  Query: "{query}"\n')

        print("  Full-text search (keyword matching):")
        results = ms.search(query, limit=3)
        for r in results:
            print(f"    {r['rank']:.4f}  {r['title']}")

        print("\n  Semantic search (meaning-based):")
        results = ms.semantic_search(query, limit=3)
        for r in results:
            print(f"    {r['similarity']:.4f}  {r['title']}")

        print("\n  Hybrid search (RRF fusion — best of both):")
        results = ms.hybrid_search(query, limit=3)
        for r in results:
            print(f"    {r['rrf_score']:.4f}  {r['title']}")

        # ── 3. RAG ────────────────────────────────────────────────────
        section("3. RAG — ask questions grounded in documents")

        questions = [
            "What is a credit default swap and how does it work?",
            "What are the Basel III minimum capital requirements?",
            "How is DV01 used in interest rate swap valuation?",
            "What is the difference between VaR and Expected Shortfall?",
        ]

        for q in questions:
            print(f'\n  Q: {q}')
            result = ai.ask(q, documents=ms, limit=3)
            # Wrap answer to 66 chars
            wrapped = textwrap.fill(result.answer, width=66, initial_indent="  A: ", subsequent_indent="     ")
            print(wrapped)
            print(f"     [{len(result.sources)} sources, {result.usage.get('total_tokens', '?')} tokens]")

        # ── 4. Structured extraction ──────────────────────────────────
        section("4. Structured extraction")

        text = """
        Goldman Sachs reported Q3 2024 earnings: revenue of $12.7 billion,
        net income of $2.99 billion, and earnings per share of $8.40.
        The investment banking division generated $1.87 billion in fees.
        Trading revenue was $6.4 billion. ROE was 10.4%.
        """

        result = ai.extract(
            text=text.strip(),
            schema={
                "type": "object",
                "properties": {
                    "company": {"type": "string"},
                    "quarter": {"type": "string"},
                    "revenue_billions": {"type": "number"},
                    "net_income_billions": {"type": "number"},
                    "eps": {"type": "number"},
                    "ib_fees_billions": {"type": "number"},
                    "trading_revenue_billions": {"type": "number"},
                    "roe_pct": {"type": "number"},
                },
                "required": ["company", "quarter", "revenue_billions"],
            },
        )

        print(f"  Input: {text.strip()[:80]}...")
        print(f"\n  Extracted:")
        for k, v in result.data.items():
            print(f"    {k:30s} = {v}")

        # ── 5. Direct generation + streaming ──────────────────────────
        section("5. Direct generation + streaming")

        print("  Generating (non-streaming):")
        response = ai.generate(
            "Explain the difference between systematic and idiosyncratic risk in one sentence.",
            temperature=0.3,
        )
        wrapped = textwrap.fill(response.content, width=66, initial_indent="  ", subsequent_indent="  ")
        print(wrapped)

        print(f"\n  Streaming:")
        print("  ", end="")
        for chunk in ai.stream("What is convexity in fixed income? Answer in one sentence."):
            print(chunk, end="", flush=True)
        print()

        # ── 6. Tool calling ───────────────────────────────────────────
        section("6. Tool calling — LLM searches autonomously")

        tools = ai.search_tools(ms)
        print(f"  Registered {len(tools)} search tools: {[t['name'] for t in tools]}")

        response = ai.run_tool_loop(
            "Search for documents about Basel capital requirements and summarize what you find.",
            tools=tools,
        )
        wrapped = textwrap.fill(response.content, width=66, initial_indent="  ", subsequent_indent="  ")
        print(f"\n  LLM response (after tool use):")
        print(wrapped)

        # ── Summary ──────────────────────────────────────────────────
        section("Summary")
        print(f"  Documents: {len(docs)} uploaded, chunked, and embedded")
        print(f"  Search:    3 modes (full-text, semantic, hybrid)")
        print(f"  RAG:       {len(questions)} questions answered with citations")
        print(f"  Extract:   {len(result.data)} fields from earnings report")
        print(f"  Tools:     {len(tools)} search tools available to LLM")
        print(f"\n  All through: from ai import AI")

    finally:
        print("\n  Shutting down...")
        ms.close()
        conn.close()
        # object store cleanup handled by atexit
        store.stop()
        print("  Done.")


if __name__ == "__main__":
    try:
        run_demo()
    except KeyboardInterrupt:
        print("\nDemo stopped.")
