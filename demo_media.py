#!/usr/bin/env python3
"""
Media Store Demo — Unstructured Data Storage & Search
=======================================================
Upload documents, extract text, full-text search, download — all with
bi-temporal audit trail and RLS access control.

  1. Start MinIO + PG
  2. Upload text, markdown, HTML, and binary files
  3. Full-text search with ranking
  4. Download and verify
  5. List and filter

Usage:
  pip install -e ".[media]"
  python3 demo_media.py
"""

import asyncio
import logging
import textwrap

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("demo_media")


def section(title: str):
    print()
    print(f"{'─' * 70}")
    print(f"  {title}")
    print(f"{'─' * 70}")


def run_demo():
    print("=" * 70)
    print("  Media Store Demo — Unstructured Data Storage & Search")
    print("=" * 70)

    # ── Start infrastructure ──────────────────────────────────────────
    section("Starting MinIO + PG")

    from lakehouse.services import MinIOManager
    from store.server import ObjectStoreServer
    from store.schema import provision_user
    from store.connection import connect

    # PG for Storable metadata
    server = ObjectStoreServer(data_dir="data/demo_media_store")
    server.start()
    admin_conn = server.admin_conn()
    provision_user(admin_conn, "demo_user", "demo_pw")

    # Bootstrap document_search table
    from media.models import bootstrap_search_schema
    bootstrap_search_schema(admin_conn)
    admin_conn.close()

    # MinIO for binary storage
    minio = MinIOManager(data_dir="data/demo_media_minio", api_port=9022, console_port=9023)
    asyncio.run(minio.start())

    # Connect as demo_user
    info = server.conn_info()
    conn = connect(user="demo_user", host=info["host"], port=info["port"],
                   dbname=info["dbname"], password="demo_pw")

    print(f"  PG:    {info['host']}:{info['port']}")
    print(f"  MinIO: localhost:9022")

    # ── Create MediaStore ─────────────────────────────────────────────
    from media import MediaStore

    ms = MediaStore(s3_endpoint="localhost:9022", s3_bucket="demo-media")

    try:
        # ── 1. Upload documents ───────────────────────────────────────
        section("1. Upload documents")

        # Plain text
        doc1 = ms.upload(
            b"This research report covers interest rate swap pricing, "
            b"credit default swap valuation, and yield curve construction. "
            b"The analysis uses Monte Carlo simulation for CVA/DVA calculations.",
            filename="irs_research.txt",
            title="Interest Rate Swap Research",
            tags=["research", "rates", "swaps"],
        )
        print(f"  Uploaded: {doc1.title} ({doc1.size} bytes, {doc1.content_type})")
        print(f"    S3: {doc1.s3_key}")
        print(f"    Extracted: {len(doc1.extracted_text)} chars")

        # Markdown
        doc2 = ms.upload(
            textwrap.dedent("""\
            # Q1 Trading Summary

            ## Equity Derivatives
            The desk generated **$12.5M** in P&L from equity options market-making.
            Volatility skew steepened significantly in March.

            ## Fixed Income
            Interest rate swap volumes increased 30% quarter-over-quarter.
            The 2s10s curve flattened by 15bps.

            ## Risk Metrics
            - VaR (99%): $2.1M
            - Expected Shortfall: $3.8M
            - Stress test loss: $8.2M
            """).encode(),
            filename="q1_summary.md",
            title="Q1 Trading Summary",
            tags=["trading", "quarterly", "risk"],
        )
        print(f"  Uploaded: {doc2.title} ({doc2.size} bytes, {doc2.content_type})")

        # HTML
        doc3 = ms.upload(
            b"""<html><body>
            <h1>Regulatory Filing</h1>
            <p>This filing covers the firm's <b>Basel III</b> capital requirements,
            including risk-weighted assets, leverage ratio, and liquidity coverage ratio.</p>
            <p>The common equity tier 1 ratio stands at 14.2%, well above the
            regulatory minimum of 4.5%.</p>
            </body></html>""",
            filename="regulatory_filing.html",
            title="Basel III Capital Filing",
            tags=["regulatory", "capital"],
        )
        print(f"  Uploaded: {doc3.title} ({doc3.size} bytes, {doc3.content_type})")

        # Binary (image — no text extraction)
        doc4 = ms.upload(
            b"\x89PNG\r\n\x1a\n" + b"\x00" * 200,
            filename="risk_heatmap.png",
            title="Risk Heatmap",
            tags=["chart", "risk"],
        )
        print(f"  Uploaded: {doc4.title} ({doc4.size} bytes, {doc4.content_type})")
        print(f"    Text extracted: {doc4.has_text}")

        # Another text doc for search variety
        doc5 = ms.upload(
            b"Portfolio optimization using mean-variance analysis. "
            b"The efficient frontier shows optimal risk-return tradeoffs. "
            b"Black-Litterman model incorporates investor views.",
            filename="portfolio_optimization.txt",
            title="Portfolio Optimization Notes",
            tags=["research", "portfolio"],
        )
        print(f"  Uploaded: {doc5.title} ({doc5.size} bytes)")

        # ── 2. Full-text search ───────────────────────────────────────
        section("2. Full-text search")

        queries = [
            ("interest rate swap", None, None),
            ("equity derivatives volatility", None, None),
            ("Basel capital", None, None),
            ("risk", None, None),
            ("swap", "text/plain", None),
            ("risk", None, ["research"]),
        ]

        for query, ct, tags in queries:
            label = f'"{query}"'
            if ct:
                label += f" (type={ct})"
            if tags:
                label += f" (tags={tags})"

            results = ms.search(query, content_type=ct, tags=tags)
            print(f"\n  Search: {label} → {len(results)} results")
            for r in results[:3]:
                print(f"    {r['rank']:.4f}  {r['title'][:40]:40s}  [{r['content_type']}]")

        # ── 3. Download ───────────────────────────────────────────────
        section("3. Download & verify")

        data = ms.download(doc1)
        print(f"  Downloaded {doc1.title}: {len(data)} bytes")
        assert b"interest rate swap" in data
        print("  Content verified ✓")

        # ── 4. List & filter ──────────────────────────────────────────
        section("4. List & filter")

        all_docs = ms.list()
        print(f"  All documents: {len(all_docs)}")
        for d in all_docs:
            print(f"    {d.title[:40]:40s}  {d.content_type:20s}  {d.size:6d} bytes  tags={d.tags}")

        research = ms.list(tags=["research"])
        print(f"\n  Research docs: {len(research)}")
        for d in research:
            print(f"    {d.title}")

        # ── 5. Storable features ──────────────────────────────────────
        section("5. Storable features (bi-temporal, audit)")

        from media.models import Document
        found = Document.find(doc1._store_entity_id)
        print(f"  Find by ID: {found.title}")

        history = found.history()
        print(f"  Version history: {len(history)} versions")
        for h in history:
            print(f"    v{h._store_version} — {h._store_event_type} at {h._store_tx_time}")

        # ── Summary ──────────────────────────────────────────────────
        section("Summary")
        print(f"  Documents uploaded: {len(all_docs)}")
        print(f"  With extracted text: {sum(1 for d in all_docs if d.has_text)}")
        print(f"  Total bytes in S3: {sum(d.size for d in all_docs)}")

    finally:
        print("\n  Shutting down...")
        ms.close()
        conn.close()
        asyncio.run(minio.stop())
        server.stop()
        print("  Done.")


if __name__ == "__main__":
    try:
        run_demo()
    except KeyboardInterrupt:
        print("\nDemo stopped.")
