"""
Datacube Lakehouse Performance Demo
=====================================
Ingest ~3M rows of NYC Yellow Taxi trip data into Lakehouse (Iceberg on S3),
then run datacube queries that aggregate down to tiny result sets.

Demonstrates the zero-Python-transit path: S3 Parquet → DuckDB Iceberg → result.

Usage:
    python3 demo_datacube.py
"""

import asyncio
import math
import tempfile
import time


def v(x, default=0):
    """Safely extract a value, returning default if NaN/None."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return default
    return x

# ── 1. Start Lakehouse infrastructure ────────────────────────────────────


def start_stack():
    from lakehouse.admin import LakehouseServer
    tmp = tempfile.mkdtemp(prefix="demo_dc_", dir="/tmp")
    srv = LakehouseServer(data_dir=tmp)
    asyncio.run(srv.start())
    return srv


def create_lakehouse(stack):
    from lakehouse import Lakehouse
    return Lakehouse(
        catalog_uri=stack.catalog_url,
        s3_endpoint=stack.s3_endpoint,
    )


# ── 2. Ingest NYC Yellow Taxi data ──────────────────────────────────────


# Public Parquet files — no auth needed, DuckDB reads directly via HTTPS
TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"


def ingest_taxi_data(lh):
    """Ingest taxi Parquet from HTTPS → Iceberg.  Zero Python transit."""

    # SQL with column selection/renaming — DuckDB reads Parquet from URL,
    # streams directly into Iceberg.  Data never enters Python memory.
    ingest_sql = f"""
        SELECT
            tpep_pickup_datetime as pickup_time,
            tpep_dropoff_datetime as dropoff_time,
            passenger_count,
            trip_distance,
            PULocationID as pickup_location_id,
            DOLocationID as dropoff_location_id,
            RatecodeID as rate_code,
            payment_type,
            fare_amount,
            tip_amount,
            tolls_amount,
            total_amount
        FROM '{TAXI_URL}'
    """

    print("\n📥 Ingesting into Lakehouse (S3/Iceberg)...")
    print(f"   Source: {TAXI_URL}")
    print("   Path:  HTTPS Parquet → DuckDB → Iceberg (zero Python transit)")

    t0 = time.perf_counter()
    n = lh.ingest("yellow_taxi", ingest_sql, mode="append")
    t_ingest = time.perf_counter() - t0
    print(f"   Ingested {n:,} rows in {t_ingest:.1f}s")

    return n


# ── 3. Timed datacube queries ───────────────────────────────────────────


def timed(label, fn):
    """Run fn(), print timing, return result."""
    t0 = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - t0
    print(f"   {label}: {elapsed:.3f}s")
    return result, elapsed


def run_queries(lh, row_count):
    """Run increasingly complex datacube queries with timing."""

    print(f"\n{'='*70}")
    print(f"  DATACUBE QUERIES — {row_count:,} rows in Iceberg")
    print("  Path: S3 Parquet → DuckDB Iceberg extension → result")
    print(f"{'='*70}\n")

    dc = lh.datacube("yellow_taxi")
    col_names = [c.name for c in dc.snapshot.columns]
    print(f"   Columns discovered: {col_names}\n")

    # ── Query 1: Flat count ──────────────────────────────────────────
    print("── Q1: SELECT count(*) ──")
    result, _ = timed("Total rows", lambda: lh.query(
        "SELECT count(*) as cnt FROM lakehouse.default.yellow_taxi"
    ))
    print(f"   → {result[0]['cnt']:,} rows\n")

    # ── Query 2: Simple GROUP BY (5 rows back from millions) ─────────
    print("── Q2: Revenue by payment type (GROUP BY → 5 rows) ──")
    dc2 = dc.set_group_by("payment_type")
    print(f"   SQL: {dc2.sql()[:120]}...")
    df, _ = timed("Query", lambda: dc2.query_df())
    print(f"   → {len(df)} rows:")
    # Map payment types
    pay_map = {1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 0: "Unknown"}
    for _, row in df.sort_values("total_amount", ascending=False).iterrows():
        ptype = pay_map.get(int(v(row.get("payment_type"))), f"Type {row.get('payment_type')}")
        print(f"     {ptype:15s}  fare=${v(row['fare_amount']):>14,.2f}  "
              f"tip=${v(row['tip_amount']):>12,.2f}  total=${v(row['total_amount']):>14,.2f}")
    print()

    # ── Query 3: Two-level GROUP BY ──────────────────────────────────
    print("── Q3: Revenue by payment_type × rate_code (GROUP BY → ~25 rows) ──")
    dc3 = dc.set_group_by("payment_type", "rate_code")
    df, _ = timed("Query", lambda: dc3.query_df())
    print(f"   → {len(df)} rows\n")

    # ── Query 4: HPivot — payment_type as columns ────────────────────
    print("── Q4: Pivot: rate_code rows × payment_type columns ──")
    dc4 = dc.set_group_by("rate_code").set_pivot_by("payment_type")
    df, _ = timed("Query", lambda: dc4.query_df())
    print(f"   → {len(df)} rows × {len(df.columns)} columns")
    print(f"   Columns: {list(df.columns)[:10]}...\n")

    # ── Query 5: Filter + GROUP BY ───────────────────────────────────
    print("── Q5: Credit card trips only, grouped by rate_code ──")
    dc5 = (
        dc.add_filter("payment_type", "eq", 1)
        .set_group_by("rate_code")
        .set_column("fare_amount", aggregate_operator="avg")
        .set_column("tip_amount", aggregate_operator="avg")
        .set_column("trip_distance", aggregate_operator="avg")
    )
    df, _ = timed("Query", lambda: dc5.query_df())
    rate_map = {1: "Standard", 2: "JFK", 3: "Newark", 4: "Nassau/Westchester", 5: "Negotiated", 6: "Group ride", 99: "Unknown"}
    print(f"   → {len(df)} rows:")
    for _, row in df.sort_values("fare_amount", ascending=False).head(6).iterrows():
        rc = rate_map.get(int(v(row.get("rate_code"))), f"Rate {row.get('rate_code')}")
        print(f"     {rc:20s}  avg_fare=${v(row['fare_amount']):>8,.2f}  "
              f"avg_tip=${v(row['tip_amount']):>6,.2f}  avg_dist={v(row['trip_distance']):>5,.1f}mi")
    print()

    # ── Query 6: Drilldown ───────────────────────────────────────────
    print("── Q6: Drilldown: payment_type → rate_code ──")
    dc6 = dc.set_group_by("payment_type", "rate_code")
    df_top, _ = timed("Level 1 (payment_type)", lambda: dc6.query_df())
    print(f"   → {len(df_top)} payment types")

    dc6_drill = dc6.drill_down(payment_type=1)
    df_drill, _ = timed("Level 2 (rate_code for credit card)", lambda: dc6_drill.query_df())
    print(f"   → {len(df_drill)} rate codes for credit card trips\n")

    # ── Query 7: Leaf extend + GROUP BY ──────────────────────────────
    print("── Q7: Computed column (tip_pct = tip/fare) + GROUP BY ──")
    dc7 = (
        dc.add_leaf_extend("tip_pct", "CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount * 100 ELSE 0 END")
        .set_group_by("payment_type")
        .set_column("tip_pct", aggregate_operator="avg")
    )
    df, _ = timed("Query", lambda: dc7.query_df())
    print(f"   → {len(df)} rows:")
    for _, row in df.sort_values("tip_pct", ascending=False).iterrows():
        ptype = pay_map.get(int(v(row.get("payment_type"))), f"Type {row.get('payment_type')}")
        print(f"     {ptype:15s}  avg_tip_pct={v(row['tip_pct']):>5.1f}%")
    print()

    # ── Query 8: Full pipeline — filter + extend + group + pivot + sort + limit ──
    print("── Q8: Full pipeline (filter + extend + group + pivot + sort + limit) ──")
    dc8 = (
        dc.add_filter("payment_type", "in", [1, 2])  # Credit + Cash only
        .add_leaf_extend("tip_pct", "CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount * 100 ELSE 0 END")
        .set_group_by("rate_code")
        .set_pivot_by("payment_type")
        .set_column("tip_pct", aggregate_operator="avg")
        .set_sort(("rate_code", False))
        .set_limit(10)
    )
    df, _ = timed("Query", lambda: dc8.query_df())
    print(f"   → {len(df)} rows × {len(df.columns)} columns")
    print(f"   Columns: {list(df.columns)}\n")


# ── Main ─────────────────────────────────────────────────────────────────


def main():
    print("=" * 70)
    print("  DATACUBE LAKEHOUSE DEMO — NYC Yellow Taxi 2024-01")
    print("  ~3M trips → Iceberg (Parquet on S3) → Datacube queries")
    print("=" * 70)

    t_total = time.perf_counter()

    print("\n🚀 Starting Lakehouse stack (PG + Lakekeeper + S3)...")
    t0 = time.perf_counter()
    stack = start_stack()
    print(f"   Stack ready in {time.perf_counter() - t0:.1f}s")

    lh = create_lakehouse(stack)

    try:
        row_count = ingest_taxi_data(lh)
        run_queries(lh, row_count)

        print(f"\n{'='*70}")
        print(f"  TOTAL DEMO TIME: {time.perf_counter() - t_total:.1f}s")
        print(f"{'='*70}\n")
    finally:
        lh.close()
        asyncio.run(stack.stop())


if __name__ == "__main__":
    main()
