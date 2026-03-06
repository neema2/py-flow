"""
Datacube UI demo — NYC Yellow Taxi data.

Usage:
    python demo_datacube_ui.py

Opens a browser with an interactive Perspective grid.
All pivoting, filtering, and aggregation is done via DuckDB SQL pushdown.
"""

import duckdb
from datacube import Datacube

TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

conn = duckdb.connect()

print("Loading taxi data (this reads directly from HTTPS, no download)...")
conn.execute(f"""
    CREATE TABLE yellow_taxi AS
    SELECT
        VendorID::VARCHAR           AS vendor,
        tpep_pickup_datetime        AS pickup_time,
        tpep_dropoff_datetime       AS dropoff_time,
        passenger_count::INTEGER    AS passengers,
        trip_distance::DOUBLE       AS distance,
        RatecodeID::VARCHAR         AS rate_code,
        PULocationID::VARCHAR       AS pickup_zone,
        DOLocationID::VARCHAR       AS dropoff_zone,
        payment_type::VARCHAR       AS payment_type,
        fare_amount::DOUBLE         AS fare,
        extra::DOUBLE               AS extra,
        mta_tax::DOUBLE             AS mta_tax,
        tip_amount::DOUBLE          AS tip,
        tolls_amount::DOUBLE        AS tolls,
        total_amount::DOUBLE        AS total,
        congestion_surcharge::DOUBLE AS congestion
    FROM read_parquet('{TAXI_URL}')
""")

row = conn.execute("SELECT count(*) FROM yellow_taxi").fetchone()
assert row is not None
row_count = row[0]
print(f"Loaded {row_count:,} rows")

dc = Datacube(conn, source_name="yellow_taxi")
print(f"Columns: {[c.name for c in dc.snapshot.columns]}")
print(f"Dimensions: {dc.available_dimensions()}")
print(f"Measures: {dc.available_measures()}")

dc.show()
