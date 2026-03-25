from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from .utils import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class TransformResult:
    silver_rows: int
    gold_rows: int
    dim_zone_rows: int


def connect(warehouse_path: Path) -> duckdb.DuckDBPyConnection:
    warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(warehouse_path))
    con.execute("SET timezone='UTC'")
    return con


def build_silver(con: duckdb.DuckDBPyConnection, bronze_dir: Path) -> None:
    logger.info("Building silver_trips")
    parquet_glob = str(bronze_dir / "*" / "*.parquet")

    con.execute(
        """
        CREATE OR REPLACE TABLE silver_trips AS
        WITH raw_trips AS (
            SELECT
                trip_id,
                CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
                CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
                passenger_count,
                trip_distance,
                pickup_zone_id,
                dropoff_zone_id,
                fare_amount,
                tip_amount,
                total_amount,
                payment_type,
                COALESCE(CAST(ingested_at AS TIMESTAMP), CAST(pickup_datetime AS TIMESTAMP)) AS ingested_at,
                COALESCE(source_batch_id, 'legacy') AS source_batch_id
            FROM read_parquet(?, union_by_name = true, hive_partitioning = true)
            WHERE
                trip_distance > 0
                AND fare_amount > 0
                AND CAST(dropoff_datetime AS TIMESTAMP) > CAST(pickup_datetime AS TIMESTAMP)
        ),
        ranked_trips AS (
            SELECT
                trip_id,
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                trip_distance,
                pickup_zone_id,
                dropoff_zone_id,
                fare_amount,
                tip_amount,
                total_amount,
                payment_type,
                ingested_at,
                source_batch_id,
                CAST(pickup_datetime AS DATE) AS pickup_date,
                EXTRACT('hour' FROM pickup_datetime) AS pickup_hour,
                DATE_DIFF('minute', pickup_datetime, dropoff_datetime) AS trip_duration_min,
                ROW_NUMBER() OVER (
                    PARTITION BY trip_id
                    ORDER BY ingested_at DESC, dropoff_datetime DESC
                ) AS trip_rank
            FROM raw_trips
        )
        SELECT
            trip_id,
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            pickup_zone_id,
            dropoff_zone_id,
            fare_amount,
            tip_amount,
            total_amount,
            payment_type,
            ingested_at,
            source_batch_id,
            pickup_date,
            pickup_hour,
            trip_duration_min
        FROM ranked_trips
        WHERE trip_rank = 1
        """,
        [parquet_glob],
    )


def build_dim_zones(con: duckdb.DuckDBPyConnection) -> None:
    logger.info("Building dim_zones")
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_zones AS
        WITH zones AS (
            SELECT pickup_zone_id AS zone_id FROM silver_trips
            UNION
            SELECT dropoff_zone_id AS zone_id FROM silver_trips
        )
        SELECT
            zone_id,
            'Zone ' || CAST(zone_id AS VARCHAR) AS zone_label
        FROM zones
        ORDER BY zone_id
        """
    )


def build_fct_trip_hourly(con: duckdb.DuckDBPyConnection) -> None:
    logger.info("Building fct_trip_hourly")
    con.execute(
        """
        CREATE OR REPLACE TABLE fct_trip_hourly AS
        SELECT
            pickup_date,
            pickup_hour,
            pickup_zone_id AS zone_id,
            COUNT(*) AS trip_count,
            ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
            ROUND(AVG(fare_amount), 2) AS avg_fare_amount,
            ROUND(SUM(total_amount), 2) AS total_revenue
        FROM silver_trips
        GROUP BY 1, 2, 3
        """
    )


def export_tables(con: duckdb.DuckDBPyConnection, silver_dir: Path, gold_dir: Path) -> None:
    silver_dir.mkdir(parents=True, exist_ok=True)
    gold_dir.mkdir(parents=True, exist_ok=True)

    con.execute(
        "COPY (SELECT * FROM silver_trips) TO ? (FORMAT PARQUET)",
        [str(silver_dir / "silver_trips.parquet")],
    )
    con.execute(
        "COPY (SELECT * FROM dim_zones) TO ? (FORMAT PARQUET)",
        [str(gold_dir / "dim_zones.parquet")],
    )
    con.execute(
        "COPY (SELECT * FROM fct_trip_hourly) TO ? (FORMAT PARQUET)",
        [str(gold_dir / "fct_trip_hourly.parquet")],
    )


def summarize_tables(con: duckdb.DuckDBPyConnection) -> TransformResult:
    silver_rows = con.execute("SELECT COUNT(*) FROM silver_trips").fetchone()[0]
    gold_rows = con.execute("SELECT COUNT(*) FROM fct_trip_hourly").fetchone()[0]
    dim_zone_rows = con.execute("SELECT COUNT(*) FROM dim_zones").fetchone()[0]
    return TransformResult(
        silver_rows=int(silver_rows),
        gold_rows=int(gold_rows),
        dim_zone_rows=int(dim_zone_rows),
    )


def run_transforms(warehouse_path: Path, bronze_dir: Path, silver_dir: Path, gold_dir: Path) -> TransformResult:
    con = connect(warehouse_path)
    try:
        build_silver(con, bronze_dir)
        build_dim_zones(con)
        build_fct_trip_hourly(con)
        export_tables(con, silver_dir, gold_dir)
        return summarize_tables(con)
    finally:
        con.close()
