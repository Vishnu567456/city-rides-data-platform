from datetime import UTC, date, datetime

import duckdb
import pandas as pd

from src.pipeline.config import Paths
from src.pipeline.generate import generate_trips
from src.pipeline.ingest import ingest_nyc_tlc, write_bronze
from src.pipeline.quality import run_quality_checks
from src.pipeline.run_pipeline import run_pipeline
from src.pipeline.transform import run_transforms


def test_transform_and_quality(tmp_path):
    paths = Paths(
        project_root=tmp_path,
        data_dir=tmp_path / "data",
        bronze_dir=tmp_path / "data" / "bronze" / "trips",
        silver_dir=tmp_path / "data" / "silver",
        gold_dir=tmp_path / "data" / "gold",
        monitoring_dir=tmp_path / "data" / "monitoring",
        warehouse_path=tmp_path / "data" / "warehouse.duckdb",
        quality_report_path=tmp_path / "data" / "quality_report.json",
    )

    df = generate_trips(start_date=date(2025, 1, 1), days=1, rows_per_day=200, seed=7)
    write_bronze(
        df,
        paths.bronze_dir,
        batch_id="test-batch",
        ingested_at=datetime(2025, 1, 1, tzinfo=UTC),
    )

    transform_result = run_transforms(
        warehouse_path=paths.warehouse_path,
        bronze_dir=paths.bronze_dir,
        silver_dir=paths.silver_dir,
        gold_dir=paths.gold_dir,
    )

    report = run_quality_checks(paths.warehouse_path, paths.quality_report_path, raise_on_failure=False)
    assert report["overall_pass"] is True
    assert transform_result.silver_rows == 200

    con = duckdb.connect(str(paths.warehouse_path), read_only=True)
    try:
        silver_count = con.execute("SELECT COUNT(*) FROM silver_trips").fetchone()[0]
        gold_count = con.execute("SELECT COUNT(*) FROM fct_trip_hourly").fetchone()[0]
        batch_ids = con.execute("SELECT COUNT(DISTINCT source_batch_id) FROM silver_trips").fetchone()[0]
    finally:
        con.close()

    assert silver_count > 0
    assert gold_count > 0
    assert batch_ids == 1


def test_run_pipeline_records_audit_summary(tmp_path):
    summary = run_pipeline(
        source="synthetic",
        start_date=date(2025, 1, 1),
        days=1,
        rows_per_day=120,
        seed=7,
        project_root=tmp_path,
        run_id="audit-run",
    )

    assert summary["status"] == "succeeded"
    assert summary["ingestion"]["row_count"] == 120
    assert summary["transforms"]["silver_rows"] == 120
    assert summary["quality"]["overall_pass"] is True

    con = duckdb.connect(str(tmp_path / "data" / "warehouse.duckdb"), read_only=True)
    try:
        stored_run = con.execute(
            """
            SELECT source_name, status, bronze_rows, silver_rows, gold_rows, quality_passed
            FROM pipeline_runs
            WHERE run_id = 'audit-run'
            """
        ).fetchone()
    finally:
        con.close()

    assert stored_run[0] == "synthetic"
    assert stored_run[1] == "succeeded"
    assert stored_run[2] == 120
    assert stored_run[3] == 120
    assert stored_run[4] > 0
    assert stored_run[5] is True
    assert (tmp_path / "data" / "monitoring" / "pipeline_runs" / "audit-run.json").exists()


def test_transforms_keep_latest_duplicate_record(tmp_path):
    paths = Paths(
        project_root=tmp_path,
        data_dir=tmp_path / "data",
        bronze_dir=tmp_path / "data" / "bronze" / "trips",
        silver_dir=tmp_path / "data" / "silver",
        gold_dir=tmp_path / "data" / "gold",
        monitoring_dir=tmp_path / "data" / "monitoring",
        warehouse_path=tmp_path / "data" / "warehouse.duckdb",
        quality_report_path=tmp_path / "data" / "quality_report.json",
    )

    df = generate_trips(start_date=date(2025, 1, 1), days=1, rows_per_day=10, seed=7)
    duplicate = df.iloc[[0]].copy()
    duplicate["fare_amount"] = 99.99
    duplicate["total_amount"] = 100.49

    write_bronze(
        df,
        paths.bronze_dir,
        batch_id="batch-1",
        ingested_at=datetime(2025, 1, 1, tzinfo=UTC),
    )
    write_bronze(
        duplicate,
        paths.bronze_dir,
        batch_id="batch-2",
        ingested_at=datetime(2025, 1, 2, tzinfo=UTC),
    )

    run_transforms(
        warehouse_path=paths.warehouse_path,
        bronze_dir=paths.bronze_dir,
        silver_dir=paths.silver_dir,
        gold_dir=paths.gold_dir,
    )

    con = duckdb.connect(str(paths.warehouse_path), read_only=True)
    try:
        row = con.execute(
            """
            SELECT COUNT(*), MAX(source_batch_id), MAX(fare_amount)
            FROM silver_trips
            WHERE trip_id = ?
            """,
            [duplicate.iloc[0]["trip_id"]],
        ).fetchone()
    finally:
        con.close()

    assert row == (1, "batch-2", 99.99)


def test_ingest_nyc_tlc_normalizes_real_schema(tmp_path):
    paths = Paths(
        project_root=tmp_path,
        data_dir=tmp_path / "data",
        bronze_dir=tmp_path / "data" / "bronze" / "trips",
        silver_dir=tmp_path / "data" / "silver",
        gold_dir=tmp_path / "data" / "gold",
        monitoring_dir=tmp_path / "data" / "monitoring",
        warehouse_path=tmp_path / "data" / "warehouse.duckdb",
        quality_report_path=tmp_path / "data" / "quality_report.json",
    )
    source_file = tmp_path / "yellow_tripdata_2025-01.parquet"
    raw_df = pd.DataFrame(
        {
            "VendorID": [1, 2],
            "tpep_pickup_datetime": ["2025-01-01 08:00:00", "2025-01-01 09:00:00"],
            "tpep_dropoff_datetime": ["2025-01-01 08:12:00", "2025-01-01 09:18:00"],
            "passenger_count": [1, 2],
            "trip_distance": [1.4, 2.6],
            "PULocationID": [161, 162],
            "DOLocationID": [230, 48],
            "fare_amount": [12.5, 18.75],
            "tip_amount": [2.5, 0.0],
            "total_amount": [18.3, 19.25],
            "payment_type": [1, 2],
        }
    )
    raw_df.to_parquet(source_file, index=False)

    result, min_pickup_date = ingest_nyc_tlc(
        bronze_dir=paths.bronze_dir,
        batch_id="tlc-batch",
        ingested_at=datetime(2025, 1, 2, tzinfo=UTC),
        dataset="yellow",
        source_path=str(source_file),
    )

    bronze_df = pd.read_parquet(result.written_paths[0])
    assert result.source_name == "nyc_tlc_yellow"
    assert result.row_count == 2
    assert min_pickup_date == date(2025, 1, 1)
    assert set(bronze_df.columns) >= {
        "trip_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_zone_id",
        "dropoff_zone_id",
        "payment_type",
        "source_batch_id",
        "ingested_at",
    }
    assert bronze_df["payment_type"].tolist() == ["card", "cash"]


def test_run_pipeline_supports_nyc_tlc_source(tmp_path):
    source_file = tmp_path / "yellow_tripdata_2025-01.parquet"
    raw_df = pd.DataFrame(
        {
            "VendorID": [1, 2],
            "tpep_pickup_datetime": ["2025-01-01 08:00:00", "2025-01-01 09:00:00"],
            "tpep_dropoff_datetime": ["2025-01-01 08:12:00", "2025-01-01 09:18:00"],
            "passenger_count": [1, 2],
            "trip_distance": [1.4, 2.6],
            "PULocationID": [161, 162],
            "DOLocationID": [230, 48],
            "fare_amount": [12.5, 18.75],
            "tip_amount": [2.5, 0.0],
            "total_amount": [18.3, 19.25],
            "payment_type": [1, 2],
        }
    )
    raw_df.to_parquet(source_file, index=False)

    summary = run_pipeline(
        source="nyc_tlc",
        tlc_dataset="yellow",
        source_path=str(source_file),
        project_root=tmp_path,
        run_id="tlc-run",
    )

    assert summary["status"] == "succeeded"
    assert summary["ingestion"]["source_name"] == "nyc_tlc_yellow"
    assert summary["ingestion"]["row_count"] == 2
    assert summary["quality"]["overall_pass"] is True
