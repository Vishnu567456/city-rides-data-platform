from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime

import duckdb

from .config import Paths
from .ingest import IngestionResult
from .transform import TransformResult


def to_utc_isoformat(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def build_run_summary(
    run_id: str,
    status: str,
    started_at: datetime,
    completed_at: datetime,
    config: dict,
    ingest_result: IngestionResult | None,
    transform_result: TransformResult | None,
    quality_report: dict | None,
    error_message: str | None,
) -> dict:
    checks = quality_report["checks"] if quality_report else []

    return {
        "run_id": run_id,
        "status": status,
        "started_at": to_utc_isoformat(started_at),
        "completed_at": to_utc_isoformat(completed_at),
        "duration_seconds": round((completed_at - started_at).total_seconds(), 2),
        "config": config,
        "ingestion": asdict(ingest_result) if ingest_result else None,
        "transforms": asdict(transform_result) if transform_result else None,
        "quality": quality_report,
        "checks_passed": sum(1 for check in checks if check["passed"]),
        "checks_failed": sum(1 for check in checks if not check["passed"]),
        "error_message": error_message,
    }


def _create_pipeline_runs_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR PRIMARY KEY,
            source_name VARCHAR,
            source_reference VARCHAR,
            status VARCHAR,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            duration_seconds DOUBLE,
            data_start_date DATE,
            days INTEGER,
            rows_per_day INTEGER,
            seed INTEGER,
            bronze_rows INTEGER,
            bronze_files INTEGER,
            silver_rows INTEGER,
            gold_rows INTEGER,
            dim_zone_rows INTEGER,
            quality_passed BOOLEAN,
            checks_passed INTEGER,
            checks_failed INTEGER,
            error_message VARCHAR,
            summary_path VARCHAR
        )
        """
    )
    con.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS source_name VARCHAR")
    con.execute("ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS source_reference VARCHAR")


def persist_run_summary(paths: Paths, summary: dict) -> dict:
    summary_dir = paths.monitoring_dir / "pipeline_runs"
    summary_dir.mkdir(parents=True, exist_ok=True)
    summary_path = summary_dir / f"{summary['run_id']}.json"
    summary["summary_path"] = str(summary_path)

    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)

    con = duckdb.connect(str(paths.warehouse_path))
    try:
        _create_pipeline_runs_table(con)

        ingestion = summary.get("ingestion") or {}
        transforms = summary.get("transforms") or {}
        config = summary["config"]
        quality = summary.get("quality") or {}

        con.execute(
            """
            INSERT OR REPLACE INTO pipeline_runs (
                run_id,
                source_name,
                source_reference,
                status,
                started_at,
                completed_at,
                duration_seconds,
                data_start_date,
                days,
                rows_per_day,
                seed,
                bronze_rows,
                bronze_files,
                silver_rows,
                gold_rows,
                dim_zone_rows,
                quality_passed,
                checks_passed,
                checks_failed,
                error_message,
                summary_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                summary["run_id"],
                ingestion.get("source_name"),
                ingestion.get("source_reference"),
                summary["status"],
                summary["started_at"],
                summary["completed_at"],
                summary["duration_seconds"],
                config["start_date"],
                config["days"],
                config["rows_per_day"],
                config["seed"],
                ingestion.get("row_count"),
                ingestion.get("file_count"),
                transforms.get("silver_rows"),
                transforms.get("gold_rows"),
                transforms.get("dim_zone_rows"),
                quality.get("overall_pass"),
                summary["checks_passed"],
                summary["checks_failed"],
                summary["error_message"],
                summary["summary_path"],
            ],
        )
    finally:
        con.close()

    return summary
