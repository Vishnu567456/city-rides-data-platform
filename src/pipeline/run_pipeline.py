from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv

from .config import resolve_paths
from .ingest import ingest_nyc_tlc, ingest_synthetic
from .monitoring import build_run_summary, persist_run_summary
from .quality import run_quality_checks
from .transform import run_transforms
from .utils import get_logger

logger = get_logger(__name__)


def env_int(name: str, default: int | None = None) -> int | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def parse_args() -> argparse.Namespace:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Run the City Rides Lakehouse pipeline")
    parser.add_argument(
        "--source",
        choices=["synthetic", "nyc_tlc"],
        default=os.getenv("PIPELINE_SOURCE", "synthetic"),
        help="Data source to ingest",
    )
    parser.add_argument("--start-date", default=os.getenv("PIPELINE_START_DATE"), help="Start date in YYYY-MM-DD format")
    parser.add_argument("--days", type=int, default=env_int("PIPELINE_DAYS", 7), help="Number of days to generate")
    parser.add_argument("--rows-per-day", type=int, default=env_int("PIPELINE_ROWS_PER_DAY", 5000), help="Rows per day")
    parser.add_argument("--seed", type=int, default=env_int("PIPELINE_SEED", 42), help="Random seed")
    parser.add_argument("--run-id", default=os.getenv("PIPELINE_RUN_ID"), help="Optional run identifier for audit tracking")
    parser.add_argument(
        "--tlc-dataset",
        choices=["yellow", "green"],
        default=os.getenv("NYC_TLC_DATASET", "yellow"),
        help="NYC TLC dataset to ingest when --source nyc_tlc is used",
    )
    parser.add_argument("--tlc-year", type=int, default=env_int("NYC_TLC_YEAR"), help="NYC TLC year")
    parser.add_argument("--tlc-month", type=int, default=env_int("NYC_TLC_MONTH"), help="NYC TLC month")
    parser.add_argument(
        "--source-path",
        default=os.getenv("NYC_TLC_SOURCE_PATH"),
        help="Optional local path or URL for a source parquet file",
    )
    return parser.parse_args()


def build_run_id() -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"run_{timestamp}_{uuid4().hex[:8]}"


def run_pipeline(
    source: str = "synthetic",
    start_date: date | None = None,
    days: int | None = 7,
    rows_per_day: int | None = 5000,
    seed: int | None = 42,
    tlc_dataset: str = "yellow",
    tlc_year: int | None = None,
    tlc_month: int | None = None,
    source_path: str | None = None,
    project_root: Path | None = None,
    run_id: str | None = None,
) -> dict:
    paths = resolve_paths(project_root)
    run_id = run_id or build_run_id()
    started_at = datetime.now(UTC)
    if source == "synthetic" and start_date is None:
        raise ValueError("start_date is required when source='synthetic'")
    if source == "nyc_tlc" and source_path is None and (tlc_year is None or tlc_month is None):
        raise ValueError("tlc_year and tlc_month are required when source='nyc_tlc' without source_path")

    ingestion_result = None
    transform_result = None
    quality_report = None
    status = "failed"
    error_message = None
    captured_error: tuple[type[BaseException], BaseException, object] | None = None
    resolved_start_date = start_date

    try:
        logger.info("Starting ingestion")
        if source == "synthetic":
            ingestion_result = ingest_synthetic(
                bronze_dir=paths.bronze_dir,
                start_date=start_date,
                days=days or 7,
                rows_per_day=rows_per_day or 5000,
                batch_id=run_id,
                ingested_at=started_at,
                seed=seed,
            )
        else:
            ingestion_result, derived_start_date = ingest_nyc_tlc(
                bronze_dir=paths.bronze_dir,
                batch_id=run_id,
                ingested_at=started_at,
                dataset=tlc_dataset,
                year=tlc_year,
                month=tlc_month,
                source_path=source_path,
            )
            resolved_start_date = resolved_start_date or derived_start_date

        logger.info("Running transforms")
        transform_result = run_transforms(
            warehouse_path=paths.warehouse_path,
            bronze_dir=paths.bronze_dir,
            silver_dir=paths.silver_dir,
            gold_dir=paths.gold_dir,
        )

        logger.info("Running quality checks")
        quality_report = run_quality_checks(
            warehouse_path=paths.warehouse_path,
            report_path=paths.quality_report_path,
            raise_on_failure=False,
        )
        if not quality_report["overall_pass"]:
            raise ValueError("Quality checks failed")

        status = "succeeded"
    except Exception:
        captured_error = sys.exc_info()
        error_message = str(captured_error[1])
        logger.exception("Pipeline failed")

    config = {
        "source": source,
        "start_date": resolved_start_date.isoformat() if resolved_start_date else None,
        "days": days if source == "synthetic" else None,
        "rows_per_day": rows_per_day if source == "synthetic" else None,
        "seed": seed if source == "synthetic" else None,
        "tlc_dataset": tlc_dataset if source == "nyc_tlc" else None,
        "tlc_year": tlc_year if source == "nyc_tlc" else None,
        "tlc_month": tlc_month if source == "nyc_tlc" else None,
        "source_path": source_path,
    }
    completed_at = datetime.now(UTC)
    summary = build_run_summary(
        run_id=run_id,
        status=status,
        started_at=started_at,
        completed_at=completed_at,
        config=config,
        ingest_result=ingestion_result,
        transform_result=transform_result,
        quality_report=quality_report,
        error_message=error_message,
    )
    persist_run_summary(paths, summary)
    logger.info("Pipeline summary written to %s", summary["summary_path"])

    if captured_error is not None:
        raise captured_error[1].with_traceback(captured_error[2])

    logger.info("Pipeline complete")
    return summary


def main() -> None:
    args = parse_args()
    start_date = date.fromisoformat(args.start_date) if args.start_date else None

    run_pipeline(
        source=args.source,
        start_date=start_date,
        days=args.days,
        rows_per_day=args.rows_per_day,
        seed=args.seed,
        tlc_dataset=args.tlc_dataset,
        tlc_year=args.tlc_year,
        tlc_month=args.tlc_month,
        source_path=args.source_path,
        run_id=args.run_id,
    )


if __name__ == "__main__":
    main()
