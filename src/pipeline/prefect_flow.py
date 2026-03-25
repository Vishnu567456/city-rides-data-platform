from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

os.environ.setdefault("PREFECT_HOME", str(Path(".prefect").resolve()))

from prefect import flow, get_run_logger, task

from .run_pipeline import env_int, run_pipeline


@task(name="run_city_rides_pipeline", retries=2, retry_delay_seconds=30)
def execute_pipeline(**kwargs) -> dict:
    return run_pipeline(**kwargs)


@flow(name="city-rides-data-platform")
def city_rides_flow(
    source: str = "synthetic",
    start_date: date | None = None,
    days: int | None = 7,
    rows_per_day: int | None = 5000,
    seed: int | None = 42,
    tlc_dataset: str = "yellow",
    tlc_year: int | None = None,
    tlc_month: int | None = None,
    source_path: str | None = None,
    run_id: str | None = None,
) -> dict:
    logger = get_run_logger()
    logger.info("Launching City Rides flow for source=%s", source)
    summary = execute_pipeline(
        source=source,
        start_date=start_date,
        days=days,
        rows_per_day=rows_per_day,
        seed=seed,
        tlc_dataset=tlc_dataset,
        tlc_year=tlc_year,
        tlc_month=tlc_month,
        source_path=source_path,
        run_id=run_id,
    )
    logger.info("Flow completed with run_id=%s and status=%s", summary["run_id"], summary["status"])
    return summary


def parse_args() -> argparse.Namespace:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Run the City Rides Lakehouse Prefect flow")
    parser.add_argument(
        "--source",
        choices=["synthetic", "nyc_tlc"],
        default=os.getenv("PIPELINE_SOURCE", "synthetic"),
    )
    parser.add_argument("--start-date", default=os.getenv("PIPELINE_START_DATE"))
    parser.add_argument("--days", type=int, default=env_int("PIPELINE_DAYS", 7))
    parser.add_argument("--rows-per-day", type=int, default=env_int("PIPELINE_ROWS_PER_DAY", 5000))
    parser.add_argument("--seed", type=int, default=env_int("PIPELINE_SEED", 42))
    parser.add_argument("--run-id", default=os.getenv("PIPELINE_RUN_ID"))
    parser.add_argument("--tlc-dataset", choices=["yellow", "green"], default=os.getenv("NYC_TLC_DATASET", "yellow"))
    parser.add_argument("--tlc-year", type=int, default=env_int("NYC_TLC_YEAR"))
    parser.add_argument("--tlc-month", type=int, default=env_int("NYC_TLC_MONTH"))
    parser.add_argument("--source-path", default=os.getenv("NYC_TLC_SOURCE_PATH"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_date = date.fromisoformat(args.start_date) if args.start_date else None
    flow_kwargs = {
        "source": args.source,
        "start_date": start_date,
        "days": args.days,
        "rows_per_day": args.rows_per_day,
        "seed": args.seed,
        "tlc_dataset": args.tlc_dataset,
        "tlc_year": args.tlc_year,
        "tlc_month": args.tlc_month,
        "source_path": args.source_path,
        "run_id": args.run_id,
    }

    try:
        city_rides_flow(**flow_kwargs)
    except RuntimeError as exc:
        if "Unable to find an available port" not in str(exc):
            raise

        print("Prefect local API server is unavailable in this environment; falling back to direct pipeline execution.")
        run_pipeline(**flow_kwargs)


if __name__ == "__main__":
    main()
