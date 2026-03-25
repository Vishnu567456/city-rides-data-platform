from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from .utils import get_logger

logger = get_logger(__name__)


def run_quality_checks(warehouse_path: Path, report_path: Path, raise_on_failure: bool = True) -> dict:
    con = duckdb.connect(str(warehouse_path), read_only=True)
    checks = [
        {
            "name": "null_pickup_datetime",
            "query": "SELECT COUNT(*) FROM silver_trips WHERE pickup_datetime IS NULL",
            "max_failures": 0,
        },
        {
            "name": "null_dropoff_datetime",
            "query": "SELECT COUNT(*) FROM silver_trips WHERE dropoff_datetime IS NULL",
            "max_failures": 0,
        },
        {
            "name": "non_positive_distance",
            "query": "SELECT COUNT(*) FROM silver_trips WHERE trip_distance <= 0",
            "max_failures": 0,
        },
        {
            "name": "non_positive_fare",
            "query": "SELECT COUNT(*) FROM silver_trips WHERE fare_amount <= 0",
            "max_failures": 0,
        },
        {
            "name": "invalid_passenger_count",
            "query": "SELECT COUNT(*) FROM silver_trips WHERE passenger_count < 1 OR passenger_count > 6",
            "max_failures": 0,
        },
        {
            "name": "duplicate_trip_id",
            "query": """
                SELECT COUNT(*)
                FROM (
                    SELECT trip_id
                    FROM silver_trips
                    GROUP BY trip_id
                    HAVING COUNT(*) > 1
                )
            """,
            "max_failures": 0,
        },
        {
            "name": "invalid_trip_duration",
            "query": "SELECT COUNT(*) FROM silver_trips WHERE trip_duration_min <= 0 OR trip_duration_min > 180",
            "max_failures": 0,
        },
        {
            "name": "total_amount_below_fare",
            "query": "SELECT COUNT(*) FROM silver_trips WHERE total_amount < fare_amount",
            "max_failures": 0,
        },
    ]

    results = []
    overall_pass = True

    try:
        for check in checks:
            failures = con.execute(check["query"]).fetchone()[0]
            passed = failures <= check["max_failures"]
            overall_pass = overall_pass and passed
            results.append(
                {
                    "name": check["name"],
                    "failures": int(failures),
                    "max_failures": check["max_failures"],
                    "passed": passed,
                }
            )
    finally:
        con.close()

    report = {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "overall_pass": overall_pass,
        "checks": results,
    }

    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)

    if not overall_pass:
        failed = [r for r in results if not r["passed"]]
        logger.error("Quality checks failed: %s", failed)
        if raise_on_failure:
            raise ValueError("Quality checks failed")
    else:
        logger.info("Quality checks passed")
    return report
