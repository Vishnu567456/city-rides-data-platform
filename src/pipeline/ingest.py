from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from .generate import generate_trips
from .sources import load_nyc_tlc_trip_data
from .utils import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class IngestionResult:
    source_name: str
    source_reference: str | None
    batch_id: str
    ingested_at: str
    row_count: int
    file_count: int
    written_paths: list[str]


def _sanitize_batch_id(batch_id: str) -> str:
    return "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in batch_id)


def write_bronze(df: pd.DataFrame, bronze_dir: Path, batch_id: str, ingested_at: datetime) -> list[Path]:
    bronze_dir.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["pickup_date"] = pd.to_datetime(df["pickup_datetime"]).dt.date
    df["source_batch_id"] = batch_id
    df["ingested_at"] = ingested_at.isoformat().replace("+00:00", "Z")

    safe_batch_id = _sanitize_batch_id(batch_id)

    written_paths: list[Path] = []
    for pickup_date, partition in df.groupby("pickup_date"):
        partition_dir = bronze_dir / f"pickup_date={pickup_date}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        output_path = partition_dir / f"trips_{safe_batch_id}.parquet"
        partition.drop(columns=["pickup_date"]).to_parquet(output_path, index=False)
        written_paths.append(output_path)

    return written_paths


def build_ingestion_result(
    source_name: str,
    source_reference: str | None,
    batch_id: str,
    ingested_at: datetime,
    df: pd.DataFrame,
    written_paths: list[Path],
) -> IngestionResult:
    return IngestionResult(
        source_name=source_name,
        source_reference=source_reference,
        batch_id=batch_id,
        ingested_at=ingested_at.isoformat().replace("+00:00", "Z"),
        row_count=len(df),
        file_count=len(written_paths),
        written_paths=[str(path) for path in written_paths],
    )


def ingest_synthetic(
    bronze_dir: Path,
    start_date: date,
    days: int,
    rows_per_day: int,
    batch_id: str,
    ingested_at: datetime,
    seed: int | None = 42,
) -> IngestionResult:
    logger.info("Generating synthetic trips")
    df = generate_trips(start_date=start_date, days=days, rows_per_day=rows_per_day, seed=seed)
    logger.info("Writing bronze partitions")
    written_paths = write_bronze(df, bronze_dir, batch_id=batch_id, ingested_at=ingested_at)
    return build_ingestion_result(
        source_name="synthetic",
        source_reference=None,
        batch_id=batch_id,
        ingested_at=ingested_at,
        df=df,
        written_paths=written_paths,
    )


def ingest_nyc_tlc(
    bronze_dir: Path,
    batch_id: str,
    ingested_at: datetime,
    dataset: str = "yellow",
    year: int | None = None,
    month: int | None = None,
    source_path: str | None = None,
) -> tuple[IngestionResult, date | None]:
    logger.info("Loading NYC TLC %s trip data", dataset)
    df, resolved_source_path = load_nyc_tlc_trip_data(
        dataset=dataset,
        year=year,
        month=month,
        source_path=source_path,
    )
    logger.info("Writing bronze partitions")
    written_paths = write_bronze(df, bronze_dir, batch_id=batch_id, ingested_at=ingested_at)
    min_pickup_date = None
    if not df.empty:
        min_pickup_date = pd.to_datetime(df["pickup_datetime"]).dt.date.min()

    return (
        build_ingestion_result(
            source_name=f"nyc_tlc_{dataset}",
            source_reference=resolved_source_path,
            batch_id=batch_id,
            ingested_at=ingested_at,
            df=df,
            written_paths=written_paths,
        ),
        min_pickup_date,
    )
