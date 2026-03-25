from __future__ import annotations

from pathlib import Path

import pandas as pd


NYC_TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
NYC_TLC_PAYMENT_TYPES = {
    1: "card",
    2: "cash",
    3: "no_charge",
    4: "dispute",
    5: "unknown",
    6: "voided",
}
NYC_TLC_DATASET_COLUMN_MAPS = {
    "yellow": {
        "pickup_datetime": "tpep_pickup_datetime",
        "dropoff_datetime": "tpep_dropoff_datetime",
    },
    "green": {
        "pickup_datetime": "lpep_pickup_datetime",
        "dropoff_datetime": "lpep_dropoff_datetime",
    },
}


def build_nyc_tlc_url(year: int, month: int, dataset: str = "yellow") -> str:
    return f"{NYC_TLC_BASE_URL}/{dataset}_tripdata_{year}-{month:02d}.parquet"


def load_nyc_tlc_trip_data(
    dataset: str = "yellow",
    year: int | None = None,
    month: int | None = None,
    source_path: str | Path | None = None,
) -> tuple[pd.DataFrame, str]:
    if dataset not in NYC_TLC_DATASET_COLUMN_MAPS:
        raise ValueError(f"Unsupported NYC TLC dataset: {dataset}")

    if source_path is None:
        if year is None or month is None:
            raise ValueError("Either source_path or both year and month must be provided for NYC TLC ingestion")
        source_path = build_nyc_tlc_url(year, month, dataset=dataset)

    column_map = NYC_TLC_DATASET_COLUMN_MAPS[dataset]
    columns = [
        column_map["pickup_datetime"],
        column_map["dropoff_datetime"],
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type",
        "VendorID",
    ]
    available_columns = [column for column in columns if column != "VendorID"]

    try:
        raw_df = pd.read_parquet(source_path, columns=columns)
    except Exception:
        raw_df = pd.read_parquet(source_path, columns=available_columns)

    normalized_df = normalize_nyc_tlc_trip_data(raw_df, dataset=dataset)
    return normalized_df, str(source_path)


def normalize_nyc_tlc_trip_data(raw_df: pd.DataFrame, dataset: str = "yellow") -> pd.DataFrame:
    if dataset not in NYC_TLC_DATASET_COLUMN_MAPS:
        raise ValueError(f"Unsupported NYC TLC dataset: {dataset}")

    column_map = NYC_TLC_DATASET_COLUMN_MAPS[dataset]
    renamed_df = raw_df.rename(
        columns={
            column_map["pickup_datetime"]: "pickup_datetime",
            column_map["dropoff_datetime"]: "dropoff_datetime",
            "PULocationID": "pickup_zone_id",
            "DOLocationID": "dropoff_zone_id",
        }
    ).copy()

    renamed_df["pickup_datetime"] = pd.to_datetime(renamed_df["pickup_datetime"], errors="coerce")
    renamed_df["dropoff_datetime"] = pd.to_datetime(renamed_df["dropoff_datetime"], errors="coerce")

    numeric_columns = [
        "passenger_count",
        "trip_distance",
        "pickup_zone_id",
        "dropoff_zone_id",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type",
    ]
    for column in numeric_columns:
        renamed_df[column] = pd.to_numeric(renamed_df[column], errors="coerce")

    trip_id_inputs = pd.DataFrame(
        {
            "pickup_datetime": renamed_df["pickup_datetime"].astype(str),
            "dropoff_datetime": renamed_df["dropoff_datetime"].astype(str),
            "pickup_zone_id": renamed_df["pickup_zone_id"].astype("Int64").astype(str),
            "dropoff_zone_id": renamed_df["dropoff_zone_id"].astype("Int64").astype(str),
            "fare_amount": renamed_df["fare_amount"].round(2).astype(str),
            "total_amount": renamed_df["total_amount"].round(2).astype(str),
            "vendor_id": renamed_df.get("VendorID", pd.Series(index=renamed_df.index, dtype="object")).astype(str),
        }
    )
    trip_hashes = pd.util.hash_pandas_object(trip_id_inputs, index=False).astype(str)

    normalized_df = pd.DataFrame(
        {
            "trip_id": "tlc_" + trip_hashes,
            "pickup_datetime": renamed_df["pickup_datetime"],
            "dropoff_datetime": renamed_df["dropoff_datetime"],
            "passenger_count": renamed_df["passenger_count"].fillna(0).astype(int),
            "trip_distance": renamed_df["trip_distance"].fillna(0.0).round(2),
            "pickup_zone_id": renamed_df["pickup_zone_id"].fillna(0).astype(int),
            "dropoff_zone_id": renamed_df["dropoff_zone_id"].fillna(0).astype(int),
            "fare_amount": renamed_df["fare_amount"].fillna(0.0).round(2),
            "tip_amount": renamed_df["tip_amount"].fillna(0.0).round(2),
            "total_amount": renamed_df["total_amount"].fillna(0.0).round(2),
            "payment_type": renamed_df["payment_type"].astype("Int64").map(NYC_TLC_PAYMENT_TYPES).fillna("unknown"),
        }
    )

    return normalized_df
