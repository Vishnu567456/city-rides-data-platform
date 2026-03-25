from __future__ import annotations

from datetime import date
from uuid import uuid4

import numpy as np
import pandas as pd


PAYMENT_TYPES = ["card", "cash", "app"]


def generate_trips(
    start_date: date,
    days: int,
    rows_per_day: int,
    seed: int | None = 42,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    total_rows = days * rows_per_day

    day_offsets = np.repeat(np.arange(days), rows_per_day)
    base_dates = pd.to_datetime(start_date) + pd.to_timedelta(day_offsets, unit="D")

    pickup_seconds = rng.integers(0, 24 * 60 * 60, size=total_rows)
    pickup_datetimes = base_dates + pd.to_timedelta(pickup_seconds, unit="s")

    duration_seconds = rng.integers(3 * 60, 45 * 60, size=total_rows)
    dropoff_datetimes = pickup_datetimes + pd.to_timedelta(duration_seconds, unit="s")

    passenger_count = rng.integers(1, 6, size=total_rows)
    trip_distance = rng.gamma(shape=2.0, scale=1.5, size=total_rows).round(2)

    base_fare = (trip_distance * rng.uniform(2.2, 3.1, size=total_rows)) + rng.normal(3.0, 1.0, size=total_rows)
    base_fare = np.clip(base_fare, 3.0, None).round(2)

    tip_rate = rng.choice([0.0, 0.1, 0.15, 0.2], size=total_rows, p=[0.2, 0.35, 0.3, 0.15])
    tip_amount = (base_fare * tip_rate).round(2)

    total_amount = (base_fare + tip_amount + 0.5).round(2)

    pickup_zone_id = rng.integers(1, 51, size=total_rows)
    dropoff_zone_id = rng.integers(1, 51, size=total_rows)

    payment_type = rng.choice(PAYMENT_TYPES, size=total_rows)

    df = pd.DataFrame(
        {
            "trip_id": [str(uuid4()) for _ in range(total_rows)],
            "pickup_datetime": pickup_datetimes,
            "dropoff_datetime": dropoff_datetimes,
            "passenger_count": passenger_count,
            "trip_distance": trip_distance,
            "pickup_zone_id": pickup_zone_id,
            "dropoff_zone_id": dropoff_zone_id,
            "fare_amount": base_fare,
            "tip_amount": tip_amount,
            "total_amount": total_amount,
            "payment_type": payment_type,
        }
    )

    return df
