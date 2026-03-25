SELECT
  pickup_date,
  pickup_hour,
  pickup_zone_id AS zone_id,
  COUNT(*) AS trip_count,
  ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
  ROUND(AVG(fare_amount), 2) AS avg_fare_amount,
  ROUND(SUM(total_amount), 2) AS total_revenue
FROM {{ ref('stg_trips') }}
GROUP BY 1, 2, 3
