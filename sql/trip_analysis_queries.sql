-- Top 10 códigos postales por ingreso total
SELECT
  pickup_zip,
  COUNT(*) AS total_trips,
  SUM(fare_amount) AS total_revenue
FROM samples.nyctaxi.trips
WHERE trip_distance > 0 AND fare_amount > 0
GROUP BY pickup_zip
ORDER BY total_revenue DESC
LIMIT 10;

-- Patrón de demanda por hora del día
SELECT
  HOUR(tpep_pickup_datetime) AS pickup_hour,
  COUNT(*) AS total_trips
FROM samples.nyctaxi.trips
GROUP BY HOUR(tpep_pickup_datetime)
ORDER BY pickup_hour;

-- Anomalías: viajes con tarifa por milla > 3x el promedio general
WITH stats AS (
  SELECT AVG(fare_amount / trip_distance) AS avg_fare_per_mile
  FROM samples.nyctaxi.trips
  WHERE trip_distance > 0
)
SELECT t.*, (t.fare_amount / t.trip_distance) AS fare_per_mile
FROM samples.nyctaxi.trips t, stats
WHERE t.trip_distance > 0
  AND (t.fare_amount / t.trip_distance) > 3 * stats.avg_fare_per_mile
ORDER BY fare_per_mile DESC
LIMIT 20;