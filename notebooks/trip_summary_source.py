# Databricks notebook source
# MAGIC %md
# MAGIC # Resumen de viajes NYC Taxi
# MAGIC Análisis exploratorio, calidad de datos, features derivados y detección de anomalías sobre `samples.nyctaxi.trips`.

# COMMAND ----------

CATALOG = "samples"
SCHEMA = "nyctaxi"
TABLE = "trips"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga y validación inicial

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE}")
print(f"Filas totales: {df.count()}")
df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

invalid_distance = df.filter(F.col("trip_distance") <= 0).count()
invalid_fare = df.filter(F.col("fare_amount") <= 0).count()
invalid_times = df.filter(F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime")).count()

print(f"Distancia <= 0: {invalid_distance}")
print(f"Tarifa <= 0: {invalid_fare}")
print(f"Dropoff antes que pickup: {invalid_times}")

df_clean = df.filter(
    (F.col("trip_distance") > 0)
    & (F.col("fare_amount") > 0)
    & (F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
)
print(f"Filas después de limpiar: {df_clean.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingeniería de features

# COMMAND ----------

df_features = (
    df_clean
    .withColumn(
        "trip_duration_min",
        (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) / 60,
    )
    .withColumn("avg_speed_mph", F.col("trip_distance") / (F.col("trip_duration_min") / 60))
    .withColumn("fare_per_mile", F.col("fare_amount") / F.col("trip_distance"))
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn("is_weekend", F.dayofweek("tpep_pickup_datetime").isin([1, 7]))
)
df_features.select("trip_duration_min", "avg_speed_mph", "fare_per_mile", "pickup_hour", "is_weekend").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregaciones exploratorias (Python)

# COMMAND ----------

top_zips = (
    df_features.groupBy("pickup_zip")
    .agg(
        F.count("*").alias("total_trips"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("trip_distance").alias("avg_distance"),
        F.avg("trip_duration_min").alias("avg_duration_min"),
    )
    .orderBy(F.desc("total_trip"))
    .limit(10)
)
top_zips.show()

# COMMAND ----------

demand_by_hour = (
    df_features.groupBy("pickup_hour")
    .agg(F.count("*").alias("total_trips"), F.avg("fare_amount").alias("avg_fare"))
    .orderBy("pickup_hour")
)
demand_by_hour.show(24)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Las mismas preguntas, en SQL puro

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   pickup_zip,
# MAGIC   COUNT(*) AS total_trips,
# MAGIC   SUM(fare_amount) AS total_revenue
# MAGIC FROM samples.nyctaxi.trips
# MAGIC WHERE trip_distance > 0 AND fare_amount > 0
# MAGIC GROUP BY pickup_zip
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   HOUR(tpep_pickup_datetime) AS pickup_hour,
# MAGIC   COUNT(*) AS total_trips
# MAGIC FROM samples.nyctaxi.trips
# MAGIC GROUP BY HOUR(tpep_pickup_datetime)
# MAGIC ORDER BY pickup_hour

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH stats AS (
# MAGIC   SELECT AVG(fare_amount / trip_distance) AS avg_fare_per_mile
# MAGIC   FROM samples.nyctaxi.trips
# MAGIC   WHERE trip_distance > 0
# MAGIC )
# MAGIC SELECT t.*, (t.fare_amount / t.trip_distance) AS fare_per_mile
# MAGIC FROM samples.nyctaxi.trips t, stats
# MAGIC WHERE t.trip_distance > 0
# MAGIC   AND (t.fare_amount / t.trip_distance) > 3 * stats.avg_fare_per_mile
# MAGIC ORDER BY fare_per_mile DESC
# MAGIC LIMIT 20