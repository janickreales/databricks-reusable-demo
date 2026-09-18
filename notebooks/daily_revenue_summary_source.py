# Databricks notebook source
from pyspark.sql import functions as F

df = spark.table("samples.nyctaxi.trips")
total = df.count()
positive_fares = df.filter(F.col("fare_amount") > 0).count()
positive_distance = df.filter(F.col("trip_distance") > 0).count()
print(total, positive_fares, positive_distance)
# COMMAND ----------
small_lookup = spark.range(5).withColumnRenamed("id", "zone_group")
result = (
    df.join(F.broadcast(df.groupBy("pickup_zip").count()), "pickup_zip")
    .groupBy("pickup_zip")
    .agg(F.sum("fare_amount").alias("total_fare"))
)
result.write.mode("overwrite").partitionBy("pickup_zip").saveAsTable(
    "prod.analytics.daily_revenue"
)
