"""Genera notebooks equivalentes en formato Source (.py) y Jupyter (.ipynb)
a partir de una única definición de celdas, para comparar cómo Copilot
Code Review revisa cada formato.
"""

import json
import os

CELLS = [
    {
        "type": "md",
        "text": "# Resumen de viajes NYC Taxi\nAnálisis exploratorio, calidad de datos, features derivados y detección de anomalías sobre `samples.nyctaxi.trips`.",
    },
    {"type": "py", "text": 'CATALOG = "samples"\nSCHEMA = "nyctaxi"\nTABLE = "trips"'},
    {"type": "md", "text": "## Carga y validación inicial"},
    {
        "type": "py",
        "text": 'df = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE}")\nprint(f"Filas totales: {df.count()}")\ndf.printSchema()',
    },
    {
        "type": "py",
        "text": (
            "from pyspark.sql import functions as F\n\n"
            'invalid_distance = df.filter(F.col("trip_distance") <= 0).count()\n'
            'invalid_fare = df.filter(F.col("fare_amount") <= 0).count()\n'
            'invalid_times = df.filter(F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime")).count()\n\n'
            'print(f"Distancia <= 0: {invalid_distance}")\n'
            'print(f"Tarifa <= 0: {invalid_fare}")\n'
            'print(f"Dropoff antes que pickup: {invalid_times}")\n\n'
            "df_clean = df.filter(\n"
            '    (F.col("trip_distance") > 0)\n'
            '    & (F.col("fare_amount") > 0)\n'
            '    & (F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))\n'
            ")\n"
            'print(f"Filas después de limpiar: {df_clean.count()}")'
        ),
    },
    {"type": "md", "text": "## Ingeniería de features"},
    {
        "type": "py",
        "text": (
            "df_features = (\n"
            "    df_clean\n"
            "    .withColumn(\n"
            '        "trip_duration_min",\n'
            '        (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) / 60,\n'
            "    )\n"
            '    .withColumn("avg_speed_mph", F.col("trip_distance") / (F.col("trip_duration_min") / 60))\n'
            '    .withColumn("fare_per_mile", F.col("fare_amount") / F.col("trip_distance"))\n'
            '    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))\n'
            '    .withColumn("is_weekend", F.dayofweek("tpep_pickup_datetime").isin([1, 7]))\n'
            ")\n"
            'df_features.select("trip_duration_min", "avg_speed_mph", "fare_per_mile", "pickup_hour", "is_weekend").show(5)'
        ),
    },
    {"type": "md", "text": "## Agregaciones exploratorias (Python)"},
    {
        "type": "py",
        "text": (
            "top_zips = (\n"
            '    df_features.groupBy("pickup_zip")\n'
            "    .agg(\n"
            '        F.count("*").alias("total_trips"),\n'
            '        F.avg("fare_amount").alias("avg_fare"),\n'
            '        F.avg("trip_distance").alias("avg_distance"),\n'
            '        F.avg("trip_duration_min").alias("avg_duration_min"),\n'
            "    )\n"
            '    .orderBy(F.desc("total_trips"))\n'
            "    .limit(10)\n"
            ")\n"
            "top_zips.show()"
        ),
    },
    {
        "type": "py",
        "text": (
            "demand_by_hour = (\n"
            '    df_features.groupBy("pickup_hour")\n'
            '    .agg(F.count("*").alias("total_trips"), F.avg("fare_amount").alias("avg_fare"))\n'
            '    .orderBy("pickup_hour")\n'
            ")\n"
            "demand_by_hour.show(24)"
        ),
    },
    {"type": "md", "text": "## Las mismas preguntas, en SQL puro"},
    {
        "type": "sql",
        "text": (
            "SELECT\n"
            "  pickup_zip,\n"
            "  COUNT(*) AS total_trips,\n"
            "  SUM(fare_amount) AS total_revenue\n"
            "FROM samples.nyctaxi.trips\n"
            "WHERE trip_distance > 0 AND fare_amount > 0\n"
            "GROUP BY pickup_zip\n"
            "ORDER BY total_revenue DESC\n"
            "LIMIT 10"
        ),
    },
    {
        "type": "sql",
        "text": (
            "SELECT\n"
            "  HOUR(tpep_pickup_datetime) AS pickup_hour,\n"
            "  COUNT(*) AS total_trips\n"
            "FROM samples.nyctaxi.trips\n"
            "GROUP BY HOUR(tpep_pickup_datetime)\n"
            "ORDER BY pickup_hour"
        ),
    },
    {
        "type": "sql",
        "text": (
            "WITH stats AS (\n"
            "  SELECT AVG(fare_amount / trip_distance) AS avg_fare_per_mile\n"
            "  FROM samples.nyctaxi.trips\n"
            "  WHERE trip_distance > 0\n"
            ")\n"
            "SELECT t.*, (t.fare_amount / t.trip_distance) AS fare_per_mile\n"
            "FROM samples.nyctaxi.trips t, stats\n"
            "WHERE t.trip_distance > 0\n"
            "  AND (t.fare_amount / t.trip_distance) > 3 * stats.avg_fare_per_mile\n"
            "ORDER BY fare_per_mile DESC\n"
            "LIMIT 20"
        ),
    },
]


def write_source_format(cells, path):
    lines = ["# Databricks notebook source"]
    for i, cell in enumerate(cells):
        if i > 0:
            lines.append("# COMMAND ----------")
        if cell["type"] in ("md", "sql"):
            tag = "%md" if cell["type"] == "md" else "%sql"
            lines.append(f"# MAGIC {tag}")
            for line in cell["text"].split("\n"):
                lines.append(f"# MAGIC {line}".rstrip())
        else:
            lines.extend(cell["text"].split("\n"))
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"Escrito {path}")


def write_ipynb_format(cells, path):
    nb_cells = []
    for cell in cells:
        text = cell["text"] if cell["type"] != "sql" else "%sql\n" + cell["text"]
        source = text.split("\n")
        src_list = [l + "\n" for l in source[:-1]] + [source[-1]] if source else []
        nb_cells.append(
            {
                "cell_type": "markdown" if cell["type"] == "md" else "code",
                **(
                    {"execution_count": None, "outputs": []}
                    if cell["type"] != "md"
                    else {}
                ),
                "metadata": {},
                "source": src_list,
            }
        )
    notebook = {
        "cells": nb_cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=1)
    print(f"Escrito {path}")


if __name__ == "__main__":
    os.makedirs("notebooks", exist_ok=True)
    write_source_format(CELLS, "notebooks/trip_summary_source.py")
    write_ipynb_format(CELLS, "notebooks/trip_summary.ipynb")
