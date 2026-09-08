"""Funciones de extracción para el pipeline de rentabilidad TPC-H.

Lee las 6 tablas necesarias desde samples.tpch usando una SparkSession
ya creada (inyectada como parámetro, no creada dentro del módulo).
"""

from pyspark.sql import DataFrame, SparkSession

from src.config import CATALOG, SCHEMA


def read_tpch_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Lee una tabla de samples.tpch por nombre."""
    return spark.read.table(f"{CATALOG}.{SCHEMA}.{table_name}")


# Funciones específicas para cada una de las 6 tablas del pipeline:
# lineitem, part, supplier, partsupp, orders, nation
def read_lineitem(spark: SparkSession) -> DataFrame:
    return read_tpch_table(spark, "lineitem")


def read_part(spark: SparkSession) -> DataFrame:
    return read_tpch_table(spark, "part")


def read_supplier(spark: SparkSession) -> DataFrame:
    return read_tpch_table(spark, "supplier")


def read_partsupp(spark: SparkSession) -> DataFrame:
    return read_tpch_table(spark, "partsupp")


def read_orders(spark: SparkSession) -> DataFrame:
    return read_tpch_table(spark, "orders")


def read_nation(spark: SparkSession) -> DataFrame:
    return read_tpch_table(spark, "nation")
