"""Configuración central del pipeline de rentabilidad TPC-H."""

CATALOG = "samples"
SCHEMA = "tpch"
PART_NAME_FILTER = "green"

# Catálogo/esquema/tabla de salida, deben ser escribibles (a diferencia de
# samples.tpch, que es de solo lectura) para persistir el reporte de margen.
OUTPUT_CATALOG = "main"
OUTPUT_SCHEMA = "default"
OUTPUT_TABLE = "profit_margin_by_nation_and_year"

# Tarifa aproximada en USD por DBU-hora para compute Serverless (Premium, US).
# Es una referencia, NO un precio oficial fijo: varía por nube, región y
# producto (SQL Serverless vs. Jobs Serverless). Verifica el valor real
# con la calculadora de precios de Databricks antes de usar esto para
# reportar costos reales a un cliente.
SERVERLESS_DBU_RATE_USD = 0.70
