"""Carga del resultado final en una tabla Delta."""

from pyspark.sql import DataFrame


def write_profit_report(
    df: DataFrame, catalog: str, schema: str, table_name: str
) -> None:
    """Escribe el reporte de margen como una tabla Delta sin particionar.

    Sin partitionBy (Decisión 4): el resultado tiene ~100-150 filas; particionar
    generaría archivos diminutos y sobrecarga de metadata en el _delta_log sin
    ningún beneficio real de partition pruning para una tabla tan pequeña.
    """
    df.write.mode("overwrite").partitionBy("n_name", "order_year").saveAsTable(
        f"{catalog}.{schema}.{table_name}"
    )
