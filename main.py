## FIRST PART: lectura y conteo de las 6 tablas fuente
from databricks.connect import DatabricksSession
from src.etl.extract import (
    read_lineitem,
    read_orders,
    read_partsupp,
    read_part,
    read_supplier,
    read_nation,
)

spark = DatabricksSession.builder.serverless().getOrCreate()

print("=" * 60)
print("CONTEO DE FILAS POR TABLA FUENTE")
print("=" * 60)
for name, df in [
    ("lineitem", read_lineitem(spark)),
    ("orders", read_orders(spark)),
    ("part", read_part(spark)),
    ("supplier", read_supplier(spark)),
    ("nation", read_nation(spark)),
    ("partsupp", read_partsupp(spark)),
]:
    print(name, df.count())


## SECOND PART: pipeline de margen de rentabilidad (partes 'green' por nación/año)
from src.etl.transform import *


# Dimensión reducida: part filtrado + partsupp + supplier + nation (Decisión 2)
green_part_dimension = build_green_part_dimension(
    part=read_part(spark),
    partsupp=read_partsupp(spark),
    supplier=read_supplier(spark),
    nation=read_nation(spark),
    part_name_filter="green",
)


# Une la dimensión contra lineitem (broadcast) y luego contra orders (Decisión 1)
joined_data = join_with_lineitem_and_orders(
    dimension=green_part_dimension,
    lineitem=read_lineitem(spark),
    orders=read_orders(spark),
)

print("\n" + "=" * 60)
print("PROMEDIOS SOBRE LAS LÍNEAS DE PEDIDO YA UNIDAS (control de calidad)")
print("=" * 60)
joined_data.select(
    F.avg("l_extendedprice").alias("avg_extendedprice"),
    F.avg("l_discount").alias("avg_discount"),
    F.avg("l_quantity").alias("avg_quantity"),
    F.avg("ps_supplycost").alias("avg_supplycost"),
    F.count("*").alias("total_lineas"),
).show()


# Agregación final: margen de rentabilidad por nación y año
profit_margin_by_nation_and_year = calculate_profit_margin(joined_data)

print("\n" + "=" * 60)
print("MARGEN DE RENTABILIDAD POR NACIÓN Y AÑO (partes 'green')")
print("=" * 60)
profit_margin_by_nation_and_year.show()
