from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


def build_green_part_dimension(
    part: DataFrame,
    partsupp: DataFrame,
    supplier: DataFrame,
    nation: DataFrame,
    part_name_filter: str,
) -> DataFrame:
    part_filtered = broadcast(
        part.filter(F.col("p_name").contains(part_name_filter)).select("p_partkey")
    )
    supplier_selected = broadcast(supplier.select("s_suppkey", "s_nationkey"))
    nation_selected = broadcast(nation.select("n_nationkey", "n_name"))

    return (
        part_filtered.join(
            partsupp.select("ps_partkey", "ps_suppkey", "ps_supplycost"),
            part_filtered.p_partkey == F.col("ps_partkey"),
        )
        .join(supplier_selected, F.col("ps_suppkey") == F.col("s_suppkey"))
        .join(nation_selected, F.col("s_nationkey") == F.col("n_nationkey"))
        .select("ps_partkey", "ps_suppkey", "ps_supplycost", "n_name")
    )


def join_with_lineitem_and_orders(
    dimension: DataFrame,
    lineitem: DataFrame,
    orders: DataFrame,
) -> DataFrame:
    return (
        lineitem.join(
            broadcast(dimension),
            (lineitem.l_partkey == dimension.ps_partkey)
            & (lineitem.l_suppkey == dimension.ps_suppkey),
        )
        .join(orders, lineitem.l_orderkey == orders.o_orderkey)
        .select(
            "n_name",
            "o_orderdate",
            "l_extendedprice",
            "l_discount",
            "l_quantity",
            "ps_supplycost",
        )
    )


def calculate_profit_margin(joined_data: DataFrame) -> DataFrame:
    return (
        joined_data.withColumn(
            "profit_margin",
            F.col("l_extendedprice") * (1 - F.col("l_discount"))
            - F.col("ps_supplycost") * F.col("l_quantity"),
        )
        .groupBy("n_name", F.year("o_orderdate").alias("order_year"))
        .agg(F.sum("profit_margin").alias("total_profit"))
    )


"""Transformaciones para el cálculo de margen de rentabilidad por nación/año.

Implementa el orden de joins documentado en DECISIONS.md: primero se construye
una dimensión pequeña (part filtrado + partsupp + supplier + nation) usando
broadcast joins encadenados, y solo al final se une contra lineitem — la tabla
de 30M filas — como el lado grande de un broadcast join, para evitar shufflearla.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


def build_green_part_dimension(
    part: DataFrame,
    partsupp: DataFrame,
    supplier: DataFrame,
    nation: DataFrame,
    part_name_filter: str,
) -> DataFrame:
    """Construye la dimensión part+partsupp+supplier+nation, ya filtrada y reducida.

    Filtra `part` por nombre ANTES de cualquier join (Decisión 3), y encadena
    broadcast joins entre las 4 tablas pequeñas (Decisión 2), proyectando solo
    las columnas necesarias en cada paso para mantener el tamaño del broadcast
    bajo control (ps_partkey, ps_suppkey, ps_supplycost, n_name).
    """
    part_filtered = part.filter(F.col("p_name").contains(part_name_filter)).select(
        "p_partkey", "p_name"
    )
    partsupp_selected = partsupp.select("ps_partkey", "ps_suppkey", "ps_supplycost")
    supplier_selected = supplier.select("s_suppkey", "s_nationkey")
    nation_selected = nation.select("n_nationkey", "n_name")

    return (
        broadcast(part_filtered)
        .join(
            broadcast(partsupp_selected),
            part_filtered.p_partkey == partsupp_selected.ps_partkey,
        )
        .join(
            broadcast(supplier_selected),
            partsupp_selected.ps_suppkey == supplier_selected.s_suppkey,
        )
        .join(
            broadcast(nation_selected),
            supplier_selected.s_nationkey == nation_selected.n_nationkey,
        )
    ).select(
        "p_partkey", "p_name", "ps_suppkey", "ps_supplycost", "s_nationkey", "n_name"
    )


def join_with_lineitem_and_orders(
    dimension: DataFrame,
    lineitem: DataFrame,
    orders: DataFrame,
) -> DataFrame:
    """Une la dimensión reducida contra lineitem (broadcast) y luego contra orders.

    lineitem nunca se shufflea: siempre es el lado grande del broadcast join
    (Decisión 1). El join con orders ocurre DESPUÉS, sobre el resultado ya
    reducido por el filtro de producto, no sobre lineitem completo.
    """
    return (
        broadcast(lineitem)
        .join(
            dimension,
            lineitem.l_partkey == dimension.p_partkey,
        )
        .join(
            orders,
            lineitem.l_orderkey == orders.o_orderkey,
        )
    ).select(
        "p_partkey",
        "p_name",
        "ps_suppkey",
        "ps_supplycost",
        "s_nationkey",
        "n_name",
        "l_orderkey",
        "l_partkey",
        "l_extendedprice",
        "l_discount",
        "l_quantity",
        "o_orderkey",
        "o_orderdate",
    )


def calculate_profit_margin(joined_data: DataFrame) -> DataFrame:
    """Calcula el margen por línea y agrega por nación y año.

    margen = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity

    La agregación final requiere un shuffle (Decisión 1, punto 5) pero ya es
    sobre datos reducidos, no sobre lineitem original.
    """
    return (
        joined_data.withColumn(
            "profit_margin",
            F.col("l_extendedprice") * (1 - F.col("l_discount"))
            - F.col("ps_supplycost") * F.col("l_quantity"),
        )
        .groupBy("s_nationkey", "n_name", F.year("o_orderdate").alias("order_year"))
        .agg(F.sum("profit_margin").alias("total_profit"))
    )
