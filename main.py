from databricks.connect import DatabricksSession
from src.etl.extract import (
    read_lineitem,
    read_orders,
    read_part,
    read_supplier,
    read_nation,
)
from src.etl.transform import (
    build_green_part_dimension,
    join_with_lineitem_and_orders,
    calculate_profit_margin,
)
from src.config import CATALOG, SCHEMA, PART_NAME_FILTER

spark = DatabricksSession.builder.serverless().getOrCreate()

dimension = build_green_part_dimension(
    read_part(spark),
    read_supplier(spark),
    read_supplier(spark),
    read_nation(spark),
    PART_NAME_FILTER,
)
joined = join_with_lineitem_and_orders(
    dimension, read_lineitem(spark), read_orders(spark)
)
result = calculate_profit_margin(joined)
result.show(20)
