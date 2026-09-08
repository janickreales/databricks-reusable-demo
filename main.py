import argparse

from databricks.connect import DatabricksSession

from src.config import OUTPUT_CATALOG, OUTPUT_SCHEMA, OUTPUT_TABLE, PART_NAME_FILTER
from src.etl.extract import (
    read_lineitem,
    read_nation,
    read_orders,
    read_part,
    read_partsupp,
    read_supplier,
)
from src.etl.load import write_profit_report
from src.etl.transform import (
    build_green_part_dimension,
    calculate_profit_margin,
    join_with_lineitem_and_orders,
)


def read_source_frames(spark, diagnostics: bool = False):
    """Load source frames once and optionally cache them for diagnostics."""
    frames = {
        "lineitem": read_lineitem(spark),
        "orders": read_orders(spark),
        "part": read_part(spark),
        "supplier": read_supplier(spark),
        "nation": read_nation(spark),
        "partsupp": read_partsupp(spark),
    }

    if diagnostics:
        print("=" * 60)
        print("CONTEO DE FILAS POR TABLA FUENTE")
        print("=" * 60)
        for name, df in frames.items():
            df.persist()
            print(f"{name}: {df.count()}")

    return frames


def main(diagnostics: bool = False) -> None:
    spark = DatabricksSession.builder.serverless().getOrCreate()
    source_frames = read_source_frames(spark, diagnostics=diagnostics)

    dimension = build_green_part_dimension(
        part=source_frames["part"],
        partsupp=source_frames["partsupp"],
        supplier=source_frames["supplier"],
        nation=source_frames["nation"],
        part_name_filter=PART_NAME_FILTER,
    )
    joined = join_with_lineitem_and_orders(
        dimension=dimension,
        lineitem=source_frames["lineitem"],
        orders=source_frames["orders"],
    )
    result = calculate_profit_margin(joined)
    result.show(20)
    write_profit_report(
        result,
        catalog=OUTPUT_CATALOG,
        schema=OUTPUT_SCHEMA,
        table_name=OUTPUT_TABLE,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TPC-H profit pipeline")
    parser.add_argument(
        "--diagnostics",
        action="store_true",
        help="Print source-table row counts before running the pipeline.",
    )
    args = parser.parse_args()
    main(diagnostics=args.diagnostics)
