# Shared utility/helper functions

from pathlib import Path
from pyspark.sql import functions as F
from config import BASE_PARQUET_PATH


def table_path(layer: str, table_name: str) -> str:
    """Returns the physical path for a logical table stored as Parquet."""
    return str(Path(BASE_PARQUET_PATH) / layer / table_name)


def save_table(df, layer: str, table_name: str, mode: str = "overwrite") -> None:
    """Saves a Spark DataFrame as a Parquet-backed logical table."""
    (
        df.write
        .format("parquet")
        .mode(mode)
        .save(table_path(layer, table_name))
    )


def read_table(spark, layer: str, table_name: str):
    """Reads a Parquet-backed logical table."""
    return spark.read.format("parquet").load(table_path(layer, table_name))


def read_csv_raw(spark, path: str):
    """Reads CSV files keeping all source columns as strings for Bronze."""
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(path)
    )


def parse_mixed_date(col_name: str):
    """
    Handles both yyyy-MM-dd and yyyy-MM values.
    When the source has yyyy-MM, it is converted to the first day of the month.
    """
    return (
        F.when(
            F.length(F.col(col_name)) == 7,
            F.to_date(F.concat(F.col(col_name), F.lit("-01")))
        )
        .otherwise(F.to_date(F.col(col_name)))
    )


def add_ingestion_metadata(df, source_name: str):
    """Adds minimal ingestion metadata to Bronze datasets."""
    return (
        df
        .withColumn("_source_file", F.lit(source_name))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def validate_input_file(path: str) -> None:
    if not Path(path).exists():
        raise FileNotFoundError(
            f"Input file not found: {path}. Place the source CSV files inside the files/ folder."
        )


def show_title(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def print_table_location(layer: str, table_name: str) -> None:
    print(f"{layer}.{table_name} -> {table_path(layer, table_name)}")
