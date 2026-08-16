from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.config import BRONZE_PATH, RAW_DATA_PATH
from src.spark_session import create_spark_session


def ingest_csv_to_bronze(
    spark,
    source_path: Path,
    target_path: Path,
    table_name: str,
) -> DataFrame:
    """Read a source CSV and persist it as a Delta Bronze table."""

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(source_path))
    )

    bronze_df = (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_path.name))
    )

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(target_path / table_name))
    )

    return bronze_df


def run_bronze_ingestion():
    spark = create_spark_session("RetailLakehouse-Bronze")

    datasets = [
        "customers",
        "products",
        "orders",
        "order_items",
    ]

    for dataset in datasets:
        source_path = RAW_DATA_PATH / f"{dataset}.csv"
        target_path = BRONZE_PATH

        ingest_csv_to_bronze(
            spark=spark,
            source_path=source_path,
            target_path=target_path,
            table_name=dataset,
        )

        print(f"Bronze ingestion completed: {dataset}")

    spark.stop()


if __name__ == "__main__":
    run_bronze_ingestion()
