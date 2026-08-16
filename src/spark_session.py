from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def create_spark_session(app_name: str = "RetailLakehouse") -> SparkSession:
    """Create a local Spark session configured for Delta Lake."""

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()
