import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "RetailLakehouse") -> SparkSession:
    """Create a deterministic local Spark session configured for Delta Lake."""

    os.environ.pop("SPARK_HOME", None)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", "python")
    os.environ["PATH"] = f"{os.path.dirname(sys.executable)}{os.pathsep}{os.environ['PATH']}"

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        .config("spark.ui.enabled", "false")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()
