from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.config import GOLD_PATH
from src.spark_session import create_spark_session


CHURN_THRESHOLD_DAYS = 90


def read_customer_sales(spark) -> DataFrame:
    """Read the Gold customer sales table."""

    return (
        spark.read
        .format("delta")
        .load(str(GOLD_PATH / "customer_sales"))
    )


def build_customer_churn(df: DataFrame) -> DataFrame:
    """Build an ML-ready customer churn dataset."""

    reference_date = F.lit("2025-01-01").cast("date")

    return (
        df
        .withColumn(
            "days_since_last_order",
            F.when(
                F.col("last_order_date").isNotNull(),
                F.datediff(
                    reference_date,
                    F.col("last_order_date"),
                ),
            ).otherwise(F.lit(None).cast("long")),
        )
        .withColumn(
            "churn_label",
            F.when(
                F.col("last_order_date").isNull(),
                F.lit(1),
            )
            .when(
                F.col("days_since_last_order") >= CHURN_THRESHOLD_DAYS,
                F.lit(1),
            )
            .otherwise(F.lit(0)),
        )
        .select(
            "customer_id",
            "country",
            "segment",
            "total_orders",
            "total_items",
            "total_spend",
            "avg_order_value",
            "last_order_date",
            "days_since_last_order",
            "churn_label",
        )
    )


def write_churn_table(df: DataFrame) -> None:
    """Write the customer churn dataset as a Gold Delta table."""

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(GOLD_PATH / "customer_churn"))
    )


def run_churn_pipeline():
    spark = create_spark_session("RetailLakehouse-Churn")

    customer_sales = read_customer_sales(spark)

    customer_churn = build_customer_churn(customer_sales)

    write_churn_table(customer_churn)

    print("Gold churn dataset completed: customer_churn")

    customer_churn.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    run_churn_pipeline()
