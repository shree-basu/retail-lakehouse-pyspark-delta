from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CHURN_THRESHOLD_DAYS = 90


def build_customer_churn(df: DataFrame) -> DataFrame:
    """Build an ML-ready customer churn dataset."""

    reference_date = F.lit("2025-01-01").cast("date")

    return (
        df.withColumn(
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
