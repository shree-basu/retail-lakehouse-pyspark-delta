from pyspark.sql import SparkSession

from src.churn import build_customer_churn


def create_test_spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("RetailLakehouse-Churn-Tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def test_build_customer_churn_labels_inactive_customers():
    spark = create_test_spark()

    data = [
        (1, "Alice", "US", "premium", 5, 10, 500.0, 100.0, "2024-12-15"),
        (2, "Bob", "UK", "standard", 2, 4, 200.0, 100.0, "2024-09-01"),
        (3, "Charlie", "IN", "standard", 0, 0, 0.0, 0.0, None),
    ]

    columns = [
        "customer_id",
        "customer_name",
        "country",
        "segment",
        "total_orders",
        "total_items",
        "total_spend",
        "avg_order_value",
        "last_order_date",
    ]

    df = (
        spark.createDataFrame(data, columns)
        .withColumn(
            "last_order_date",
            __import__("pyspark.sql.functions").sql.functions.to_date(
                "last_order_date"
            ),
        )
    )

    result = build_customer_churn(df)

    alice = result.filter("customer_id = 1").first()
    bob = result.filter("customer_id = 2").first()
    charlie = result.filter("customer_id = 3").first()

    assert alice.churn_label == 0
    assert bob.churn_label == 1
    assert charlie.churn_label == 1

    spark.stop()
