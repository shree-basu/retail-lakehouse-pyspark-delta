from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.config import GOLD_PATH, SILVER_PATH
from src.spark_session import create_spark_session


def read_silver_table(spark, table_name: str) -> DataFrame:
    """Read a Delta table from the Silver layer."""

    return (
        spark.read
        .format("delta")
        .load(str(SILVER_PATH / table_name))
    )


def write_gold_table(df: DataFrame, table_name: str) -> None:
    """Write a DataFrame as a Gold Delta table."""

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(GOLD_PATH / table_name))
    )


def build_customer_sales(
    customers: DataFrame,
    orders: DataFrame,
    order_items: DataFrame,
) -> DataFrame:
    """Build customer-level sales metrics."""

    completed_orders = orders.filter(
        F.col("status") == "completed"
    )

    order_totals = (
        completed_orders
        .join(order_items, on="order_id", how="inner")
        .withColumn(
            "line_revenue",
            F.col("quantity") * F.col("unit_price"),
        )
        .groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_items"),
            F.round(F.sum("line_revenue"), 2).alias("total_spend"),
            F.round(F.avg("line_revenue"), 2).alias("avg_line_revenue"),
            F.max("order_date").alias("last_order_date"),
        )
    )

    return (
        customers
        .join(order_totals, on="customer_id", how="left")
        .fillna(
            {
                "total_orders": 0,
                "total_items": 0,
                "total_spend": 0.0,
                "avg_line_revenue": 0.0,
            }
        )
        .withColumn(
            "avg_order_value",
            F.when(
                F.col("total_orders") > 0,
                F.round(
                    F.col("total_spend") / F.col("total_orders"),
                    2,
                ),
            ).otherwise(F.lit(0.0)),
        )
        .select(
            "customer_id",
            "customer_name",
            "country",
            "segment",
            "total_orders",
            "total_items",
            "total_spend",
            "avg_order_value",
            "last_order_date",
        )
    )


def build_product_sales(
    products: DataFrame,
    orders: DataFrame,
    order_items: DataFrame,
) -> DataFrame:
    """Build product-level sales metrics."""

    completed_orders = orders.filter(
        F.col("status") == "completed"
    )

    return (
        order_items
        .join(completed_orders, on="order_id", how="inner")
        .withColumn(
            "line_revenue",
            F.col("quantity") * F.col("unit_price"),
        )
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("line_revenue"), 2).alias("revenue"),
            F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        )
        .join(products, on="product_id", how="left")
        .select(
            "product_id",
            "product_name",
            "category",
            "units_sold",
            "revenue",
            "avg_unit_price",
        )
    )


def build_daily_sales(
    orders: DataFrame,
    order_items: DataFrame,
) -> DataFrame:
    """Build daily sales metrics."""

    completed_orders = orders.filter(
        F.col("status") == "completed"
    )

    return (
        completed_orders
        .join(order_items, on="order_id", how="inner")
        .withColumn(
            "line_revenue",
            F.col("quantity") * F.col("unit_price"),
        )
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("items_sold"),
            F.round(F.sum("line_revenue"), 2).alias("revenue"),
        )
        .orderBy("order_date")
    )


def run_gold_transformation():
    spark = create_spark_session("RetailLakehouse-Gold")

    customers = read_silver_table(spark, "customers")
    products = read_silver_table(spark, "products")
    orders = read_silver_table(spark, "orders")
    order_items = read_silver_table(spark, "order_items")

    customer_sales = build_customer_sales(
        customers,
        orders,
        order_items,
    )

    product_sales = build_product_sales(
        products,
        orders,
        order_items,
    )

    daily_sales = build_daily_sales(
        orders,
        order_items,
    )

    write_gold_table(customer_sales, "customer_sales")
    print("Gold transformation completed: customer_sales")

    write_gold_table(product_sales, "product_sales")
    print("Gold transformation completed: product_sales")

    write_gold_table(daily_sales, "daily_sales")
    print("Gold transformation completed: daily_sales")

    spark.stop()


if __name__ == "__main__":
    run_gold_transformation()
