from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_customer_sales(
    customers: DataFrame,
    orders: DataFrame,
    order_items: DataFrame,
) -> DataFrame:
    """Build customer-level sales metrics."""

    completed_orders = orders.filter(F.col("status") == "completed")

    order_totals = (
        completed_orders.join(order_items, on="order_id", how="inner")
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
        customers.join(order_totals, on="customer_id", how="left")
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

    completed_orders = orders.filter(F.col("status") == "completed")

    return (
        order_items.join(completed_orders, on="order_id", how="inner")
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

    completed_orders = orders.filter(F.col("status") == "completed")

    return (
        completed_orders.join(order_items, on="order_id", how="inner")
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
