"""Spark transformations with explicit filter and join-strategy decisions."""

from __future__ import annotations

from typing import Any


def optimized_order_lines(
    orders: Any,
    order_items: Any,
    products: Any,
    *,
    start_date: str,
) -> Any:
    """Prune early, narrow rows, and broadcast the bounded product dimension."""

    from pyspark.sql import functions as F

    completed = orders.filter(
        (F.col("order_date") >= F.lit(start_date).cast("date")) & (F.col("status") == "completed")
    ).select("order_id", "customer_id", "order_date")
    items = order_items.select("order_item_id", "order_id", "product_id", "quantity", "unit_price")
    product_dimension = products.select("product_id", "category")
    return (
        completed.join(items, on="order_id", how="inner")
        .join(F.broadcast(product_dimension), on="product_id", how="left")
        .withColumn("line_revenue", F.col("quantity") * F.col("unit_price"))
    )


def executed_plan(frame: Any) -> str:
    """Return the physical plan for testable, evidence-based tuning decisions."""

    frame.count()
    return frame._jdf.queryExecution().executedPlan().toString()
