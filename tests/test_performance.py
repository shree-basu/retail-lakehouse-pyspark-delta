from __future__ import annotations

from datetime import date
from decimal import Decimal

from retail_lakehouse.performance import executed_plan, optimized_order_lines


def test_plan_prunes_dates_and_broadcasts_product_dimension(spark) -> None:
    orders = spark.createDataFrame(
        [(1, 10, date(2026, 9, 1), "completed"), (2, 10, date(2025, 1, 1), "completed")],
        ["order_id", "customer_id", "order_date", "status"],
    )
    items = spark.createDataFrame(
        [(100, 1, 7, 2, Decimal("9.50")), (101, 2, 7, 1, Decimal("9.50"))],
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price"],
    )
    products = spark.createDataFrame([(7, "books")], ["product_id", "category"])
    lines = optimized_order_lines(orders, items, products, start_date="2026-01-01")

    assert lines.count() == 1
    plan = executed_plan(lines)
    assert "BroadcastHashJoin" in plan
    assert "order_date" in plan
