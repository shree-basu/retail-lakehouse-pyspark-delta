"""Explicit raw and curated Spark schemas; production reads never infer types."""

from __future__ import annotations

from typing import Any


def bronze_schemas() -> dict[str, Any]:
    from pyspark.sql.types import MapType, StringType, StructField, StructType

    text = StringType()

    def strings(*names: str) -> StructType:
        return StructType([StructField(name, text, True) for name in names])

    return {
        "customers": strings(
            "customer_id",
            "customer_name",
            "email",
            "signup_date",
            "country",
            "segment",
            "updated_at",
        ),
        "products": strings("product_id", "product_name", "category", "unit_price", "updated_at"),
        "orders": strings(
            "order_id",
            "customer_id",
            "order_ts",
            "status",
            "payment_method",
            "currency",
            "updated_at",
        ),
        "order_items": strings(
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "currency",
            "updated_at",
        ),
        "customer_events": StructType(
            [
                StructField("event_id", text, True),
                StructField("customer_id", text, True),
                StructField("event_ts", text, True),
                StructField("event_type", text, True),
                StructField("session_id", text, True),
                StructField(
                    "device",
                    StructType([StructField("type", text, True), StructField("os", text, True)]),
                    True,
                ),
                StructField("attributes", MapType(text, text, True), True),
                StructField("_corrupt_record", text, True),
            ]
        ),
    }


CURATED_NATURAL_KEYS: dict[str, tuple[str, ...]] = {
    "customers": ("customer_id",),
    "products": ("product_id",),
    "orders": ("order_id",),
    "order_items": ("order_item_id",),
    "customer_events": ("event_id",),
}


PARTITION_COLUMNS: dict[str, tuple[str, ...]] = {
    "customers": (),
    "products": (),
    "orders": ("order_date",),
    "order_items": ("order_date",),
    "customer_events": ("event_date",),
}
