"""Deterministic Silver validation, quarantine, and reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class QualityResult:
    accepted: Any
    quarantined: Any


class ReconciliationFailure(RuntimeError):
    """Raised when input does not equal accepted plus quarantined."""


def assert_reconciled(
    entity: str, input_rows: int, accepted_rows: int, quarantined_rows: int
) -> None:
    if input_rows != accepted_rows + quarantined_rows:
        raise ReconciliationFailure(
            f"{entity} reconciliation failed: input={input_rows}, "
            f"accepted={accepted_rows}, quarantined={quarantined_rows}"
        )


def _reason_array(*conditions: tuple[Any, str]) -> Any:
    from pyspark.sql import functions as F

    candidates = F.array(
        *[F.when(condition, F.lit(code)).otherwise(F.lit(None)) for condition, code in conditions]
    )
    return F.filter(candidates, lambda value: value.isNotNull())


def _split_quality(
    frame: Any,
    *,
    entity: str,
    keys: tuple[str, ...],
    conditions: tuple[tuple[Any, str], ...],
) -> QualityResult:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    ranked = frame.withColumn(
        "_natural_key_rank",
        F.row_number().over(
            Window.partitionBy(*keys).orderBy(
                F.col("updated_at").desc_nulls_last(), F.col("_record_hash").desc()
            )
        ),
    )
    with_reasons = ranked.withColumn(
        "_dq_reasons",
        _reason_array(
            *conditions,
            (F.col("_natural_key_rank") > 1, "DUPLICATE_NATURAL_KEY"),
        ),
    )
    accepted = with_reasons.filter(F.size("_dq_reasons") == 0).drop(
        "_dq_reasons",
        "_natural_key_rank",
        "_customer_exists",
        "_product_exists",
        "_order_exists",
        "_corrupt_record",
    )
    raw_columns = [column for column in frame.columns if not column.startswith("_")]
    quarantined = with_reasons.filter(F.size("_dq_reasons") > 0).select(
        F.lit(entity).alias("entity"),
        F.to_json(F.struct(*[F.col(key) for key in keys])).alias("natural_key"),
        F.col("_record_hash").alias("record_hash"),
        F.col("_source_batch_id").alias("source_batch_id"),
        F.col("_source_business_date").alias("business_date"),
        F.col("_source_file").alias("source_file"),
        F.col("_dq_reasons").alias("reason_codes"),
        F.to_json(F.struct(*[F.col(column) for column in raw_columns])).alias("raw_payload"),
        F.current_timestamp().alias("quarantined_at"),
    )
    return QualityResult(accepted, quarantined)


def clean_customers(frame: Any) -> QualityResult:
    from pyspark.sql import functions as F

    cleaned = (
        frame.withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("customer_name", F.trim("customer_name"))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("signup_date", F.to_date("signup_date"))
        .withColumn("country", F.upper(F.trim("country")))
        .withColumn("segment", F.lower(F.trim("segment")))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )
    return _split_quality(
        cleaned,
        entity="customers",
        keys=("customer_id",),
        conditions=(
            (F.col("customer_id").isNull(), "INVALID_CUSTOMER_ID"),
            (
                F.col("customer_name").isNull() | (F.length("customer_name") == 0),
                "MISSING_CUSTOMER_NAME",
            ),
            (
                F.col("email").isNull() | ~F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
                "INVALID_EMAIL",
            ),
            (
                F.col("country").isNull() | ~F.col("country").isin("US", "UK", "DE", "IN", "CA"),
                "INVALID_COUNTRY",
            ),
            (
                F.col("segment").isNull()
                | ~F.col("segment").isin("standard", "premium", "enterprise"),
                "INVALID_SEGMENT",
            ),
            (F.col("signup_date").isNull(), "INVALID_SIGNUP_DATE"),
            (F.col("updated_at").isNull(), "INVALID_UPDATED_AT"),
        ),
    )


def clean_products(frame: Any) -> QualityResult:
    from pyspark.sql import functions as F

    cleaned = (
        frame.withColumn("product_id", F.col("product_id").cast("long"))
        .withColumn("product_name", F.trim("product_name"))
        .withColumn("category", F.lower(F.trim("category")))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )
    return _split_quality(
        cleaned,
        entity="products",
        keys=("product_id",),
        conditions=(
            (F.col("product_id").isNull(), "INVALID_PRODUCT_ID"),
            (
                F.col("product_name").isNull() | (F.length("product_name") == 0),
                "MISSING_PRODUCT_NAME",
            ),
            (
                F.col("category").isNull() | (F.length("category") == 0),
                "MISSING_CATEGORY",
            ),
            (F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "INVALID_UNIT_PRICE"),
            (F.col("updated_at").isNull(), "INVALID_UPDATED_AT"),
        ),
    )


def clean_orders(frame: Any, valid_customers: Any | None = None) -> QualityResult:
    from pyspark.sql import functions as F

    cleaned = (
        frame.withColumn("order_id", F.col("order_id").cast("long"))
        .withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("status", F.lower(F.trim("status")))
        .withColumn("payment_method", F.lower(F.trim("payment_method")))
        .withColumn("currency", F.upper(F.trim("currency")))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )
    if valid_customers is not None:
        customer_refs = (
            valid_customers.select("customer_id")
            .dropDuplicates()
            .withColumn("_customer_exists", F.lit(True))
        )
        cleaned = cleaned.join(
            F.broadcast(customer_refs),
            on="customer_id",
            how="left",
        ).fillna(False, subset=["_customer_exists"])
    else:
        cleaned = cleaned.withColumn("_customer_exists", F.lit(True))
    return _split_quality(
        cleaned,
        entity="orders",
        keys=("order_id",),
        conditions=(
            (F.col("order_id").isNull(), "INVALID_ORDER_ID"),
            (F.col("customer_id").isNull(), "INVALID_CUSTOMER_ID"),
            (~F.col("_customer_exists"), "ORPHAN_CUSTOMER"),
            (F.col("order_ts").isNull(), "INVALID_ORDER_TIMESTAMP"),
            (
                F.col("status").isNull()
                | ~F.col("status").isin("completed", "cancelled", "refunded"),
                "INVALID_STATUS",
            ),
            (
                F.col("payment_method").isNull()
                | ~F.col("payment_method").isin("card", "paypal", "bank_transfer"),
                "INVALID_PAYMENT_METHOD",
            ),
            (
                F.col("currency").isNull() | (F.col("currency") != "USD"),
                "UNSUPPORTED_CURRENCY",
            ),
            (F.col("updated_at").isNull(), "INVALID_UPDATED_AT"),
        ),
    )


def clean_order_items(
    frame: Any, valid_orders: Any | None = None, valid_products: Any | None = None
) -> QualityResult:
    from pyspark.sql import functions as F

    cleaned = (
        frame.withColumn("order_item_id", F.col("order_item_id").cast("long"))
        .withColumn("order_id", F.col("order_id").cast("long"))
        .withColumn("product_id", F.col("product_id").cast("long"))
        .withColumn("quantity", F.col("quantity").cast("long"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
        .withColumn("currency", F.upper(F.trim("currency")))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )
    if valid_orders is not None:
        order_refs = (
            valid_orders.select("order_id", "order_date")
            .dropDuplicates(["order_id"])
            .withColumn("_order_exists", F.lit(True))
        )
        cleaned = cleaned.join(order_refs, on="order_id", how="left").fillna(
            False, subset=["_order_exists"]
        )
    else:
        cleaned = cleaned.withColumn("order_date", F.lit(None).cast("date")).withColumn(
            "_order_exists", F.lit(True)
        )
    if valid_products is not None:
        product_refs = (
            valid_products.select("product_id")
            .dropDuplicates()
            .withColumn("_product_exists", F.lit(True))
        )
        cleaned = cleaned.join(F.broadcast(product_refs), on="product_id", how="left").fillna(
            False, subset=["_product_exists"]
        )
    else:
        cleaned = cleaned.withColumn("_product_exists", F.lit(True))
    return _split_quality(
        cleaned,
        entity="order_items",
        keys=("order_item_id",),
        conditions=(
            (F.col("order_item_id").isNull(), "INVALID_ORDER_ITEM_ID"),
            (F.col("order_id").isNull(), "INVALID_ORDER_ID"),
            (~F.col("_order_exists"), "ORPHAN_ORDER"),
            (F.col("product_id").isNull(), "INVALID_PRODUCT_ID"),
            (~F.col("_product_exists"), "ORPHAN_PRODUCT"),
            (F.col("quantity").isNull() | (F.col("quantity") <= 0), "INVALID_QUANTITY"),
            (F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "INVALID_UNIT_PRICE"),
            (
                F.col("currency").isNull() | (F.col("currency") != "USD"),
                "UNSUPPORTED_CURRENCY",
            ),
            (F.col("updated_at").isNull(), "INVALID_UPDATED_AT"),
        ),
    )


def clean_customer_events(frame: Any, valid_customers: Any | None = None) -> QualityResult:
    from pyspark.sql import functions as F

    cleaned = (
        frame.withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_type", F.lower(F.trim("event_type")))
        .withColumn("device_type", F.lower(F.trim("device.type")))
        .withColumn("device_os", F.lower(F.trim("device.os")))
        .drop("device")
        .withColumn("updated_at", F.col("event_ts"))
    )
    if valid_customers is not None:
        customer_refs = (
            valid_customers.select("customer_id")
            .dropDuplicates()
            .withColumn("_customer_exists", F.lit(True))
        )
        cleaned = cleaned.join(
            F.broadcast(customer_refs),
            on="customer_id",
            how="left",
        ).fillna(False, subset=["_customer_exists"])
    else:
        cleaned = cleaned.withColumn("_customer_exists", F.lit(True))
    return _split_quality(
        cleaned,
        entity="customer_events",
        keys=("event_id",),
        conditions=(
            (F.col("_corrupt_record").isNotNull(), "MALFORMED_JSON"),
            (F.col("event_id").isNull(), "INVALID_EVENT_ID"),
            (F.col("customer_id").isNull(), "INVALID_CUSTOMER_ID"),
            (~F.col("_customer_exists"), "ORPHAN_CUSTOMER"),
            (F.col("event_ts").isNull(), "INVALID_EVENT_TIMESTAMP"),
            (
                F.col("event_type").isNull()
                | ~F.col("event_type").isin("page_view", "search", "add_to_cart", "checkout"),
                "INVALID_EVENT_TYPE",
            ),
            (
                F.col("device_type").isNull()
                | ~F.col("device_type").isin("mobile", "desktop", "tablet"),
                "INVALID_DEVICE",
            ),
        ),
    )
