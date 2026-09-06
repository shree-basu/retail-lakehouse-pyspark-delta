from __future__ import annotations

from datetime import date

import pytest

from retail_lakehouse.delta_io import transaction_version
from retail_lakehouse.quality import (
    ReconciliationFailure,
    assert_reconciled,
    clean_customer_events,
    clean_order_items,
    clean_orders,
)
from retail_lakehouse.schemas import bronze_schemas


def _metadata(frame, batch_id: str = "retail-20260906"):
    from pyspark.sql import functions as F

    payload = [F.col(column) for column in frame.columns]
    return (
        frame.withColumn("_source_batch_id", F.lit(batch_id))
        .withColumn("_source_business_date", F.to_date(F.lit("2026-09-06")))
        .withColumn("_source_file", F.lit("test"))
        .withColumn("_source_sha256", F.lit("0" * 64))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_hash", F.sha2(F.to_json(F.struct(*payload)), 256))
    )


def test_explicit_schemas_cover_structured_and_nested_sources() -> None:
    schemas = bronze_schemas()
    assert schemas["orders"]["order_ts"].dataType.simpleString() == "string"
    assert schemas["customer_events"]["device"].dataType.simpleString().startswith("struct")
    assert schemas["customer_events"]["attributes"].dataType.simpleString() == "map<string,string>"


def test_order_duplicate_is_deterministic_and_orphan_is_quarantined(spark) -> None:
    frame = spark.createDataFrame(
        [
            ("10", "1", "2026-09-05T10:00:00Z", "completed", "card", "USD", "2026-09-06T01:00:00Z"),
            ("10", "1", "2026-09-05T10:00:00Z", "refunded", "card", "USD", "2026-09-06T02:00:00Z"),
            (
                "11",
                "999",
                "2026-09-05T10:00:00Z",
                "completed",
                "card",
                "USD",
                "2026-09-06T01:00:00Z",
            ),
        ],
        [
            "order_id",
            "customer_id",
            "order_ts",
            "status",
            "payment_method",
            "currency",
            "updated_at",
        ],
    )
    customers = spark.createDataFrame([(1,)], ["customer_id"])
    result = clean_orders(_metadata(frame), customers)

    accepted = result.accepted.collect()
    quarantined = result.quarantined.collect()
    assert len(accepted) == 1
    assert accepted[0].status == "refunded"
    assert {code for row in quarantined for code in row.reason_codes} == {
        "DUPLICATE_NATURAL_KEY",
        "ORPHAN_CUSTOMER",
    }
    assert_reconciled("orders", 3, len(accepted), len(quarantined))


def test_order_item_rejects_orphan_and_nonpositive_amount(spark) -> None:
    frame = spark.createDataFrame(
        [
            ("1", "100", "7", "2", "10.00", "USD", "2026-09-06T01:00:00Z"),
            ("2", "999", "7", "1", "-0.01", "USD", "2026-09-06T01:00:00Z"),
        ],
        [
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "currency",
            "updated_at",
        ],
    )
    orders = spark.createDataFrame([(100, date(2026, 9, 5))], ["order_id", "order_date"])
    products = spark.createDataFrame([(7,)], ["product_id"])
    result = clean_order_items(_metadata(frame), orders, products)

    assert result.accepted.count() == 1
    reasons = set(result.quarantined.first().reason_codes)
    assert reasons == {"ORPHAN_ORDER", "INVALID_UNIT_PRICE"}


def test_semistructured_event_preserves_evolving_attributes(spark) -> None:
    schema = bronze_schemas()["customer_events"]
    frame = spark.createDataFrame(
        [
            (
                "evt-1",
                "1",
                "2026-09-06T01:00:00Z",
                "page_view",
                "session-1",
                ("mobile", "synthetic"),
                {"campaign": "email", "experiment_id": "checkout-v2"},
                None,
            )
        ],
        schema,
    )
    customers = spark.createDataFrame([(1,)], ["customer_id"])
    result = clean_customer_events(_metadata(frame), customers)

    row = result.accepted.first()
    assert row.attributes["experiment_id"] == "checkout-v2"
    assert row.device_type == "mobile"


def test_reconciliation_and_transaction_identity_fail_safely() -> None:
    assert transaction_version("batch-a") == transaction_version("batch-a")
    assert transaction_version("batch-a") != transaction_version("batch-b")
    with pytest.raises(ReconciliationFailure, match="reconciliation failed"):
        assert_reconciled("orders", 10, 8, 1)
