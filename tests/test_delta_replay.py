from __future__ import annotations

from datetime import date
from pathlib import Path

from data.generate_sample_data import generate_batch
from retail_lakehouse.delta_io import merge_quarantine
from retail_lakehouse.pipeline import process_batch


def _delta_counts(spark, root: Path, layer: str, entities: list[str]) -> dict[str, int]:
    return {
        entity: spark.read.format("delta").load(str(root / layer / entity)).count()
        for entity in entities
    }


def test_complete_delta_batch_is_replay_safe(tmp_path: Path, spark) -> None:
    batch = generate_batch(
        tmp_path / "input",
        business_date=date(2026, 9, 6),
        batch_id="retail-20260906",
        seed=9,
        customer_count=8,
        product_count=4,
        order_count=12,
        scenario="orphan-item",
    )
    lakehouse = tmp_path / "lakehouse"
    first = process_batch(spark, batch, lakehouse)
    entities = list(first["entities"])
    first_bronze_counts = _delta_counts(spark, lakehouse, "bronze", entities)
    first_silver_counts = _delta_counts(spark, lakehouse, "silver", entities)
    second = process_batch(spark, batch, lakehouse)
    second_bronze_counts = _delta_counts(spark, lakehouse, "bronze", entities)
    second_silver_counts = _delta_counts(spark, lakehouse, "silver", entities)
    quarantine_count = (
        spark.read.format("delta").load(str(lakehouse / "quarantine" / "records")).count()
    )
    audit_count = (
        spark.read.format("delta").load(str(lakehouse / "audit" / "pipeline_metrics")).count()
    )
    run_status = spark.read.format("delta").load(str(lakehouse / "audit" / "run_status"))
    successful_attempts = run_status.filter("status = 'SUCCESS'").count()
    distinct_attempts = run_status.select("attempt_id").distinct().count()

    assert first["run_id"] == second["run_id"]
    assert first["attempt_id"] != second["attempt_id"]
    assert first_bronze_counts == second_bronze_counts
    assert first_silver_counts == second_silver_counts
    assert quarantine_count == 1
    assert audit_count == 5
    assert successful_attempts == 2
    assert distinct_attempts == 2


def test_same_batch_id_on_different_dates_retains_both_bronze_deliveries(
    tmp_path: Path, spark
) -> None:
    first_batch = generate_batch(
        tmp_path / "input",
        business_date=date(2026, 9, 6),
        batch_id="shared-batch",
        seed=17,
        customer_count=6,
        product_count=3,
        order_count=8,
    )
    second_batch = generate_batch(
        tmp_path / "input",
        business_date=date(2026, 9, 7),
        batch_id="shared-batch",
        seed=17,
        customer_count=6,
        product_count=3,
        order_count=8,
    )
    lakehouse = tmp_path / "lakehouse"

    first = process_batch(spark, first_batch, lakehouse)
    second = process_batch(spark, second_batch, lakehouse)

    assert first["run_id"] != second["run_id"]
    for entity in first["entities"]:
        bronze = spark.read.format("delta").load(str(lakehouse / "bronze" / entity))
        deliveries = (
            bronze.filter("_source_batch_id = 'shared-batch'")
            .select("_source_business_date")
            .distinct()
            .count()
        )
        expected_rows = (
            first["entities"][entity]["input_rows"] + second["entities"][entity]["input_rows"]
        )
        assert deliveries == 2
        assert bronze.count() == expected_rows


def test_quarantine_identity_includes_business_date(tmp_path: Path, spark) -> None:
    from datetime import datetime

    from pyspark.sql.types import (
        ArrayType,
        DateType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("entity", StringType(), False),
            StructField("natural_key", StringType(), False),
            StructField("record_hash", StringType(), False),
            StructField("source_batch_id", StringType(), False),
            StructField("business_date", DateType(), False),
            StructField("source_file", StringType(), False),
            StructField("reason_codes", ArrayType(StringType(), False), False),
            StructField("raw_payload", StringType(), False),
            StructField("quarantined_at", TimestampType(), False),
        ]
    )
    common_identity = (
        "order_items",
        '{"order_item_id":1}',
        "a" * 64,
        "shared-batch",
    )
    first = spark.createDataFrame(
        [
            (
                *common_identity,
                date(2026, 9, 6),
                "order_items.csv",
                ["ORPHAN_ORDER"],
                '{"order_item_id":"1"}',
                datetime(2026, 9, 6, 1),
            )
        ],
        schema,
    )
    second = spark.createDataFrame(
        [
            (
                *common_identity,
                date(2026, 9, 7),
                "order_items.csv",
                ["ORPHAN_ORDER"],
                '{"order_item_id":"1"}',
                datetime(2026, 9, 7, 1),
            )
        ],
        schema,
    )
    path = tmp_path / "lakehouse" / "quarantine" / "records"

    merge_quarantine(first, path)
    merge_quarantine(first, path)
    merge_quarantine(second, path)
    history = spark.read.format("delta").load(str(path))

    assert history.count() == 2
    assert history.select("business_date").distinct().count() == 2
