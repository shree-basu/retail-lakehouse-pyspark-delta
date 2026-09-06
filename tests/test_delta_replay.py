from __future__ import annotations

from datetime import date
from pathlib import Path

from data.generate_sample_data import generate_batch
from retail_lakehouse.pipeline import process_batch


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
    first_counts = {
        entity: spark.read.format("delta").load(str(lakehouse / "silver" / entity)).count()
        for entity in first["entities"]
    }
    second = process_batch(spark, batch, lakehouse)
    second_counts = {
        entity: spark.read.format("delta").load(str(lakehouse / "silver" / entity)).count()
        for entity in second["entities"]
    }
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
    assert first_counts == second_counts
    assert quarantine_count == 1
    assert audit_count == 5
    assert successful_attempts == 2
    assert distinct_attempts == 2
