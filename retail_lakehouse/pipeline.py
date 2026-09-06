"""Manifest-driven local/Databricks-compatible Bronze and Silver batch runner."""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from retail_lakehouse.contracts import ENTITY_CONTRACTS, load_and_validate_manifest
from retail_lakehouse.delta_io import (
    append_bronze,
    merge_audit,
    merge_curated,
    merge_quarantine,
    read_source_frame,
)
from retail_lakehouse.quality import (
    QualityResult,
    assert_reconciled,
    clean_customer_events,
    clean_customers,
    clean_order_items,
    clean_orders,
    clean_products,
)
from retail_lakehouse.schemas import CURATED_NATURAL_KEYS


@dataclass(frozen=True)
class LakehousePaths:
    root: Path

    @property
    def bronze(self) -> Path:
        return self.root / "bronze"

    @property
    def silver(self) -> Path:
        return self.root / "silver"

    @property
    def quarantine(self) -> Path:
        return self.root / "quarantine" / "records"

    @property
    def audit(self) -> Path:
        return self.root / "audit" / "pipeline_metrics"


def run_id(manifest: dict[str, Any]) -> str:
    return f"retail-{manifest['business_date']}-{manifest['batch_id']}"


def _audit_frame(
    spark: Any,
    *,
    run_identifier: str,
    entity: str,
    manifest: dict[str, Any],
    input_rows: int,
    accepted_rows: int,
    quarantined_rows: int,
    status: str,
) -> Any:
    from pyspark.sql import Row

    return spark.createDataFrame(
        [
            Row(
                run_id=run_identifier,
                entity=entity,
                business_date=manifest["business_date"],
                batch_id=manifest["batch_id"],
                schema_version=manifest["schema_version"],
                input_rows=input_rows,
                accepted_rows=accepted_rows,
                quarantined_rows=quarantined_rows,
                status=status,
                recorded_at=datetime.now(UTC).replace(tzinfo=None),
            )
        ]
    )


def _existing_or_batch_keys(spark: Any, path: Path, accepted: Any, key: str) -> Any:
    from delta.tables import DeltaTable

    current = accepted.select(key)
    if DeltaTable.isDeltaTable(spark, str(path)):
        current = current.unionByName(spark.read.format("delta").load(str(path)).select(key))
    return current.dropDuplicates()


def process_batch(spark: Any, batch_dir: Path, lakehouse_root: Path) -> dict[str, Any]:
    """Process one immutable batch; deterministic errors fail permanently."""

    manifest = load_and_validate_manifest(batch_dir)
    paths = LakehousePaths(lakehouse_root)
    identifier = run_id(manifest)
    bronze: dict[str, Any] = {}
    for entity in ENTITY_CONTRACTS:
        frame = read_source_frame(spark, batch_dir, entity, manifest)
        append_bronze(
            frame,
            paths.bronze / entity,
            entity=entity,
            batch_id=manifest["batch_id"],
        )
        bronze[entity] = frame

    results: dict[str, QualityResult] = {}
    results["customers"] = clean_customers(bronze["customers"])
    customer_keys = _existing_or_batch_keys(
        spark, paths.silver / "customers", results["customers"].accepted, "customer_id"
    )
    results["products"] = clean_products(bronze["products"])
    product_keys = _existing_or_batch_keys(
        spark, paths.silver / "products", results["products"].accepted, "product_id"
    )
    results["orders"] = clean_orders(bronze["orders"], customer_keys)
    # Items need the order partition column as well as the key for partitioned publication.
    order_dates = results["orders"].accepted.select("order_id", "order_date")
    if (paths.silver / "orders").exists():
        order_dates = order_dates.unionByName(
            spark.read.format("delta")
            .load(str(paths.silver / "orders"))
            .select("order_id", "order_date")
        )
    results["order_items"] = clean_order_items(
        bronze["order_items"], order_dates.dropDuplicates(["order_id"]), product_keys
    )
    results["customer_events"] = clean_customer_events(bronze["customer_events"], customer_keys)

    metrics: dict[str, dict[str, int]] = {}
    for entity, result in results.items():
        input_rows = int(manifest["entities"][entity]["row_count"])
        accepted_rows = result.accepted.count()
        quarantined_rows = result.quarantined.count()
        assert_reconciled(entity, input_rows, accepted_rows, quarantined_rows)
        merge_curated(
            result.accepted,
            paths.silver / entity,
            entity=entity,
            keys=CURATED_NATURAL_KEYS[entity],
        )
        if quarantined_rows:
            merge_quarantine(result.quarantined, paths.quarantine)
        merge_audit(
            _audit_frame(
                spark,
                run_identifier=identifier,
                entity=entity,
                manifest=manifest,
                input_rows=input_rows,
                accepted_rows=accepted_rows,
                quarantined_rows=quarantined_rows,
                status="SUCCESS",
            ),
            paths.audit,
        )
        metrics[entity] = {
            "input_rows": input_rows,
            "accepted_rows": accepted_rows,
            "quarantined_rows": quarantined_rows,
        }
    return {"run_id": identifier, "status": "SUCCESS", "entities": metrics}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-dir", type=Path, required=True)
    parser.add_argument("--lakehouse-root", type=Path, required=True)
    return parser


def main(session_factory: Callable[[str], Any] | None = None) -> None:
    args = build_parser().parse_args()
    if session_factory is None:
        from src.spark_session import create_spark_session

        session_factory = create_spark_session
    spark = session_factory("RetailLakehouse-IncrementalBatch")
    try:
        result = process_batch(spark, args.batch_dir, args.lakehouse_root)
        print(json.dumps(result, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
