"""Idempotent Delta persistence primitives for Bronze, Silver, quarantine, and audit."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from retail_lakehouse.contracts import ENTITY_CONTRACTS
from retail_lakehouse.schemas import PARTITION_COLUMNS


def transaction_version(batch_id: str) -> int:
    return int(hashlib.sha256(batch_id.encode("utf-8")).hexdigest()[:15], 16)


def read_source_frame(spark: Any, batch_dir: Path, entity: str, manifest: dict[str, Any]) -> Any:
    from pyspark.sql import functions as F

    from retail_lakehouse.schemas import bronze_schemas

    contract = ENTITY_CONTRACTS[entity]
    path = batch_dir / contract.filename
    reader = spark.read.schema(bronze_schemas()[entity]).option("mode", "PERMISSIVE")
    if contract.file_format == "csv":
        frame = reader.option("header", True).csv(str(path))
    else:
        frame = reader.option("columnNameOfCorruptRecord", "_corrupt_record").json(str(path))
    payload_columns = [F.col(column) for column in bronze_schemas()[entity].fieldNames()]
    return (
        frame.withColumn("_source_batch_id", F.lit(manifest["batch_id"]))
        .withColumn("_source_business_date", F.to_date(F.lit(manifest["business_date"])))
        .withColumn("_source_file", F.lit(contract.filename))
        .withColumn("_source_sha256", F.lit(manifest["entities"][entity]["sha256"]))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn(
            "_record_hash",
            F.sha2(
                F.to_json(F.struct(*payload_columns), options={"ignoreNullFields": "false"}), 256
            ),
        )
    )


def append_bronze(frame: Any, path: Path, *, entity: str, batch_id: str) -> None:
    writer = (
        frame.write.format("delta")
        .mode("append")
        .option("mergeSchema", "false")
        .option("txnAppId", f"retail-bronze-{entity}")
        .option("txnVersion", transaction_version(batch_id))
    )
    if not path.exists():
        writer = writer.partitionBy("_source_business_date")
    writer.save(str(path))


def merge_curated(frame: Any, path: Path, *, entity: str, keys: tuple[str, ...]) -> None:
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(frame.sparkSession, str(path)):
        writer = (
            frame.write.format("delta")
            .mode("overwrite")
            .option("delta.enableChangeDataFeed", "true")
        )
        partitions = PARTITION_COLUMNS[entity]
        if partitions:
            writer = writer.partitionBy(*partitions)
        writer.save(str(path))
        return
    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
    (
        DeltaTable.forPath(frame.sparkSession, str(path))
        .alias("target")
        .merge(frame.alias("source"), condition)
        .whenMatchedUpdateAll(condition="source.updated_at >= target.updated_at")
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_quarantine(frame: Any, path: Path) -> None:
    from delta.tables import DeltaTable

    keys = ("source_batch_id", "entity", "record_hash")
    if not DeltaTable.isDeltaTable(frame.sparkSession, str(path)):
        frame.write.format("delta").mode("overwrite").partitionBy("business_date", "entity").save(
            str(path)
        )
        return
    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
    (
        DeltaTable.forPath(frame.sparkSession, str(path))
        .alias("target")
        .merge(frame.alias("source"), condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_audit(frame: Any, path: Path) -> None:
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(frame.sparkSession, str(path)):
        frame.write.format("delta").mode("overwrite").save(str(path))
        return
    (
        DeltaTable.forPath(frame.sparkSession, str(path))
        .alias("target")
        .merge(
            frame.alias("source"),
            "target.run_id = source.run_id AND target.entity = source.entity",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_run_status(frame: Any, path: Path) -> None:
    """Insert or update one execution attempt in the Delta run ledger."""

    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(frame.sparkSession, str(path)):
        frame.write.format("delta").mode("overwrite").partitionBy("business_date").save(str(path))
        return
    (
        DeltaTable.forPath(frame.sparkSession, str(path))
        .alias("target")
        .merge(frame.alias("source"), "target.attempt_id = source.attempt_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
