"""Run a local, reproducible Spark plan benchmark without cloud services."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F

from retail_lakehouse.performance import optimized_order_lines
from src.spark_session import create_spark_session


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1_000 <= args.rows <= 10_000_000:
        parser.error("--rows must be between 1,000 and 10,000,000")

    spark = create_spark_session("RetailLakehouse-LocalBenchmark")
    try:
        base = spark.range(args.rows)
        orders = base.select(
            F.col("id").alias("order_id"),
            (F.col("id") % 10_000).alias("customer_id"),
            F.date_add(F.lit("2024-01-01").cast("date"), (F.col("id") % 1000).cast("int")).alias(
                "order_date"
            ),
            F.when((F.col("id") % 5) == 0, "cancelled").otherwise("completed").alias("status"),
        )
        items = base.select(
            F.col("id").alias("order_item_id"),
            F.col("id").alias("order_id"),
            (F.col("id") % 100).alias("product_id"),
            F.lit(1).alias("quantity"),
            F.lit(9.99).cast("decimal(18,2)").alias("unit_price"),
        )
        products = spark.range(100).select(
            F.col("id").alias("product_id"),
            F.concat(F.lit("category-"), F.col("id") % 10).alias("category"),
        )
        lines = optimized_order_lines(orders, items, products, start_date="2026-01-01")
        started = time.perf_counter()
        lines.groupBy("order_date").agg(F.sum("line_revenue")).collect()
        uncached_seconds = time.perf_counter() - started

        cached = lines.persist(StorageLevel.MEMORY_AND_DISK)
        cached.count()
        started = time.perf_counter()
        cached.groupBy("order_date").agg(F.sum("line_revenue")).collect()
        cached.groupBy("category").agg(F.sum("line_revenue")).collect()
        reused_cache_seconds = time.perf_counter() - started
        cached.unpersist()

        evidence = {
            "rows": args.rows,
            "spark_version": spark.version,
            "uncached_single_aggregation_seconds": round(uncached_seconds, 6),
            "cached_two_aggregations_seconds": round(reused_cache_seconds, 6),
            "scope": "local synthetic benchmark; not a production SLA or cost claim",
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
