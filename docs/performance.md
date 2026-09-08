# Spark performance decisions

- Orders are filtered by date and completed status before joins where the use case permits it.
- Transformations project only required columns before shuffles.
- The bounded product dimension is explicitly broadcast; a test asserts `BroadcastHashJoin` in the executed plan.
- Date partitions support pruning for orders, items, and events when consumers filter those columns.
- Adaptive Query Execution, post-shuffle coalescing, and skew handling are enabled.
- Global automatic schema merging is disabled.
- Caching is confined to the local benchmark where one derived frame serves two actions, followed by `unpersist`; one-use production frames are not cached.
- dbt daily aggregation recomputes only the prior and current dates affected by incremental corrections.

`scripts/benchmark_spark.py` captures Spark version, row count, and local timings. Results vary by machine and do not establish production scale, cloud cost, or SLA. Production performance evidence requires representative data, a defined cluster, repeated runs, plan/spill/skew metrics, and cost normalization.
