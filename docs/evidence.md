# Evidence and portfolio claim boundary

## Directly supported after CI passes

- Implemented distributed transformations with PySpark/Spark using explicit structured and nested semi-structured schemas.
- Implemented an incremental Delta Lake medallion design with immutable batches, schema enforcement, deterministic DQ/quarantine, reconciliation, transaction identity, merges, and Change Data Feed.
- Implemented dbt staging/mart models, documentation, generic/business tests, and an incremental correction pattern that rebuilds both previous and current affected dates.
- Implemented concrete Spark tuning decisions: early filters/projections, bounded broadcast, date layout, AQE settings, plan inspection, and reuse-only caching in a benchmark.
- Implemented replay, run-state audit, recovery guidance, repository safety checks, and cloud-free CI.

## Must be qualified

- **Databricks:** the design and dbt adapter are Databricks-compatible; no workspace execution is evidenced.
- **Large datasets:** Spark code is distributed in design, but representative production-scale throughput is not measured.
- **Optimized:** code and plan choices are tested; no universal percentage improvement or cloud cost reduction is claimed.
- **Production:** call it a production-pattern reference implementation, not a live production system.

## Unsupported

- Deployed Databricks, Unity Catalog enforcement, production jobs, active alerts, production users, measured SLA, or cloud cost.
- ML training or prediction. `customer_360` can support later feature work, but this repository does not train a model.

## Validation matrix

| Evidence | Local | GitHub CI |
|---|---:|---:|
| Manifest/contract scenarios | Yes | Yes |
| Spark schema, DQ, reconciliation | Windows/Python 3.11 | Linux/Python 3.11 |
| Complete Delta replay | Windows blocked by Hadoop NativeIO dependency | Authoritative |
| Physical-plan broadcast assertion | Yes | Yes |
| dbt date-moving correction contract | Cloud-free semantic/static test | Cloud-free semantic/static test |
| dbt parse with warnings as errors | Inert profile | Inert profile |
| Databricks/cloud execution | Not performed | Impossible by workflow design |
