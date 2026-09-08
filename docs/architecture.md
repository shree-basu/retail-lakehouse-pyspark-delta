# Architecture and decision record

## Scope

The platform models a daily retail batch containing customer and product reference data, orders, order items, and semi-structured customer events. Open-source Spark/Delta is the executable reference runtime; paths and SQL remain suitable for a Databricks adaptation.

## Layer responsibilities

### Source boundary

The manifest is the atomic delivery contract. A batch is accepted only when its directory identity, schema version, exact entity set, filenames, formats, row counts, and SHA-256 checksums match. The generator stages all files and renames the directory only after the batch is complete.

### Bronze

Bronze preserves every delivered record with lineage columns. Each entity write uses a deterministic `txnAppId` containing the complete `business_date/batch_id` delivery identity and `txnVersion = 0`. An exact delivery retry therefore repeats the same Delta transaction, while a reused batch ID on another business date remains a distinct delivery. Bronze is partitioned by source business date.

### Silver

Silver enforces types and rules, selects the latest record per natural key using `updated_at` and a record-hash tie-breaker, checks reference integrity, and sends every rejected row to quarantine. Current-state tables are merged on natural keys; late updates replace a target only when their `updated_at` is not older. Orders, order items, and events use date partitions. Change Data Feed is enabled at table creation.

This is intentionally not SCD Type 2. Bronze is the immutable record of deliveries and CDF records Silver changes. A Type 2 dimension would be justified only by a stated point-in-time analytical requirement.

### Analytics

dbt separates SQL transformations from ingestion. `fct_order_items` incrementally merges one row per item, retains the prior date when an item moves, and uses the latest Silver ingestion timestamp as its processing watermark, so a late business update is not missed by a global business-time high watermark. `agg_daily_sales` unions the prior and current affected dates and recomputes each complete total from current fact state. A date emptied by a correction is overwritten with zero totals, preventing stale revenue and additive double counting. `customer_360` combines governed purchase and engagement measures.

## Correctness decisions

- USD is the explicit monetary contract; other currencies are quarantined rather than silently aggregated.
- Orders must reference accepted/current customers. Items must reference accepted/current orders and products. Events must reference accepted/current customers.
- Latest `updated_at` wins duplicate resolution; record hash resolves equal timestamps deterministically.
- Reconciliation must hold before publication for every entity.
- Business run IDs are deterministic; attempt IDs are unique so retries remain observable.
- Delta business tables are replay safe while the run ledger preserves each attempt.

## Concurrency boundary

Delta provides atomic table transactions, but this runner does not implement a distributed lock across simultaneous processes for the same batch. Production scheduling must enforce one active run per `run_id`, or add a lock protocol before concurrent writers are allowed.

## Schema evolution

Top-level fields are versioned and enforced. Compatible keys inside the event `attributes` map require no table-schema change and are demonstrated by the `schema-evolution` scenario. A new top-level column or incompatible type requires a schema-version change, reviewed contract changes, and an explicit migration; global automatic schema merging is disabled.
