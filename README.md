# Retail Lakehouse — Spark, Delta Lake, and dbt

A production-pattern retail data platform that turns immutable structured and semi-structured batches into governed Delta tables and tested dbt marts. It is runnable with open-source Spark and Delta Lake while remaining compatible with a Databricks execution model.

## What makes this more than a demo

- Immutable `business_date/batch_id` paths with an exact five-entity manifest, SHA-256 checksums, row counts, formats, filenames, and schema version.
- Explicit Spark schemas for four CSV entities and nested JSON customer events; schema inference is never used.
- Bronze append transactions keyed by application and batch, deterministic Silver deduplication, Delta `MERGE`, Change Data Feed, and replay-safe quarantine/audit publication.
- Domain, type, natural-key, foreign-key, amount, currency, and malformed-record checks with `input = accepted + quarantined` reconciliation.
- A separate execution ledger recording every `RUNNING`, `SUCCESS`, or `FAILED` attempt without changing the deterministic business run ID.
- dbt staging models, an incremental order-item fact, affected-date recomputation for daily sales, a customer-360 mart, documentation, generic tests, and business-rule tests.
- Early date/status filtering, narrow projections, explicit product-dimension broadcast, Adaptive Query Execution settings, and physical-plan assertions.
- SHA-pinned GitHub actions, hash-locked Python environments, Linux Spark/Delta replay tests, and an inert dbt profile that cannot contact a warehouse.

## Architecture

```text
Immutable source batch
  customers.csv | products.csv | orders.csv | order_items.csv | customer_events.jsonl
         │ manifest + count + SHA-256 + exact path validation
         ▼
Bronze Delta ── append-only payload + lineage + idempotent transaction ID
         │ explicit typing and deterministic quality rules
         ├──────────────► Quarantine Delta ── reason codes + rejected payload
         ▼
Silver Delta ── one current row per natural key + MERGE + CDF + date partitions
         │
         ├──────────────► Delta audit metrics and per-attempt run ledger
         ▼
dbt / Databricks SQL-compatible layer
  staging views → incremental fct_order_items → agg_daily_sales + customer_360
         ▼
Tested analytical consumption
```

See [architecture](docs/architecture.md), [data contract](docs/data-contract.md), [quality and recovery](docs/data-quality-recovery.md), and [performance decisions](docs/performance.md).

## Data grains

| Dataset | Grain | Publication behavior |
|---|---|---|
| Bronze entities | One delivered source record | Append with deterministic Delta transaction identity |
| Silver entities | One current row per declared natural key | Latest valid `updated_at` wins; Delta `MERGE` |
| Quarantine | One rejected record per batch/entity/hash | Insert-only merge; replay safe |
| `fct_order_items` | One row per order item | dbt incremental merge |
| `agg_daily_sales` | One row per order date | Recomputes dates affected by changed fact rows |
| `customer_360` | One row per customer | Full table rebuild from governed marts |

The current Silver design is a current-state model, not SCD Type 2. Source history remains in Bronze and Silver CDF records committed changes.

## Run locally

Python 3.11 and Java 17 are the tested runtime.

```bash
python -m venv .venv
source .venv/Scripts/activate  # Windows Git Bash
python -m pip install --require-hashes -r requirements-dev.lock

python data/generate_sample_data.py \
  --business-date 2026-09-06 \
  --batch-id retail-20260906 \
  --scenario normal

python -m retail_lakehouse.pipeline \
  --batch-dir data/input/2026-09-06/retail-20260906 \
  --lakehouse-root data/lakehouse
```

The generator also provides `duplicate-order`, `orphan-item`, `invalid-amount`, and compatible `schema-evolution` scenarios. It refuses to overwrite an existing batch.

### Validate dbt without Databricks

```bash
python -m pip install --require-hashes -r requirements-dbt.lock
dbt parse --profiles-dir .github/dbt --no-partial-parse --warn-error
```

The CI profile points to `example.invalid` with a fake token. `dbt run` and `dbt build` are intentionally absent from CI because they require a real warehouse.

### Inspect optimization evidence

```bash
python scripts/benchmark_spark.py --rows 100000 --output evidence/performance/local.json
```

This optional benchmark records local synthetic timings only. It compares plan behavior and cache reuse on one machine; it is not production throughput, cost, or SLA evidence.

## Validation

```bash
python -m ruff check .
python -m ruff format --check .
python -m compileall -q retail_lakehouse src data scripts tests
python scripts/check_repo_safety.py
python -m pytest -p no:cacheprovider -q
dbt parse --profiles-dir .github/dbt --no-partial-parse --warn-error
```

Linux CI is authoritative for end-to-end local Delta replay because standard Windows Spark installations require Hadoop NativeIO binaries for some Delta filesystem operations. Pure Spark transformations are also validated locally on Windows with Python 3.11.

## Cost and deployment safety

This repository has no deployment workflow, infrastructure apply path, Databricks/cloud credentials, OIDC permission, or use of GitHub secrets. CI can lint, parse, and test only. Browsing, cloning, opening a pull request, merging, or running CI cannot create Databricks or cloud resources.

Someone can incur cost only by taking code outside this repository and deliberately configuring their own compute and warehouse credentials. That cost belongs to their account, not the repository owner.

## Evidence boundary

This is a **production-pattern reference implementation**, not evidence of a deployed production system. The repository proves local Spark transformations, Delta transaction/replay behavior on Linux CI, dbt graph compilation, contract enforcement, DQ/reconciliation logic, and physical-plan choices. It does not prove a Databricks workspace deployment, Unity Catalog controls, production-scale volume, measured cloud cost, production SLA, or active operations.

See [evidence](docs/evidence.md) for safe and qualified portfolio claims and [runbook](docs/runbook.md) for operation and recovery.

## Author

**Shreetama Basu** — Data Engineering portfolio project
