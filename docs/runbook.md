# Local and production-adaptation runbook

## Preflight

- Use Python 3.11 and Java 17.
- Install `requirements-dev.lock` with `--require-hashes`.
- Confirm the immutable batch has `manifest.json` plus all five files.
- Use a new local lakehouse root for destructive experiments.

## Execute one batch

```bash
python -m retail_lakehouse.pipeline \
  --batch-dir data/input/2026-09-06/retail-20260906 \
  --lakehouse-root data/lakehouse
```

Check the JSON result and Delta audit tables. Success requires reconciliation for every entity and a final `SUCCESS` attempt state.

## Backfill

Process immutable batches in business-date order. Replaying a processed path repeats the same entity/date/batch Bronze transaction and is safe for business tables; a new attempt is intentionally recorded. A batch ID reused on another business date remains a distinct delivery. The current-state Silver model uses `updated_at` protection, so an older record does not overwrite a newer one.

## dbt

CI performs `dbt parse` only. The incremental fact retains a prior order date when a correction moves an item; the daily aggregate then rebuilds both prior and current dates from current fact state. Retaining the prior date makes rerunning after an aggregate failure safe, and an emptied prior date is merged with zero totals rather than left stale. To execute models, an operator must create a separate uncommitted Databricks profile, select a governed catalog/schema, and run dbt deliberately from an authenticated environment. Never replace the inert CI profile with credentials.

## Databricks adaptation checklist

Before real deployment, add workspace-specific governance outside this repository: Unity Catalog ownership/grants, secret or workload identity, cluster/job policies, environment promotion, service principals, budgets, alert routing, and delete protection. Validate runtime compatibility and run the replay/DQ suite in a non-production workspace.

No deploy command or workspace provisioning is supplied here. This is deliberate cost protection.
