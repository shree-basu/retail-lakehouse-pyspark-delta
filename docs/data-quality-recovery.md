# Data quality, observability, and recovery

## Quality controls

Silver validation covers cast failures, required fields, domains, positive quantities/prices, USD-only semantics, deterministic natural-key resolution, customer/order/product foreign keys, and malformed JSON.

Every record lands in exactly one of accepted or quarantined output. A failed `input_rows = accepted_rows + quarantined_rows` assertion stops publication.

## Quarantine and audit

Quarantine stores entity, serialized key, record hash, batch ID, business date, source file, all reason codes, rejected payload, and timestamp. Its `(source_batch_id, entity, record_hash)` merge key makes retries safe.

`audit/pipeline_metrics` holds deterministic per-run/per-entity counts. `audit/run_status` preserves each execution attempt with timestamps and bounded error details. The business `run_id` is repeatable; `attempt_id` distinguishes retries.

## Recovery procedure

1. Read the failed attempt and entity metrics/quarantine reasons.
2. For a transient executor/filesystem failure, rerun the same immutable path. Bronze transaction identity and downstream merges make the replay safe.
3. For a contract error, do not edit the existing batch. Correct the producer and publish a new batch ID.
4. For a deterministic DQ rejection, retain it or correct it in a later immutable batch.
5. Confirm all five reconciliations, final run state, and dbt tests.

The scheduler must prevent concurrent executions for one `run_id`. Simultaneous same-batch processes are outside the reference scope.
