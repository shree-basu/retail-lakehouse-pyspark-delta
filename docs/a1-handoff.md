# A1 focused post-implementation audit

Use this prompt after the pull request CI is green.

```text
FOCUSED POST-IMPLEMENTATION AUDIT — RETAIL LAKEHOUSE

Review the actual pull-request branch for shree-basu/retail-lakehouse-pyspark-delta as the source of truth. Compare it with baseline af2272098e450fd3cc5c6a14094bbabaf126717c. Do not propose optional cloud services, architecture expansion, or résumé theatre.

Verify only:
1. immutable path/manifest/count/checksum and structured/semi-structured schema contracts;
2. deterministic DQ, quarantine, reconciliation, natural/FK handling, and schema-evolution boundary;
3. Bronze transaction idempotency plus Silver/quarantine/audit replay safety and concurrent-run limitation;
4. dbt grains, incremental fact and affected-date aggregate correctness, tests, and documentation;
5. Spark filter/projection/broadcast/AQE/layout/cache claims against code and tests;
6. cloud-free CI, SHA-pinned actions, hash locks, inert dbt profile, and impossibility of repository-driven deployment/charges;
7. README/evidence language versus what tests actually prove.

Treat Databricks deployment, representative production-scale performance, cloud cost, SLA, Unity Catalog, and active operations as explicitly unclaimed—not blockers for this reference scope.

Output only:
A. PASS or FAIL for correctness/security blockers, with exact file/line evidence for failures;
B. smallest necessary fixes only;
C. final classification: NO MAJOR CHANGES, MINOR HARDENING, MODERATE PRODUCTIONIZATION, or ARCHITECTURE DISCUSSION;
D. whether engineering can freeze and the project can move to interview defense.
```
