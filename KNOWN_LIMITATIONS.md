# Known Limitations (PySpark Implementation)

This document outlines known limitations, design choices, and potential edge cases in the PySpark implementation of the Kimball Framework.

## 1. Delete Semantics (Kimball-Compliant)

**Default Behavior**: `delete_strategy: soft` (preserves referential integrity)

| SCD Type | Delete Behavior                                             |
| -------- | ----------------------------------------------------------- |
| **SCD1** | Sets `__is_deleted = true` (row preserved)                  |
| **SCD2** | Expires row (`__is_current = false`, `__is_deleted = true`) |

**Why Soft Deletes?** Kimball requires dimension rows to remain for FK integrity. Facts that referenced a deleted customer must still join correctly.

**Hard Delete Option**: `delete_strategy: hard` removes rows (use for GDPR compliance, not general ETL).

## 2. Crash Consistency (Single-Writer Compensating Rollback)

- **Risk:** A driver crash occurring after the Delta `MERGE` but before the Watermark/Audit commit (`batch_complete`) can leave the system in an inconsistent state (Data committed, Watermark stale).
- **Remediation:** The framework implements a **compensating rollback protocol**:
  - Commits are tagged with `batch_id`.
  - On startup, `Orchestrator` checks for "RUNNING" (zombie) batches.
  - If found, it performs a **Rollback** (`RESTORE TABLE`) to the state prior to the crashed batch using Delta Time Travel.
- **Limitation:** If `VACUUM` has removed the history required for rollback, manual intervention is required.
- **Not atomic across tables:** the target Delta commit and `etl_control` watermark are separate transactions. Recovery compensates after failure; it cannot provide cross-table ACID.
- **Single-writer contract:** safety requires one active writer for each target and no unrelated commit between the failed write and `RESTORE`. Compiled Databricks jobs set `max_concurrent_runs: 1`; external schedulers must enforce the same rule.
- **Serverless Limitation:** On Databricks Serverless, setting `spark.databricks.delta.commitInfo.userMetadata` is restricted.
  - **Impact:** Crashed batches cannot be tagged, so zombie recovery cannot identify their commits automatically. The pipeline continues without compensating rollback protection.

## 3. Concurrency & Locking

- **Single Writer Per Target Table:** The framework assumes a single active pipeline writer per target table. Concurrent pipelines writing to the _same_ target table are not supported and may result in partial rollbacks or conflicts.
  - _Workaround:_ Use Databricks Jobs concurrency control (max 1 run) for the specific pipeline job.
- **Shared Control Table (etl_control):** Parallel pipelines writing to _different_ target tables share a single `etl_control` table. The framework retries `ConcurrentAppendException` on `etl_control` MERGEs with exponential backoff (5 attempts). This is tested and working.

## 4. Retry Mechanism

- **Implementation:** Retries for concurrent write conflicts (`ConcurrentAppendException`) rely on string matching of exception messages.
- **Fragility:** This is brittle and may break if Databricks/Spark changes exception class names or messages in future runtime versions.

## 5. Schema Evolution

- **SCD2:** Adding new columns to the source _without_ adding them to `track_history_columns` (if explicit tracking is on) will result in those columns being updated in place (SCD1 behavior) for existing rows. A warning is logged.

## 6. Surrogate Key Generation

- **Implementation:** All surrogate keys are generated via `xxhash64(natural_keys)` (SCD1/SCD4/SCD6) or `xxhash64(natural_keys + version_column)` (SCD2). This is deterministic, distributed-safe, and idempotent.
- **Collision Risk:** `xxhash64` produces a 64-bit signed BIGINT. Birthday-paradox collisions become statistically significant at very large cardinalities. A full reload does not heal a deterministic collision. Managed junk dimensions detect conflicting combinations; general dimensions require collision monitoring or an identity/key-registry design where the risk is unacceptable.
- **Negative values:** `xxhash64` produces signed BIGINTs, so roughly half of all keys are negative. This is expected and does not affect correctness.

## 7. Idempotency Contracts

The framework provides the following guarantees for repeated runs of the same batch:

| Merge Strategy          | Idempotency Guarantee              | Notes                                                                                                                    |
| ----------------------- | ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| **upsert** (default)    | ✅ **Idempotent**                  | Same batch replayed → same result. Delta MERGE is inherently idempotent when matching on natural/merge keys.             |
| **append**              | ❌ **NOT idempotent**              | Repeated runs will create duplicates. User must ensure upstream deduplication or use watermark to prevent re-processing. |
| **partition_overwrite** | ✅ **Idempotent within partition** | Same partition rewritten → same result. Idempotent as long as partition bounds are deterministic.                        |

### Ensuring Idempotency

1. **Watermarks**: The framework tracks `last_processed_version` (CDF) or `last_processed_timestamp` to prevent re-processing the same data.

2. **Batch Tracking**: Each run is assigned a unique `batch_id`. Crashed batches are detected and rolled back on restart.

3. **Grain Enforcement**: Pre-merge validation ensures source data has unique keys, preventing silent duplicates.

### When Idempotency Breaks

- **Watermark reset**: Manually resetting watermarks will cause re-processing.
- **Non-deterministic transforms**: `current_timestamp()` or `rand()` in `transformation_sql` produces different results on replay.
- **External state**: Joins to external tables that change between runs.

### Best Practices

- Use `upsert` (default) for dimensions and transaction facts.
- Reserve `append` for immutable event logs where source has exactly-once semantics.
- Use `partition_overwrite` for bulk restatements with date-partitioned tables.

## 8. Streaming CDF Limitations

The streaming module (`StreamingOrchestrator`) is a first-class feature but has
the following limitations compared to the batch path:

| Feature | Batch | Streaming |
|---------|-------|-----------|
| Target table creation | ✅ Auto-creates + seeds defaults | ✅ Auto-creates on first micro-batch |
| Schema evolution | ✅ Supported | ✅ Supported (passed to merger) |
| Fingerprint caching | ✅ Supported | ✅ Saved after each micro-batch |
| FK validation | ✅ Supported | ✅ Supported (pre-merge gate) |
| Adaptive column pruning | ✅ Supported | ❌ Not implemented |
| Grain violation checks | ✅ Supported | ✅ Supported |
| Zombie batch recovery | ✅ Supported | ❌ Not implemented |
| Per-version SCD2 history | ✅ `preserve_all_changes` | ✅ `streaming.per_version: true` |
| PII masking | ✅ Supported | ✅ Supported |
| Multi-source pipelines | ✅ Single run | ✅ One query per CDF source |
| `transformation_sql` | ✅ `spark.sql()` | ✅ `spark.sql()` inside `foreachBatch` |

| Contract DQ checks | Supported | Supported per micro-batch |
| Cross-batch temporal state | Post-merge/watermark commit | Post-merge/watermark commit |

**When to use streaming:**
- Sub-minute freshness requirements
- Continuous ingestion pipelines
- Sources already using `cdc_strategy: cdf`

**When to stay on batch:**
- One-off backfills or historical loads
- Schema changes are frequent
- You need zombie batch recovery

**Checkpoint portability:**
Checkpoint directories are tied to the Spark application. Moving or copying
them may cause corruption. Always use a persistent location (DBFS, S3, ADLS)
for production streaming checkpoints.
