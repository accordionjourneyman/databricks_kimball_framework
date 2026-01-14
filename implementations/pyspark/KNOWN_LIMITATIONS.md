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

## 2. Crash Consistency (Atomic Batch Recovery)

- **Risk:** A driver crash occurring after the Delta `MERGE` but before the Watermark/Audit commit (`batch_complete`) can leave the system in an inconsistent state (Data committed, Watermark stale).
- **Remediation:** The framework implements **Transactional Recovery**:
  - Commits are tagged with `batch_id`.
  - On startup, `Orchestrator` checks for "RUNNING" (zombie) batches.
  - If found, it performs a **Rollback** (`RESTORE TABLE`) to the state prior to the crashed batch using Delta Time Travel.
- **Limitation:** If `VACUUM` has removed the history required for rollback, manual intervention is required.
- **Serverless Limitation:** On Databricks Serverless, setting `spark.databricks.delta.commitInfo.userMetadata` is restricted.
  - **Impact:** Crashed batches cannot be tagged, so Zombie Recovery will not find them to rollback automatically. The pipeline will proceed safely but without this protection.

## 3. Concurrency & Locking

- **Single Writer Per Table:** The framework assumes a single active pipeline writer per target table. Concurrent pipelines writing to the _same_ target table are not supported and may result in partial rollbacks or conflicts.
  - _Workaround:_ Use Databricks Jobs concurrency control (max 1 run) for the specific pipeline job.

## 4. Retry Mechanism

- **Implementation:** Retries for concurrent write conflicts (`ConcurrentAppendException`) rely on string matching of exception messages.
- **Fragility:** This is brittle and may break if Databricks/Spark changes exception class names or messages in future runtime versions.

## 5. Schema Evolution

- **SCD2:** Adding new columns to the source _without_ adding them to `track_history_columns` (if explicit tracking is on) will result in those columns being updated in place (SCD1 behavior) for existing rows. A warning is logged.

## 6. Sequence Key Generation (Blocked)

- **Issue:** The `SequenceKeyGenerator` (using `Window.orderBy(lit(1))`) forces a global sort on a single executor, causing OOM errors on large datasets.
- **Status:** This strategy is now **BLOCKED** by default.
- **Mitigation:** Use `surrogate_key_strategy: identity` (Delta Identity Columns) or `hash`.
- **Override:** Can be force-enabled via `KIMBALL_ALLOW_UNSAFE_SEQUENCE_KEY=1` (Not Recommended).

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
