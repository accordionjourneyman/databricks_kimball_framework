# Known Limitations (PySpark Implementation)

This document outlines known limitations, design choices, and potential edge cases in the PySpark implementation of the Kimball Framework.

## 1. Delete Propagation (Delete Semantics)

- **Behavior:** The framework **propagates** deletes from Source to Target.
  - **Source:** `loader.py` reads CDF data and filters out `update_preimage` rows but **includes** `delete` rows.
  - **Target (SCD1):** Deletes are applied (rows removed).
  - **Target (SCD2):** Deletes are handled as "Logical Deletes" if configured (`__is_deleted` = true) or hard deletes (`whenMatchedDelete`).
- **Documentation Correction:** Previous documentation stating "Deletes are filtered out" was incorrect. The code correctly handles deletes.

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
