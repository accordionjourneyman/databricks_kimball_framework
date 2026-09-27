# Operational Runbook

Procedures for diagnosing and fixing Kimball pipeline incidents from the CLI.
Every dangerous state below has a signature that `kimball inspect` /
`kimball explain` detects and a remediation that `kimball recover` performs.
The state-reasoning lives in the `kimball.ops` harness; this runbook is the
human procedure around it.

> **Scope.** This covers the Gold-layer runtime: watermarks, zombies,
> crash-recovery drift, CDF retention, single-writer. It does **not** cover
> Bronze ingestion, Unity Catalog grants, or cluster provisioning.

## Verify your harness (before an incident)

Confirm you can reach `etl_control` and the target **before** you need to:

```bash
kimball inspect --target prod --table gold.fact_sales
```

A healthy target prints `reconciliation: consistent` and exits 0. If this
command cannot build a Spark session or resolve the ETL schema now, it will not
work at 03:00 either — fix the session/target resolution first.

The standalone entry point produces the same verdicts:

```bash
python tools/inspect_etl_control.py --target prod --table gold.fact_sales
```

For a project-wide overview (every target in `etl_control`), omit `--table`:

```bash
python tools/inspect_etl_control.py --target prod
```

## Decision flow

1. `kimball inspect --target <env> --table <t>` → read `reconciliation.verdict`.
2. If not `consistent`, `kimball explain --target <env> --table <t>` for the
   category + recommended command, or `--batch-id <b>` to attribute a recorded
   failure.
3. Run the recommended `kimball recover ...`. Always try `--dry-run` first.
4. Re-run `kimball inspect` to confirm `consistent`.

---

## Dangerous-state signatures

<a id="control-table-missing"></a>
### Control table missing

**Meaning.** `etl_control` does not exist in the target's ETL schema. Either no
pipeline has ever run in this environment, or the wrong `--target` / ETL schema
was resolved.

**Detect.** `kimball inspect` → `reconciliation.verdict: control_table_missing`
(`control_table_exists: false`).

**Fix.** Run any pipeline once to create the control schema:
`kimball run --config <cfg> --target <env>`. Then re-inspect. Do **not** treat
"no control table" as "no zombies" — it means you have no observability yet.

<a id="target-missing"></a>
### Target missing

**Meaning.** `etl_control` exists but the target table does not. A pipeline
never created it, or someone dropped it.

**Detect.** `reconciliation.verdict: target_missing`.

**Fix.** Run the pipeline once to create the target, or restore from a
known-good state. If the target was dropped intentionally, full-reload.

<a id="zombie-with-committed-data"></a>
### Zombie with committed data

**Meaning.** A batch is `RUNNING` in `etl_control` and the target has commits
tagged with that batch_id — the driver crashed **after** the MERGE but **before**
`batch_complete`. The target and the watermark are now inconsistent.

**Detect.** `reconciliation.verdict: zombie_with_committed_data`,
`zombie_commits >= 1`.

**Fix.**
```bash
kimball recover --target prod --table gold.fact_sales --dry-run
# review the planned RESTORE version + watermark rewinds, then:
kimball recover --target prod --table gold.fact_sales
```
This is **two-phase**: RESTORE the target to the pre-batch version, then mark the
batch FAILED and rewind the watermark to the previous SUCCESS watermark (or
`None` → full CDF replay next run). If phase 2 fails, the result is reported
`partial: true` — re-run with `--rewind-watermark`.

**Pitfall.** If the pre-batch version was VACUUMed, `recover` refuses to RESTORE
and falls back: re-run with `--full-reload` (drops the watermark; the next run
rebuilds from CDF/snapshot).

<a id="zombie-no-commit"></a>
### Zombie, no commit

**Meaning.** A batch is `RUNNING` but no commit on the target is tagged with
its batch_id. The crash happened before the MERGE committed — the target is
clean, only the control row is stale.

**Detect.** `reconciliation.verdict: zombie_no_commit`, `zombie_commits: 0`.

**Fix.**
```bash
kimball recover --target prod --table gold.fact_sales
```
No RESTORE is needed; `recover` just clears the stale `RUNNING` rows. Safe.

<a id="fresh-zombie-startup"></a>
### Fresh zombie at startup

**Meaning.** A driver can crash moments after creating a `RUNNING` control row.
Startup recovery therefore checks **all** `RUNNING` rows, including rows younger
than the former 60-minute staleness window; it does not wait for a timeout.

**Detect.** Run `kimball inspect --target prod --table gold.fact_sales` before
starting a second writer. A fresh `RUNNING` batch is handled by startup recovery
as either `zombie_with_committed_data` or `zombie_no_commit`.

**Fix.** Restart the pipeline only after the prior driver is confirmed stopped.
The normal startup path recovers the row automatically. If startup recovery is
disabled, run `kimball recover --target prod --table gold.fact_sales --dry-run`
first, then run the displayed recovery command.

**Pitfall.** A live batch looks the same as a fresh crashed batch. Preserve the
single-writer contract: never start a second driver until the first is known to
be stopped.
<a id="serverless-no-tagging"></a>
### Serverless: no commit tagging

**Meaning.** On Serverless compute, `spark.databricks.delta.commitInfo.userMetadata`
is restricted (KNOWN_LIMITATIONS §2), so commits cannot be attributed to a
batch_id. Auto-recovery cannot identify the zombie's commits.

**Detect.** `runtime.flavor: serverless`, `supports_commit_tagging: false`;
`reconciliation.verdict: zombie_no_commit` with evidence mentioning Serverless.

**Fix.** You must supply the restore target manually:
```bash
# inspect target history to find the pre-batch version
kimball recover --target prod --table gold.fact_sales --version <pre-batch-version>
# or by timestamp:
kimball recover --target prod --table gold.fact_sales --timestamp 2026-07-20T12:00:00
```
If you cannot determine the version, `--full-reload`.

**Pitfall.** Without tagging, `inspect` cannot distinguish zombie-with-committed-
data from no-commit on Serverless — assume committed data and supply `--version`.

<a id="watermark-ahead-of-target"></a>
### Watermark ahead of target (post-rollback drift)

**Meaning.** The target was RESTOREd (a RESTORE operation is in its history)
but `etl_control` was **not** rewound — the watermark now points past the
target's actual state. The next incremental run reads CDF from `watermark+1`
and **silently skips** the versions between the restored-from and the watermark.

**Detect.** `reconciliation.verdict: watermark_ahead_of_target`. This is the
highest-risk state: data loss without an error.

**Fix.**
```bash
kimball recover --target prod --table gold.fact_sales --rewind-watermark
```
Rewinds the watermark to `None` for each source, forcing a full CDF replay on
the next run (idempotent MERGE reconciles the target). Expensive but correct.

**Pitfall.** Do not just re-run the pipeline — that is exactly what skips data.

<a id="target-ahead-of-watermark"></a>
### Target ahead of watermark

**Meaning.** The target has commits tagged with batch_ids `etl_control` does not
know about (e.g. after a control-table reset, or a batch whose control row was
deleted). Usually benign if a batch is genuinely in-flight.

**Detect.** `reconciliation.verdict: target_ahead_of_watermark`.

**Fix.** If the orphan is a crashed/leftover batch (not an in-flight run),
RESTORE it explicitly:
```bash
kimball recover --target prod --table gold.fact_sales --batch-id <orphan> --force
```
(`reconciliation.orphan_batch_ids` lists them.) If a control-table reset
occurred or the orphan should be accepted as the new baseline, re-run the
pipeline (idempotent MERGE) to advance the watermark, or full-reload. Inspect
history to decide.

<a id="concurrent-writer"></a>
### Concurrent writer / single-writer violation

**Meaning.** The target has commits tagged with batch_ids unknown to
`etl_control` — another kimball run, or an external writer reusing the tagging
convention. The single-writer contract (one writer per target) is broken;
RESTORE-based recovery is unsafe until this is resolved.

**Detect.** `writer_contract.verdict: suspected_violation`, or
`reconciliation.verdict: target_ahead_of_watermark`. `kimball recover` refuses
to proceed without `--force`.

**Fix.** Confirm no other writer is active, enforce `max_concurrent_runs: 1` on
the job. Find the orphan batch_id in the inspect output
(`reconciliation.orphan_batch_ids`), then RESTORE it explicitly:
```bash
kimball inspect --target prod --table gold.fact_sales   # note the orphan batch_id
kimball recover --target prod --table gold.fact_sales --batch-id <orphan> --force
```
`--force` is required because orphan RESTORE is destructive (it removes a commit
the control table cannot vouch for). `--force` alone (without `--batch-id`) does
**not** recover an orphan - there is no `RUNNING` batch to target.

**Pitfall.** RESTORE rolls back **all** commits after the target version,
including an external writer's. Never `--force` blindly.

<a id="cdf-gap"></a>
### CDF gap (source vacuumed past watermark)

**Meaning.** A CDF source was VACUUMed past the watermark, so the next
incremental run cannot resume from `watermark+1` — the change data for the
skipped versions is gone.

**Detect.** `source_health[].verdict: cdf_gap` ("need version N, earliest
available is M"). This is the most common real-world CDF failure and is
invisible until the job dies.

**Fix.**
```bash
kimball recover --target prod --table gold.fact_sales --full-reload
```
Resets the watermark to `None`; the next run rebuilds the target from a full
snapshot/CDF-from-earliest. Increase `delta.logRetentionDuration` /
`delta.deletedFileRetentionDuration` on the source to prevent recurrence.

<a id="source-unavailable"></a>
### Source unavailable

**Meaning.** A source table is missing or unreachable from this target's
catalog/schema.

**Detect.** `source_health[].verdict: missing`, or `reconciliation.verdict:
target_missing` / `control_table_missing`.

**Fix.** Confirm the source exists in this environment (right catalog/schema,
UC grants). Re-run.

<a id="schema-drift"></a>
### Schema drift

**Meaning.** A source's schema changed since the last successful run (recorded
`source_schema_fingerprint` ≠ current), or the pipeline config changed since
the recorded run (`config_fingerprint` mismatch).

**Detect.** `kimball explain --target prod --table <t> --config <cfg>` →
`category: SCHEMA_DRIFT` or `CONFIG`, with `config_drift: true` /
`sources[].schema_drift: true`.

**Fix.** Update the pipeline YAML to match the new source schema and re-validate
(`kimball validate`), or full-reload if the target must be rebuilt to match.

<a id="config"></a>
### Config error

**Meaning.** A compile-time / configuration failure. No `etl_control` row is
written (the run never reached the engine, and synchronous RUNNING writes are
skipped by default), so this is the **common** failure class, not an edge case.

**Detect.** `kimball explain --target prod --config <cfg>` → `category: CONFIG`
with remediation + `kimball validate` recommendation.

**Fix.** Fix the YAML, then `kimball validate --config <cfg> --target <env>`.

<a id="recovery"></a>
### Recovery

**Meaning.** A recovery action is required (zombie or drift). See the specific
signature above.

**Fix.** `kimball recover --target <env> --table <t>` (with `--dry-run` first).
Always re-`inspect` afterwards to confirm `consistent`.

<a id="source-repair"></a>
### Corrected source version (semantic data repair)

**Meaning.** The source table has a newer corrected version, but one or more
targets already contain data derived from the bad version. This is different
from a failed or zombie batch: use `kimball recover` for a failed batch and
`kimball repair` when a completed run produced semantically bad data.

Delta history stores snapshots and commits per table. It can restore one table
to a retained version, but it does not plan a multi-table rebuild or keep one
repair's inputs, attempts, output commits, timings, and cursor state together.
The framework stores those facts in normalized `etl_repair_*` Delta tables.
Each table has a specific audit grain:

| Repair fact | Ledger table |
|---|---|
| Bad and replacement source versions | `etl_repair_input_correction` |
| Planned target steps and dependency order | `etl_repair_step`, `etl_repair_step_dependency` |
| Pre-repair output versions used as rollback points | `etl_repair_write` |
| Planned and actually consumed source versions | `etl_repair_read`, `etl_repair_attempt_read` |
| Actual target Delta commits and their operation tokens | `etl_repair_commit` |
| Attempt identity and per-step start state | `etl_repair_attempt`, `etl_repair_step_attempt` |
| Attempt outcome and elapsed milliseconds | `etl_repair_event` |

Source versions belong to these repair/read records; business rows do not need
a combined source-version dictionary column.

**Plan.** Use the same repair ID for the plan, apply, status, and rollback. The
bad version identifies the source correction; by default the replacement is
the source's current version when the plan is created. Set `--read-version`
when the replacement must be a specific retained version:

```bash
kimball repair plan \
  --target prod \
  --config configs/dim_customer.yml configs/fact_orders.yml \
  --repair-id customer-feed-2026-09-27 \
  --reason "replace the malformed customer feed" \
  --bad-version silver.customers=42 \
  --read-version silver.customers=43
```

The plan shows affected targets, each configured input and whether it is a
snapshot or an internal dependency, the dependency order, physical table
generations, and each planned output's exact pre-repair Delta version.
External inputs are pinned to snapshots at plan time; internal dependencies
resolve to the output version produced by an earlier repair step. Consumers
of the corrected source and their configured downstream consumers are included
automatically. Use `--table`
to add an explicit output target; its downstream consumers are included too.
If `--table` is used without `--bad-version`, the plan rebuilds those targets
from the input snapshots selected at planning time.
Every `--read-version TABLE=VERSION` must have a matching
`--bad-version TABLE=VERSION`.
Transformation SQL must be a single read-only `SELECT`/`WITH` and reference
declared sources so their Delta versions can be pinned. A downstream step that
reads an output rebuilt by an earlier step consumes that step's produced
version; apply records the actual version in the attempt-read ledger. A plan
is frozen: use a new repair ID for a different version vector or configuration.
Each external table is pinned independently; the set of versions is not
automatically a transactionally consistent source release. Select source
versions that belong to the intended release before applying the repair.

**Apply and inspect.** Snapshot rebuild currently supports batch Delta sources
and SCD1 targets. Apply with the same `--config` set and target used to create
the plan; it rejects a different project manifest. Pause scheduled and
streaming writers, manual writers, and readers that need a consistent
multi-table view for the full operation. The flag below records that operator
acknowledgement; it does not acquire a lock.
Tables are rebuilt in dependency order, so the series is not a cross-table
atomic publication.

```bash
kimball repair apply --target prod --config configs/dim_customer.yml configs/fact_orders.yml \
  --repair-id customer-feed-2026-09-27 --writers-stopped
kimball repair status --target prod --repair-id customer-feed-2026-09-27
```

`status` includes the correction, per-step input versions and prior source
watermarks, target writes, tagged Delta commit versions, and elapsed times.
`PARTIAL` means inspect every listed table before retrying or rolling back.
Once rollback has started, that repair ID cannot be applied again; create a
new ID to reapply after a successful rollback. SCD2/SCD4 history, separate
history tables, streaming checkpoints, temporal contracts,
`preserve_all_changes`, and non-read-only or undeclared SQL inputs need their
own replay strategies and are rejected by this snapshot-rebuild path.

**Rollback.** Rollback returns each changed model or auxiliary output to the
version recorded before planning, in reverse dependency order, and rewinds each
pipeline's source cursors to their recorded prior watermarks. It proceeds only
when the target generation and complete retained history match the repair's
tagged commits; any unrelated write after the rollback point blocks whole-table
RESTORE because it would be discarded too. Newly created outputs are also
rejected because a Delta version cannot represent the table's prior absence.

```bash
kimball repair rollback --target prod \
  --repair-id customer-feed-2026-09-27 --writers-stopped
```

Keep the maintenance window active until rollback and cursor rewinds complete.
The `--writers-stopped` flag is an operator acknowledgement, not a lock.
Repairs update tables in place, so readers may see different tables at
different points in the ordered rebuild; there is no cross-table atomic
publication. Do not VACUUM the source or target history needed by an open
repair. If the source's corrected version is unavailable, restore it from the
upstream archive first and create a new plan.

<a id="resource"></a>
### Resource / transient

**Meaning.** Transient cluster/executor issue (lost executor, shuffle failure,
OOM, source busy with OPTIMIZE/VACUUM).

**Detect.** Batch `error_message` contains `timeout`/`memory`/`oom`/`executor`,
or `kimball explain --batch-id <b>` infers `category: RESOURCE`.

**Fix.** Retry with backoff. If OOM is recurring, raise driver/executor memory
or reduce `spark.sql.shuffle.partitions`. Check the source isn't being
OPTIMIZEd/VACUUMed concurrently.

<a id="data-quality"></a>
### Data quality

**Meaning.** A DQ rule (contract gate / expectation) failed — nulls in required
columns, duplicate natural keys, referential integrity.

**Detect.** Batch `error_message` references a DQ failure; findings are in the
evidence/`findings` table.

**Fix.** Inspect the failing rows in the findings table, fix upstream, and
re-run. Do not rewind the watermark — the target was not mutated past the
failure.

<a id="unknown"></a>
### Unknown

**Meaning.** The target appears consistent and no recorded error matches a
known category.

**Fix.** `kimball inspect --target <env> --table <t>` for the full state; if the
batch recorded an error, `kimball explain --target <env> --table <t> --batch-id
<b>`. Fall back to `system.query.history` (Phase 1.4, Databricks-only) for
query-level evidence.

---

## Reference: exit codes

`kimball inspect` / `kimball explain`: `0` consistent, `1` attention required.
`kimball recover`: `0` success (or dry-run), `1` partial recovery (phase 2 did
not complete — re-run) or a `StructuredError` (e.g. control-table-missing /
target-missing). `kimball deploy`: `0` clear (gate passed), `2` blocked
(breaking without `--allow-breaking`, or a pre-flight blocker).
`tools/inspect_etl_control.py`: `0` consistent, `1` attention, `2` could not
run (session/read failure).
