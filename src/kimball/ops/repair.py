"""Explicit repair operations with durable intent and guarded Delta rollback."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from kimball.ops.errors import ErrorCategory, StructuredError
from kimball.ops.providers import OpsProviders, TargetDeltaState
from kimball.ops.repair_ledger import (
    DeltaRepairLedger,
    RepairPlanStep,
    stable_repair_id,
)
from kimball.ops.runtime_profile import RuntimeProfile

RUNBOOK = "docs/RUNBOOK.md"


@dataclass(frozen=True)
class RepairRestoreResult:
    repair_id: str
    attempt_id: str
    target_table: str
    target_generation: str
    version_before: int
    rollback_version: int
    version_after: int
    duration_ms: int
    reconciled_existing_commit: bool


@dataclass(frozen=True)
class RepairRollbackResult:
    repair_id: str
    attempt_id: str
    status: str
    restored_tables: tuple[dict[str, Any], ...]
    duration_ms: int
    errors: tuple[str, ...] = ()


def restore_target(
    *,
    target_table: str,
    rollback_version: int,
    expected_current_version: int,
    origin_batch_id: str,
    reason: str,
    providers: OpsProviders,
    ledger: DeltaRepairLedger,
    runtime: RuntimeProfile,
    repair_id: str | None = None,
    owner: str | None = None,
    environment: str | None = None,
    writers_stopped: bool = False,
    history_limit: int = 10_000,
) -> RepairRestoreResult:
    """Restore one target after proving the entire reverted range is one batch.

    This is an immediate whole-table rollback. It is safe only when every
    commit after ``rollback_version`` belongs to ``origin_batch_id`` and the
    caller has stopped all writers. It is not a selective undo or a replay.
    """
    if not writers_stopped:
        raise StructuredError(
            "repair rollback requires all writers to the target to be stopped",
            category=ErrorCategory.CONCURRENT_WRITER,
            remediation=(
                "Quiesce scheduled, streaming, and manual writers, then pass "
                "--writers-stopped to acknowledge the maintenance window."
            ),
            runbook_link=f"{RUNBOOK}#recovery",
        )
    if not runtime.supports_commit_tagging:
        raise StructuredError(
            "repair rollback requires Delta commit metadata tagging",
            category=ErrorCategory.RECOVERY,
            remediation="Run the repair on a runtime that supports Delta userMetadata.",
        )
    if rollback_version < 0 or expected_current_version < 0:
        raise StructuredError(
            "Delta versions must be non-negative", category=ErrorCategory.CONFIG
        )
    if rollback_version >= expected_current_version:
        raise StructuredError(
            "rollback version must be lower than the expected current version",
            category=ErrorCategory.CONFIG,
        )
    if not origin_batch_id.strip() or not reason.strip():
        raise StructuredError(
            "origin batch ID and repair reason are required",
            category=ErrorCategory.CONFIG,
        )

    repair_id = repair_id or str(uuid.uuid4())
    started_clock = time.monotonic()
    state = providers.history.get_target_delta_state(
        target_table, history_limit=history_limit
    )
    operation_token = _operation_token(repair_id)
    prior_repair_commit = next(
        (c for c in state.commits if c.operation_token == operation_token), None
    )
    if prior_repair_commit is None:
        _validate_target_state(
            target_table,
            state,
            expected_current_version,
            rollback_version,
            origin_batch_id,
        )
    else:
        if (
            not state.table_exists
            or not state.generation_id
            or state.current_version != prior_repair_commit.version
            or prior_repair_commit.version <= expected_current_version
        ):
            raise StructuredError(
                f"target {target_table} changed after the tagged repair RESTORE",
                category=ErrorCategory.CONCURRENT_WRITER,
                remediation="Inspect current Delta history before reconciling the repair.",
            )

    current_control = providers.control.get_target_state(target_table)
    running = [b for b in current_control.batches if b.status.upper() == "RUNNING"]
    if running:
        raise StructuredError(
            f"target {target_table} has {len(running)} RUNNING batch record(s)",
            category=ErrorCategory.CONCURRENT_WRITER,
            remediation="Wait for or recover the RUNNING batch before repairing this target.",
        )

    ledger.create_plan(
        repair_id=repair_id,
        reason=reason,
        owner=owner,
        strategy="whole_table_restore",
        publication_policy="in_place_single_writer",
        steps=(
            RepairPlanStep(
                target_table=target_table,
                target_generation=state.generation_id,
                strategy="restore",
                expected_current_version=expected_current_version,
                rollback_version=rollback_version,
                scope_ref=f"delta-batch:{origin_batch_id}",
            ),
        ),
    )
    attempt_id, step_attempt_id = ledger.start_attempt(
        repair_id=repair_id,
        target_existed_before=True,
        target_version_before=state.current_version,
        actor=owner,
        environment=environment,
    )

    try:
        if prior_repair_commit is None:
            if state.current_version != expected_current_version:
                raise StructuredError(
                    f"target advanced from version {expected_current_version} "
                    f"to {state.current_version} before RESTORE",
                    category=ErrorCategory.CONCURRENT_WRITER,
                    remediation="Re-inspect the target and create a fresh repair plan.",
                )
            tagged_restore = getattr(
                providers.history, "restore_to_version_tagged", None
            )
            if not callable(tagged_restore):
                raise StructuredError(
                    "Delta history provider cannot tag repair RESTORE commits",
                    category=ErrorCategory.RECOVERY,
                )
            metadata = (
                f"repair_id={repair_id}; operation_token={operation_token}; "
                f"batch_id={origin_batch_id}"
            )
            tagged_restore(target_table, rollback_version, metadata)
            after = providers.history.get_target_delta_state(
                target_table, history_limit=history_limit
            )
            restore_commit = next(
                (c for c in after.commits if c.operation_token == operation_token), None
            )
            if restore_commit is None:
                raise StructuredError(
                    "RESTORE returned, but its tagged Delta commit was not found",
                    category=ErrorCategory.RECOVERY,
                    remediation=(
                        "Do not retry with a new repair ID. Inspect Delta history and "
                        "reconcile the existing repair operation token."
                    ),
                )
            state_after = after
            restored_existing = False
        else:
            restore_commit = prior_repair_commit
            state_after = state
            restored_existing = True

        assert restore_commit is not None
        assert state_after.current_version is not None
        ledger.record_commit(
            repair_id=repair_id,
            step_attempt_id=step_attempt_id,
            target_table=target_table,
            target_generation=state.generation_id,
            target_version_before=expected_current_version,
            target_version=restore_commit.version,
            operation=restore_commit.operation or "RESTORE",
            operation_token=operation_token,
            repair_operation_token=operation_token,
            attributed=True,
            rollback_version=rollback_version,
            committed_at=restore_commit.timestamp,
        )
        elapsed = int((time.monotonic() - started_clock) * 1000)
        ledger.append_event(
            attempt_id=attempt_id,
            step_attempt_id=step_attempt_id,
            event_type="COMPLETED",
            status="SUCCEEDED",
            occurred_at=datetime.now(timezone.utc),
            elapsed_ms=elapsed,
            evidence_ref=f"{target_table}@{restore_commit.version}",
        )
        return RepairRestoreResult(
            repair_id=repair_id,
            attempt_id=attempt_id,
            target_table=target_table,
            target_generation=state.generation_id or "",
            version_before=expected_current_version,
            rollback_version=rollback_version,
            version_after=restore_commit.version,
            duration_ms=elapsed,
            reconciled_existing_commit=restored_existing,
        )
    except Exception as exc:
        elapsed = int((time.monotonic() - started_clock) * 1000)
        try:
            ledger.append_event(
                attempt_id=attempt_id,
                step_attempt_id=step_attempt_id,
                event_type="COMPLETED",
                status="FAILED",
                occurred_at=datetime.now(timezone.utc),
                elapsed_ms=elapsed,
                error_message=f"{type(exc).__name__}: {exc}",
            )
        except Exception:
            pass
        raise


def rollback_repair(
    *,
    repair_id: str,
    providers: OpsProviders,
    ledger: DeltaRepairLedger,
    runtime: RuntimeProfile,
    owner: str | None = None,
    environment: str | None = None,
    writers_stopped: bool = False,
    history_limit: int = 10_000,
) -> RepairRollbackResult:
    """Restore every repair-written table to the version frozen at planning.

    Rollback is allowed only when each commit after the saved rollback point is
    present in Delta history, recorded by the ledger, and tagged with the
    operation token owned by this repair. Tables created by the repair are
    rejected because Delta RESTORE cannot return them to a nonexistent state.
    """
    if not writers_stopped:
        raise StructuredError(
            "repair rollback requires all writers and consistent-view readers to be stopped",
            category=ErrorCategory.CONCURRENT_WRITER,
            remediation=(
                "Pause scheduled, streaming, and manual writers and coordinate downstream "
                "readers, then pass --writers-stopped for the maintenance window."
            ),
            runbook_link=f"{RUNBOOK}#recovery",
        )
    if not runtime.supports_commit_tagging:
        raise StructuredError(
            "repair rollback requires Delta commit metadata tagging",
            category=ErrorCategory.RECOVERY,
            remediation="Run the repair on a runtime that supports Delta userMetadata.",
        )

    report = ledger.status(repair_id)
    if not report["plan"] or not report["steps"]:
        raise StructuredError(
            f"repair plan not found or incomplete: {repair_id}",
            category=ErrorCategory.RECOVERY,
        )
    step_by_id = {row["step_id"]: row for row in report["steps"]}
    reads_by_step: dict[str, list[dict[str, Any]]] = {}
    for read in report.get("reads", []):
        reads_by_step.setdefault(read["step_id"], []).append(read)
    writes_by_table: dict[str, list[dict[str, Any]]] = {}
    for write in report["writes"]:
        writes_by_table.setdefault(write["target_table"], []).append(write)
    commits_by_table: dict[str, list[dict[str, Any]]] = {}
    for commit in report["commits"]:
        commits_by_table.setdefault(commit["target_table"], []).append(commit)

    rollback_items: list[dict[str, Any]] = []
    for table, writes in writes_by_table.items():
        table_commits = commits_by_table.get(table, [])
        if any(not write["target_existed_before"] for write in writes):
            raise StructuredError(
                f"cannot restore {table}: it did not exist before the repair",
                category=ErrorCategory.RECOVERY,
                remediation=(
                    "Restore newly created outputs through a separately reviewed "
                    "catalog and storage cleanup operation."
                ),
            )
        rollback_versions = {write.get("rollback_version") for write in writes}
        generations = {write.get("target_generation") for write in writes}
        if len(rollback_versions) != 1:
            raise StructuredError(
                f"repair plan has no single exact rollback version for {table}",
                category=ErrorCategory.RECOVERY,
            )
        baseline = next(iter(rollback_versions))
        if not isinstance(baseline, int) or isinstance(baseline, bool):
            raise StructuredError(
                f"repair plan has no valid exact rollback version for {table}",
                category=ErrorCategory.RECOVERY,
            )
        if len(generations) != 1 or None in generations:
            raise StructuredError(
                f"repair plan has no single physical generation for {table}",
                category=ErrorCategory.RECOVERY,
            )
        allowed_tokens = {
            step_by_id[write["step_id"]]["operation_token"]
            for write in writes
            if write["step_id"] in step_by_id
        }
        if not allowed_tokens:
            raise StructuredError(
                f"repair ledger cannot attribute a writer step for {table}",
                category=ErrorCategory.RECOVERY,
            )
        recorded: dict[int, dict[str, Any]] = {}
        for row in table_commits:
            if not row.get("attributed"):
                continue
            target_version = row.get("target_version")
            if not isinstance(target_version, int) or isinstance(target_version, bool):
                raise StructuredError(
                    f"attributed repair commit for {table} has no valid target version",
                    category=ErrorCategory.RECOVERY,
                )
            recorded[target_version] = row
        current = providers.history.get_target_delta_state(
            table, history_limit=history_limit
        )
        generation = next(iter(generations))
        if (
            not current.table_exists
            or current.current_version is None
            or current.generation_id != generation
        ):
            raise StructuredError(
                f"target generation changed or disappeared since repair: {table}",
                category=ErrorCategory.CONCURRENT_WRITER,
            )
        if baseline not in {commit.version for commit in current.commits}:
            raise StructuredError(
                f"rollback version {baseline} is no longer retained for {table}",
                category=ErrorCategory.RECOVERY,
            )
        history = {commit.version: commit for commit in current.commits}
        rollback_token = stable_repair_id(repair_id, f"rollback:{table}")
        prior_restore = next(
            (
                commit
                for commit in current.commits
                if commit.operation_token == rollback_token
            ),
            None,
        )
        if (
            prior_restore is not None
            and current.current_version != prior_restore.version
        ):
            raise StructuredError(
                f"{table} advanced after its repair rollback commit",
                category=ErrorCategory.CONCURRENT_WRITER,
            )
        if baseline < current.current_version:
            for version in range(baseline + 1, current.current_version + 1):
                commit = history.get(version)
                ledger_commit = recorded.get(version)
                if commit is None:
                    raise StructuredError(
                        f"retained Delta history for {table} is incomplete at version {version}",
                        category=ErrorCategory.RECOVERY,
                    )
                is_prior_restore = (
                    prior_restore is not None
                    and commit.version == prior_restore.version
                    and commit.operation_token == rollback_token
                )
                if not is_prior_restore and (
                    commit.operation_token not in allowed_tokens
                    or ledger_commit is None
                    or ledger_commit.get("operation_token") != commit.operation_token
                ):
                    raise StructuredError(
                        f"{table}@{version} is outside the recorded repair write set",
                        category=ErrorCategory.CONCURRENT_WRITER,
                        remediation=(
                            "Preserve later or unattributed writes with a compensating "
                            "correction instead of whole-table RESTORE."
                        ),
                    )
        owner_steps = [
            step_by_id[write["step_id"]]
            for write in writes
            if write["step_id"] in step_by_id
        ]
        rollback_items.append(
            {
                "table": table,
                "baseline": baseline,
                "current": current.current_version,
                "generation": generation,
                "step_index": max(int(row["ordinal"]) for row in owner_steps),
                "prior_restore": prior_restore,
                "needs_restore": current.current_version != baseline
                and prior_restore is None,
            }
        )
    if not rollback_items:
        raise StructuredError(
            f"repair {repair_id} has no planned output tables to roll back",
            category=ErrorCategory.RECOVERY,
        )
    rollback_items.sort(
        key=lambda item: (item["step_index"], item["table"]), reverse=True
    )

    # A cursor rewind is safe only if the cursor still has a value explained
    # by the plan or one of this repair's recorded attempts. This protects a
    # later successful target/source pair even when etl_control is shared.
    expected_cursor_versions: dict[tuple[str, str], set[int | None]] = {}
    for step in report["steps"]:
        for read in reads_by_step.get(step["step_id"], []):
            if read.get("source_role") == "source":
                expected_cursor_versions.setdefault(
                    (step["target_table"], read["dataset_table"]), set()
                ).add(read.get("prior_watermark"))
    for attempt_read in report.get("attempt_reads", []):
        step = step_by_id.get(attempt_read.get("step_id"))
        if (
            step is not None
            and attempt_read.get("source_role") == "source"
            and attempt_read.get("snapshot_version") is not None
        ):
            expected_cursor_versions.setdefault(
                (step["target_table"], attempt_read["dataset_table"]), set()
            ).add(int(attempt_read["snapshot_version"]))
    for (
        target_table,
        source_table,
    ), allowed_versions in expected_cursor_versions.items():
        state = providers.control.get_target_state(target_table)
        current_row = next(
            (batch for batch in state.batches if batch.source_table == source_table),
            None,
        )
        current_watermark = (
            current_row.last_processed_version if current_row is not None else None
        )
        if current_watermark not in allowed_versions:
            raise StructuredError(
                f"watermark for {target_table} <- {source_table} advanced outside "
                "the repair's recorded read versions",
                category=ErrorCategory.CONCURRENT_WRITER,
                remediation="Inspect etl_control and the target history before retrying rollback.",
            )

    for item in rollback_items:
        control = providers.control.get_target_state(item["table"])
        if any(batch.status.upper() == "RUNNING" for batch in control.batches):
            raise StructuredError(
                f"target {item['table']} has a RUNNING batch record",
                category=ErrorCategory.CONCURRENT_WRITER,
            )

    attempt_id = ledger.start_repair_attempt(
        repair_id=repair_id, actor=owner, environment=environment
    )
    started = time.monotonic()
    ledger.append_event(
        attempt_id=attempt_id,
        step_attempt_id=None,
        event_type="ROLLBACK_STARTED",
        status="RUNNING",
    )
    restored: list[dict[str, Any]] = []
    errors: list[str] = []
    status = "SUCCEEDED"
    completed_output_tables: set[str] = set()
    step_attempts: dict[int, str] = {}
    for item in rollback_items:
        table = item["table"]
        step_index = item["step_index"]
        if not item["needs_restore"] and item["prior_restore"] is None:
            completed_output_tables.add(table)
            continue
        step_attempt_id = ledger.start_step_attempt(
            repair_id=repair_id,
            attempt_id=attempt_id,
            step_index=step_index,
            target_existed_before=True,
            target_version_before=item["current"],
        )
        step_attempts[step_index] = step_attempt_id
        step_started = time.monotonic()
        operation_token = stable_repair_id(repair_id, f"rollback:{table}")
        try:
            if item["prior_restore"] is None:
                metadata = (
                    f"repair_id={repair_id}; attempt_id={attempt_id}; "
                    f"operation_token={operation_token}; rollback_version={item['baseline']}"
                )
                tagged_restore = getattr(
                    providers.history, "restore_to_version_tagged", None
                )
                if not callable(tagged_restore):
                    raise StructuredError(
                        "Delta history provider cannot tag repair RESTORE commits",
                        category=ErrorCategory.RECOVERY,
                    )
                tagged_restore(table, item["baseline"], metadata)
                after = providers.history.get_target_delta_state(
                    table, history_limit=history_limit
                )
                restore_commit = next(
                    (
                        commit
                        for commit in after.commits
                        if commit.operation_token == operation_token
                    ),
                    None,
                )
            else:
                restore_commit = item["prior_restore"]
                after = providers.history.get_target_delta_state(
                    table, history_limit=history_limit
                )
            if (
                restore_commit is None
                or after.current_version != restore_commit.version
                or after.generation_id != item["generation"]
            ):
                raise StructuredError(
                    f"could not reconcile tagged RESTORE for {table}",
                    category=ErrorCategory.RECOVERY,
                    remediation="Inspect Delta history and reconcile the operation token before retrying.",
                )
            ledger.record_commit(
                repair_id=repair_id,
                step_attempt_id=step_attempt_id,
                target_table=table,
                target_generation=item["generation"],
                target_version_before=(
                    restore_commit.version - 1
                    if item["prior_restore"] is not None
                    else item["current"]
                ),
                target_version=restore_commit.version,
                operation=restore_commit.operation or "RESTORE",
                operation_token=operation_token,
                repair_operation_token=operation_token,
                attributed=True,
                rollback_version=item["baseline"],
                committed_at=restore_commit.timestamp,
            )
            elapsed = int((time.monotonic() - step_started) * 1000)
            ledger.append_event(
                attempt_id=attempt_id,
                step_attempt_id=step_attempt_id,
                event_type="COMPLETED",
                status="SUCCEEDED",
                elapsed_ms=elapsed,
                evidence_ref=f"{table}@{restore_commit.version}; restored={item['baseline']}",
            )
            restored.append(
                {
                    "table": table,
                    "generation": item["generation"],
                    "version_before": item["current"],
                    "restored_to_version": item["baseline"],
                    "restore_commit_version": restore_commit.version,
                    "duration_ms": elapsed,
                }
            )
            completed_output_tables.add(table)
        except Exception as exc:
            errors.append(f"{table}: {type(exc).__name__}: {exc}")
            status = "PARTIAL" if restored else "FAILED"
            ledger.append_event(
                attempt_id=attempt_id,
                step_attempt_id=step_attempt_id,
                event_type="COMPLETED",
                status="FAILED",
                elapsed_ms=int((time.monotonic() - step_started) * 1000),
                error_message=errors[-1],
            )
            break

    # Rewind per-target source cursors after that target and every auxiliary
    # output it owns have reached their frozen state. etl_control is shared, so
    # restoring its whole Delta version would destroy unrelated targets' rows.
    for step in sorted(
        report["steps"], key=lambda row: int(row["ordinal"]), reverse=True
    ):
        step_id = step["step_id"]
        step_writes = [row for row in report["writes"] if row["step_id"] == step_id]
        if not step_writes or any(
            row["target_table"] not in completed_output_tables for row in step_writes
        ):
            continue
        step_index = int(step["ordinal"])
        rollback_step_attempt_id = step_attempts.get(step_index)
        if rollback_step_attempt_id is None:
            rollback_step_attempt_id = ledger.start_step_attempt(
                repair_id=repair_id,
                attempt_id=attempt_id,
                step_index=step_index,
                target_existed_before=True,
                target_version_before=step.get("expected_current_version"),
            )
            step_attempts[step_index] = rollback_step_attempt_id
        for read in reads_by_step.get(step_id, []):
            if read.get("source_role") != "source":
                continue
            source_table = read["dataset_table"]
            prior_watermark = read.get("prior_watermark")
            state = providers.control.get_target_state(step["target_table"])
            prior_row = next(
                (
                    batch
                    for batch in state.batches
                    if batch.source_table == source_table
                ),
                None,
            )
            control_table = getattr(providers.control, "control_table_name", None)
            cursor_token = stable_repair_id(
                repair_id,
                f"cursor-rollback:{step['target_table']}:{source_table}",
            )
            already_rewound = (prior_row is None and prior_watermark is None) or (
                prior_row is not None
                and prior_row.last_processed_version == prior_watermark
                and prior_row.status.upper() == "RECOVERED"
            )
            if already_rewound:
                if control_table:
                    cursor_state = providers.history.get_target_delta_state(
                        control_table, history_limit=history_limit
                    )
                    cursor_commit = next(
                        (
                            commit
                            for commit in cursor_state.commits
                            if commit.operation_token == cursor_token
                        ),
                        None,
                    )
                    if cursor_commit is not None:
                        ledger.record_commit(
                            repair_id=repair_id,
                            step_attempt_id=rollback_step_attempt_id,
                            target_table=control_table,
                            target_generation=cursor_state.generation_id,
                            target_version_before=cursor_commit.version - 1,
                            target_version=cursor_commit.version,
                            operation=cursor_commit.operation or "MERGE",
                            operation_token=cursor_token,
                            repair_operation_token=cursor_token,
                            attributed=True,
                            rollback_version=None,
                            committed_at=cursor_commit.timestamp,
                        )
                continue
            rewind_tagged = getattr(providers.control, "rewind_watermark_tagged", None)
            if not callable(rewind_tagged) or not control_table:
                errors.append(
                    f"could not durably attribute cursor rewind for "
                    f"{step['target_table']} <- {source_table}"
                )
                status = "PARTIAL" if restored else "FAILED"
                break
            before_cursor = providers.history.get_target_delta_state(
                control_table, history_limit=history_limit
            )
            try:
                rewind_tagged(
                    step["target_table"],
                    source_table,
                    prior_watermark,
                    f"repair_id={repair_id}; operation_token={cursor_token}; "
                    f"target_table={step['target_table']}; source_table={source_table}",
                )
                after_cursor = providers.history.get_target_delta_state(
                    control_table, history_limit=history_limit
                )
                cursor_commit = next(
                    (
                        commit
                        for commit in after_cursor.commits
                        if commit.operation_token == cursor_token
                    ),
                    None,
                )
                if cursor_commit is None:
                    raise StructuredError(
                        f"could not reconcile tagged etl_control rewind for "
                        f"{step['target_table']} <- {source_table}",
                        category=ErrorCategory.RECOVERY,
                    )
                ledger.record_commit(
                    repair_id=repair_id,
                    step_attempt_id=rollback_step_attempt_id,
                    target_table=control_table,
                    target_generation=after_cursor.generation_id,
                    target_version_before=before_cursor.current_version,
                    target_version=cursor_commit.version,
                    operation=cursor_commit.operation or "MERGE",
                    operation_token=cursor_token,
                    repair_operation_token=cursor_token,
                    attributed=True,
                    rollback_version=None,
                    committed_at=cursor_commit.timestamp,
                )
                ledger.append_event(
                    attempt_id=attempt_id,
                    step_attempt_id=rollback_step_attempt_id,
                    event_type="CURSOR_REWOUND",
                    status="SUCCEEDED",
                    elapsed_ms=0,
                    evidence_ref=(
                        f"{step['target_table']}<-{source_table}; "
                        f"last_processed_version={prior_watermark}"
                    ),
                )
            except Exception as exc:
                errors.append(
                    f"cursor {step['target_table']} <- {source_table}: "
                    f"{type(exc).__name__}: {exc}"
                )
                status = "PARTIAL" if restored else "FAILED"
                break
        if status != "SUCCEEDED":
            break

    duration = int((time.monotonic() - started) * 1000)
    ledger.append_event(
        attempt_id=attempt_id,
        step_attempt_id=None,
        event_type="COMPLETED",
        status=status,
        elapsed_ms=duration,
        error_message="; ".join(errors) or None,
    )
    return RepairRollbackResult(
        repair_id=repair_id,
        attempt_id=attempt_id,
        status=status,
        restored_tables=tuple(restored),
        duration_ms=duration,
        errors=tuple(errors),
    )


def _validate_target_state(
    target_table: str,
    state: TargetDeltaState,
    expected_current_version: int,
    rollback_version: int,
    origin_batch_id: str,
) -> None:
    if not state.table_exists or state.current_version is None:
        raise StructuredError(
            f"target Delta table does not exist: {target_table}",
            category=ErrorCategory.SOURCE_UNAVAILABLE,
        )
    if not state.generation_id:
        raise StructuredError(
            f"could not identify the physical Delta table generation for {target_table}",
            category=ErrorCategory.RECOVERY,
            remediation="Use a runtime that supports DESCRIBE DETAIL and re-plan the repair.",
        )
    if state.current_version != expected_current_version:
        raise StructuredError(
            f"target version is {state.current_version}, expected {expected_current_version}",
            category=ErrorCategory.CONCURRENT_WRITER,
            remediation="Re-inspect the target and create a fresh repair plan.",
        )
    versions = {commit.version: commit for commit in state.commits}
    if rollback_version not in versions:
        raise StructuredError(
            f"rollback version {rollback_version} is not present in retained Delta history",
            category=ErrorCategory.RECOVERY,
            remediation="Use retained backup/source data to build a new correction plan.",
        )
    missing = [
        version
        for version in range(rollback_version + 1, expected_current_version + 1)
        if version not in versions
    ]
    if missing:
        raise StructuredError(
            "Delta history is incomplete for the requested rollback range: "
            + ", ".join(map(str, missing[:10])),
            category=ErrorCategory.RECOVERY,
        )
    foreign = [
        version
        for version in range(rollback_version + 1, expected_current_version + 1)
        if versions[version].batch_id != origin_batch_id
    ]
    if foreign:
        raise StructuredError(
            "rollback would remove commits outside the named bad batch at version(s): "
            + ", ".join(map(str, foreign[:10])),
            category=ErrorCategory.CONCURRENT_WRITER,
            remediation=(
                "Use a compensating correction or rebuild plan that preserves later "
                "valid writes; whole-table RESTORE is not selective undo."
            ),
        )


def _operation_token(repair_id: str) -> str:
    return str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"kimball-repair:{repair_id}:operation:0")
    )


def result_dict(result: RepairRestoreResult) -> dict[str, Any]:
    return asdict(result)


def status_json(ledger: DeltaRepairLedger, repair_id: str) -> str:
    return json.dumps(ledger.status(repair_id), indent=2, sort_keys=True, default=str)
