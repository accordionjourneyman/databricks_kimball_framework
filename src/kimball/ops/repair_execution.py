"""Planning and execution of dependency-aware, version-pinned repairs."""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import asdict, dataclass
from typing import Any

from kimball.common.utils import quote_table_name
from kimball.ops.errors import ErrorCategory, StructuredError
from kimball.ops.providers import OpsProviders, TargetDeltaState
from kimball.ops.repair_ledger import (
    DeltaRepairLedger,
    RepairInputCorrection,
    RepairPlanStep,
    RepairRead,
    RepairWriteTarget,
    stable_repair_id,
)
from kimball.ops.runtime_profile import RuntimeProfile
from kimball.planning.compiler import CompiledProject
from kimball.planning.manifest import build_manifest

HISTORY_LIMIT = 10_000


@dataclass(frozen=True)
class RepairPlanPreview:
    repair_id: str
    plan_id: str
    strategy: str
    targets: tuple[str, ...]
    correction_inputs: tuple[dict[str, Any], ...]
    pinned_reads: tuple[dict[str, Any], ...]
    rollback_points: tuple[dict[str, Any], ...]
    manifest_digest: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RepairExecutionResult:
    repair_id: str
    attempt_id: str
    status: str
    completed_steps: tuple[str, ...]
    failed_step: str | None
    updated_tables: tuple[str, ...]
    duration_ms: int
    errors: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def plan_source_reprocess(
    *,
    spark: Any,
    project: CompiledProject,
    providers: OpsProviders,
    ledger: DeltaRepairLedger,
    repair_id: str,
    reason: str,
    bad_versions: dict[str, int],
    replacement_versions: dict[str, int] | None = None,
    target_tables: tuple[str, ...] = (),
    owner: str | None = None,
    environment: str | None = None,
    runtime: RuntimeProfile,
) -> RepairPlanPreview:
    """Freeze a source correction and the complete controlled DAG rebuild."""
    if not repair_id.strip() or not reason.strip():
        raise StructuredError(
            "repair ID and reason are required", category=ErrorCategory.CONFIG
        )
    if not bad_versions and not target_tables:
        raise StructuredError(
            "provide --bad-version or --table to identify the repair scope",
            category=ErrorCategory.CONFIG,
        )
    for table, version in bad_versions.items():
        if version < 0:
            raise StructuredError(
                f"bad source version for {table} must be non-negative",
                category=ErrorCategory.CONFIG,
            )
    overrides = dict(replacement_versions or {})
    if unknown := sorted(set(overrides) - set(bad_versions)):
        raise StructuredError(
            "--read-version requires a matching --bad-version for: "
            + ", ".join(unknown),
            category=ErrorCategory.CONFIG,
        )

    ordered_targets = _affected_targets(project, bad_versions, target_tables)
    manifest = build_manifest(project)
    indices = {table: index for index, table in enumerate(ordered_targets)}
    write_indices = {
        written_table: index
        for index, table in enumerate(ordered_targets)
        for written_table in project.nodes[table].writes
    }
    candidate_writes: dict[str, set[str]] = {}
    write_owners: dict[str, set[int]] = {}
    for index, table in enumerate(ordered_targets):
        node = project.nodes[table]
        for written in node.writes:
            candidate_writes.setdefault(written, set()).add("declared_output")
            write_owners.setdefault(written, set()).add(index)
        for fk in node.config.foreign_keys or []:
            if fk.references and fk.lookup and fk.lookup.early_arriving == "skeleton":
                candidate_writes.setdefault(fk.references, set()).add("skeleton_member")
                write_owners.setdefault(fk.references, set()).add(index)

    conflicting_writers = {
        table: owners for table, owners in write_owners.items() if len(owners) > 1
    }
    if conflicting_writers:
        details = "; ".join(
            f"{table}: {sorted(ordered_targets[index] for index in owners)}"
            for table, owners in sorted(conflicting_writers.items())
        )
        raise StructuredError(
            "repair has multiple pipeline steps writing the same table: " + details,
            category=ErrorCategory.CONFIG,
        )
    for table, owners in write_owners.items():
        write_indices.setdefault(table, next(iter(owners)))

    target_states: dict[str, TargetDeltaState] = {}
    for table in set(write_indices) | set(candidate_writes):
        target_states[table] = providers.history.get_target_delta_state(
            table, history_limit=HISTORY_LIMIT
        )
        state = target_states[table]
        if state.table_exists and (
            state.current_version is None or not state.generation_id
        ):
            raise StructuredError(
                f"repair output {table} lacks a physical generation or current Delta version",
                category=ErrorCategory.RECOVERY,
                remediation=(
                    "Confirm the output is a Delta table and the runtime supports "
                    "DESCRIBE DETAIL before planning a repair."
                ),
            )

    current_versions: dict[str, tuple[int, str]] = {}
    correction_rows: list[RepairInputCorrection] = []
    for source_table, bad_version in sorted(bad_versions.items()):
        current, selected_version = _source_read_version(
            source_table,
            providers,
            spark,
            overrides.get(source_table),
        )
        if bad_version == selected_version:
            raise StructuredError(
                f"replacement version for {source_table} equals the bad version {bad_version}",
                category=ErrorCategory.CONFIG,
            )
        if not any(commit.version == bad_version for commit in current.commits):
            try:
                _pinned_schema_digest(spark, source_table, bad_version)
            except Exception as exc:
                raise StructuredError(
                    f"bad source version {source_table}@{bad_version} is not present in retained Delta history",
                    category=ErrorCategory.RECOVERY,
                ) from exc
        current_versions[source_table] = (selected_version, current.generation_id or "")
        correction_rows.append(
            RepairInputCorrection(
                source_table=source_table,
                bad_version=bad_version,
                replacement_version=selected_version,
                source_generation=current.generation_id or "",
            )
        )

    plan_steps: list[RepairPlanStep] = []
    preview_reads: list[dict[str, Any]] = []
    for step_index, table in enumerate(ordered_targets):
        node = project.nodes[table]
        _validate_rebuild_config(node.config)
        _validate_declared_sql_inputs(node.config)
        control_state = providers.control.get_target_state(table)
        prior_watermarks = {
            batch.source_table: batch.last_processed_version
            for batch in control_state.batches
        }
        reads: list[RepairRead] = []
        for dataset_table, role in _read_specs(node.config):
            writer_index = write_indices.get(dataset_table)
            if writer_index is not None and writer_index < step_index:
                source_state = target_states[dataset_table]
                read = RepairRead(
                    dataset_table=dataset_table,
                    source_role=role,
                    read_mode="dependency",
                    dataset_generation=source_state.generation_id,
                    source_step_id=stable_repair_id(repair_id, f"step:{writer_index}"),
                    prior_watermark=(
                        prior_watermarks.get(dataset_table)
                        if role == "source"
                        else None
                    ),
                )
            else:
                override = overrides.get(dataset_table)
                source_state, version = _source_read_version(
                    dataset_table, providers, spark, override
                )
                current_versions[dataset_table] = (
                    version,
                    source_state.generation_id or "",
                )
                schema_digest = _pinned_schema_digest(spark, dataset_table, version)
                read = RepairRead(
                    dataset_table=dataset_table,
                    source_role=role,
                    read_mode="snapshot",
                    snapshot_version=version,
                    schema_digest=schema_digest,
                    dataset_generation=source_state.generation_id,
                    prior_watermark=(
                        prior_watermarks.get(dataset_table)
                        if role == "source"
                        else None
                    ),
                )
            reads.append(read)
            preview_reads.append(
                {
                    "step": table,
                    "dataset_table": read.dataset_table,
                    "role": read.source_role,
                    "mode": read.read_mode,
                    "snapshot_version": read.snapshot_version,
                    "depends_on_step": read.source_step_id,
                    "schema_digest": read.schema_digest,
                    "prior_watermark": read.prior_watermark,
                }
            )

        state = target_states[table]
        scopes = [
            f"{name}:{bad_versions[name]}->{current_versions[name][0]}"
            for name in sorted(bad_versions)
            if any(read.dataset_table == name for read in reads)
        ]
        step_digest = hashlib.sha256(
            json.dumps(
                [asdict(read) for read in reads], sort_keys=True, default=str
            ).encode()
        ).hexdigest()
        dependencies = tuple(
            sorted(
                indices[dependency]
                for dependency in node.dependencies
                if dependency in indices
            )
        )
        write_targets = tuple(
            RepairWriteTarget(
                target_table=output_table,
                write_role=role,
                target_existed_before=target_states[output_table].table_exists,
                target_generation=target_states[output_table].generation_id,
                target_version_before=target_states[output_table].current_version,
                rollback_version=target_states[output_table].current_version,
            )
            for output_table, roles in sorted(candidate_writes.items())
            if write_owners[output_table] == {step_index}
            for role in sorted(roles)
        )
        plan_steps.append(
            RepairPlanStep(
                target_table=table,
                strategy="snapshot_rebuild_scd1",
                target_generation=state.generation_id,
                scope_ref=",".join(scopes) or "explicit-table-rebuild",
                scope_digest=step_digest,
                expected_current_version=state.current_version,
                rollback_version=state.current_version,
                reads=tuple(reads),
                writes=write_targets,
                depends_on=dependencies,
            )
        )

    if bad_versions and not any(
        read["dataset_table"] in bad_versions for read in preview_reads
    ):
        raise StructuredError(
            "the selected repair targets do not read any corrected source table",
            category=ErrorCategory.CONFIG,
        )

    ledger.create_plan(
        repair_id=repair_id,
        reason=reason,
        owner=owner,
        steps=tuple(plan_steps),
        manifest_digest=manifest["project_digest"],
        config_digest=manifest["project_digest"],
        runtime_profile=runtime.flavor.value,
        strategy="dependency_snapshot_rebuild",
        publication_policy="quiesced_in_place_multi_step",
        corrections=tuple(correction_rows),
    )
    return RepairPlanPreview(
        repair_id=repair_id,
        plan_id=stable_repair_id(repair_id, "plan"),
        strategy="dependency_snapshot_rebuild",
        targets=ordered_targets,
        correction_inputs=tuple(
            {
                "source_table": correction.source_table,
                "bad_version": correction.bad_version,
                "replacement_version": correction.replacement_version,
                "source_generation": correction.source_generation,
            }
            for correction in correction_rows
        ),
        pinned_reads=tuple(preview_reads),
        rollback_points=tuple(
            {
                "table": write.target_table,
                "generation": write.target_generation,
                "version": write.rollback_version,
                "step": plan_steps[index].target_table,
                "role": write.write_role,
            }
            for index, step in enumerate(plan_steps)
            for write in step.writes
        ),
        manifest_digest=manifest["project_digest"],
    )


def apply_source_reprocess(
    *,
    spark: Any,
    project: CompiledProject,
    providers: OpsProviders,
    ledger: DeltaRepairLedger,
    runtime: RuntimeProfile,
    repair_id: str,
    etl_schema: str,
    target_config: Any | None = None,
    owner: str | None = None,
    environment: str | None = None,
    writers_stopped: bool = False,
) -> RepairExecutionResult:
    """Apply a frozen rebuild plan and audit every observed Delta commit.

    The execution is ordered by the compiled dependency DAG. Each model is
    rebuilt from its frozen source vector, in place, under one Delta-table
    compensating transaction. The group of model and side-table commits is
    not a cross-table transaction; a failed step is reported as PARTIAL when
    earlier durable commits exist.
    """
    if not writers_stopped:
        raise StructuredError(
            "repair apply requires all writers and readers that require a consistent view to be quiesced",
            category=ErrorCategory.CONCURRENT_WRITER,
            remediation=(
                "Pause scheduled, streaming, and manual writers, coordinate downstream "
                "readers, then pass --writers-stopped for the maintenance window."
            ),
        )
    if not runtime.supports_commit_tagging:
        raise StructuredError(
            "repair apply requires Delta commit metadata tagging",
            category=ErrorCategory.RECOVERY,
            remediation="Run the repair on a runtime that supports Delta userMetadata.",
        )

    report = ledger.status(repair_id)
    if not report["plan"] or not report["steps"]:
        raise StructuredError(
            f"repair plan not found or incomplete: {repair_id}",
            category=ErrorCategory.RECOVERY,
        )
    plan_record = report["plan"][0]
    if any(event.get("event_type") == "ROLLBACK_STARTED" for event in report["events"]):
        raise StructuredError(
            f"repair {repair_id} has entered rollback and cannot be applied again",
            category=ErrorCategory.RECOVERY,
            remediation="Complete or reconcile the rollback, then create a new repair ID to reapply corrected data.",
        )
    manifest = build_manifest(project)
    if plan_record.get("manifest_digest") != manifest["project_digest"]:
        raise StructuredError(
            "compiled project differs from the frozen repair plan",
            category=ErrorCategory.CONFIG,
            remediation="Re-plan the repair using the current project configuration.",
        )

    steps = sorted(report["steps"], key=lambda row: int(row["ordinal"]))
    if any(row["target_table"] not in project.nodes for row in steps):
        raise StructuredError(
            "one or more planned targets are absent from the compiled project",
            category=ErrorCategory.CONFIG,
        )
    plan_reads: dict[str, list[dict[str, Any]]] = {}
    for read in report["reads"]:
        plan_reads.setdefault(read["step_id"], []).append(read)
    plan_tokens = {step["operation_token"] for step in steps}
    _assert_repair_output_versions(
        providers,
        report["writes"],
        report["commits"],
        plan_tokens,
    )
    _preflight_pinned_reads(spark, providers, plan_reads)
    inventory = _capture_inventory(spark, providers, project, etl_schema, target_config)

    running_targets = []
    for step in steps:
        control_state = providers.control.get_target_state(step["target_table"])
        if any(batch.status.upper() == "RUNNING" for batch in control_state.batches):
            running_targets.append(step["target_table"])
    if running_targets:
        raise StructuredError(
            "RUNNING target batches block repair: " + ", ".join(running_targets),
            category=ErrorCategory.CONCURRENT_WRITER,
        )

    attempt_id = ledger.start_repair_attempt(
        repair_id=repair_id, actor=owner, environment=environment
    )
    started_clock = time.monotonic()
    completed_step_ids = _completed_step_ids(report)
    completed: list[str] = []
    updated_tables: set[str] = set()
    errors: list[str] = []
    failed_step: str | None = None
    status = "SUCCEEDED"
    first_rollback_point = {
        row["target_table"]: row.get("rollback_version") for row in report["writes"]
    }
    for commit_row in sorted(report["commits"], key=lambda row: row["committed_at"]):
        table = commit_row["target_table"]
        if table not in first_rollback_point:
            first_rollback_point[table] = commit_row.get("rollback_version")

    for step in steps:
        step_id = step["step_id"]
        target_table = step["target_table"]
        step_index = int(step["ordinal"])
        current_state = providers.history.get_target_delta_state(
            target_table, history_limit=HISTORY_LIMIT
        )
        step_attempt_id = ledger.start_step_attempt(
            repair_id=repair_id,
            attempt_id=attempt_id,
            step_index=step_index,
            target_existed_before=current_state.table_exists,
            target_version_before=current_state.current_version,
        )
        step_started = time.monotonic()
        before = inventory
        after: dict[str, TargetDeltaState] | None = None

        try:
            if step_id in completed_step_ids:
                ledger.append_event(
                    attempt_id=attempt_id,
                    step_attempt_id=step_attempt_id,
                    event_type="COMPLETED",
                    status="SUCCEEDED",
                    elapsed_ms=0,
                    evidence_ref="reconciled from an earlier attempt",
                )
                completed.append(target_table)
                continue

            snapshot_versions, actual_reads = _resolve_step_reads(
                spark,
                providers,
                step,
                plan_reads.get(step_id, []),
                completed_step_ids,
                steps,
            )
            ledger.record_attempt_reads(
                repair_id=repair_id,
                attempt_id=attempt_id,
                step_index=step_index,
                reads=actual_reads,
            )
            before = _capture_inventory(
                spark, providers, project, etl_schema, target_config
            )
            node = project.nodes[target_table]
            from kimball.orchestration.orchestrator import Orchestrator

            orchestrator = Orchestrator.from_config(
                node.config,
                spark=spark,
                etl_schema=etl_schema,
                target=target_config,
            )
            run_result = orchestrator.run_repair(
                snapshot_versions=snapshot_versions,
                repair_id=repair_id,
                operation_token=step["operation_token"],
            )
            after = _capture_inventory(
                spark, providers, project, etl_schema, target_config
            )
            commit_capture_errors, step_updates = _record_step_commits(
                repair_id=repair_id,
                step_attempt_id=step_attempt_id,
                step=step,
                before=before,
                after=after,
                providers=providers,
                ledger=ledger,
                first_rollback_point=first_rollback_point,
            )
            updated_tables.update(step_updates)
            if run_result.get("status") != "SUCCESS":
                raise RuntimeError(
                    f"pipeline returned {run_result.get('status')} for {target_table}"
                )
            if commit_capture_errors:
                raise StructuredError(
                    "repair step wrote tables without complete attributable Delta history: "
                    + "; ".join(commit_capture_errors),
                    category=ErrorCategory.RECOVERY,
                    remediation=(
                        "Treat the repair as partially applied. Inspect `kimball repair status` "
                        "and reconcile each listed table before retrying."
                    ),
                )
            elapsed = int((time.monotonic() - step_started) * 1000)
            ledger.append_event(
                attempt_id=attempt_id,
                step_attempt_id=step_attempt_id,
                event_type="COMPLETED",
                status="SUCCEEDED",
                elapsed_ms=elapsed,
                evidence_ref=f"{target_table}; batch_id={run_result.get('batch_id')}",
            )
            completed.append(target_table)
            completed_step_ids.add(step_id)
        except Exception as exc:
            failed_step = target_table
            errors.append(f"{type(exc).__name__}: {exc}")
            try:
                after = _capture_inventory(
                    spark, providers, project, etl_schema, target_config
                )
                _, step_updates = _record_step_commits(
                    repair_id=repair_id,
                    step_attempt_id=step_attempt_id,
                    step=step,
                    before=before,
                    after=after,
                    providers=providers,
                    ledger=ledger,
                    first_rollback_point=first_rollback_point,
                )
                updated_tables.update(step_updates)
            except Exception as capture_exc:
                errors.append(
                    "commit reconciliation failed: "
                    f"{type(capture_exc).__name__}: {capture_exc}"
                )
            elapsed = int((time.monotonic() - step_started) * 1000)
            try:
                ledger.append_event(
                    attempt_id=attempt_id,
                    step_attempt_id=step_attempt_id,
                    event_type="COMPLETED",
                    status="OUTCOME_UNKNOWN"
                    if any("reconciliation failed" in item for item in errors)
                    else "FAILED",
                    elapsed_ms=elapsed,
                    error_message=errors[-1],
                )
            except Exception as event_exc:
                errors.append(f"failed to write terminal repair event: {event_exc}")
            status = "PARTIAL" if completed or updated_tables else "FAILED"
            break
        finally:
            if after is not None:
                inventory = after

    duration = int((time.monotonic() - started_clock) * 1000)
    ledger.append_event(
        attempt_id=attempt_id,
        step_attempt_id=None,
        event_type="COMPLETED",
        status=status,
        elapsed_ms=duration,
        error_message="; ".join(errors) or None,
    )
    return RepairExecutionResult(
        repair_id=repair_id,
        attempt_id=attempt_id,
        status=status,
        completed_steps=tuple(completed),
        failed_step=failed_step,
        updated_tables=tuple(sorted(updated_tables)),
        duration_ms=duration,
        errors=tuple(errors),
    )


def _completed_step_ids(report: dict[str, Any]) -> set[str]:
    step_attempts = {
        row["step_attempt_id"]: row["step_id"] for row in report["step_attempts"]
    }
    return {
        step_attempts[event["step_attempt_id"]]
        for event in report["events"]
        if event.get("step_attempt_id") in step_attempts
        and event.get("event_type") == "COMPLETED"
        and event.get("status") == "SUCCEEDED"
    }


def _assert_repair_output_versions(
    providers: OpsProviders,
    write_rows: list[dict[str, Any]],
    prior_commits: list[dict[str, Any]],
    operation_tokens: set[str],
) -> None:
    """Reject stale write assumptions before any target mutation."""
    for write in write_rows:
        table = write["target_table"]
        state = providers.history.get_target_delta_state(
            table, history_limit=HISTORY_LIMIT
        )
        planned_exists = bool(write["target_existed_before"])
        planned_generation = write.get("target_generation")
        if planned_exists and (
            not state.table_exists or state.generation_id != planned_generation
        ):
            raise StructuredError(
                f"target generation changed since planning: {table}",
                category=ErrorCategory.CONCURRENT_WRITER,
            )
        if not planned_exists and not state.table_exists:
            continue
        baseline = write.get("target_version_before")
        start_version = 0 if baseline is None else int(baseline) + 1
        current = state.current_version
        if current is None:
            raise StructuredError(
                f"could not read current Delta version for {table}",
                category=ErrorCategory.RECOVERY,
            )
        if baseline is not None and current <= int(baseline):
            continue
        commits = {commit.version: commit for commit in state.commits}
        allowed_versions = {
            row["target_version"]
            for row in prior_commits
            if row["target_table"] == table
        }
        for version in range(start_version, current + 1):
            commit = commits.get(version)
            if commit is None:
                raise StructuredError(
                    f"retained history for {table} is incomplete at version {version}",
                    category=ErrorCategory.RECOVERY,
                )
            if (
                commit.operation_token not in operation_tokens
                and version not in allowed_versions
            ):
                raise StructuredError(
                    f"{table} has an unattributed write after the repair plan at version {version}",
                    category=ErrorCategory.CONCURRENT_WRITER,
                )


def _preflight_pinned_reads(
    spark: Any,
    providers: OpsProviders,
    plan_reads: dict[str, list[dict[str, Any]]],
) -> None:
    checked: set[tuple[str, int]] = set()
    for reads in plan_reads.values():
        for read in reads:
            if read["read_mode"] != "snapshot":
                continue
            table = read["dataset_table"]
            version = int(read["snapshot_version"])
            state = providers.history.get_target_delta_state(
                table, history_limit=HISTORY_LIMIT
            )
            if not state.table_exists or state.generation_id != read.get(
                "dataset_generation"
            ):
                raise StructuredError(
                    f"source table generation changed since planning: {table}",
                    category=ErrorCategory.CONCURRENT_WRITER,
                )
            if (table, version) not in checked:
                digest = _pinned_schema_digest(spark, table, version)
                if read.get("schema_digest") and digest != read["schema_digest"]:
                    raise StructuredError(
                        f"source schema differs from the frozen plan at {table}@{version}",
                        category=ErrorCategory.SCHEMA_DRIFT,
                    )
                checked.add((table, version))


def _resolve_step_reads(
    spark: Any,
    providers: OpsProviders,
    step: dict[str, Any],
    planned_reads: list[dict[str, Any]],
    completed_step_ids: set[str],
    steps: list[dict[str, Any]],
) -> tuple[dict[str, int], tuple[RepairRead, ...]]:
    step_ids = {row["step_id"] for row in steps}
    versions: dict[str, int] = {}
    actual: list[RepairRead] = []
    for planned in planned_reads:
        table = planned["dataset_table"]
        mode = planned["read_mode"]
        if mode == "dependency":
            source_step = planned.get("source_step_id")
            if source_step not in step_ids or source_step not in completed_step_ids:
                raise StructuredError(
                    f"dependency output {table} has not completed before {step['target_table']}",
                    category=ErrorCategory.RECOVERY,
                )
            state = providers.history.get_target_delta_state(
                table, history_limit=HISTORY_LIMIT
            )
            expected_generation = planned.get("dataset_generation")
            if (
                not state.table_exists
                or state.current_version is None
                or (expected_generation and state.generation_id != expected_generation)
            ):
                raise StructuredError(
                    f"dependency table is unavailable or recreated: {table}",
                    category=ErrorCategory.RECOVERY,
                )
            version = state.current_version
            generation = state.generation_id
        else:
            version = int(planned["snapshot_version"])
            generation = planned.get("dataset_generation")
        digest = _pinned_schema_digest(spark, table, version)
        previous = versions.get(table)
        if previous is not None and previous != version:
            raise StructuredError(
                f"repair plan resolved conflicting versions for input {table}",
                category=ErrorCategory.CONFIG,
            )
        versions[table] = version
        actual.append(
            RepairRead(
                dataset_table=table,
                source_role=planned["source_role"],
                read_mode="snapshot",
                snapshot_version=version,
                schema_digest=digest,
                dataset_generation=generation,
            )
        )
    if not versions:
        raise StructuredError(
            f"repair step {step['target_table']} has no pinned input reads",
            category=ErrorCategory.CONFIG,
        )
    return versions, tuple(actual)


def _capture_inventory(
    spark: Any,
    providers: OpsProviders,
    project: CompiledProject,
    etl_schema: str,
    target_config: Any | None,
) -> dict[str, TargetDeltaState]:
    """Snapshot Delta versions in every project and ETL schema touched here."""
    tables: set[str] = set()
    schemas: set[str] = {etl_schema}
    if target_config is not None:
        for attr in ("silver_schema", "gold_schema", "etl_schema"):
            schema = getattr(target_config, attr, None)
            catalog = getattr(target_config, "catalog", None)
            if schema:
                schemas.add(f"{catalog}.{schema}" if catalog else schema)
    for node in project.nodes.values():
        config = node.config
        tables.add(config.table_name)
        tables.update(node.writes)
        tables.update(table for table, _ in _read_specs(config))
        for name in tables.copy():
            schema = _table_schema(name, spark)
            if schema:
                schemas.add(schema)
    for table in tuple(tables):
        schema = _table_schema(table, spark)
        if schema:
            schemas.add(schema)

    for schema in schemas:
        try:
            rows = spark.sql(f"SHOW TABLES IN {quote_table_name(schema)}").collect()
        except Exception as exc:
            raise StructuredError(
                f"cannot enumerate repair scope schema {schema}; exact write attribution is unavailable",
                category=ErrorCategory.RECOVERY,
                remediation="Grant table-list access for all source, target, and ETL schemas, then retry.",
            ) from exc
        for row in rows:
            values = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
            table_name = (
                values.get("tableName")
                or values.get("table_name")
                or values.get("name")
            )
            if isinstance(table_name, str) and table_name:
                tables.add(f"{schema}.{table_name}")

    inventory: dict[str, TargetDeltaState] = {}
    for table in sorted(tables):
        try:
            state = providers.history.get_target_delta_state(
                table, history_limit=HISTORY_LIMIT
            )
        except Exception:
            # Catalog-listed non-Delta tables have no Delta version to capture.
            continue
        if state.table_exists:
            inventory[table] = state
        else:
            inventory[table] = state
    return inventory


def _table_schema(table: str, spark: Any) -> str | None:
    parts = [part.strip("` ") for part in table.split(".")]
    if len(parts) == 3:
        return ".".join(parts[:2])
    if len(parts) == 2:
        return parts[0]
    if len(parts) == 1:
        try:
            database = spark.catalog.currentDatabase()
        except Exception:
            return None
        return database if isinstance(database, str) else None
    return None


def _record_step_commits(
    *,
    repair_id: str,
    step_attempt_id: str,
    step: dict[str, Any],
    before: dict[str, TargetDeltaState],
    after: dict[str, TargetDeltaState],
    providers: OpsProviders,
    ledger: DeltaRepairLedger,
    first_rollback_point: dict[str, int | None],
) -> tuple[list[str], set[str]]:
    errors: list[str] = []
    updated_tables: set[str] = set()
    operation_token = step["operation_token"]
    table_names = sorted(set(before) | set(after))
    for table in table_names:
        previous = before.get(table)
        current = after.get(table)
        if current is None:
            continue
        if previous is not None and previous.table_exists and not current.table_exists:
            errors.append(f"{table} disappeared during the repair step")
            continue
        before_version = (
            previous.current_version
            if previous is not None and previous.table_exists
            else None
        )
        after_version = current.current_version
        if after_version is None:
            continue
        if before_version is not None and after_version <= before_version:
            if previous and previous.generation_id != current.generation_id:
                errors.append(
                    f"{table} was dropped and recreated without a version advance"
                )
            continue
        if (
            previous
            and previous.table_exists
            and previous.generation_id != current.generation_id
        ):
            errors.append(f"{table} physical generation changed during the repair")

        first_version = 0 if before_version is None else before_version + 1
        commits = {commit.version: commit for commit in current.commits}
        rollback_version = first_rollback_point.get(table)
        if table not in first_rollback_point:
            rollback_version = before_version
            first_rollback_point[table] = rollback_version
        for version in range(first_version, after_version + 1):
            commit = commits.get(version)
            if commit is None:
                errors.append(f"{table} history is missing changed version {version}")
                continue
            attributed = commit.operation_token == operation_token
            ledger.record_commit(
                repair_id=repair_id,
                step_attempt_id=step_attempt_id,
                target_table=table,
                target_generation=current.generation_id,
                target_version_before=before_version,
                target_version=version,
                operation=commit.operation,
                operation_token=commit.operation_token,
                repair_operation_token=operation_token,
                attributed=attributed,
                rollback_version=(
                    rollback_version
                    if previous is None
                    or previous.generation_id == current.generation_id
                    else None
                ),
                committed_at=commit.timestamp,
            )
            if attributed:
                updated_tables.add(table)
            else:
                errors.append(
                    f"{table}@{version} lacks repair operation token {operation_token}"
                )
    return errors, updated_tables


def _source_read_version(
    table: str,
    providers: OpsProviders,
    spark: Any,
    requested_version: int | None,
) -> tuple[TargetDeltaState, int]:
    state = providers.history.get_target_delta_state(table, history_limit=HISTORY_LIMIT)
    if not state.table_exists or state.current_version is None:
        raise StructuredError(
            f"repair input is not an existing Delta table: {table}",
            category=ErrorCategory.SOURCE_UNAVAILABLE,
        )
    if not state.generation_id:
        raise StructuredError(
            f"physical Delta generation is unavailable for {table}",
            category=ErrorCategory.RECOVERY,
        )
    version = state.current_version if requested_version is None else requested_version
    try:
        _pinned_schema_digest(spark, table, version)
    except Exception as exc:
        raise StructuredError(
            f"Delta snapshot {table}@{version} is unavailable",
            category=ErrorCategory.RECOVERY,
            remediation="Choose a retained version or restore the source from an archive.",
        ) from exc
    return state, version


def _pinned_schema_digest(spark: Any, table: str, version: int) -> str:
    frame = spark.read.format("delta").option("versionAsOf", version).table(table)
    return hashlib.sha256(frame.schema.json().encode()).hexdigest()


def _affected_targets(
    project: CompiledProject,
    corrections: dict[str, int],
    requested_targets: tuple[str, ...],
) -> tuple[str, ...]:
    unknown = sorted(set(requested_targets) - set(project.nodes))
    if unknown:
        raise StructuredError(
            "unknown repair target(s): " + ", ".join(unknown),
            category=ErrorCategory.CONFIG,
        )
    roots = set(requested_targets)
    for source in corrections:
        direct = [
            table
            for table, node in project.nodes.items()
            if any(name == source for name, _ in _read_specs(node.config))
        ]
        roots.update(direct)
    if not roots:
        raise StructuredError(
            "no configured pipeline consumes the selected source; pass --table",
            category=ErrorCategory.CONFIG,
        )
    affected = set(roots)
    changed = True
    while changed:
        changed = False
        for table, node in project.nodes.items():
            if table not in affected and set(node.dependencies) & affected:
                affected.add(table)
                changed = True
    ordered = tuple(
        table for level in project.levels for table in level if table in affected
    )
    return ordered


def _read_specs(config: Any) -> tuple[tuple[str, str], ...]:
    result: list[tuple[str, str]] = []
    for source in config.sources:
        result.append((source.name, "source"))
    for fk in config.foreign_keys or []:
        if fk.references:
            result.append((fk.references, "dimension_lookup"))
        if fk.lookup and fk.lookup.identity_map:
            result.append((fk.lookup.identity_map, "identity_map"))
    for junk in config.junk_dimensions:
        result.append((junk.dimension_table, "junk_dimension"))
    # Preserve multiple roles while preventing duplicate rows for identical use.
    return tuple(dict.fromkeys(result))


def _validate_rebuild_config(config: Any) -> None:
    reasons = []
    if config.scd_type != 1:
        reasons.append(
            f"SCD{config.scd_type} history requires a model-specific rebuild"
        )
    if config.history_table:
        reasons.append("separate history tables require coordinated rebuild support")
    if config.preserve_all_changes:
        reasons.append("preserve_all_changes requires replaying retained CDF history")
    for source in config.sources:
        if source.format != "delta" or source.streaming:
            reasons.append(f"source {source.name} is not a batch Delta table")
        if source.contract and source.contract.temporal:
            reasons.append(f"source {source.name} has temporal contract state")
    if reasons:
        raise StructuredError(
            f"{config.table_name} cannot use snapshot rebuild: " + "; ".join(reasons),
            category=ErrorCategory.CONFIG,
            remediation=(
                "Define and select a strategy that rebuilds the model's temporal "
                "history and stateful side effects."
            ),
        )


def _validate_declared_sql_inputs(config: Any) -> None:
    sql = config.transformation_sql or ""
    if not sql:
        return
    cleaned = re.sub(r"/\*.*?\*/|--[^\n]*", " ", sql, flags=re.DOTALL)
    cleaned = re.sub(r"'(?:''|[^'])*'", "''", cleaned)
    statement = cleaned.strip().rstrip(";").strip()
    if (
        ";" in statement
        or not re.match(r"^(?:WITH\b|SELECT\b)", statement, flags=re.IGNORECASE)
        or re.search(
            r"\b(?:INSERT\s+INTO|UPDATE\s+\w|DELETE\s+FROM|MERGE\s+INTO|CREATE\s|DROP\s|ALTER\s)\b",
            statement,
            flags=re.IGNORECASE,
        )
    ):
        raise StructuredError(
            f"{config.table_name} uses non-read-only transformation SQL",
            category=ErrorCategory.CONFIG,
            remediation=(
                "Snapshot repair supports a single SELECT or WITH query. Move writes "
                "into a declared, separately managed pipeline output."
            ),
        )
    aliases = {source.alias.casefold() for source in config.sources}
    aliases.update(source.name.casefold() for source in config.sources)
    ctes = {
        name.strip('`"').casefold()
        for name in re.findall(
            r"(?:\bWITH\b|,)\s*([`\"]?[\w]+[`\"]?)\s+AS\s*\(",
            cleaned,
            flags=re.IGNORECASE,
        )
    }
    relations = re.findall(
        r"\b(?:FROM|JOIN)\s+([`\"]?[\w]+(?:\.[`\"]?[\w]+){0,2}[`\"]?)",
        cleaned,
        flags=re.IGNORECASE,
    )
    unknown = sorted(
        {
            name.replace("`", "").replace('"', "").casefold()
            for name in relations
            if name.replace("`", "").replace('"', "").casefold() not in aliases | ctes
        }
    )
    if unknown:
        raise StructuredError(
            f"{config.table_name} reads undeclared SQL relation(s): "
            + ", ".join(unknown),
            category=ErrorCategory.CONFIG,
            remediation=(
                "Declare every physical input under sources so repair can pin its "
                "Delta version, then reference its alias in transformation_sql."
            ),
        )
