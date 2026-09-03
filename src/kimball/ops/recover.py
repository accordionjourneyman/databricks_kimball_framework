"""``kimball recover`` command logic (ROADMAP 1.2) - two-phase recovery.

Phase 1: RESTORE the target to the version before the zombie's first commit
(or to an operator-supplied ``--version`` / ``--timestamp`` when commit
tagging is unavailable on Serverless).
Phase 2: reconcile ``etl_control`` - mark the zombie batch FAILED and rewind
the watermark to the last SUCCESS watermark for each source (or None, forcing
a full CDF replay on the next run). The two stores are separate transactions,
so phase 2 failing is reported loudly and is re-runnable.

Pre-flight:
* Refuses a RESTORE that would land on a version Delta history no longer
  retains (VACUUM removed it), falling back to a documented full-reload path.
* Blocks on a suspected single-writer violation unless ``--force`` is given.
* Warns when upstream targets (DAG) also have RUNNING batches.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from kimball.ops.errors import ErrorCategory, StructuredError
from kimball.ops.providers import (
    BatchInfo,
    DeltaHistoryProvider,
    ETLControlStore,
    OpsProviders,
    TargetControlState,
    TargetDeltaState,
)
from kimball.ops.runtime_profile import RuntimeProfile
from kimball.ops.state_reconciler import ReconciliationVerdict, StateReconciler
from kimball.ops.writer_contract import WriterVerdict, check_writer_contract

RUNBOOK = "docs/RUNBOOK.md"


@dataclass(frozen=True)
class ZombieRecoveryPlan:
    batch_id: str
    sources: tuple[str, ...]
    has_committed_data: bool
    restore_version: int | None
    restore_timestamp: datetime | None = None
    rewind_watermarks: dict[str, int | None] = field(default_factory=dict)
    fallback: str | None = None  # set when RESTORE is impossible (history vacuumed)


@dataclass
class RecoverResult:
    target_table: str
    dry_run: bool
    plans: list[ZombieRecoveryPlan]
    executed: list[str]  # human-readable actions taken
    partial: bool  # phase 2 did not fully complete
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_table": self.target_table,
            "dry_run": self.dry_run,
            "plans": [
                {
                    "batch_id": p.batch_id,
                    "sources": list(p.sources),
                    "has_committed_data": p.has_committed_data,
                    "restore_version": p.restore_version,
                    "restore_timestamp": p.restore_timestamp.isoformat()
                    if p.restore_timestamp
                    else None,
                    "rewind_watermarks": p.rewind_watermarks,
                    "fallback": p.fallback,
                }
                for p in self.plans
            ],
            "executed": self.executed,
            "partial": self.partial,
            "warnings": self.warnings,
        }


def recover_target(
    target_table: str,
    providers: OpsProviders,
    runtime: RuntimeProfile,
    *,
    batch_id: str | None = None,
    dry_run: bool = False,
    rewind_only: bool = False,
    full_reload: bool = False,
    version: int | None = None,
    timestamp: datetime | None = None,
    force: bool = False,
    upstream_targets: tuple[str, ...] = (),
    history_limit: int = 500,
) -> RecoverResult:
    if version is not None and timestamp is not None:
        raise StructuredError(
            "pass either --version or --timestamp, not both",
            category=ErrorCategory.CONFIG,
        )

    control = providers.control
    history = providers.history
    reconciler = StateReconciler(control, history, runtime)
    reconciliation = reconciler.reconcile(target_table)
    _fail_on_unrecoverable(target_table, reconciliation)

    control_state = control.get_target_state(target_table)
    delta_state = history.get_target_delta_state(
        target_table, history_limit=history_limit
    )
    warnings: list[str] = list(_upstream_warnings(control, upstream_targets))
    _enforce_single_writer(target_table, control_state, delta_state, runtime, force)

    # -- rewind-only mode -------------------------------------------------
    if rewind_only:
        return _recover_rewind_only(
            target_table, control, control_state, reconciliation, dry_run, warnings
        )

    # -- zombie / orphan recovery -----------------------------------------
    zombies = list(reconciliation.zombie_batches)
    orphan_ids = {c.batch_id for c in reconciliation.orphan_commits if c.batch_id}

    if batch_id is not None:
        if any(b.batch_id == batch_id for b in zombies):
            zombies = [b for b in zombies if b.batch_id == batch_id]
        elif batch_id in orphan_ids:
            return _recover_orphan(
                target_table,
                history,
                delta_state,
                batch_id,
                dry_run,
                force,
                full_reload,
            )
        else:
            raise StructuredError(
                f"no RUNNING batch or orphan commit with batch_id={batch_id} for {target_table}",
                category=ErrorCategory.RECOVERY,
                remediation="Run `kimball inspect --table <target>` to list RUNNING/orphan batches.",
                runbook_link=f"{RUNBOOK}#recovery",
            )

    if not zombies:
        extra: list[str] = []
        if orphan_ids:
            extra.append(
                f"{len(orphan_ids)} orphan commit(s) present (batch_ids: "
                f"{sorted(orphan_ids)}). Recover with "
                f"`kimball recover --table {target_table} --batch-id <id> --force`."
            )
        extra.append(
            "no RUNNING batches to recover; target is healthy or already reconciled."
        )
        return RecoverResult(target_table, dry_run, [], [], False, warnings + extra)

    plans: list[ZombieRecoveryPlan] = []
    executed: list[str] = []
    partial = False

    for zombie in zombies:
        plan = _plan_zombie(
            zombie, control_state, delta_state, runtime, version, timestamp
        )
        plans.append(plan)

        if plan.fallback:
            _handle_fallback(
                target_table,
                control,
                control_state,
                zombie,
                plan,
                dry_run,
                full_reload,
                executed,
                warnings,
            )
            continue

        if not plan.has_committed_data:
            _clear_running_only(target_table, control, zombie, plan, dry_run, executed)
            continue

        partial |= _execute_restore(
            target_table, control, history, zombie, plan, dry_run, executed, warnings
        )

    return RecoverResult(target_table, dry_run, plans, executed, partial, warnings)


def _handle_fallback(
    target_table: str,
    control: ETLControlStore,
    control_state,
    zombie: BatchInfo,
    plan: ZombieRecoveryPlan,
    dry_run: bool,
    full_reload: bool,
    executed: list[str],
    warnings: list[str],
) -> None:
    """No safe point-in-time restore exists for this zombie.

    Either full-reload (re-running the pipeline for the batch's sources)
    or an advisory to RESTORE manually — never a silent partial state.
    """
    warnings.append(f"batch {zombie.batch_id}: {plan.fallback}")
    if full_reload:
        if not dry_run:
            _full_reload(target_table, control, control_state, zombie, executed)
            executed.append(f"batch {zombie.batch_id}: full-reload executed")
        else:
            executed.append(f"batch {zombie.batch_id}: would full-reload (--dry-run)")
    else:
        warnings.append(
            f"batch {zombie.batch_id}: re-run with --full-reload, or RESTORE manually"
        )


def _clear_running_only(
    target_table: str,
    control: ETLControlStore,
    zombie: BatchInfo,
    plan: ZombieRecoveryPlan,
    dry_run: bool,
    executed: list[str],
) -> None:
    """Zombie committed nothing: just clear its RUNNING rows."""
    if not dry_run:
        for source in plan.sources:
            control.set_batch_failed(
                target_table,
                source,
                "CRASH_RECOVERY: no commit, cleared RUNNING",
            )
            executed.append(f"batch {zombie.batch_id}: cleared RUNNING for {source}")
    else:
        executed.append(f"batch {zombie.batch_id}: would clear RUNNING (no RESTORE)")


def _execute_restore(
    target_table: str,
    control: ETLControlStore,
    history: DeltaHistoryProvider,
    zombie: BatchInfo,
    plan: ZombieRecoveryPlan,
    dry_run: bool,
    executed: list[str],
    warnings: list[str],
) -> bool:
    """RESTORE the target to the plan's safe point, then reconcile watermarks.

    Ordering is deliberate: the table is restored first; if etl_control
    reconciliation then fails, the run is marked partial with an explicit
    'TARGET RESTORED BUT WATERMARK STALE' warning and the rewind-only
    recovery path finishes the job — never the other way round. Returns
    whether any partial failure occurred.
    """
    if dry_run:
        if plan.restore_timestamp is not None:
            target_spec = f"timestamp {plan.restore_timestamp.isoformat()}"
        elif plan.restore_version is not None:
            target_spec = f"version {plan.restore_version}"
        else:
            target_spec = "(no restore -- plan.fallback applies)"
        executed.append(
            f"batch {zombie.batch_id}: would RESTORE -> {target_spec} "
            f"and rewind watermarks {plan.rewind_watermarks} (--dry-run)"
        )
        return False
    try:
        if plan.restore_timestamp is not None:
            history.restore_to_timestamp(target_table, plan.restore_timestamp)
            executed.append(
                f"batch {zombie.batch_id}: RESTORE target -> timestamp "
                f"{plan.restore_timestamp.isoformat()}"
            )
        else:
            history.restore_to_version(target_table, plan.restore_version)  # type: ignore[arg-type]
            executed.append(
                f"batch {zombie.batch_id}: RESTORE target -> version {plan.restore_version}"
            )
    except Exception as exc:  # noqa: BLE001
        warnings.append(
            f"batch {zombie.batch_id}: RESTORE failed: {exc} "
            "(target untouched; aborting before etl_control reconcile)"
        )
        return True
    partial = False
    for source in plan.sources:
        try:
            control.set_batch_failed(
                target_table, source, "CRASH_RECOVERY: rolled back"
            )
            control.rewind_watermark(
                target_table, source, plan.rewind_watermarks.get(source)
            )
            executed.append(
                f"batch {zombie.batch_id}: reconciled {source} "
                f"(watermark -> {plan.rewind_watermarks.get(source)})"
            )
        except Exception as exc:  # noqa: BLE001
            partial = True
            warnings.append(
                f"batch {zombie.batch_id}: etl_control reconcile FAILED for "
                f"{source}: {exc} - TARGET RESTORED BUT WATERMARK STALE; "
                f"re-run `kimball recover --table {target_table} --rewind-watermark`"
            )
    return partial


def _fail_on_unrecoverable(target_table: str, reconciliation) -> None:
    """Refuse recovery when the control plane or the target itself is gone."""
    if reconciliation.verdict is ReconciliationVerdict.CONTROL_TABLE_MISSING:
        raise StructuredError(
            "cannot recover: etl_control table not found",
            category=ErrorCategory.RECOVERY,
            remediation="Run `kimball run` once to create the control schema.",
            runbook_link=f"{RUNBOOK}#control-table-missing",
        )
    if reconciliation.verdict is ReconciliationVerdict.TARGET_MISSING:
        raise StructuredError(
            "cannot recover: target table does not exist",
            category=ErrorCategory.RECOVERY,
            remediation="Run the pipeline to create the target, then re-inspect.",
            runbook_link=f"{RUNBOOK}#target-missing",
        )


def _enforce_single_writer(
    target_table: str,
    control_state: TargetControlState,
    delta_state: TargetDeltaState,
    runtime: RuntimeProfile,
    force: bool,
) -> None:
    """Refuse recovery on suspected concurrent-writer violations unless forced."""
    known_ids = tuple(sorted({b.batch_id for b in control_state.batches}))
    writer = check_writer_contract(
        delta_state, known_ids, runtime.supports_commit_tagging
    )
    if writer.verdict is WriterVerdict.SUSPECTED_VIOLATION and not force:
        raise StructuredError(
            f"single-writer violation suspected on {target_table}: "
            f"{len(writer.suspicious_commits)} commit(s) tagged with batch_ids "
            "unknown to etl_control",
            category=ErrorCategory.CONCURRENT_WRITER,
            remediation="Confirm no other writer is active on the target, then "
            "re-run with --force.",
            runbook_link=f"{RUNBOOK}#concurrent-writer",
        )


def _recover_rewind_only(
    target_table: str,
    control: ETLControlStore,
    control_state: TargetControlState,
    reconciliation,
    dry_run: bool,
    warnings: list[str],
) -> RecoverResult:
    """--rewind-watermark mode: only reset watermarks, never touch the table.

    Valid exclusively for the WATERMARK_AHEAD_OF_TARGET verdict; every other
    verdict has nothing this mode can rewind.
    """
    if reconciliation.verdict is not ReconciliationVerdict.WATERMARK_AHEAD_OF_TARGET:
        return RecoverResult(
            target_table,
            dry_run,
            [],
            [],
            False,
            [
                f"rewind-only requested but verdict is "
                f"{reconciliation.verdict.value} (not watermark-ahead-of-target); "
                "nothing to rewind."
            ],
        )
    sources = sorted({b.source_table for b in control_state.batches})
    plan = ZombieRecoveryPlan(
        batch_id="(rewind-only)",
        sources=tuple(sources),
        has_committed_data=False,
        restore_version=None,
        rewind_watermarks={s: None for s in sources},
        fallback=None,
    )
    executed: list[str] = []
    partial = False
    if not dry_run:
        for source in sources:
            try:
                control.rewind_watermark(target_table, source, None)
                executed.append(
                    f"rewound watermark for {source} -> None (full replay next run)"
                )
            except Exception as exc:  # noqa: BLE001
                partial = True
                executed.append(f"FAILED to rewind watermark for {source}: {exc}")
    return RecoverResult(target_table, dry_run, [plan], executed, partial, warnings)


def _upstream_warnings(control: ETLControlStore, upstream_targets: tuple[str, ...]):
    if not upstream_targets:
        return
    for up in upstream_targets:
        try:
            up_state = control.get_target_state(up)
        except Exception:  # noqa: BLE001
            yield f"upstream target {up} does not exist or has no etl_control state"
            continue
        if any(b.status == "RUNNING" for b in up_state.batches):
            yield (
                f"upstream target {up} has RUNNING batches; recover in topological "
                "order (upstream first) to avoid re-introducing the same state."
            )


def _plan_zombie(
    zombie: BatchInfo,
    control_state: TargetControlState,
    delta_state: TargetDeltaState,
    runtime: RuntimeProfile,
    version: int | None,
    timestamp: datetime | None,
) -> ZombieRecoveryPlan:
    sources = tuple(
        sorted(
            {
                b.source_table
                for b in control_state.batches
                if b.batch_id == zombie.batch_id
            }
        )
    )
    rewind = {
        s: _previous_success_watermark(control_state, s, zombie.batch_id)
        for s in sources
    }
    available_versions = {c.version for c in delta_state.commits}
    current = delta_state.current_version

    # Operator-supplied restore target (Serverless or manual).
    if timestamp is not None:
        return ZombieRecoveryPlan(
            zombie.batch_id, sources, True, None, timestamp, rewind, None
        )
    if version is not None:
        if version < 0:
            return ZombieRecoveryPlan(
                zombie.batch_id,
                sources,
                True,
                None,
                None,
                rewind,
                f"--version {version} is negative; cannot RESTORE below creation - use --full-reload",
            )
        if current is not None and version > current:
            return ZombieRecoveryPlan(
                zombie.batch_id,
                sources,
                True,
                None,
                None,
                rewind,
                f"--version {version} is ahead of current target version {current}; nothing to RESTORE",
            )
        return ZombieRecoveryPlan(
            zombie.batch_id, sources, True, version, None, rewind, None
        )

    # Auto-attribution path. On Serverless, commits are untagged so we cannot
    # safely attribute or RESTORE precisely - require an operator-supplied
    # --version / --timestamp (or --full-reload).
    if not runtime.supports_commit_tagging:
        return ZombieRecoveryPlan(
            zombie.batch_id,
            sources,
            True,
            None,
            None,
            rewind,
            "Serverless: commits cannot be attributed to a batch_id; "
            "provide --version or --timestamp, or use --full-reload",
        )

    commits = [c for c in delta_state.commits if c.batch_id == zombie.batch_id]
    if not commits:
        return ZombieRecoveryPlan(
            zombie.batch_id, sources, False, None, None, rewind, None
        )

    first_version = min(c.version for c in commits)
    restore_version = first_version - 1
    if restore_version < 0:
        return ZombieRecoveryPlan(
            zombie.batch_id,
            sources,
            True,
            None,
            None,
            rewind,
            "zombie commit is version 0; cannot RESTORE below creation - use --full-reload",
        )
    if restore_version not in available_versions:
        return ZombieRecoveryPlan(
            zombie.batch_id,
            sources,
            True,
            None,
            None,
            rewind,
            f"pre-batch version {restore_version} not in retained history "
            "(VACUUMed); use --full-reload or increase history retention",
        )
    return ZombieRecoveryPlan(
        zombie.batch_id, sources, True, restore_version, None, rewind, None
    )


def _previous_success_watermark(
    control_state: TargetControlState, source: str, exclude_batch_id: str
) -> int | None:
    # etl_control is keyed by (target, source), so a RUNNING record supersedes
    # its prior SUCCESS record. batch_start_all persists that prior watermark
    # explicitly for exact crash recovery.
    running_record = next(
        (
            b
            for b in control_state.batches
            if b.source_table == source and b.batch_id == exclude_batch_id
        ),
        None,
    )
    if running_record and running_record.previous_success_watermark is not None:
        return running_record.previous_success_watermark
    candidates = [
        b.last_processed_version
        for b in control_state.batches
        if b.source_table == source
        and b.status == "SUCCESS"
        and b.batch_id != exclude_batch_id
        and b.last_processed_version is not None
    ]
    return max(candidates, default=None)


def _full_reload(
    target_table: str,
    control: ETLControlStore,
    control_state: TargetControlState,
    zombie: BatchInfo,
    executed: list[str],
) -> None:
    for source in {
        b.source_table for b in control_state.batches if b.batch_id == zombie.batch_id
    }:
        control.rewind_watermark(target_table, source, None)
        control.set_batch_failed(
            target_table, source, "CRASH_RECOVERY: full reload scheduled"
        )
        executed.append(
            f"batch {zombie.batch_id}: reset watermark for {source} -> None"
        )


def _recover_orphan(
    target_table: str,
    history: DeltaHistoryProvider,
    delta_state: TargetDeltaState,
    batch_id: str,
    dry_run: bool,
    force: bool,
    full_reload: bool,
) -> RecoverResult:
    """RESTORE an orphan commit (tagged with a batch_id etl_control does not
    know). No phase-2 etl_control reconciliation - there is no control row.

    Destructive: RESTORE removes a commit the control table cannot vouch for,
    so it requires ``--force`` (the commit could belong to an active writer).
    """
    if not force:
        raise StructuredError(
            f"orphan commit batch_id={batch_id} on {target_table}: RESTORE is "
            "destructive (removes a commit the control table does not know "
            "about); re-run with --force",
            category=ErrorCategory.CONCURRENT_WRITER,
            remediation="Confirm the commit is a crashed/leftover batch (not "
            "an active writer), then re-run with --force.",
            runbook_link=f"{RUNBOOK}#concurrent-writer",
        )
    commits = [c for c in delta_state.commits if c.batch_id == batch_id]
    if not commits:
        raise StructuredError(
            f"no orphan commit tagged batch_id={batch_id} in target history",
            category=ErrorCategory.RECOVERY,
            runbook_link=f"{RUNBOOK}#recovery",
        )
    first_version = min(c.version for c in commits)
    restore_version = first_version - 1
    available = {c.version for c in delta_state.commits}
    fallback: str | None = None
    if restore_version < 0:
        fallback = "orphan commit is version 0; cannot RESTORE below creation - re-run the pipeline"
    elif restore_version not in available:
        fallback = (
            f"pre-batch version {restore_version} not in retained history "
            "(VACUUMed); re-run the pipeline to accept the orphan as baseline"
        )
    plan = ZombieRecoveryPlan(
        batch_id=batch_id,
        sources=(),
        has_committed_data=True,
        restore_version=None if fallback else restore_version,
        rewind_watermarks={},
        fallback=fallback,
    )
    executed: list[str] = []
    warns: list[str] = []
    if fallback:
        warns.append(f"orphan {batch_id}: {fallback}")
        return RecoverResult(target_table, dry_run, [plan], executed, False, warns)
    if not dry_run:
        try:
            history.restore_to_version(target_table, restore_version)
            executed.append(
                f"orphan {batch_id}: RESTORE target -> version {restore_version} "
                "(no etl_control row to reconcile)"
            )
        except Exception as exc:  # noqa: BLE001
            return RecoverResult(
                target_table,
                dry_run,
                [plan],
                executed,
                True,
                [f"orphan {batch_id}: RESTORE failed: {exc}"],
            )
    else:
        executed.append(
            f"orphan {batch_id}: would RESTORE -> {restore_version} (--dry-run)"
        )
    return RecoverResult(target_table, dry_run, [plan], executed, False, warns)
