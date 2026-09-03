"""State reconciler (ROADMAP 1.A) - the crown jewel of Phase 1.

The framework's known correctness boundary is that the Delta target and
``etl_control`` are separate transactions (KNOWN_LIMITATIONS S2). Recovery
RESTOREs the target but does not rewind ``etl_control``, so after a crash
the watermark can be ahead of the target's actual state - and the next run
silently skips CDF versions.

This module joins the two stores on the one key they share - ``batch_id`` -
rather than comparing watermark (a *source* CDF version) to target_version
(a *target* Delta commit count), which are different number spaces. The
signals used are:

* a RUNNING batch in ``etl_control`` -> zombie
* a commit tagged with a RUNNING batch_id -> zombie with committed data
* a RESTORE operation in target history + the last SUCCESS batch's tagged
  commit now absent -> watermark ahead of target (post-rollback drift)
* a commit tagged with a batch_id unknown to ``etl_control`` -> target
  ahead of watermark
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from kimball.ops.providers import (
    BatchInfo,
    DeltaCommit,
    DeltaHistoryProvider,
    ETLControlStore,
    TargetControlState,
    TargetDeltaState,
)
from kimball.ops.runtime_profile import RuntimeProfile

RUNBOOK = "docs/RUNBOOK.md"


class ReconciliationVerdict(str, Enum):
    CONSISTENT = "consistent"
    WATERMARK_AHEAD_OF_TARGET = "watermark_ahead_of_target"
    TARGET_AHEAD_OF_WATERMARK = "target_ahead_of_watermark"
    CONTROL_TABLE_MISSING = "control_table_missing"
    TARGET_MISSING = "target_missing"
    ZOMBIE_WITH_COMMITTED_DATA = "zombie_with_committed_data"
    ZOMBIE_NO_COMMIT = "zombie_no_commit"


@dataclass(frozen=True)
class ReconciliationReport:
    target_table: str
    verdict: ReconciliationVerdict
    watermark_version: int | None
    target_version: int | None
    zombie_batches: tuple[BatchInfo, ...]
    zombie_commits: tuple[DeltaCommit, ...]
    evidence: str
    remediation: str
    runbook_link: str | None
    orphan_commits: tuple[DeltaCommit, ...] = ()


class StateReconciler:
    def __init__(
        self,
        control: ETLControlStore,
        history: DeltaHistoryProvider,
        runtime: RuntimeProfile,
    ) -> None:
        self._control = control
        self._history = history
        self._runtime = runtime

    def reconcile(self, target_table: str) -> ReconciliationReport:
        """Reconcile etl_control watermarks against the target's Delta history.

        Verdict ladder (first match wins, ADR-004 grade-A pass):
        1. missing control table / missing target (pre-flight)
        2. RUNNING zombies (three sub-cases: committed, tagging-off, clean)
        3. post-rollback drift (RESTORE present, watermark not rewound)
        4. orphan commits (target ahead of control table)
        5. consistent
        """
        if not self._control.control_table_exists():
            return _report(
                target_table,
                ReconciliationVerdict.CONTROL_TABLE_MISSING,
                None,
                None,
                (),
                (),
                "etl_control table not found",
                "Initialise the control schema for this target's environment "
                "(a `kimball run` will create it), then re-inspect.",
                f"{RUNBOOK}#control-table-missing",
            )

        delta = self._history.get_target_delta_state(target_table)
        if not delta.table_exists:
            return _report(
                target_table,
                ReconciliationVerdict.TARGET_MISSING,
                None,
                None,
                (),
                (),
                "target table does not exist",
                "Run the pipeline once to create it, or restore from a known-good state.",
                f"{RUNBOOK}#target-missing",
            )

        control_state = self._control.get_target_state(target_table)
        zombies = tuple(b for b in control_state.batches if b.status == "RUNNING")
        running_ids = {b.batch_id for b in zombies}
        all_ids = {b.batch_id for b in control_state.batches}

        tagged = tuple(c for c in delta.commits if c.batch_id is not None)
        zombie_commits = tuple(c for c in tagged if c.batch_id in running_ids)
        orphan_commits = tuple(c for c in tagged if c.batch_id not in all_ids)
        restore_present = any(c.operation == "RESTORE" for c in delta.commits)

        last_success = _last_success_batch(control_state)
        last_success_present = last_success is not None and any(
            c.batch_id == last_success.batch_id for c in tagged
        )

        if zombies:
            return self._zombie_verdict(
                target_table, control_state, delta, zombies, zombie_commits
            )
        if restore_present and (
            drift := self._restore_drift_verdict(
                target_table, control_state, delta, last_success, last_success_present
            )
        ):
            return drift
        if orphan_commits:
            return _report(
                target_table,
                ReconciliationVerdict.TARGET_AHEAD_OF_WATERMARK,
                _max_watermark(control_state),
                delta.current_version,
                (),
                (),
                f"{len(orphan_commits)} commit(s) tagged with a batch_id "
                "unknown to etl_control (target ahead of control table)",
                "Re-run the pipeline to advance the watermark, or inspect "
                "history if a control-table reset occurred.",
                f"{RUNBOOK}#target-ahead-of-watermark",
                orphan_commits=orphan_commits,
            )
        return _report(
            target_table,
            ReconciliationVerdict.CONSISTENT,
            _max_watermark(control_state),
            delta.current_version,
            (),
            (),
            "watermark and target agree; no RUNNING batches; no RESTORE in history",
            "No action required.",
            None,
        )

    def _zombie_verdict(
        self,
        target_table: str,
        control_state: TargetControlState,
        delta: TargetDeltaState,
        zombies: tuple[BatchInfo, ...],
        zombie_commits: tuple[DeltaCommit, ...],
    ) -> ReconciliationReport:
        """RUNNING batches present: split on tagging support and commit evidence."""
        if self._runtime.supports_commit_tagging and zombie_commits:
            return _report(
                target_table,
                ReconciliationVerdict.ZOMBIE_WITH_COMMITTED_DATA,
                _max_watermark(control_state),
                delta.current_version,
                zombies,
                zombie_commits,
                f"{len(zombie_commits)} commit(s) tagged with a RUNNING "
                "batch_id exist on the target",
                "Run `kimball recover --table <target>` (two-phase: RESTORE "
                "target, then rewind watermark + clear RUNNING).",
                f"{RUNBOOK}#zombie-with-committed-data",
            )
        if not self._runtime.supports_commit_tagging:
            return _report(
                target_table,
                ReconciliationVerdict.ZOMBIE_NO_COMMIT,
                _max_watermark(control_state),
                delta.current_version,
                zombies,
                (),
                "RUNNING batch(es) present but commit tagging is "
                "unavailable (Serverless); committed data cannot be "
                "auto-attributed",
                "On Serverless run `kimball recover --table <target> "
                "--version <N>` with the pre-batch target version "
                "(inspect history manually).",
                f"{RUNBOOK}#serverless-no-tagging",
            )
        return _report(
            target_table,
            ReconciliationVerdict.ZOMBIE_NO_COMMIT,
            _max_watermark(control_state),
            delta.current_version,
            zombies,
            (),
            "RUNNING batch(es) with no committed data attributed to them",
            "Run `kimball recover --table <target>` to clear the stale "
            "RUNNING rows; no RESTORE needed.",
            f"{RUNBOOK}#zombie-no-commit",
        )

    def _restore_drift_verdict(
        self,
        target_table: str,
        control_state: TargetControlState,
        delta: TargetDeltaState,
        last_success: BatchInfo | None,
        last_success_present: bool,
    ) -> ReconciliationReport | None:
        """A RESTORE happened: drift unless the last success is still there.

        Returns the drift report, or ``None`` when reconciliation is
        confirmed (tagging on, last SUCCESS commit re-present) and the
        caller must continue to the orphan/consistent checks -- the
        original control flow fell through in that case.
        """
        if self._runtime.supports_commit_tagging and last_success is not None:
            if not last_success_present:
                return _report(
                    target_table,
                    ReconciliationVerdict.WATERMARK_AHEAD_OF_TARGET,
                    _max_watermark(control_state),
                    delta.current_version,
                    (),
                    (),
                    "a RESTORE operation is present in target history and "
                    "the last SUCCESS batch's tagged commit is absent - the "
                    "target was rolled back but etl_control was not rewound",
                    "Rewind the watermark via "
                    "`kimball recover --table <target> --rewind-watermark`, "
                    "or full reload.",
                    f"{RUNBOOK}#watermark-ahead-of-target",
                )
            return None
        return _report(
            target_table,
            ReconciliationVerdict.WATERMARK_AHEAD_OF_TARGET,
            _max_watermark(control_state),
            delta.current_version,
            (),
            (),
            "a RESTORE operation is present in target history; "
            "commit tagging is off so reconciliation could not be "
            "confirmed",
            "Verify `DESCRIBE HISTORY` manually; rewind the watermark or full reload.",
            f"{RUNBOOK}#watermark-ahead-of-target",
        )
        return _report(
            target_table,
            ReconciliationVerdict.WATERMARK_AHEAD_OF_TARGET,
            _max_watermark(control_state),
            delta.current_version,
            (),
            (),
            "a RESTORE operation is present in target history; "
            "commit tagging is off so reconciliation could not be "
            "confirmed",
            "Verify `DESCRIBE HISTORY` manually; rewind the watermark or full reload.",
            f"{RUNBOOK}#watermark-ahead-of-target",
        )


def _max_watermark(state: TargetControlState) -> int | None:
    values = [
        b.last_processed_version
        for b in state.batches
        if b.last_processed_version is not None
    ]
    return max(values, default=None)


def _last_success_batch(state: TargetControlState) -> BatchInfo | None:
    candidates = [
        b
        for b in state.batches
        if b.status == "SUCCESS" and b.last_processed_version is not None
    ]
    return (
        max(candidates, key=lambda b: b.last_processed_version or 0)
        if candidates
        else None
    )


def _report(
    target_table: str,
    verdict: ReconciliationVerdict,
    watermark: int | None,
    target_version: int | None,
    zombies: tuple[BatchInfo, ...],
    zombie_commits: tuple[DeltaCommit, ...],
    evidence: str,
    remediation: str,
    runbook_link: str | None,
    orphan_commits: tuple[DeltaCommit, ...] = (),
) -> ReconciliationReport:
    return ReconciliationReport(
        target_table,
        verdict,
        watermark,
        target_version,
        zombies,
        zombie_commits,
        evidence,
        remediation,
        runbook_link,
        orphan_commits,
    )
