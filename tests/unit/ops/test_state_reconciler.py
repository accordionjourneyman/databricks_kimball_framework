"""Tests for the state reconciler (1.A) - the core crash-recovery detector."""

from __future__ import annotations

from kimball.ops.runtime_profile import RuntimeFlavor, RuntimeProfile
from kimball.ops.state_reconciler import ReconciliationVerdict, StateReconciler
from tests.unit.ops.fakes import FakeControl, FakeHistory, batch, commit

CLASSIC = RuntimeProfile(RuntimeFlavor.CLASSIC, True)
SERVERLESS = RuntimeProfile(RuntimeFlavor.SERVERLESS, False)


def reconciler(control, history, runtime=CLASSIC):
    return StateReconciler(control, history, runtime)


def test_control_table_missing():
    r = reconciler(FakeControl(exists=False), FakeHistory()).reconcile("gold.t")
    assert r.verdict is ReconciliationVerdict.CONTROL_TABLE_MISSING
    assert r.runbook_link and "control-table-missing" in r.runbook_link


def test_target_missing():
    r = reconciler(
        FakeControl(exists=True, batches=()),
        FakeHistory(exists=False, current_version=None),
    ).reconcile("gold.t")
    assert r.verdict is ReconciliationVerdict.TARGET_MISSING


def test_consistent_no_zombies_no_restore():
    batches = (batch("b1", "silver.s", "SUCCESS", 5),)
    history = FakeHistory(exists=True, current_version=3, commits=(commit(3, "b1"),))
    r = reconciler(FakeControl(True, batches), history).reconcile("gold.t")
    assert r.verdict is ReconciliationVerdict.CONSISTENT
    assert r.watermark_version == 5


def test_zombie_with_committed_data():
    batches = (batch("z1", "silver.s", "RUNNING", 5),)
    commits = (commit(6, "z1"), commit(5, "b1"))
    history = FakeHistory(True, 6, commits)
    r = reconciler(FakeControl(True, batches), history).reconcile("gold.t")
    assert r.verdict is ReconciliationVerdict.ZOMBIE_WITH_COMMITTED_DATA
    assert len(r.zombie_commits) == 1
    assert (
        "two-phase" in r.remediation
        or "RESTORE" in r.remediation
        or "recover" in r.remediation
    )


def test_zombie_no_commit():
    batches = (batch("z1", "silver.s", "RUNNING", 5),)
    history = FakeHistory(True, 5, (commit(5, "b1"),))  # no commit tagged z1
    r = reconciler(FakeControl(True, batches), history).reconcile("gold.t")
    assert r.verdict is ReconciliationVerdict.ZOMBIE_NO_COMMIT
    assert r.zombie_commits == ()


def test_watermark_ahead_of_target_after_restore():
    # last SUCCESS b1 watermark=10; a RESTORE removed b1's commit -> drift.
    batches = (batch("b1", "silver.s", "SUCCESS", 10),)
    commits = (commit(8, None, operation="RESTORE"), commit(7, "older"))
    history = FakeHistory(True, 8, commits)
    r = reconciler(FakeControl(True, batches), history).reconcile("gold.t")
    assert r.verdict is ReconciliationVerdict.WATERMARK_AHEAD_OF_TARGET
    assert r.watermark_version == 10
    assert (
        "rewind" in r.remediation.lower()
        or "rewind" in r.evidence.lower()
        or "rolled back" in r.evidence.lower()
    )


def test_no_drift_when_restored_then_re_run_reconciled():
    # RESTORE present but the last SUCCESS commit is present again (re-run after restore).
    batches = (batch("b2", "silver.s", "SUCCESS", 11),)
    commits = (commit(11, "b2"), commit(9, None, operation="RESTORE"))
    history = FakeHistory(True, 11, commits)
    r = reconciler(FakeControl(True, batches), history).reconcile("gold.t")
    assert r.verdict is ReconciliationVerdict.CONSISTENT


def test_serverless_zombie_cannot_attribute():
    batches = (batch("z1", "silver.s", "RUNNING", 5),)
    history = FakeHistory(True, 6, (commit(6, "z1"),))
    r = reconciler(FakeControl(True, batches), history, SERVERLESS).reconcile("gold.t")
    assert r.verdict is ReconciliationVerdict.ZOMBIE_NO_COMMIT
    assert "Serverless" in r.evidence or "Serverless" in r.remediation


def test_serverless_restore_is_drift_suspected():
    batches = (batch("b1", "silver.s", "SUCCESS", 10),)
    commits = (commit(8, None, operation="RESTORE"),)
    history = FakeHistory(True, 8, commits)
    r = reconciler(FakeControl(True, batches), history, SERVERLESS).reconcile("gold.t")
    assert r.verdict is ReconciliationVerdict.WATERMARK_AHEAD_OF_TARGET


def test_target_ahead_of_watermark_orphan_commit():
    # a commit tagged with a batch_id etl_control does not know about.
    batches = (batch("b1", "silver.s", "SUCCESS", 5),)
    commits = (commit(6, "ghost"), commit(5, "b1"))
    history = FakeHistory(True, 6, commits)
    r = reconciler(FakeControl(True, batches), history).reconcile("gold.t")
    assert r.verdict is ReconciliationVerdict.TARGET_AHEAD_OF_WATERMARK


def test_target_ahead_report_carries_orphan_commits():
    batches = (batch("b1", "silver.s", "SUCCESS", 5),)
    commits = (commit(6, "ghost"), commit(5, "b1"))
    history = FakeHistory(True, 6, commits)
    r = reconciler(FakeControl(True, batches), history).reconcile("gold.t")
    assert r.verdict is ReconciliationVerdict.TARGET_AHEAD_OF_WATERMARK
    assert len(r.orphan_commits) == 1
    assert r.orphan_commits[0].batch_id == "ghost"
