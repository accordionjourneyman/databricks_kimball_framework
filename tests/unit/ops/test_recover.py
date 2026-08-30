"""Tests for two-phase recovery (1.2) with fake providers."""

from __future__ import annotations

from datetime import datetime

import pytest

from kimball.ops.errors import StructuredError
from kimball.ops.recover import recover_target
from kimball.ops.runtime_profile import RuntimeFlavor, RuntimeProfile
from tests.unit.ops.fakes import (
    FakeControl,
    FakeHistory,
    FakeSources,
    batch,
    commit,
    providers,
)


def FakeSources_none():
    return FakeSources({})


CLASSIC = RuntimeProfile(RuntimeFlavor.CLASSIC, True)
SERVERLESS = RuntimeProfile(RuntimeFlavor.SERVERLESS, False)


def test_recover_raises_when_control_table_missing():
    with pytest.raises(StructuredError):
        recover_target(
            "gold.t",
            providers(FakeControl(exists=False), FakeHistory(), FakeSources_none()),
            CLASSIC,
        )


def test_recover_no_zombies_returns_warning():
    batches = (batch("b1", "silver.s", "SUCCESS", 5),)
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 5, (commit(5, "b1"),))
    res = recover_target("gold.t", providers(ctrl, hist, FakeSources_none()), CLASSIC)
    assert res.plans == []
    assert "no RUNNING batches" in res.warnings[0]


def test_dry_run_plans_restore_and_rewind():
    batches = (
        batch("z1", "silver.s", "RUNNING", 5),
        batch("b1", "silver.s", "SUCCESS", 3),
    )
    commits = (commit(6, "z1"), commit(5, None), commit(4, "b1"), commit(3, None))
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 6, commits)
    res = recover_target(
        "gold.t", providers(ctrl, hist, FakeSources_none()), CLASSIC, dry_run=True
    )
    assert len(res.plans) == 1
    plan = res.plans[0]
    assert plan.has_committed_data
    assert plan.restore_version == 5  # first zombie commit (6) - 1
    assert plan.rewind_watermarks["silver.s"] == 3  # previous SUCCESS watermark
    assert hist.restored == []  # dry-run does not restore


def test_two_phase_restores_and_rewinds_watermark():
    batches = (
        batch("z1", "silver.s", "RUNNING", 5),
        batch("b1", "silver.s", "SUCCESS", 3),
    )
    commits = (commit(6, "z1"), commit(5, None), commit(3, "b1"))
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 6, commits)
    res = recover_target("gold.t", providers(ctrl, hist, FakeSources_none()), CLASSIC)
    assert hist.restored == [("gold.t", 5)]
    # phase 2: set_batch_failed + rewind_watermark to 3
    actions = [c[0] for c in ctrl.calls]
    assert "fail" in actions and "rewind" in actions
    rewinds = [c for c in ctrl.calls if c[0] == "rewind"]
    assert rewinds[0][3] == 3
    assert not res.partial


def test_fallback_when_pre_batch_version_vacuumed():
    batches = (
        batch("z1", "silver.s", "RUNNING", 5),
        batch("b1", "silver.s", "SUCCESS", 3),
    )
    # zombie commit at version 6 -> restore_version 5, but 5 not in history.
    commits = (commit(6, "z1"), commit(4, "b1"))
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 6, commits)
    res = recover_target(
        "gold.t", providers(ctrl, hist, FakeSources_none()), CLASSIC, dry_run=True
    )
    plan = res.plans[0]
    assert plan.fallback and "VACUUM" in plan.fallback
    assert plan.restore_version is None
    assert hist.restored == []


def test_rewind_only_when_watermark_ahead():
    from kimball.ops.state_reconciler import ReconciliationVerdict  # noqa: F401

    batches = (batch("b1", "silver.s", "SUCCESS", 10),)
    commits = (commit(8, None, operation="RESTORE"),)
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 8, commits)
    res = recover_target(
        "gold.t", providers(ctrl, hist, FakeSources_none()), CLASSIC, rewind_only=True
    )
    assert len(res.plans) == 1
    # watermark rewound to None for each source
    rewinds = [c for c in ctrl.calls if c[0] == "rewind"]
    assert rewinds and rewinds[0][3] is None


def test_serverless_version_path_restores_to_operator_version():
    batches = (batch("z1", "silver.s", "RUNNING", 5),)
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 6, (commit(6, None),))
    recover_target(
        "gold.t", providers(ctrl, hist, FakeSources_none()), SERVERLESS, version=4
    )
    assert hist.restored == [("gold.t", 4)]
    rewinds = [c for c in ctrl.calls if c[0] == "rewind"]
    assert rewinds and rewinds[0][3] is None


def test_serverless_without_version_falls_back():
    batches = (batch("z1", "silver.s", "RUNNING", 5),)
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 6, (commit(6, None),))
    res = recover_target(
        "gold.t", providers(ctrl, hist, FakeSources_none()), SERVERLESS, dry_run=True
    )
    plan = res.plans[0]
    assert plan.fallback and "Serverless" in plan.fallback
    assert plan.restore_version is None


def test_timestamp_path_calls_restore_to_timestamp():
    batches = (batch("z1", "silver.s", "RUNNING", 5),)
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 6, (commit(6, None),))
    ts = datetime(2026, 7, 20, 12, 0)
    recover_target(
        "gold.t", providers(ctrl, hist, FakeSources_none()), SERVERLESS, timestamp=ts
    )
    assert hist.restored_ts == [("gold.t", ts)]


def test_writer_contract_violation_blocks_without_force():
    batches = (
        batch("z1", "silver.s", "RUNNING", 5),
        batch("b1", "silver.s", "SUCCESS", 3),
    )
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 6, (commit(6, "z1"), commit(5, "external")))
    with pytest.raises(StructuredError):
        recover_target("gold.t", providers(ctrl, hist, FakeSources_none()), CLASSIC)


def test_writer_contract_violation_proceeds_with_force():
    batches = (
        batch("z1", "silver.s", "RUNNING", 5),
        batch("b1", "silver.s", "SUCCESS", 3),
    )
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 6, (commit(6, "z1"), commit(5, "external")))
    res = recover_target(
        "gold.t", providers(ctrl, hist, FakeSources_none()), CLASSIC, force=True
    )
    assert hist.restored == [("gold.t", 5)]
    assert not res.partial


def test_upstream_running_emits_warning():
    batches = (batch("z1", "silver.s", "RUNNING", 5),)
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 6, (commit(6, "z1"),))
    res = recover_target(
        "gold.t",
        providers(ctrl, hist, FakeSources_none()),
        CLASSIC,
        upstream_targets=("silver.up",),
    )
    assert any("silver.up" in w for w in res.warnings)


def test_version_and_timestamp_mutually_exclusive():
    batches = (batch("z1", "silver.s", "RUNNING", 5),)
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 6, (commit(6, "z1"),))
    with pytest.raises(StructuredError):
        recover_target(
            "gold.t",
            providers(ctrl, hist, FakeSources_none()),
            CLASSIC,
            version=4,
            timestamp=datetime(2026, 1, 1),
        )


def test_version_out_of_range_fallback():
    batches = (batch("z1", "silver.s", "RUNNING", 5),)
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 6, (commit(6, "z1"),))
    res = recover_target(
        "gold.t",
        providers(ctrl, hist, FakeSources_none()),
        CLASSIC,
        version=99,
        dry_run=True,
    )
    plan = res.plans[0]
    assert plan.fallback and "ahead of current" in plan.fallback


def test_recover_orphan_without_force_raises():
    batches = (batch("b1", "silver.s", "SUCCESS", 3),)
    commits = (commit(6, "external"), commit(5, "b1"))
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 6, commits)
    with pytest.raises(StructuredError):
        recover_target(
            "gold.t",
            providers(ctrl, hist, FakeSources_none()),
            CLASSIC,
            batch_id="external",
        )


def test_recover_orphan_with_force_restores():
    batches = (batch("b1", "silver.s", "SUCCESS", 3),)
    commits = (commit(6, "external"), commit(5, "b1"))
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 6, commits)
    res = recover_target(
        "gold.t",
        providers(ctrl, hist, FakeSources_none()),
        CLASSIC,
        batch_id="external",
        force=True,
    )
    assert hist.restored == [("gold.t", 5)]
    assert not res.partial


def test_recover_orphan_batch_id_not_found_raises():
    batches = (batch("b1", "silver.s", "SUCCESS", 3),)
    commits = (commit(5, "b1"),)
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 5, commits)
    with pytest.raises(StructuredError):
        recover_target(
            "gold.t",
            providers(ctrl, hist, FakeSources_none()),
            CLASSIC,
            batch_id="ghost",
        )


def test_recover_no_zombies_but_orphan_warns_with_ids():
    batches = (batch("b1", "silver.s", "SUCCESS", 3),)
    commits = (commit(6, "external"), commit(5, "b1"))
    ctrl = FakeControl(True, batches)
    hist = FakeHistory(True, 6, commits)
    res = recover_target(
        "gold.t", providers(ctrl, hist, FakeSources_none()), CLASSIC, force=True
    )
    assert any("external" in w and "--force" in w for w in res.warnings)


def test_recovery_uses_watermark_preserved_on_running_record():
    from kimball.ops.providers import BatchInfo, TargetControlState
    from kimball.ops.recover import _previous_success_watermark

    state = TargetControlState(
        "gold.t",
        True,
        (
            BatchInfo(
                batch_id="z1",
                source_table="silver.s",
                status="RUNNING",
                last_processed_version=7,
                previous_success_watermark=3,
            ),
        ),
    )

    assert _previous_success_watermark(state, "silver.s", "z1") == 3
