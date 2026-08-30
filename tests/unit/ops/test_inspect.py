"""Tests for the inspect command composition (1.1)."""

from __future__ import annotations

from kimball.ops.inspect import inspect_target
from kimball.ops.providers import SourceHealthReport
from kimball.ops.runtime_profile import RuntimeFlavor, RuntimeProfile
from tests.unit.ops.fakes import (
    FakeControl,
    FakeHistory,
    FakeSources,
    batch,
    commit,
    providers,
)

CLASSIC = RuntimeProfile(RuntimeFlavor.CLASSIC, True)


def test_inspect_consistent_target():
    batches = (batch("b1", "silver.s", "SUCCESS", 5),)
    hist = FakeHistory(True, 3, (commit(3, "b1"),))
    src = FakeSources(
        {"silver.s": SourceHealthReport("silver.s", True, True, 0, 5, None, None)}
    )
    report = inspect_target(
        "gold.t", providers(FakeControl(True, batches), hist, src), CLASSIC
    )
    assert report["reconciliation"]["verdict"] == "consistent"
    assert report["control_table_exists"] is True
    assert report["writer_contract"]["verdict"] == "clean"
    assert report["source_health"][0]["verdict"] == "healthy"


def test_inspect_control_table_missing():
    report = inspect_target(
        "gold.t",
        providers(FakeControl(exists=False), FakeHistory(), FakeSources()),
        CLASSIC,
    )
    assert report["reconciliation"]["verdict"] == "control_table_missing"
    assert report["control_table_exists"] is False


def test_inspect_zombie_surfaces_writer_and_source():
    batches = (batch("z1", "silver.s", "RUNNING", 5),)
    hist = FakeHistory(True, 6, (commit(6, "z1"),))
    src = FakeSources(
        {"silver.s": SourceHealthReport("silver.s", True, True, 0, 5, None, None)}
    )
    report = inspect_target(
        "gold.t", providers(FakeControl(True, batches), hist, src), CLASSIC
    )
    assert report["reconciliation"]["verdict"] == "zombie_with_committed_data"
    assert report["writer_contract"]["verdict"] == "clean"  # z1 is known


def test_inspect_surfaces_orphan_commits():
    batches = (batch("b1", "silver.s", "SUCCESS", 5),)
    hist = FakeHistory(True, 6, (commit(6, "ghost"), commit(5, "b1")))
    report = inspect_target(
        "gold.t", providers(FakeControl(True, batches), hist, FakeSources()), CLASSIC
    )
    assert report["reconciliation"]["verdict"] == "target_ahead_of_watermark"
    assert report["reconciliation"]["orphan_commits"] == 1
    assert "ghost" in report["reconciliation"]["orphan_batch_ids"]
