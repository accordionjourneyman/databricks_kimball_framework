"""Tests for the single-writer contract checker (1.C)."""

from __future__ import annotations

from kimball.ops.providers import TargetDeltaState
from kimball.ops.writer_contract import WriterVerdict, check_writer_contract
from tests.unit.ops.fakes import commit


def _state(commits):
    return TargetDeltaState("gold.t", True, 5, commits)


def test_clean_when_all_tagged_commits_known():
    report = check_writer_contract(
        _state((commit(5, "b1"), commit(4, "b2"))), ("b1", "b2"), True
    )
    assert report.verdict is WriterVerdict.CLEAN
    assert report.suspicious_commits == ()


def test_suspected_violation_for_unknown_tagged_commit():
    report = check_writer_contract(
        _state((commit(5, "b1"), commit(4, "bX"))), ("b1",), True
    )
    assert report.verdict is WriterVerdict.SUSPECTED_VIOLATION
    assert len(report.suspicious_commits) == 1
    assert report.suspicious_commits[0].batch_id == "bX"


def test_untagged_commits_are_not_flagged():
    report = check_writer_contract(
        _state((commit(5, None), commit(4, "b1"))), ("b1",), True
    )
    assert report.verdict is WriterVerdict.CLEAN


def test_unknown_when_tagging_off_serverless():
    report = check_writer_contract(_state((commit(5, "bX"),)), ("b1",), False)
    assert report.verdict is WriterVerdict.UNKNOWN
