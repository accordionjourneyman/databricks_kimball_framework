from __future__ import annotations

import re
from datetime import datetime, timezone

import pytest

from kimball.ops.errors import StructuredError
from kimball.ops.providers import (
    BatchInfo,
    DeltaCommit,
    OpsProviders,
    TargetControlState,
    TargetDeltaState,
)
from kimball.ops.repair import rollback_repair
from kimball.ops.repair_ledger import stable_repair_id
from kimball.ops.runtime_profile import RuntimeFlavor, RuntimeProfile


class FakeControl:
    control_table_name = "ops.etl_control"

    def __init__(self, history):
        self.history = history
        self.rows = {
            ("silver.dim", "silver.customers"): BatchInfo(
                "b1", "silver.customers", "SUCCESS", 10
            ),
            ("gold.fact", "silver.dim"): BatchInfo("b2", "silver.dim", "SUCCESS", 4),
            ("gold.fact", "silver.orders"): BatchInfo(
                "b2", "silver.orders", "SUCCESS", 7
            ),
        }
        self.rewinds = []

    def get_target_state(self, table):
        batches = tuple(
            row for (target, _), row in self.rows.items() if target == table
        )
        return TargetControlState(table, True, batches)

    def rewind_watermark_tagged(self, target, source, version, metadata):
        token = re.search(r"operation_token=([^; ]+)", metadata).group(1)
        self.rewinds.append((target, source, version, token))
        key = (target, source)
        if version is None:
            self.rows.pop(key, None)
        else:
            self.rows[key] = BatchInfo("", source, "RECOVERED", version)
        state = self.history.states[self.control_table_name]
        next_version = state.current_version + 1
        commit = DeltaCommit(
            next_version,
            "MERGE",
            None,
            datetime.now(timezone.utc),
            token,
        )
        self.history.states[self.control_table_name] = TargetDeltaState(
            self.control_table_name,
            True,
            next_version,
            (*state.commits, commit),
            state.generation_id,
        )


class FakeHistory:
    def __init__(self):
        self.states = {
            "silver.dim": _state("silver.dim", 4, 3, "op-dim", "dim-generation"),
            "gold.fact": _state("gold.fact", 6, 5, "op-fact", "fact-generation"),
            "ops.etl_control": _state(
                "ops.etl_control", 12, 12, "", "control-generation"
            ),
        }
        self.restored = []

    def get_target_delta_state(self, table, history_limit=200):
        return self.states[table]

    def restore_to_version_tagged(self, table, version, metadata):
        token = re.search(r"operation_token=([^; ]+)", metadata).group(1)
        state = self.states[table]
        next_version = state.current_version + 1
        self.restored.append((table, version, token))
        commits = (
            *state.commits,
            DeltaCommit(
                next_version,
                "RESTORE",
                None,
                datetime.now(timezone.utc),
                token,
            ),
        )
        self.states[table] = TargetDeltaState(
            table, True, next_version, commits, state.generation_id
        )


def _state(table, current, baseline, repair_token, generation):
    commits = [DeltaCommit(version, "WRITE", None) for version in range(baseline + 1)]
    for version in range(baseline + 1, current + 1):
        commits.append(
            DeltaCommit(
                version,
                "WRITE",
                None,
                datetime.now(timezone.utc),
                repair_token,
            )
        )
    return TargetDeltaState(table, True, current, tuple(commits), generation)


class FakeLedger:
    def __init__(self):
        self.report = {
            "plan": [{"repair_id": "repair-1"}],
            "steps": [
                {
                    "step_id": "step-dim",
                    "ordinal": 0,
                    "operation_token": "op-dim",
                    "target_table": "silver.dim",
                    "expected_current_version": 4,
                },
                {
                    "step_id": "step-fact",
                    "ordinal": 1,
                    "operation_token": "op-fact",
                    "target_table": "gold.fact",
                    "expected_current_version": 6,
                },
            ],
            "writes": [
                {
                    "target_table": "silver.dim",
                    "step_id": "step-dim",
                    "target_existed_before": True,
                    "target_generation": "dim-generation",
                    "rollback_version": 3,
                },
                {
                    "target_table": "gold.fact",
                    "step_id": "step-fact",
                    "target_existed_before": True,
                    "target_generation": "fact-generation",
                    "rollback_version": 5,
                },
            ],
            "reads": [
                {
                    "step_id": "step-dim",
                    "source_role": "source",
                    "dataset_table": "silver.customers",
                    "prior_watermark": 2,
                },
                {
                    "step_id": "step-fact",
                    "source_role": "source",
                    "dataset_table": "silver.dim",
                    "prior_watermark": 3,
                },
                {
                    "step_id": "step-fact",
                    "source_role": "source",
                    "dataset_table": "silver.orders",
                    "prior_watermark": 4,
                },
            ],
            "attempt_reads": [
                {
                    "step_id": "step-dim",
                    "source_role": "source",
                    "dataset_table": "silver.customers",
                    "snapshot_version": 10,
                },
                {
                    "step_id": "step-fact",
                    "source_role": "source",
                    "dataset_table": "silver.dim",
                    "snapshot_version": 4,
                },
                {
                    "step_id": "step-fact",
                    "source_role": "source",
                    "dataset_table": "silver.orders",
                    "snapshot_version": 7,
                },
            ],
            "commits": [
                _commit_row("silver.dim", 3, 4, "op-dim"),
                _commit_row("gold.fact", 5, 6, "op-fact"),
            ],
        }
        self.attempts = 0
        self.step_attempts = []
        self.recorded_commits = []
        self.events = []

    def status(self, repair_id):
        assert repair_id == "repair-1"
        return self.report

    def start_repair_attempt(self, **kwargs):
        self.attempts += 1
        return f"rollback-attempt-{self.attempts}"

    def start_step_attempt(self, **kwargs):
        self.step_attempts.append(kwargs)
        return f"step-attempt-{len(self.step_attempts)}"

    def record_commit(self, **kwargs):
        version = kwargs["target_version"]
        key = (kwargs["target_table"], version)
        if not any(
            (row["target_table"], row["target_version"]) == key
            for row in self.report["commits"]
        ):
            self.report["commits"].append(kwargs)
        self.recorded_commits.append(kwargs)

    def append_event(self, **kwargs):
        self.events.append(kwargs)


def _commit_row(table, before, version, token):
    return {
        "target_table": table,
        "target_version_before": before,
        "target_version": version,
        "operation_token": token,
        "attributed": True,
    }


def _providers(history):
    return OpsProviders(FakeControl(history), history, object())


RUNTIME = RuntimeProfile(RuntimeFlavor.CLASSIC, True)


def test_rollback_restores_exact_versions_in_reverse_dependency_order():
    history = FakeHistory()
    ledger = FakeLedger()
    providers = _providers(history)

    result = rollback_repair(
        repair_id="repair-1",
        providers=providers,
        ledger=ledger,
        runtime=RUNTIME,
        writers_stopped=True,
    )

    assert result.status == "SUCCEEDED"
    assert [row["table"] for row in result.restored_tables] == [
        "gold.fact",
        "silver.dim",
    ]
    assert [(table, version) for table, version, _ in history.restored] == [
        ("gold.fact", 5),
        ("silver.dim", 3),
    ]
    assert all(row["attributed"] for row in ledger.recorded_commits)
    assert {row["rollback_version"] for row in ledger.recorded_commits} == {
        3,
        5,
        None,
    }
    assert [event["status"] for event in ledger.events[-1:]] == ["SUCCEEDED"]
    assert sorted(providers.control.rewinds, key=lambda row: (row[0], row[1])) == [
        (
            "gold.fact",
            "silver.dim",
            3,
            stable_repair_id("repair-1", "cursor-rollback:gold.fact:silver.dim"),
        ),
        (
            "gold.fact",
            "silver.orders",
            4,
            stable_repair_id("repair-1", "cursor-rollback:gold.fact:silver.orders"),
        ),
        (
            "silver.dim",
            "silver.customers",
            2,
            stable_repair_id("repair-1", "cursor-rollback:silver.dim:silver.customers"),
        ),
    ]


def test_rollback_retry_reconciles_tagged_restore_without_restoring_again():
    history = FakeHistory()
    ledger = FakeLedger()
    providers = _providers(history)
    kwargs = {
        "repair_id": "repair-1",
        "providers": providers,
        "ledger": ledger,
        "runtime": RUNTIME,
        "writers_stopped": True,
    }
    first = rollback_repair(**kwargs)
    restore_count = len(history.restored)
    second = rollback_repair(**kwargs)

    assert first.status == second.status == "SUCCEEDED"
    assert len(history.restored) == restore_count == 2
    assert len(providers.control.rewinds) == 3


def test_rollback_rejects_unrelated_commit_after_repair_before_any_restore():
    history = FakeHistory()
    state = history.states["gold.fact"]
    history.states["gold.fact"] = TargetDeltaState(
        state.target_table,
        True,
        7,
        (
            *state.commits,
            DeltaCommit(7, "WRITE", "another-batch", operation_token="other"),
        ),
        state.generation_id,
    )
    ledger = FakeLedger()

    with pytest.raises(StructuredError, match="outside the recorded repair write set"):
        rollback_repair(
            repair_id="repair-1",
            providers=_providers(history),
            ledger=ledger,
            runtime=RUNTIME,
            writers_stopped=True,
        )

    assert history.restored == []
    assert ledger.attempts == 0


def test_rollback_requires_quiesced_writers():
    with pytest.raises(StructuredError, match="writers and consistent-view readers"):
        rollback_repair(
            repair_id="repair-1",
            providers=_providers(FakeHistory()),
            ledger=FakeLedger(),
            runtime=RUNTIME,
        )


def test_rollback_refuses_to_rewind_a_cursor_advanced_outside_the_repair():
    history = FakeHistory()
    ledger = FakeLedger()
    providers = _providers(history)
    providers.control.rows[("gold.fact", "silver.orders")] = BatchInfo(
        "later", "silver.orders", "SUCCESS", 8
    )

    with pytest.raises(StructuredError, match="advanced outside the repair"):
        rollback_repair(
            repair_id="repair-1",
            providers=providers,
            ledger=ledger,
            runtime=RUNTIME,
            writers_stopped=True,
        )

    assert history.restored == []
    assert ledger.attempts == 0


def test_stable_rollback_token_is_repeatable():
    assert stable_repair_id("repair-1", "rollback:gold.fact") == stable_repair_id(
        "repair-1", "rollback:gold.fact"
    )
