from __future__ import annotations

import pytest

from kimball.ops.repair_ledger import (
    DeltaRepairLedger,
    RepairInputCorrection,
    RepairPlanStep,
    RepairRead,
    RepairWriteTarget,
    stable_repair_id,
)


class InMemoryLedger(DeltaRepairLedger):
    def __init__(self):
        self.data = {
            key: []
            for key in (
                "revision",
                "correction",
                "plan",
                "step",
                "write",
                "read",
                "dependency",
            )
        }

    def _rows(self, key, value):
        column = {
            "revision": "repair_id",
            "correction": "repair_id",
            "plan": "repair_id",
            "step": "plan_id",
        }[key]
        return [row.copy() for row in self.data[key] if row[column] == value]

    def _append(self, key, rows):
        self.data[key].extend(row.copy() for row in rows)

    def _rows_for_plan_steps(self, repair_id, key):
        plan_id = stable_repair_id(repair_id, "plan")
        steps = self._rows("step", plan_id)
        ids = {row["step_id"] for row in steps}
        return [row.copy() for row in self.data[key] if row.get("step_id") in ids]


def _step(snapshot_version=7):
    return RepairPlanStep(
        target_table="gold.dim_customer",
        strategy="snapshot_rebuild_scd1",
        target_generation="target-generation",
        expected_current_version=4,
        rollback_version=4,
        reads=(
            RepairRead(
                dataset_table="silver.customers",
                snapshot_version=snapshot_version,
                schema_digest="schema-digest",
                dataset_generation="source-generation",
                prior_watermark=5,
            ),
        ),
        writes=(
            RepairWriteTarget(
                target_table="gold.dim_customer",
                target_existed_before=True,
                target_generation="target-generation",
                target_version_before=4,
                rollback_version=4,
            ),
        ),
    )


def _create(ledger, snapshot_version=7):
    return ledger.create_plan(
        repair_id="repair-1",
        reason="correct source snapshot",
        steps=(_step(snapshot_version),),
        owner="data-owner",
        manifest_digest="manifest-1",
        config_digest="config-1",
        runtime_profile="classic",
        strategy="dependency_snapshot_rebuild",
        corrections=(
            RepairInputCorrection(
                "silver.customers", 6, snapshot_version, "source-generation"
            ),
        ),
    )


def test_plan_persists_normalized_sources_writes_watermark_and_correction():
    ledger = InMemoryLedger()

    plan_id = _create(ledger)

    assert len(ledger.data["revision"]) == 1
    assert len(ledger.data["correction"]) == 1
    assert ledger.data["plan"][0]["plan_id"] == plan_id
    assert ledger.data["read"][0]["dataset_table"] == "silver.customers"
    assert ledger.data["read"][0]["snapshot_version"] == 7
    assert ledger.data["read"][0]["prior_watermark"] == 5
    assert ledger.data["write"][0]["rollback_version"] == 4


def test_same_repair_id_cannot_be_reused_for_a_different_frozen_plan():
    ledger = InMemoryLedger()
    _create(ledger)

    with pytest.raises(RuntimeError, match="different frozen"):
        _create(ledger, snapshot_version=8)


def test_retry_detects_missing_normalized_plan_rows():
    ledger = InMemoryLedger()
    _create(ledger)
    ledger.data["read"].clear()

    with pytest.raises(RuntimeError, match="incomplete or different frozen plan"):
        _create(ledger)


@pytest.mark.parametrize(
    "read",
    [
        RepairRead(dataset_table="silver.s", snapshot_version=-1),
        RepairRead(
            dataset_table="silver.s",
            read_mode="cdf",
            cdf_start_version=-1,
            cdf_end_version=2,
        ),
    ],
)
def test_read_versions_must_be_non_negative(read):
    with pytest.raises(ValueError, match="non-negative"):
        read.validate()
