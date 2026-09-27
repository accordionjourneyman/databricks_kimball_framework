from __future__ import annotations

import pytest

from kimball.common.config import SourceConfig, TableConfig
from kimball.ops.errors import StructuredError
from kimball.ops.providers import (
    BatchInfo,
    DeltaCommit,
    TargetControlState,
    TargetDeltaState,
)
from kimball.ops.repair_execution import (
    _validate_declared_sql_inputs,
    plan_source_reprocess,
)
from kimball.ops.runtime_profile import RuntimeFlavor, RuntimeProfile
from kimball.planning.compiler import ProjectCompiler


class FakeHistory:
    def __init__(self):
        self.states = {
            "silver.customers": _state("silver.customers", 10),
            "silver.crm_status": _state("silver.crm_status", 7),
            "gold.dim_customer": _state("gold.dim_customer", 4),
        }

    def get_target_delta_state(self, table, history_limit=200):
        return self.states[table]


class FakeControl:
    def get_target_state(self, table):
        return TargetControlState(
            table,
            True,
            (
                BatchInfo("old-1", "silver.customers", "SUCCESS", 5),
                BatchInfo("old-2", "silver.crm_status", "SUCCESS", 6),
            ),
        )


class FakeSpark:
    class Frame:
        schema = type(
            "Schema",
            (),
            {"json": lambda self: '{"type":"struct","fields":[]}'},
        )()

    class Reader:
        def format(self, _):
            return self

        def option(self, *_):
            return self

        def table(self, _):
            return FakeSpark.Frame()

    @property
    def read(self):
        return self.Reader()


class FakeLedger:
    def create_plan(self, **kwargs):
        self.kwargs = kwargs
        return "plan-1"


def _state(table, version):
    commits = tuple(DeltaCommit(i, "WRITE", None) for i in range(version + 1))
    return TargetDeltaState(table, True, version, commits, f"generation:{table}")


def _project():
    config = TableConfig(
        table_name="gold.dim_customer",
        table_type="dimension",
        surrogate_key="customer_sk",
        natural_keys=["customer_id"],
        sources=[
            SourceConfig(
                name="silver.customers", alias="customer", cdc_strategy="full"
            ),
            SourceConfig(name="silver.crm_status", alias="status", cdc_strategy="full"),
        ],
        table_description="Customer dimension.",
    )
    return ProjectCompiler(profile="production").compile([("dim.yml", config)])


def test_plan_freezes_correction_and_all_source_versions_with_rollback_point():
    ledger = FakeLedger()
    providers = type(
        "Providers",
        (),
        {"history": FakeHistory(), "control": FakeControl()},
    )()

    preview = plan_source_reprocess(
        spark=FakeSpark(),
        project=_project(),
        providers=providers,
        ledger=ledger,
        repair_id="repair-x-to-y",
        reason="replace bad customer feed",
        bad_versions={"silver.customers": 8},
        replacement_versions={"silver.customers": 9},
        runtime=RuntimeProfile(RuntimeFlavor.CLASSIC, True),
    )

    assert preview.targets == ("gold.dim_customer",)
    assert preview.correction_inputs == (
        {
            "source_table": "silver.customers",
            "bad_version": 8,
            "replacement_version": 9,
            "source_generation": "generation:silver.customers",
        },
    )
    reads = {read.dataset_table: read for read in ledger.kwargs["steps"][0].reads}
    assert reads["silver.customers"].snapshot_version == 9
    assert reads["silver.customers"].prior_watermark == 5
    assert reads["silver.crm_status"].snapshot_version == 7
    assert reads["silver.crm_status"].prior_watermark == 6
    assert ledger.kwargs["steps"][0].writes[0].target_table == "gold.dim_customer"
    assert ledger.kwargs["steps"][0].writes[0].rollback_version == 4


def test_snapshot_work_plan_pins_each_configured_source_exactly():
    from kimball.orchestration.services.work_plan import build_snapshot_work_plan

    config = _project().nodes["gold.dim_customer"].config
    plan = build_snapshot_work_plan(
        config.sources,
        {"silver.customers": 9, "silver.crm_status": 7},
    )

    assert {item.source_name: item.snapshot_version for item in plan.items} == {
        "silver.customers": 9,
        "silver.crm_status": 7,
    }
    assert all(
        item.active and item.delete_mode == "full_snapshot" for item in plan.items
    )


def test_snapshot_repair_rejects_mutating_transformation_sql():
    from types import SimpleNamespace

    config = SimpleNamespace(
        table_name="gold.dim_customer",
        transformation_sql="DELETE FROM gold.other_table",
        sources=[],
    )

    with pytest.raises(StructuredError, match="non-read-only"):
        _validate_declared_sql_inputs(config)


def test_orchestrator_tags_control_and_target_commits_for_entire_repair_run():
    from types import SimpleNamespace

    from kimball.orchestration.orchestrator import Orchestrator

    class Conf:
        def __init__(self):
            self.values = {"spark.databricks.delta.commitInfo.userMetadata": "outer"}

        def get(self, key):
            return self.values[key]

        def set(self, key, value):
            self.values[key] = value

        def unset(self, key):
            self.values.pop(key, None)

    conf = Conf()
    orchestrator = Orchestrator.__new__(Orchestrator)
    orchestrator.config = SimpleNamespace(
        scd_type=1,
        preserve_all_changes=False,
        history_table=None,
        sources=[],
        table_name="gold.dim_customer",
    )
    orchestrator.spark = SimpleNamespace(conf=conf)
    orchestrator._repair_snapshot_versions = None
    orchestrator._repair_id = None
    orchestrator._repair_operation_token = None
    orchestrator._apply_spark_configs = lambda: {}
    orchestrator._restore_spark_configs = lambda _: None
    orchestrator.transaction_manager = SimpleNamespace(
        invalidate_version=lambda _: None
    )
    observed = []

    def run_once():
        observed.append(conf.get("spark.databricks.delta.commitInfo.userMetadata"))
        return {"status": "SUCCESS"}

    orchestrator._run_pipeline_once = run_once
    result = orchestrator.run_repair(
        snapshot_versions={"silver.customers": 9},
        repair_id="repair-1",
        operation_token="operation-1",
    )

    assert result == {"status": "SUCCESS"}
    assert observed == ["repair_id=repair-1; operation_token=operation-1"]
    assert conf.get("spark.databricks.delta.commitInfo.userMetadata") == "outer"
    assert orchestrator._repair_snapshot_versions is None
