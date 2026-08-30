from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from kimball.cli import main
from kimball.common.config import SourceConfig, TableConfig, TargetConfig
from kimball.contracts.odcs import ODCSContractLoader
from kimball.planning.compiler import ProjectCompiler
from kimball.planning.manifest import build_manifest
from tests.unit.test_odcs_contracts import _contract


def _project():
    config = TableConfig(
        table_name="gold.dim_customer",
        table_type="dimension",
        surrogate_key="customer_sk",
        natural_keys=["customer_id"],
        sources=[
            SourceConfig(
                name="silver.customers", alias="src", primary_keys=["customer_id"]
            )
        ],
        table_description="Customer dimension fixture.",
    )
    return ProjectCompiler(profile="production").compile([("dim.yml", config)])


def _target():
    return TargetConfig(
        name="prod",
        catalog="workspace",
        silver_schema="prod_silver",
        gold_schema="prod_gold",
        etl_schema="prod_ops",
    )


def test_validate_command_compiles_project(capsys):
    project = _project()
    with (
        patch("kimball.cli.load_compiled_project", return_value=project) as load,
        patch("kimball.cli.load_target", return_value=_target()),
    ):
        result = main(["validate", "--config", "configs", "--target", "prod"])

    assert result == 0
    load.assert_called_once_with(["configs"], _target())
    assert "Validated 1 pipelines" in capsys.readouterr().out


def test_compile_command_emits_manifest_json(capsys):
    project = _project()
    with (
        patch("kimball.cli.load_compiled_project", return_value=project),
        patch("kimball.cli.load_target", return_value=_target()),
    ):
        result = main(["compile", "--config", "dim.yml", "--target", "prod"])

    output = json.loads(capsys.readouterr().out)
    assert result == 0
    assert output["pipelines"][0]["table_name"] == "gold.dim_customer"


def test_plan_command_returns_nonzero_for_breaking_change(capsys):
    previous = build_manifest(_project(), framework_version="1")
    with (
        patch("kimball.cli.load_compiled_project") as load,
        patch("kimball.cli.Path.read_text", return_value=json.dumps(previous)),
        patch("kimball.cli.load_target", return_value=_target()),
    ):
        load.return_value = ProjectCompiler(profile="production").compile([])
        result = main(
            [
                "plan",
                "--config",
                "configs",
                "--against",
                "manifest.json",
                "--target",
                "prod",
                "--fail-on-breaking",
            ]
        )

    assert result == 2
    assert '"classification": "breaking"' in capsys.readouterr().out


def test_run_command_invokes_one_pipeline(capsys):
    orchestrator = MagicMock()
    orchestrator.run.return_value = {"status": "SUCCESS", "rows_written": 3}
    with (
        patch(
            "kimball.orchestration.orchestrator.Orchestrator", return_value=orchestrator
        ),
        patch("kimball.cli.load_target", return_value=_target()),
        patch(
            "kimball.cli.ConfigLoader.load_config",
            return_value=_project().nodes["gold.dim_customer"].config,
        ),
    ):
        result = main(
            [
                "run",
                "--config",
                "dim.yml",
                "--target",
                "prod",
            ]
        )

    assert result == 0
    orchestrator.run.assert_called_once_with()
    assert '"rows_written": 3' in capsys.readouterr().out


def test_contract_validate_command_uses_pinned_odcs_schema(capsys):
    contract = ODCSContractLoader().load_mapping(_contract())
    with patch(
        "kimball.cli.ODCSContractLoader.load_file", return_value=contract
    ) as load:
        result = main(
            ["contract", "validate", "--contract", "contracts/customer/1.0.0.odcs.yaml"]
        )

    assert result == 0
    load.assert_called_once()
    assert "customer-contract 1.0.0" in capsys.readouterr().out


def test_contract_check_fails_invalid_versioning(capsys):
    previous = ODCSContractLoader().load_mapping(_contract())
    changed = _contract()
    changed["schema"][0]["properties"].pop()
    current = ODCSContractLoader().load_mapping(changed)
    with patch(
        "kimball.cli.ODCSContractLoader.load_file",
        side_effect=[previous, current],
    ):
        result = main(
            [
                "contract",
                "check",
                "--previous",
                "old.odcs.yaml",
                "--current",
                "new.odcs.yaml",
            ]
        )

    assert result == 2
    assert '"allowed": false' in capsys.readouterr().out


def test_contract_publish_records_deployed_version(capsys):
    contract = ODCSContractLoader().load_mapping(_contract())
    registry = MagicMock()
    registry.publish_contract.return_value = True
    with (
        patch("kimball.cli.ODCSContractLoader.load_file", return_value=contract),
        patch(
            "kimball.contracts.registry.DeltaContractRegistry", return_value=registry
        ),
        patch("kimball.common.spark_session.get_spark", return_value=MagicMock()),
    ):
        result = main(
            [
                "contract",
                "publish",
                "--contract",
                "customer.odcs.yaml",
                "--etl-schema",
                "ops",
                "--published-by",
                "ci",
            ]
        )

    assert result == 0
    registry.publish_contract.assert_called_once()
    assert "Published customer-contract 1.0.0" in capsys.readouterr().out


def test_cli_config_error_renders_structured_fix_hint(capsys):
    with patch("kimball.cli.load_target", return_value=_target()):
        result = main(["validate", "--config", "nonexistent/*.yml", "--target", "prod"])
    assert result == 1
    err = capsys.readouterr().err
    assert "CONFIG" in err
    assert "Fix:" in err


def test_deploy_without_against_uses_empty_manifest(capsys):
    project = _project()
    fake_result = MagicMock()
    fake_result.to_dict.return_value = {"blocked": False}
    fake_result.blocked = False
    with (
        patch("kimball.cli.load_target", return_value=_target()),
        patch("kimball.cli.load_compiled_project", return_value=project),
        patch("kimball.cli.build_manifest", return_value={"pipelines": []}),
        patch("kimball.common.spark_session.get_spark", return_value=MagicMock()),
        patch(
            "kimball.ops.runtime_profile.detect_runtime_profile",
            return_value=MagicMock(),
        ),
        patch("kimball.ops.spark_adapters.build_providers", return_value=MagicMock()),
        patch("kimball.ops.deploy.deploy", return_value=fake_result) as dfn,
    ):
        result = main(["deploy", "--config", "configs", "--target", "prod"])
    assert result == 0
    dfn.assert_called_once()
    # First positional arg is the previous manifest; omitted --against -> empty.
    assert dfn.call_args.args[0] == {"pipelines": []}


def test_inspect_limit_truncates_batch_list(capsys):
    report = {
        "target_table": "gold.t",
        "runtime": {"flavor": "classic", "supports_commit_tagging": True},
        "control_table_exists": True,
        "reconciliation": {
            "verdict": "consistent",
            "watermark_version": 1,
            "target_version": 1,
            "zombie_batches": 0,
            "zombie_commits": 0,
            "evidence": "",
            "remediation": "",
            "runbook_link": None,
        },
        "writer_contract": {"verdict": "clean", "suspicious_commits": 0},
        "source_health": [],
        "batches": [
            {
                "status": "SUCCESS",
                "source_table": "s",
                "batch_id": str(i),
                "last_processed_version": i,
            }
            for i in range(12)
        ],
    }
    with (
        patch(
            "kimball.cli._ops_runtime_and_providers",
            return_value=(MagicMock(), MagicMock()),
        ),
        patch("kimball.ops.inspect.inspect_target", return_value=report),
    ):
        result = main(
            ["inspect", "--target", "prod", "--table", "gold.t", "--limit", "5"]
        )
    out = json.loads(capsys.readouterr().out)
    assert result == 0
    assert len(out["batches"]) == 5
