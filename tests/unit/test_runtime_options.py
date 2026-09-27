from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.common.config import SourceConfig, TableConfig, TargetConfig
from kimball.common.runtime import RuntimeOptions
from kimball.orchestration.runtime import PipelineRuntime


def test_removed_validation_skip_environment_variable_fails_closed(monkeypatch):
    monkeypatch.setenv("KIMBALL_SKIP_VALIDATION_IF_UNCHANGED", "1")

    with pytest.raises(ValueError, match="was removed"):
        RuntimeOptions.from_environment()


def test_invalid_runtime_values_name_the_environment_setting():
    with pytest.raises(
        ValueError, match=r"KIMBALL_MODE: Input should be 'lite' or 'full'"
    ):
        RuntimeOptions.from_environment({"KIMBALL_MODE": "ful"})

    with pytest.raises(ValueError, match="KIMBALL_SKEW_THRESHOLD_MB"):
        RuntimeOptions.from_environment({"KIMBALL_SKEW_THRESHOLD_MB": "0"})

    with pytest.raises(ValueError, match="KIMBALL_ENABLE_METRICS must be '0' or '1'"):
        RuntimeOptions.from_environment({"KIMBALL_ENABLE_METRICS": "true"})


def test_mode_defaults_and_explicit_flags_are_validated_once():
    full = RuntimeOptions.from_environment({"KIMBALL_MODE": "full"})
    overridden = RuntimeOptions.from_environment(
        {"KIMBALL_MODE": "full", "KIMBALL_ENABLE_METRICS": "0"}
    )

    assert full.enable_checkpoints is True
    assert full.enable_metrics is True
    assert overridden.enable_metrics is False
    assert overridden.enable_auto_cluster is True


def test_runtime_precedence_is_explicit_then_target_then_environment():
    environment = RuntimeOptions.from_environment(
        {
            "KIMBALL_ETL_SCHEMA": "env_ops",
            "KIMBALL_CHECKPOINT_ROOT": "/env/spark-checkpoints",
            "KIMBALL_STREAMING_CHECKPOINT_ROOT": "/env/stream-checkpoints",
        }
    )
    target = environment.resolved(
        target_etl_schema="target_ops",
        target_checkpoint_root="/target/checkpoints",
    )
    explicit = environment.resolved(
        etl_schema="cli_ops",
        target_etl_schema="target_ops",
        checkpoint_root="/cli/checkpoints",
        target_checkpoint_root="/target/checkpoints",
    )

    assert target.etl_schema == "target_ops"
    assert target.checkpoint_root == "/target/checkpoints"
    assert target.streaming_checkpoint_root == "/target/checkpoints"
    assert explicit.etl_schema == "cli_ops"
    assert explicit.checkpoint_root == "/cli/checkpoints"
    assert explicit.streaming_checkpoint_root == "/cli/checkpoints"


def test_streaming_environment_root_is_fallback_without_target_root():
    options = RuntimeOptions.from_environment(
        {
            "KIMBALL_CHECKPOINT_ROOT": "/env/spark-checkpoints",
            "KIMBALL_STREAMING_CHECKPOINT_ROOT": "/env/stream-checkpoints",
        }
    ).resolved()

    assert options.checkpoint_root == "/env/spark-checkpoints"
    assert options.streaming_checkpoint_root == "/env/stream-checkpoints"


def test_legacy_approximate_unique_flag_keeps_its_existing_parse_contract():
    assert (
        RuntimeOptions.from_environment(
            {"KIMBALL_USE_APPROXIMATE_UNIQUE": "true"}
        ).use_approximate_unique
        is False
    )
    assert (
        RuntimeOptions.from_environment(
            {"KIMBALL_USE_APPROXIMATE_UNIQUE": "1"}
        ).use_approximate_unique
        is True
    )


def test_pipeline_runtime_applies_target_etl_schema_and_checkpoint_root():
    target = TargetConfig(
        name="dev",
        catalog="workspace",
        silver_schema="dev_silver",
        gold_schema="dev_gold",
        etl_schema="target_ops",
        checkpoint_root="/target/checkpoints",
    )
    config = TableConfig(
        table_name="gold.dim_customer",
        table_type="dimension",
        surrogate_key="customer_sk",
        natural_keys=["customer_id"],
        sources=[SourceConfig(name="silver.customers", alias="customers")],
    )
    spark = MagicMock()
    environment = RuntimeOptions(
        etl_schema="environment_ops", checkpoint_root="/env/checkpoints"
    )

    with (
        patch("kimball.orchestration.runtime.ETLControlManager"),
        patch("kimball.orchestration.runtime.DataLoader"),
        patch("kimball.orchestration.runtime.TransactionManager"),
        patch("kimball.orchestration.runtime.TableCreator"),
    ):
        runtime = PipelineRuntime.for_config(
            config, spark=spark, runtime_options=environment, target=target
        )

    assert runtime.etl_schema == "target_ops"
    assert runtime.options.checkpoint_root == "/target/checkpoints"
    assert runtime.options.streaming_checkpoint_root == "/target/checkpoints"
    spark.sparkContext.setCheckpointDir.assert_called_once_with("/target/checkpoints")
