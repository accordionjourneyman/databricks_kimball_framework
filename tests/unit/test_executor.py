from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.common.config import SourceConfig, TableConfig, TargetConfig
from kimball.common.runtime import RuntimeOptions
from kimball.orchestration.executor import (
    ExecutionSummary,
    PipelineExecutor,
    PipelineResult,
)


def _dim_config(name="dim_customer", depends_on=None):
    return TableConfig(
        table_name=name,
        table_type="dimension",
        surrogate_key="dimension_sk",
        natural_keys=["id"],
        depends_on=depends_on or [],
        sources=[SourceConfig(name=f"silver.{name}", alias="src", primary_keys=["id"])],
        table_description=f"{name} fixture.",
    )


def _fact_config(name="fact_sales", depends_on=None):
    return TableConfig(
        table_name=name,
        table_type="fact",
        merge_keys=["id"],
        depends_on=depends_on or [],
        sources=[SourceConfig(name="silver.sales", alias="src", primary_keys=["id"])],
        table_description=f"{name} fixture.",
    )


@pytest.fixture
def mock_config_loader():
    with patch("kimball.orchestration.executor.ConfigLoader") as mock:
        instance = MagicMock()
        mock.return_value = instance
        yield instance


@pytest.fixture
def mock_get_etl_schema():
    with patch(
        "kimball.orchestration.executor.RuntimeOptions.from_environment",
        return_value=RuntimeOptions(etl_schema="test_schema"),
    ) as mock:
        yield mock


class TestPipelineExecutorInit:
    def test_raises_when_no_etl_schema(self, mock_config_loader, monkeypatch):
        monkeypatch.delenv("KIMBALL_ETL_SCHEMA", raising=False)
        with patch(
            "kimball.orchestration.executor.RuntimeOptions.from_environment",
            return_value=RuntimeOptions(),
        ):
            with pytest.raises(ValueError, match="ETL schema must be specified"):
                PipelineExecutor(config_paths=[])

    def test_uses_etl_schema_directly(self, mock_config_loader, mock_get_etl_schema):
        executor = PipelineExecutor(config_paths=[], etl_schema="direct_schema")
        assert executor.etl_schema == "direct_schema"


class TestCategorizePipelines:
    def test_categorizes_dimensions_and_facts(
        self, mock_config_loader, mock_get_etl_schema
    ):
        dim_config = _dim_config()
        fact_config = _fact_config()
        mock_config_loader.load_config.side_effect = [dim_config, fact_config]

        executor = PipelineExecutor(config_paths=["dim.yml", "fact.yml"])
        assert len(executor.dimensions) == 1
        assert len(executor.facts) == 1
        assert executor.dimensions[0]["table_name"] == "dim_customer"
        assert executor.facts[0]["table_name"] == "fact_sales"

    def test_raises_on_invalid_config(self, mock_config_loader, mock_get_etl_schema):
        mock_config_loader.load_config.side_effect = Exception("bad config")
        from kimball.common.errors import NonRetriableError

        with pytest.raises(NonRetriableError, match="Invalid config file"):
            PipelineExecutor(config_paths=["bad.yml"])


def test_executor_reuses_compiled_config_and_runtime_options(
    mock_config_loader, mock_get_etl_schema
):
    config = _dim_config()
    mock_config_loader.load_config.return_value = config
    orchestrator = MagicMock()
    runtime = MagicMock()

    with (
        patch(
            "kimball.orchestration.executor.RuntimeOptions.from_environment",
            return_value=RuntimeOptions(),
        ) as read_environment,
        patch(
            "kimball.orchestration.executor.PipelineRuntime.for_config",
            return_value=runtime,
        ) as build_runtime,
        patch(
            "kimball.orchestration.executor.Orchestrator",
            return_value=orchestrator,
        ) as build_orchestrator,
    ):
        executor = PipelineExecutor(config_paths=["dim.yml"], etl_schema="test")
        result = executor._create_orchestrator("dim.yml")
        executor._create_orchestrator("dim.yml")

    assert result is orchestrator
    mock_config_loader.load_config.assert_called_once_with("dim.yml")
    assert read_environment.call_count == 1
    assert build_runtime.call_count == 2
    assert all(call.args[0] is config for call in build_runtime.call_args_list)
    assert all(
        call.kwargs["runtime_options"] is executor.runtime_options
        for call in build_runtime.call_args_list
    )
    assert build_orchestrator.call_count == 2


class TestRunSinglePipeline:
    def test_successful_run(self, mock_config_loader, mock_get_etl_schema):
        executor = PipelineExecutor(config_paths=[], etl_schema="test")
        orchestrator = MagicMock()
        orchestrator.run.return_value = {
            "rows_read": 10,
            "rows_written": 5,
            "batch_id": "b1",
        }
        executor._create_orchestrator = MagicMock(return_value=orchestrator)
        result = executor._run_single_pipeline(
            {"path": "p.yml", "table_name": "t", "table_type": "dimension"}
        )
        assert result.status == "SUCCESS"
        assert result.rows_read == 10
        assert result.rows_written == 5
        assert result.batch_id == "b1"

    def test_failed_run(self, mock_config_loader, mock_get_etl_schema):
        executor = PipelineExecutor(config_paths=[], etl_schema="test")
        orchestrator = MagicMock()
        orchestrator.run.side_effect = ValueError("pipeline error")
        executor._create_orchestrator = MagicMock(return_value=orchestrator)
        result = executor._run_single_pipeline(
            {"path": "p.yml", "table_name": "t", "table_type": "dimension"}
        )
        assert result.status == "FAILED"
        assert "ValueError" in result.error_message


class TestRunSequential:
    def test_stops_on_failure(self, mock_config_loader, mock_get_etl_schema):
        executor = PipelineExecutor(
            config_paths=[], etl_schema="test", stop_on_failure=True
        )
        executor._run_single_pipeline = MagicMock(
            side_effect=[
                MagicMock(status="SUCCESS"),
                MagicMock(status="FAILED", table_name="bad"),
                MagicMock(status="SUCCESS"),
            ]
        )
        results = executor._run_sequential(
            [{"path": "a"}, {"path": "b"}, {"path": "c"}]
        )
        assert len(results) == 2


class TestRunParallel:
    def test_serializes_for_spark_safety(self, mock_config_loader, mock_get_etl_schema):
        executor = PipelineExecutor(
            config_paths=[], etl_schema="test", stop_on_failure=True
        )
        executor._run_sequential = MagicMock(return_value=[])
        results = executor._run_sequential([{"path": "a"}])
        assert results == []
        executor._run_sequential.assert_called_once()


class TestRun:
    def test_full_run(self, mock_config_loader, mock_get_etl_schema):
        dim_config = _dim_config()
        fact_config = _fact_config(depends_on=["dim_customer"])
        mock_config_loader.load_config.side_effect = [dim_config, fact_config]

        executor = PipelineExecutor(
            config_paths=["dim.yml", "fact.yml"], profile="production"
        )
        executor._run_single_pipeline = MagicMock(
            side_effect=[
                PipelineResult(
                    "dim.yml", "dim_customer", "dimension", "SUCCESS", 10, 5
                ),
                PipelineResult("fact.yml", "fact_sales", "fact", "SUCCESS", 20, 8),
            ]
        )
        summary = executor.run()
        assert summary.total_pipelines == 2
        assert summary.successful == 2
        assert summary.total_rows_read == 30
        assert summary.total_rows_written == 13

    def test_skips_facts_on_dim_failure(self, mock_config_loader, mock_get_etl_schema):
        dim_config = _dim_config()
        fact_config = _fact_config(depends_on=["dim_customer"])
        mock_config_loader.load_config.side_effect = [dim_config, fact_config]

        executor = PipelineExecutor(
            config_paths=["dim.yml", "fact.yml"], profile="production"
        )
        executor._run_single_pipeline = MagicMock(
            return_value=PipelineResult(
                "dim.yml", "dim_customer", "dimension", "FAILED"
            )
        )
        summary = executor.run()
        assert summary.skipped == 1
        assert summary.failed == 1


class TestExecutionSummary:
    def test_str_representation(self):
        summary = ExecutionSummary(
            total_pipelines=10,
            successful=7,
            failed=2,
            skipped=1,
            total_rows_read=1000,
            total_rows_written=500,
            total_duration_seconds=30.5,
        )
        s = str(summary)
        assert "Total Pipelines: 10" in s
        assert "Successful: 7" in s
        assert "Failed: 2" in s
        assert "Skipped: 1" in s


def test_executor_passes_target_context_and_resolved_options():
    config = _dim_config()
    target = TargetConfig(
        name="dev",
        catalog="workspace",
        silver_schema="dev_silver",
        gold_schema="dev_gold",
        etl_schema="target_ops",
        checkpoint_root="/target/checkpoints",
    )
    options = RuntimeOptions(
        etl_schema="environment_ops", checkpoint_root="/env/checkpoints"
    )

    explicit_context = {
        "application_schema": "app_gold",
        "target": {"gold_schema": "untrusted_override"},
    }
    expected_context = {
        **explicit_context,
        **target.template_context(),
    }
    with patch("kimball.orchestration.executor.ConfigLoader") as loader_class:
        loader_class.return_value.load_config.return_value = config
        executor = PipelineExecutor(
            ["dim.yml"],
            target=target,
            runtime_options=options,
            template_context=explicit_context,
            allow_implicit_environment=False,
        )
        loader_class.assert_called_once_with(
            template_context=expected_context,
            allow_implicit_environment=False,
        )

    assert executor.runtime_options.etl_schema == "target_ops"
    assert executor.runtime_options.checkpoint_root == "/target/checkpoints"
    with patch("kimball.orchestration.executor.PipelineRuntime.for_config") as build:
        build.return_value = MagicMock()
        executor._create_orchestrator("dim.yml")
    assert build.call_args.kwargs["runtime_options"] is executor.runtime_options
    assert build.call_args.kwargs["target"] is target
