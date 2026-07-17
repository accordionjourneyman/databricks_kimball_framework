from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from kimball.common.config import SourceConfig, TableConfig
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
        sources=[SourceConfig(name=f"silver.{name}", alias="src")],
    )


def _fact_config(name="fact_sales", depends_on=None):
    return TableConfig(
        table_name=name,
        table_type="fact",
        merge_keys=["id"],
        depends_on=depends_on or [],
        sources=[SourceConfig(name="silver.sales", alias="src")],
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
        "kimball.orchestration.executor.get_etl_schema", return_value="test_schema"
    ) as mock:
        yield mock


class TestPipelineExecutorInit:
    def test_watermark_deprecation_warning(
        self, mock_config_loader, mock_get_etl_schema
    ):
        with pytest.warns(DeprecationWarning, match="watermark_database"):
            PipelineExecutor(
                config_paths=[],
                watermark_database="old_schema",
            )

    def test_watermark_deprecation_sets_etl_schema(
        self, mock_config_loader, mock_get_etl_schema
    ):
        with pytest.warns(DeprecationWarning):
            executor = PipelineExecutor(
                config_paths=[],
                watermark_database="old_schema",
            )
        assert executor.etl_schema == "old_schema"

    def test_raises_when_no_etl_schema(self, mock_config_loader):
        with patch("kimball.orchestration.executor.get_etl_schema", return_value=None):
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


class TestRunWave:
    def test_empty_wave_returns_empty(self, mock_config_loader, mock_get_etl_schema):
        executor = PipelineExecutor(config_paths=[], etl_schema="test")
        result = executor._run_wave("Test", [])
        assert result == []

    def test_sequential_when_max_workers_1(
        self, mock_config_loader, mock_get_etl_schema
    ):
        executor = PipelineExecutor(config_paths=[], etl_schema="test", max_workers=1)
        executor._run_sequential = MagicMock(return_value=[])
        executor._run_wave("Test", [MagicMock()])
        executor._run_sequential.assert_called_once()

    def test_parallel_when_max_workers_gt_1(
        self, mock_config_loader, mock_get_etl_schema
    ):
        executor = PipelineExecutor(config_paths=[], etl_schema="test", max_workers=2)
        executor._run_sequential = MagicMock(return_value=[])
        executor._run_wave("Test", [MagicMock()])
        executor._run_sequential.assert_called_once()


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
        with pytest.warns(RuntimeWarning, match="share a Spark session"):
            results = executor._run_parallel([{"path": "a"}])
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


class TestDryRun:
    def test_dry_run_logs(self, mock_config_loader, mock_get_etl_schema):
        dim_config = _dim_config()
        mock_config_loader.load_config.return_value = dim_config

        executor = PipelineExecutor(config_paths=["dim.yml"])
        with patch.object(
            logging.getLogger("kimball.orchestration.executor"), "info"
        ) as mock_log:
            executor.dry_run()
            assert mock_log.called


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
