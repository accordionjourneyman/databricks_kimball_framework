"""Unit tests for Orchestrator refactored methods."""

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark_mock():
    mock = MagicMock(spec=SparkSession)
    mock.catalog.tableExists.return_value = False
    return mock


@pytest.fixture
def etl_control_mock():
    return MagicMock()


@pytest.fixture
def transaction_manager_mock():
    return MagicMock()


@pytest.fixture
def loader_mock():
    return MagicMock()


@pytest.fixture
def merger_mock():
    return MagicMock()


@pytest.fixture
def config_mock():
    mock = MagicMock()
    mock.preserve_all_changes = False
    mock.scd_type = 2
    mock.sources = []
    mock.table_name = "test_table"
    return mock


@pytest.fixture
def orchestrator(
    spark_mock,
    etl_control_mock,
    transaction_manager_mock,
    loader_mock,
    merger_mock,
    config_mock,
):
    with (
        patch("kimball.orchestration.orchestrator.ConfigLoader") as mock_loader_cls,
        patch("kimball.orchestration.orchestrator.RuntimeOptions") as mock_runtime,
        patch(
            "kimball.orchestration.orchestrator.ETLControlManager",
            return_value=etl_control_mock,
        ),
        patch(
            "kimball.orchestration.orchestrator.DataLoader", return_value=loader_mock
        ),
        patch("kimball.orchestration.orchestrator._merger"),
        patch("kimball.orchestration.orchestrator.TableCreator"),
        patch(
            "kimball.orchestration.orchestrator.TransactionManager",
            return_value=transaction_manager_mock,
        ),
        patch("kimball.orchestration.orchestrator.QueryMetricsCollector"),
        patch("kimball.orchestration.orchestrator.PipelineCheckpoint"),
        patch("kimball.orchestration.orchestrator.StagingCleanupManager"),
        patch(
            "kimball.orchestration.orchestrator._feature_enabled", return_value=False
        ),
    ):
        mock_loader_cls.return_value.load_config.return_value = config_mock
        mock_runtime.from_environment.return_value = MagicMock(
            shuffle_partitions="auto",
            skew_threshold_mb=512,
            skew_factor=2.0,
        )
        from kimball.orchestration.orchestrator import Orchestrator

        orch = Orchestrator.__new__(Orchestrator)
        orch.config = config_mock
        orch.spark = spark_mock
        orch.etl_control = etl_control_mock
        orch.loader = loader_mock
        orch.table_creator = MagicMock()
        orch.transaction_manager = transaction_manager_mock
        orch.metrics_collector = None
        orch.checkpoint_manager = None
        orch.cleanup_manager = None
        return orch


class TestRecoverZombies:
    def test_recover_zombies_disabled(self, orchestrator):
        orchestrator.config.enable_crash_recovery = False
        result = orchestrator._recover_zombies()
        assert result is True
        orchestrator.etl_control.get_running_batches.assert_not_called()

    def test_recover_zombies_no_commit_tagging(self, orchestrator):
        orchestrator.config.enable_crash_recovery = True
        mock_spark = MagicMock()
        mock_spark.conf.set.side_effect = Exception("blocked")
        orchestrator.spark = mock_spark

        result = orchestrator._recover_zombies()
        assert result is False
        orchestrator.etl_control.get_running_batches.assert_not_called()

    def test_recover_zombies_with_running_batches(self, orchestrator):
        orchestrator.config.enable_crash_recovery = True
        orchestrator.etl_control.get_running_batches.return_value = [
            {"batch_id": "b1", "source_table": "src_a"},
        ]
        orchestrator.transaction_manager.recover_zombies.return_value = None

        result = orchestrator._recover_zombies()
        assert result is True
        orchestrator.transaction_manager.recover_zombies.assert_called_once_with(
            "test_table", "b1"
        )
        orchestrator.etl_control.batch_fail.assert_called_once()


def test_run_scopes_and_restores_spark_configuration(orchestrator):
    orchestrator.config.preserve_all_changes = False
    orchestrator.config.scd_type = 1
    orchestrator._apply_spark_configs = MagicMock(return_value={"spark.test": "before"})
    orchestrator._restore_spark_configs = MagicMock()
    orchestrator._run_pipeline_once = MagicMock(return_value={"rows_written": 1})

    result = orchestrator.run()

    assert result == {"rows_written": 1}
    orchestrator._apply_spark_configs.assert_called_once_with()
    orchestrator._restore_spark_configs.assert_called_once_with(
        {"spark.test": "before"}
    )


class TestLoadActiveSources:
    def test_load_cdf_full_when_no_watermark(self, orchestrator):
        source = MagicMock()
        source.format = "delta"
        source.cdc_strategy = "cdf"
        source.name = "src1"
        source.starting_version = 0
        source.primary_keys = ["id"]
        source.alias = "src1_view"
        orchestrator.config.sources = [source]

        orchestrator.etl_control.get_states.return_value = {}
        orchestrator.loader.get_latest_version.return_value = 10
        mock_df = MagicMock()
        orchestrator.loader.load_cdf.return_value = mock_df
        orchestrator.loader.deduplicate_cdf.return_value = mock_df

        source_versions, active_dfs = orchestrator._load_active_sources("batch-1")

        orchestrator.loader.load_cdf.assert_called_once_with(
            "src1", starting_version=0, deduplicate_keys=None, ending_version=10
        )
        orchestrator.loader.deduplicate_cdf.assert_called_once_with(mock_df, ["id"])
        assert source_versions == {"src1": 10}
        assert active_dfs == {"src1": mock_df}

    def test_skip_source_already_current(self, orchestrator):
        source = MagicMock()
        source.format = "delta"
        source.cdc_strategy = "cdf"
        source.name = "src1"
        source.starting_version = 0
        source.primary_keys = ["id"]
        source.alias = "src1_view"
        orchestrator.config.sources = [source]

        orchestrator.etl_control.get_states.return_value = {
            "src1": {"last_processed_version": 10}
        }
        orchestrator.loader.get_latest_version.return_value = 10

        source_versions, active_dfs = orchestrator._load_active_sources("batch-1")

        orchestrator.loader.load_cdf.assert_not_called()
        assert active_dfs == {}
        orchestrator.etl_control.batch_complete.assert_not_called()

    def test_load_full_snapshot_source(self, orchestrator):
        source = MagicMock()
        source.format = "delta"
        source.cdc_strategy = "full"
        source.name = "src1"
        source.options = {}
        source.alias = "src1_view"
        orchestrator.config.sources = [source]

        mock_df = MagicMock()
        orchestrator.loader.load_full_snapshot.return_value = mock_df

        source_versions, active_dfs = orchestrator._load_active_sources("batch-1")

        orchestrator.loader.load_full_snapshot.assert_called_once_with(
            "src1", format="delta", options={}
        )
        assert active_dfs == {"src1": mock_df}


class TestTransformAndValidate:
    def test_unsafe_transformation_sql_raises(self, orchestrator):
        orchestrator.config.transformation_sql = "DROP TABLE foo"
        orchestrator.config.sources = []
        orchestrator.config.foreign_keys = []
        orchestrator.config.tests = None
        orchestrator.config.table_type = "dimension"
        orchestrator.config.natural_keys = None

        active_dfs = {}
        with pytest.raises(ValueError, match="SELECT or WITH"):
            orchestrator._transform_and_validate(active_dfs)

    def test_multi_source_without_transformation_raises(self, orchestrator):
        orchestrator.config.transformation_sql = None
        src1 = MagicMock()
        src1.name = "src1"
        src2 = MagicMock()
        src2.name = "src2"
        orchestrator.config.sources = [src1, src2]
        orchestrator.config.foreign_keys = []
        orchestrator.config.tests = None
        orchestrator.config.table_type = "dimension"
        orchestrator.config.natural_keys = None

        active_dfs = {"src1": MagicMock(), "src2": MagicMock()}
        with pytest.raises(ValueError, match="multi-source pipelines"):
            orchestrator._transform_and_validate(active_dfs)


class TestFullReload:
    """Orchestrator.run(full_reload=True) drops the target, resets watermarks,
    and runs a full snapshot."""

    def test_full_reload_drops_target_and_resets_watermarks(self, orchestrator):
        orchestrator.spark.catalog.tableExists.return_value = True
        source_a = MagicMock()
        source_a.name = "src_a"
        source_b = MagicMock()
        source_b.name = "src_b"
        orchestrator.config.sources = [source_a, source_b]
        orchestrator.config.scd_type = 1
        orchestrator.config.history_table = None

        with patch.object(
            orchestrator, "_run_pipeline_once", return_value={"status": "SUCCESS"}
        ) as mock_run:
            result = orchestrator._run_full_reload()

        assert result["status"] == "SUCCESS"

        # Target table was dropped
        drop_calls = [
            c[0][0]
            for c in orchestrator.spark.sql.call_args_list
            if "DROP TABLE" in c[0][0]
        ]
        assert any("test_table" in c for c in drop_calls)

        # Watermarks were reset for both sources
        assert orchestrator.etl_control.reset_watermark.call_count == 2
        orchestrator.etl_control.reset_watermark.assert_any_call("test_table", "src_a")
        orchestrator.etl_control.reset_watermark.assert_any_call("test_table", "src_b")

        # _run_pipeline_once was called
        mock_run.assert_called_once()

    def test_full_reload_skips_drop_when_table_missing(self, orchestrator):
        orchestrator.spark.catalog.tableExists.return_value = False
        orchestrator.config.sources = []
        orchestrator.config.scd_type = 1
        orchestrator.config.history_table = None

        with patch.object(
            orchestrator, "_run_pipeline_once", return_value={"status": "SUCCESS"}
        ):
            result = orchestrator._run_full_reload()

        assert result["status"] == "SUCCESS"
        # No DROP TABLE calls because the table didn't exist
        drop_calls = [
            c for c in orchestrator.spark.sql.call_args_list if "DROP TABLE" in str(c)
        ]
        assert len(drop_calls) == 0

    def test_run_dispatches_to_full_reload(self, orchestrator):
        with patch.object(
            orchestrator, "_run_full_reload", return_value={"status": "SUCCESS"}
        ) as mock_reload:
            result = orchestrator.run(full_reload=True)

        assert result["status"] == "SUCCESS"
        mock_reload.assert_called_once()


class TestGrainValidation:
    """grain_validation config controls whether the orchestrator checks
    for duplicate natural keys before merging."""

    def test_config_accepts_grain_validation_values(self):
        from kimball.common.config import SourceConfig, TableConfig

        for mode in ("error", "warn", "skip"):
            cfg = TableConfig(
                table_name="test.dim",
                table_type="dimension",
                scd_type=1,
                keys={"surrogate_key": "sk", "natural_keys": ["id"]},
                sources=[SourceConfig(name="src", alias="s")],
                grain_validation=mode,
            )
            assert cfg.grain_validation == mode

    def test_config_defaults_to_error(self):
        from kimball.common.config import SourceConfig, TableConfig

        cfg = TableConfig(
            table_name="test.dim",
            table_type="dimension",
            scd_type=1,
            keys={"surrogate_key": "sk", "natural_keys": ["id"]},
            sources=[SourceConfig(name="src", alias="s")],
        )
        assert cfg.grain_validation == "error"
