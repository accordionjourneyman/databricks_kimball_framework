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
def orchestrator(spark_mock, etl_control_mock, transaction_manager_mock, loader_mock, merger_mock, config_mock):
    with (
        patch("kimball.orchestration.orchestrator.ConfigLoader") as mock_loader_cls,
        patch("kimball.orchestration.orchestrator.RuntimeOptions") as mock_runtime,
        patch("kimball.orchestration.orchestrator.ETLControlManager", return_value=etl_control_mock),
        patch("kimball.orchestration.orchestrator.DataLoader", return_value=loader_mock),
        patch("kimball.orchestration.orchestrator._merger"),
        patch("kimball.orchestration.orchestrator.SkeletonGenerator"),
        patch("kimball.orchestration.orchestrator.TableCreator"),
        patch("kimball.orchestration.orchestrator.TransactionManager", return_value=transaction_manager_mock),
        patch("kimball.orchestration.orchestrator.QueryMetricsCollector"),
        patch("kimball.orchestration.orchestrator.PipelineCheckpoint"),
        patch("kimball.orchestration.orchestrator.StagingCleanupManager"),
        patch("kimball.orchestration.orchestrator._feature_enabled", return_value=False),
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
        orch.skeleton_generator = MagicMock()
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

        orchestrator.etl_control.get_watermark.return_value = None
        orchestrator.loader.get_latest_version.return_value = 10
        mock_df = MagicMock()
        orchestrator.loader.load_cdf.return_value = mock_df

        source_versions, active_dfs = orchestrator._load_active_sources("batch-1")

        orchestrator.loader.load_cdf.assert_called_once_with(
            "src1", starting_version=0, deduplicate_keys=["id"], ending_version=10
        )
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

        orchestrator.etl_control.get_watermark.return_value = 10
        orchestrator.loader.get_latest_version.return_value = 10

        source_versions, active_dfs = orchestrator._load_active_sources("batch-1")

        orchestrator.loader.load_cdf.assert_not_called()
        assert active_dfs == {}
        orchestrator.etl_control.batch_complete.assert_called_once_with(
            "test_table", "src1", new_version=10, rows_read=0, rows_written=0
        )

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


class TestGenerateSkeletons:
    def test_generate_skeletons_skips_when_no_eaf(self, orchestrator):
        orchestrator.config.early_arriving_facts = []
        mock_df = MagicMock()
        active_dfs = {"src1": mock_df}

        orchestrator._generate_skeletons(active_dfs, "batch-1")
        orchestrator.skeleton_generator.generate_skeletons.assert_not_called()

    def test_generate_skeletons_calls_generator(self, orchestrator):
        eaf = {
            "fact_join_key": "fk_customer",
            "dimension_table": "dim_customer",
            "dimension_join_key": "customer_sk",
            "surrogate_key_col": "customer_sk",
            "surrogate_key_strategy": "identity",
        }
        orchestrator.config.early_arriving_facts = [eaf]
        mock_df = MagicMock()
        mock_df.columns = ["fk_customer", "val"]
        active_dfs = {"src1": mock_df}

        orchestrator._generate_skeletons(active_dfs, "batch-1")
        orchestrator.skeleton_generator.generate_skeletons.assert_called_once_with(
            fact_df=mock_df,
            dim_table_name="dim_customer",
            fact_join_key="fk_customer",
            dim_join_key="customer_sk",
            surrogate_key_col="customer_sk",
            surrogate_key_strategy="identity",
            batch_id="batch-1",
        )


class TestIdentityBridge:
    def test_bridge_resolves_keys(self, orchestrator):
        bridge = MagicMock()
        bridge.table = "dim_identity_map"
        bridge.join_on = "business_key"
        bridge.target_column = "resolved_key"
        orchestrator.config.identity_bridge = bridge

        mock_df = MagicMock()
        mock_df.columns = ["business_key", "val"]

        mock_spark = MagicMock()
        mock_bridge_df = MagicMock()
        mock_bridge_df.columns = ["business_key", "resolved_key", "extra_col"]
        mock_spark.table.return_value = mock_bridge_df
        mock_spark.sql.return_value = MagicMock()
        orchestrator.spark = mock_spark

        result = orchestrator._apply_identity_bridge(mock_df)

        mock_df.createOrReplaceTempView.assert_called_once_with("_identity_bridge_src")
        mock_spark.sql.assert_called_once()
        assert result is mock_spark.sql.return_value

    def test_bridge_preserves_unmapped_keys(self, orchestrator):
        bridge = MagicMock()
        bridge.table = "dim_identity_map"
        bridge.join_on = "business_key"
        bridge.target_column = "resolved_key"
        orchestrator.config.identity_bridge = bridge

        mock_df = MagicMock()
        mock_df.columns = ["business_key", "val"]

        mock_spark = MagicMock()
        mock_bridge_df = MagicMock()
        mock_bridge_df.columns = ["business_key", "resolved_key"]
        mock_spark.table.return_value = mock_bridge_df
        mock_spark.sql.return_value = MagicMock()
        orchestrator.spark = mock_spark

        result = orchestrator._apply_identity_bridge(mock_df)

        mock_df.createOrReplaceTempView.assert_called_once()
        mock_spark.sql.assert_called_once()
        assert result is mock_spark.sql.return_value

    def test_bridge_skipped_when_not_configured(self, orchestrator):
        orchestrator.config.identity_bridge = None
        mock_df = MagicMock()
        result = orchestrator._apply_identity_bridge(mock_df)
        assert result is mock_df
        mock_df.createOrReplaceTempView.assert_not_called()
