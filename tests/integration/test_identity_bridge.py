"""Integration tests for Identity Bridge key resolution.

Uses a real local SparkSession with temp views to verify the orchestrator
can discover the bridge table via the Spark Catalog and execute the join.
"""

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from kimball.common.config import IdentityBridgeConfig


pytestmark = pytest.mark.skipif(
    not hasattr(SparkSession.builder, "getOrCreate"),
    reason="No local SparkSession available (PySpark 4.x requires Databricks Connect)",
)


@pytest.fixture
def bridge_view_name() -> str:
    return "test_identity_bridge"


@pytest.fixture
def bridge_data(spark: SparkSession, bridge_view_name: str):
    data = [("A", "C"), ("B", "C"), ("D", "E")]
    columns = ["business_key", "target_key"]
    spark.createDataFrame(data, columns).createOrReplaceTempView(bridge_view_name)
    yield
    try:
        spark.catalog.dropTempView(bridge_view_name)
    except Exception:
        pass


@pytest.fixture
def orchestrator_with_bridge(bridge_data, bridge_view_name):
    with (
        patch("kimball.orchestration.orchestrator.ConfigLoader") as mock_loader_cls,
        patch("kimball.orchestration.orchestrator.RuntimeOptions") as mock_runtime,
        patch("kimball.orchestration.orchestrator.ETLControlManager"),
        patch("kimball.orchestration.orchestrator.DataLoader"),
        patch("kimball.orchestration.orchestrator._merger"),
        patch("kimball.orchestration.orchestrator.SkeletonGenerator"),
        patch("kimball.orchestration.orchestrator.TableCreator"),
        patch("kimball.orchestration.orchestrator.TransactionManager"),
        patch("kimball.orchestration.orchestrator.QueryMetricsCollector"),
        patch("kimball.orchestration.orchestrator.PipelineCheckpoint"),
        patch("kimball.orchestration.orchestrator.StagingCleanupManager"),
        patch("kimball.orchestration.orchestrator._feature_enabled", return_value=False),
    ):
        mock_loader_cls.return_value.load_config.return_value = MagicMock()
        mock_runtime.from_environment.return_value = MagicMock(
            shuffle_partitions="auto", skew_threshold_mb=512, skew_factor=2.0,
        )
        from kimball.orchestration.orchestrator import Orchestrator

        orch = Orchestrator.__new__(Orchestrator)
        orch.config = MagicMock()
        orch.config.identity_bridge = IdentityBridgeConfig(
            table=bridge_view_name,
            join_on="business_key",
            target_column="target_key",
        )
        orch.config.foreign_keys = []
        orch.config.tests = None
        orch.config.table_type = "dimension"
        orch.config.natural_keys = None
        orch.config.transformation_sql = None
        orch.config.sources = []
        orch.etl_control = MagicMock()
        orch.loader = MagicMock()
        orch.skeleton_generator = MagicMock()
        orch.table_creator = MagicMock()
        orch.transaction_manager = MagicMock()
        orch.metrics_collector = None
        orch.checkpoint_manager = None
        orch.cleanup_manager = None
        orch._validator = MagicMock()
        yield orch


class TestIdentityBridgeIntegration:
    def test_resolves_merged_keys(self, spark: SparkSession, orchestrator_with_bridge):
        source_data = [("A", 100), ("B", 200)]
        source_df = spark.createDataFrame(source_data, ["business_key", "val"])

        result_df = orchestrator_with_bridge._apply_identity_bridge(source_df)

        rows = {r.business_key: r.val for r in result_df.collect()}
        assert rows.get("C") is not None, "A and B should both resolve to C"
        assert rows["C"] == 100 or rows["C"] == 200

    def test_preserves_unmapped_keys(self, spark: SparkSession, orchestrator_with_bridge):
        source_data = [("X", 300)]
        source_df = spark.createDataFrame(source_data, ["business_key", "val"])

        result_df = orchestrator_with_bridge._apply_identity_bridge(source_df)

        rows = result_df.collect()
        assert len(rows) == 1
        assert rows[0].business_key == "X"

    def test_handles_empty_bridge_table(self, spark: SparkSession, bridge_view_name):
        spark.createDataFrame([], schema="business_key string, target_key string").createOrReplaceTempView(bridge_view_name)
        with (
            patch("kimball.orchestration.orchestrator.ConfigLoader") as mock_loader_cls,
            patch("kimball.orchestration.orchestrator.RuntimeOptions") as mock_runtime,
            patch("kimball.orchestration.orchestrator.ETLControlManager"),
            patch("kimball.orchestration.orchestrator.DataLoader"),
            patch("kimball.orchestration.orchestrator._merger"),
            patch("kimball.orchestration.orchestrator.SkeletonGenerator"),
            patch("kimball.orchestration.orchestrator.TableCreator"),
            patch("kimball.orchestration.orchestrator.TransactionManager"),
            patch("kimball.orchestration.orchestrator.QueryMetricsCollector"),
            patch("kimball.orchestration.orchestrator.PipelineCheckpoint"),
            patch("kimball.orchestration.orchestrator.StagingCleanupManager"),
            patch("kimball.orchestration.orchestrator._feature_enabled", return_value=False),
        ):
            mock_loader_cls.return_value.load_config.return_value = MagicMock()
            mock_runtime.from_environment.return_value = MagicMock(
                shuffle_partitions="auto", skew_threshold_mb=512, skew_factor=2.0,
            )
            from kimball.orchestration.orchestrator import Orchestrator

            orch = Orchestrator.__new__(Orchestrator)
            orch.config = MagicMock()
            orch.config.identity_bridge = IdentityBridgeConfig(
                table=bridge_view_name,
                join_on="business_key",
                target_column="target_key",
            )
            orch.config.foreign_keys = []
            orch.config.tests = None
            orch.config.table_type = "dimension"
            orch.config.natural_keys = None
            orch.config.transformation_sql = None
            orch.config.sources = []
            orch.etl_control = MagicMock()
            orch.loader = MagicMock()
            orch.skeleton_generator = MagicMock()
            orch.table_creator = MagicMock()
            orch.transaction_manager = MagicMock()
            orch.metrics_collector = None
            orch.checkpoint_manager = None
            orch.cleanup_manager = None
            orch._validator = MagicMock()

            source_data = [("A", 100)]
            source_df = spark.createDataFrame(source_data, ["business_key", "val"])

            result_df = orch._apply_identity_bridge(source_df)
            rows = result_df.collect()
            assert len(rows) == 1
            assert rows[0].business_key == "A"
