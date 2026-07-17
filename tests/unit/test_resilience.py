from __future__ import annotations

from unittest.mock import MagicMock, patch

from kimball.observability.resilience import (
    PipelineCheckpoint,
    QueryMetricsCollector,
    StagingCleanupManager,
    StagingTableManager,
    _ensure_delta_table,
    _feature_enabled,
    _safe_drop_table,
)


class TestFeatureEnabled:
    def test_full_mode_returns_true(self):
        with patch.dict("os.environ", {"KIMBALL_MODE": "full"}):
            assert _feature_enabled("checkpoints") is True

    def test_specific_flag_returns_true(self):
        with patch.dict("os.environ", {"KIMBALL_ENABLE_CHECKPOINTS": "1"}):
            assert _feature_enabled("checkpoints") is True

    def test_not_enabled_returns_false(self):
        with patch.dict("os.environ", {}, clear=True):
            assert _feature_enabled("checkpoints") is False


class TestEnsureDeltaTable:
    def test_skips_when_table_exists(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        with patch("kimball.observability.resilience.get_spark", return_value=spark):
            _ensure_delta_table("t", MagicMock())
        spark.createDataFrame.assert_not_called()

    def test_creates_table_when_missing(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        schema = MagicMock()
        with patch("kimball.observability.resilience.get_spark", return_value=spark):
            _ensure_delta_table("t", schema)
        spark.createDataFrame.assert_called_once_with([], schema)
        spark.createDataFrame.return_value.write.format.assert_called_once_with("delta")

    def test_creates_with_partition(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        with patch("kimball.observability.resilience.get_spark", return_value=spark):
            _ensure_delta_table("t", MagicMock(), partition_by="p")
        spark.createDataFrame.return_value.write.format.return_value.partitionBy.assert_called_once_with(
            "p"
        )


class TestSafeDropTable:
    def test_skips_invalid_name(self):
        spark = MagicMock()
        result = _safe_drop_table(spark, "bad name!")
        assert result is False
        spark.sql.assert_not_called()

    def test_drops_valid_table(self):
        spark = MagicMock()
        result = _safe_drop_table(spark, "catalog.schema.table")
        assert result is True
        spark.sql.assert_called_once_with(
            "DROP TABLE IF EXISTS `catalog`.`schema`.`table`"
        )


class TestQueryMetricsCollector:
    def test_start_collection_initializes(self):
        c = QueryMetricsCollector()
        c.start_collection()
        assert c.start_time is not None
        assert c.metrics == []

    def test_add_operation_metric(self):
        c = QueryMetricsCollector()
        c.add_operation_metric("test_op", duration_ms=100)
        assert len(c.metrics) == 1
        assert c.metrics[0]["operation"] == "test_op"
        assert c.metrics[0]["duration_ms"] == 100

    def test_add_metric_with_dataframe(self):
        from pyspark.sql import DataFrame

        c = QueryMetricsCollector()
        df = MagicMock(spec=DataFrame)
        c.add_operation_metric("test_op", df=df)
        assert c.metrics[0]["has_logical_plan"] is True

    def test_stop_collection_adds_total(self):
        c = QueryMetricsCollector()
        c.start_collection()
        c.add_operation_metric("op1", duration_ms=50)
        result = c.stop_collection()
        assert len(result) == 2
        assert result[1]["operation"] == "total_pipeline"

    def test_get_summary_empty(self):
        c = QueryMetricsCollector()
        assert c.get_summary() == {}

    def test_get_summary_with_metrics(self):
        c = QueryMetricsCollector()
        c.add_operation_metric("op1", duration_ms=100)
        c.add_operation_metric("op2", duration_ms=200)
        summary = c.get_summary()
        assert summary["total_operations"] == 2
        assert summary["total_execution_time_ms"] == 300
        assert summary["avg_operation_time_ms"] == 150.0


class TestStagingCleanupManager:
    def test_init_uses_default_registry(self):
        with patch("kimball.observability.resilience._ensure_delta_table"):
            m = StagingCleanupManager()
        assert "kimball_staging_registry" in m.registry_table

    def test_init_uses_custom_registry(self):
        with patch("kimball.observability.resilience._ensure_delta_table"):
            m = StagingCleanupManager("custom.registry")
        assert m.registry_table == "custom.registry"

    def test_ensure_registry_table(self):
        with patch(
            "kimball.observability.resilience._ensure_delta_table"
        ) as mock_ensure:
            m = StagingCleanupManager.__new__(StagingCleanupManager)
            m.registry_table = "test.registry"
            m._ensure_registry_table()
        mock_ensure.assert_called_once()

    def test_register_staging_table(self):
        spark = MagicMock()
        delta_table = MagicMock()
        with patch("kimball.observability.resilience.get_spark", return_value=spark):
            with patch("delta.tables.DeltaTable.forName", return_value=delta_table):
                with patch("kimball.observability.resilience._ensure_delta_table"):
                    m = StagingCleanupManager("test.registry")
                    m.register_staging_table("staging_tbl", "p1", "b1")
        delta_table.alias.assert_called_once()

    def test_unregister_staging_table(self):
        delta_table = MagicMock()
        with patch("delta.tables.DeltaTable.forName", return_value=delta_table):
            with patch("kimball.observability.resilience._ensure_delta_table"):
                m = StagingCleanupManager("test.registry")
                m.unregister_staging_table("staging_tbl")
        delta_table.delete.assert_called_once()

    def test_cleanup_staging_tables_no_candidates(self):
        spark = MagicMock()
        spark.table.return_value.limit.return_value.collect.return_value = []
        with patch("kimball.observability.resilience._ensure_delta_table"):
            m = StagingCleanupManager("test.registry")
            cleaned, failed = m.cleanup_staging_tables(spark)
        assert cleaned == 0
        assert failed == 0

    def test_cleanup_staging_tables_with_candidates(self):
        spark = MagicMock()
        row = MagicMock()
        row.staging_table = "staging_tbl"
        spark.table.return_value.limit.return_value.collect.return_value = [row]
        with patch(
            "kimball.observability.resilience._safe_drop_table", return_value=True
        ):
            with patch("kimball.observability.resilience._ensure_delta_table"):
                m = StagingCleanupManager("test.registry")
                m.unregister_staging_table = MagicMock()
                cleaned, failed = m.cleanup_staging_tables(spark, max_age_hours=0)
        assert cleaned == 1
        assert failed == 0

    def test_cleanup_with_pipeline_filter(self):
        spark = MagicMock()
        filtered_df = MagicMock()
        filtered_df.limit.return_value.collect.return_value = []
        spark.table.return_value.filter.return_value = filtered_df
        with patch("kimball.observability.resilience._ensure_delta_table"):
            m = StagingCleanupManager("test.registry")
            m.cleanup_staging_tables(spark, pipeline_id="p1", max_age_hours=0)
        spark.table.return_value.filter.assert_called()

    def test_cleanup_with_max_age(self):
        spark = MagicMock()
        filtered_df = MagicMock()
        filtered_df.limit.return_value.collect.return_value = []
        spark.table.return_value.filter.return_value = filtered_df
        with patch("kimball.observability.resilience._ensure_delta_table"):
            m = StagingCleanupManager("test.registry")
            m.cleanup_staging_tables(spark, max_age_hours=48)
        spark.table.return_value.filter.assert_called()

    def test_cleanup_handles_drop_failure(self):
        spark = MagicMock()
        row = MagicMock()
        row.staging_table = "staging_tbl"
        spark.table.return_value.limit.return_value.collect.return_value = [row]
        with patch(
            "kimball.observability.resilience._safe_drop_table", return_value=False
        ):
            with patch("kimball.observability.resilience._ensure_delta_table"):
                m = StagingCleanupManager("test.registry")
                cleaned, failed = m.cleanup_staging_tables(spark, max_age_hours=0)
        assert cleaned == 0
        assert failed == 1

    def test_cleanup_handles_exception(self):
        spark = MagicMock()
        row = MagicMock()
        row.staging_table = "staging_tbl"
        spark.table.return_value.limit.return_value.collect.return_value = [row]
        with patch(
            "kimball.observability.resilience._safe_drop_table",
            side_effect=Exception("boom"),
        ):
            with patch("kimball.observability.resilience._ensure_delta_table"):
                m = StagingCleanupManager("test.registry")
                cleaned, failed = m.cleanup_staging_tables(spark, max_age_hours=0)
        assert cleaned == 0
        assert failed == 1


class TestStagingTableManager:
    def test_init_stores_cleanup_manager(self):
        cm = MagicMock()
        m = StagingTableManager(cm)
        assert m.cleanup_manager is cm
        assert m.staging_tables == []

    def test_register_staging_table(self):
        cm = MagicMock()
        m = StagingTableManager(cm)
        m.register_staging_table("staging_tbl", "p1", "b1")
        assert "staging_tbl" in m.staging_tables
        cm.register_staging_table.assert_called_once_with("staging_tbl", "p1", "b1")

    def test_context_manager(self):
        cm = MagicMock()
        with StagingTableManager(cm) as m:
            assert isinstance(m, StagingTableManager)

    def test_exit_cleans_up(self):
        cm = MagicMock()
        m = StagingTableManager(cm)
        m.staging_tables = ["staging_tbl"]
        with patch(
            "kimball.observability.resilience._safe_drop_table", return_value=True
        ):
            m.__exit__(None, None, None)
        cm.unregister_staging_table.assert_called_once_with("staging_tbl")

    def test_exit_handles_exception(self):
        cm = MagicMock()
        m = StagingTableManager(cm)
        m.staging_tables = ["staging_tbl"]
        with patch(
            "kimball.observability.resilience._safe_drop_table",
            side_effect=Exception("boom"),
        ):
            m.__exit__(None, None, None)
        cm.unregister_staging_table.assert_not_called()


class TestPipelineCheckpoint:
    def test_init_uses_default_table(self):
        with patch("kimball.observability.resilience._ensure_delta_table"):
            p = PipelineCheckpoint()
        assert "kimball_pipeline_checkpoints" in p.checkpoint_table

    def test_init_uses_custom_table(self):
        with patch("kimball.observability.resilience._ensure_delta_table"):
            p = PipelineCheckpoint("custom.checkpoints")
        assert p.checkpoint_table == "custom.checkpoints"

    def test_ensure_checkpoint_table(self):
        with patch(
            "kimball.observability.resilience._ensure_delta_table"
        ) as mock_ensure:
            p = PipelineCheckpoint.__new__(PipelineCheckpoint)
            p.checkpoint_table = "test.ckpt"
            p._ensure_checkpoint_table()
        mock_ensure.assert_called_once()

    def test_save_checkpoint(self):
        spark = MagicMock()
        delta_table = MagicMock()
        with patch("kimball.observability.resilience.get_spark", return_value=spark):
            with patch("delta.tables.DeltaTable.forName", return_value=delta_table):
                with patch("kimball.observability.resilience._ensure_delta_table"):
                    p = PipelineCheckpoint("test.ckpt")
                    p.save_checkpoint("p1", "stage1", {"key": "val"})
        delta_table.alias.assert_called_once()

    def test_load_checkpoint_found(self):
        spark = MagicMock()
        row = MagicMock()
        row.__getitem__.side_effect = lambda k: (
            '{"key": "val"}' if k == "state" else None
        )
        spark.table.return_value.filter.return_value.orderBy.return_value.limit.return_value.first.return_value = row
        with patch("kimball.observability.resilience.get_spark", return_value=spark):
            with patch("kimball.observability.resilience._ensure_delta_table"):
                p = PipelineCheckpoint("test.ckpt")
                state = p.load_checkpoint("p1", "stage1")
        assert state == {"key": "val"}

    def test_load_checkpoint_not_found(self):
        spark = MagicMock()
        spark.table.return_value.filter.return_value.orderBy.return_value.limit.return_value.first.return_value = None
        with patch("kimball.observability.resilience.get_spark", return_value=spark):
            with patch("kimball.observability.resilience._ensure_delta_table"):
                p = PipelineCheckpoint("test.ckpt")
                state = p.load_checkpoint("p1", "stage1")
        assert state is None

    def test_load_checkpoint_exception(self):
        spark = MagicMock()
        spark.table.return_value.filter.return_value.orderBy.return_value.limit.return_value.first.side_effect = Exception(
            "boom"
        )
        with patch("kimball.observability.resilience.get_spark", return_value=spark):
            with patch("kimball.observability.resilience._ensure_delta_table"):
                p = PipelineCheckpoint("test.ckpt")
                state = p.load_checkpoint("p1", "stage1")
        assert state is None

    def test_clear_checkpoint(self):
        delta_table = MagicMock()
        with patch("delta.tables.DeltaTable.forName", return_value=delta_table):
            with patch("kimball.observability.resilience._ensure_delta_table"):
                p = PipelineCheckpoint("test.ckpt")
                p.clear_checkpoint("p1", "stage1")
        delta_table.delete.assert_called_once()

    def test_list_checkpoints_all(self):
        spark = MagicMock()
        with patch("kimball.observability.resilience.get_spark", return_value=spark):
            with patch("kimball.observability.resilience._ensure_delta_table"):
                p = PipelineCheckpoint("test.ckpt")
                p.list_checkpoints()
        spark.table.assert_called_once_with("test.ckpt")

    def test_list_checkpoints_filtered(self):
        spark = MagicMock()
        with patch("kimball.observability.resilience.get_spark", return_value=spark):
            with patch("kimball.observability.resilience._ensure_delta_table"):
                p = PipelineCheckpoint("test.ckpt")
                p.list_checkpoints(pipeline_id="p1")
        spark.table.return_value.filter.assert_called_once()
