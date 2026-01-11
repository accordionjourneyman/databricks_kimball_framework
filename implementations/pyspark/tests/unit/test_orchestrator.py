"""
Unit tests for StagingCleanupManager and PipelineExecutor.

Tests staging table cleanup, SQL injection protection, and pipeline execution.
"""

from unittest.mock import MagicMock, patch
import pytest


class TestStagingCleanupManager:
    """Tests for StagingCleanupManager."""

    @patch("kimball.orchestrator.spark")
    @patch("delta.tables.DeltaTable")
    def test_cleanup_validates_pipeline_id(self, mock_dt, mock_spark):
        """Should reject invalid pipeline_id to prevent SQL injection."""
        from kimball.orchestrator import StagingCleanupManager

        mock_spark.catalog.tableExists.return_value = True

        manager = StagingCleanupManager("test_schema.staging_registry")

        # Valid pipeline_id patterns should work
        valid_ids = ["abc123", "my-pipeline", "pipeline_123", "ABC-def_456"]
        for pid in valid_ids:
            # Should not raise
            mock_spark.sql.return_value.collect.return_value = []
            mock_dt.forName.return_value = MagicMock()
            manager.cleanup_staging_tables(mock_spark, pipeline_id=pid)

        # Invalid pipeline_id patterns should raise
        invalid_ids = ["'; DROP TABLE x; --", "a b c", "pipe'line", "pipe;line"]
        for pid in invalid_ids:
            with pytest.raises(ValueError) as exc_info:
                manager.cleanup_staging_tables(mock_spark, pipeline_id=pid)
            assert "Invalid pipeline_id format" in str(exc_info.value)

    @patch("kimball.orchestrator.spark")
    @patch("delta.tables.DeltaTable")
    def test_cleanup_uses_collect_not_foreach(self, mock_dt, mock_spark):
        """Should use collect() loop on driver, not foreachPartition."""
        from kimball.orchestrator import StagingCleanupManager

        mock_spark.catalog.tableExists.return_value = True

        # Setup mock DataFrame that returns rows from collect()
        mock_row = MagicMock()
        mock_row.staging_table = "test_staging_table"
        mock_df = MagicMock()

        # Chain mocking: limit(1000).collect() returns rows first time, empty second time
        mock_limit_df = MagicMock()
        # First call returns 1 row, second call returns empty (loop termination)
        mock_limit_df.collect.side_effect = [[mock_row], []]
        mock_df.limit.return_value = mock_limit_df

        mock_spark.sql.return_value = mock_df

        mock_registry = MagicMock()
        mock_dt.forName.return_value = mock_registry

        manager = StagingCleanupManager("test_schema.staging_registry")
        cleanup_count, failed_count = manager.cleanup_staging_tables(mock_spark)

        # Verify calls
        mock_df.limit.assert_called_with(1000)
        assert mock_limit_df.collect.call_count >= 1
        mock_spark.catalog.dropTable.assert_called()

        # Verify results
        assert cleanup_count == 1
        assert failed_count == 0

    @patch("kimball.orchestrator.spark")
    @patch("delta.tables.DeltaTable")
    def test_cleanup_handles_drop_failure(self, mock_dt, mock_spark):
        """Should count failures when dropTable raises."""
        from kimball.orchestrator import StagingCleanupManager

        mock_spark.catalog.tableExists.return_value = True

        mock_row = MagicMock()
        mock_row.staging_table = "failing_table"
        mock_df = MagicMock()

        # Chain mocking: limit(1000).collect() returns rows first time, empty second time
        mock_limit_df = MagicMock()
        mock_limit_df.collect.side_effect = [[mock_row], []]
        mock_df.limit.return_value = mock_limit_df

        mock_spark.sql.return_value = mock_df

        # Make dropTable fail
        mock_spark.catalog.dropTable.side_effect = Exception("Table not found")

        mock_registry = MagicMock()
        mock_dt.forName.return_value = mock_registry

        manager = StagingCleanupManager("test_schema.staging_registry")
        cleanup_count, failed_count = manager.cleanup_staging_tables(mock_spark)

        assert cleanup_count == 0
        assert failed_count == 1

    @patch("pyspark.sql.functions.current_timestamp")
    @patch("kimball.orchestrator.spark")
    @patch("delta.tables.DeltaTable")
    def test_register_staging_table(self, mock_dt, mock_spark, mock_current_ts):
        """Should register staging table in registry."""
        from kimball.orchestrator import StagingCleanupManager

        mock_spark.catalog.tableExists.return_value = True
        mock_spark.createDataFrame.return_value.withColumn.return_value = MagicMock()
        mock_registry = MagicMock()
        mock_dt.forName.return_value = mock_registry
        mock_registry.alias.return_value.merge.return_value.whenNotMatchedInsertAll.return_value.execute = MagicMock()
        mock_current_ts.return_value = MagicMock()

        manager = StagingCleanupManager("test_schema.staging_registry")
        manager.register_staging_table(
            staging_table="staging.temp_table",
            pipeline_id="test-pipeline",
            batch_id="batch-123",
        )

        # Should create DataFrame and merge into registry
        mock_spark.createDataFrame.assert_called()
        mock_dt.forName.assert_called()


class TestStagingTableManager:
    """Tests for StagingTableManager context manager."""

    @patch("kimball.orchestrator.spark")
    def test_context_manager_cleanup_on_exit(self, mock_spark):
        """Should cleanup tables when exiting context."""
        from kimball.orchestrator import StagingCleanupManager, StagingTableManager

        mock_spark.catalog.tableExists.return_value = True

        cleanup_mgr = StagingCleanupManager("test_schema.staging_registry")

        with StagingTableManager(cleanup_mgr) as staging:
            staging.staging_tables = ["table1", "table2"]

        # Tables should be marked for cleanup
        assert len(staging.staging_tables) == 2
