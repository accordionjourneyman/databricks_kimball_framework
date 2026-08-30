"""Tests for autoOptimize TBLPROPERTIES in table creator."""

from unittest.mock import MagicMock, patch

from kimball.processing.table_creator import TableCreator


class TestAutoOptimize:
    def test_enable_delta_features_includes_auto_optimize(self):
        """enable_delta_features must emit autoOptimize.optimizeWrite and autoCompact."""
        tc = TableCreator()
        mock_spark = MagicMock()
        with patch(
            "kimball.processing.table_creator.get_spark", return_value=mock_spark
        ):
            tc.enable_delta_features("my_table")

        call_args = mock_spark.sql.call_args[0][0]
        assert "delta.autoOptimize.optimizeWrite" in call_args
        assert "delta.autoOptimize.autoCompact" in call_args
        assert "'true'" in call_args

    def test_enable_delta_features_preserves_existing_props(self):
        """enable_delta_features must still emit deletionVectors and predictiveOptimization."""
        tc = TableCreator()
        mock_spark = MagicMock()
        with patch(
            "kimball.processing.table_creator.get_spark", return_value=mock_spark
        ):
            tc.enable_delta_features("my_table")

        call_args = mock_spark.sql.call_args[0][0]
        assert "delta.enableDeletionVectors" in call_args
        assert "delta.enablePredictiveOptimization" in call_args
