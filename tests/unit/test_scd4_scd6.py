"""
Unit tests for SCD4 and SCD6 functional merge strategies.
"""

import pytest
from unittest.mock import MagicMock, patch

from kimball.processing import merger as _merger


class TestCreateMergeStrategy:
    """Test the factory function with new SCD types."""

    def test_create_scd4_requires_history_table(self):
        with pytest.raises(ValueError, match="requires history_table"):
            _merger.merge(
                MagicMock(),
                scd_type=4,
                target_table_name="dim_product",
                join_keys=["product_id"],
            )

    def test_create_scd6_requires_current_value_columns(self):
        with pytest.raises(ValueError, match="requires current_value_columns"):
            _merger.merge(
                MagicMock(),
                scd_type=6,
                target_table_name="dim_customer",
                join_keys=["customer_id"],
            )

    def test_create_scd4_returns_callable(self):
        with patch("kimball.processing.merger.merge_scd4") as mock_scd4:
            _merger.merge(
                MagicMock(),
                scd_type=4,
                target_table_name="dim_product",
                join_keys=["product_id"],
                history_table="dim_product_history",
            )
            mock_scd4.assert_called_once()

    def test_create_scd6_returns_callable(self):
        with patch("kimball.processing.merger.merge_scd6") as mock_scd6:
            _merger.merge(
                MagicMock(),
                scd_type=6,
                target_table_name="dim_customer",
                join_keys=["customer_id"],
                current_value_columns=["city", "status"],
            )
            mock_scd6.assert_called_once()


class TestSCD4Function:
    def test_merge_scd4_calls_scd1_and_history(self):
        with patch("kimball.processing.merger.merge_scd1") as mock_scd1, \
             patch("kimball.processing.merger._merge_history") as mock_hist:
            _merger.merge_scd4(
                MagicMock(),
                target_table_name="dim_product",
                history_table_name="dim_product_history",
                join_keys=["product_id"],
                track_history_columns=["name"],
            )
            mock_scd1.assert_called_once()
            mock_hist.assert_called_once()


class TestSCD6Function:
    def test_merge_scd6_with_deletes(self):
        mock_df = MagicMock()
        mock_df.columns = ["product_id", "name", "_change_type"]
        mock_df.filter.return_value.isEmpty.return_value = False
        mock_spark = MagicMock()
        mock_df.sparkSession = mock_spark

        with patch("kimball.processing.merger.DeltaTable") as mock_dt, \
             patch("kimball.processing.merger.broadcast", lambda x: x), \
             patch("kimball.processing.merger.compute_hashdiff", return_value="hash"):
            mock_dt.forName.return_value = MagicMock()
            _merger.merge_scd6(
                mock_df,
                target_table_name="dim_product",
                join_keys=["product_id"],
                track_history_columns=["name"],
                current_value_columns=["name"],
            )
            mock_dt.forName.assert_called()
