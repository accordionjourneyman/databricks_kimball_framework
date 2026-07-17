"""Unit tests for SCD4 and SCD6 functional merge strategies."""

from unittest.mock import MagicMock, patch

import pytest

from kimball.processing.dispatcher import merge
from kimball.processing.scd4 import merge_scd4
from kimball.processing.scd6 import merge_scd6


class TestCreateMergeStrategy:
    @patch("kimball.processing.dispatcher.current_timestamp")
    def test_create_scd4_requires_history_table(self, _mock_ts):
        with pytest.raises(ValueError, match="requires history_table"):
            merge(
                MagicMock(),
                scd_type=4,
                target_table_name="dim_product",
                join_keys=["product_id"],
                surrogate_key_col="product_sk",
            )

    @patch("kimball.processing.dispatcher.current_timestamp")
    def test_create_scd6_requires_current_value_columns(self, _mock_ts):
        with pytest.raises(ValueError, match="requires current_value_columns"):
            merge(
                MagicMock(),
                scd_type=6,
                target_table_name="dim_customer",
                join_keys=["customer_id"],
                surrogate_key_col="customer_sk",
            )

    @patch("kimball.processing.dispatcher.current_timestamp")
    def test_create_scd4_dispatches_to_merge_scd4(self, _mock_ts):
        with patch("kimball.processing.dispatcher.merge_scd4") as mock_scd4:
            merge(
                MagicMock(),
                scd_type=4,
                target_table_name="dim_product",
                join_keys=["product_id"],
                history_table="dim_product_history",
                surrogate_key_col="product_sk",
            )
            # merge() dispatches to merge_scd4 with the configured table/keys --
            # verify it actually wired the history table through (not just that
            # something was called).
            mock_scd4.assert_called_once()
            kwargs = mock_scd4.call_args.kwargs
            assert kwargs["target_table_name"] == "dim_product"
            assert kwargs["history_table_name"] == "dim_product_history"
            assert kwargs["join_keys"] == ["product_id"]

    @patch("kimball.processing.dispatcher.current_timestamp")
    def test_create_scd6_dispatches_to_merge_scd6(self, _mock_ts):
        with patch("kimball.processing.dispatcher.merge_scd6") as mock_scd6:
            merge(
                MagicMock(),
                scd_type=6,
                target_table_name="dim_customer",
                join_keys=["customer_id"],
                current_value_columns=["city", "status"],
                surrogate_key_col="customer_sk",
            )
            mock_scd6.assert_called_once()
            kwargs = mock_scd6.call_args.kwargs
            assert kwargs["target_table_name"] == "dim_customer"
            assert kwargs["current_value_columns"] == ["city", "status"]
            assert kwargs["join_keys"] == ["customer_id"]


class TestSCD4Function:
    def test_merge_scd4_calls_scd1_and_history(self):
        with (
            patch("kimball.processing.scd4.merge_scd1") as mock_scd1,
            patch("kimball.processing.scd4._merge_history") as mock_hist,
        ):
            merge_scd4(
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

        with (
            patch("kimball.processing.scd6.DeltaTable") as mock_dt,
            patch(
                "kimball.processing.scd6.filter_cdf_deletes",
                return_value=(mock_df, None),
            ),
            patch("kimball.processing.scd6.compute_hashdiff", return_value="hash"),
            patch("kimball.processing.scd6.col", return_value=MagicMock()),
            patch("kimball.processing.scd6.lit", return_value=MagicMock()),
            patch("kimball.processing.scd6.when", return_value=MagicMock()),
            patch("kimball.processing.scd6.HashKeyGenerator") as mock_gen,
        ):
            mock_gen.return_value.generate_keys.return_value = mock_df
            mock_dt.forName.return_value = MagicMock()
            merge_scd6(
                mock_df,
                target_table_name="dim_product",
                join_keys=["product_id"],
                track_history_columns=["name"],
                current_value_columns=["name"],
            )
            mock_dt.forName.assert_called()
