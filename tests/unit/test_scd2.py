"""Tests for SCD2 dispatch."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.processing.scd2 import merge_scd2


@pytest.fixture(autouse=True)
def _patch_spark_fns():
    """Patch Spark functions that require an active SparkContext."""
    with (
        patch("kimball.processing.scd2.lit", return_value=MagicMock()),
        patch("kimball.processing.scd2.col", return_value=MagicMock()),
        patch("kimball.processing.scd2.current_timestamp", return_value=MagicMock()),
        patch("kimball.processing.scd2.expr", return_value=MagicMock()),
    ):
        yield


class TestMergeScd2Dispatch:
    def test_empty_track_history_raises(self):
        df = MagicMock()
        with pytest.raises(ValueError, match="track_history_columns"):
            merge_scd2(
                df,
                target_table_name="dim",
                join_keys=["customer_id"],
                track_history_columns=[],
                surrogate_key_col="sk",
            )

    @pytest.mark.parametrize(
        "columns",
        [
            ["customer_id", "name"],
            ["customer_id", "name", "_change_type", "_commit_version"],
        ],
        ids=["snapshot_or_zero_version", "single_or_multi_version_cdf"],
    )
    def test_all_batches_take_single_pass_path(self, columns):
        df = MagicMock()
        df.columns = columns
        with patch("kimball.processing.scd2._merge_single_pass") as mock_sp:
            merge_scd2(
                df,
                target_table_name="dim",
                join_keys=["customer_id"],
                track_history_columns=["name"],
                surrogate_key_col="sk",
            )
        mock_sp.assert_called_once()
