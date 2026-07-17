"""Tests for SCD2 dispatch and payload helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.processing.scd2 import (
    _select_payload_columns,
    merge_scd2,
)


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


class TestSelectPayloadColumns:
    def test_keeps_history_and_keys(self):
        df = MagicMock()
        df.columns = [
            "customer_id",
            "name",
            "email",
            "_change_type",
            "_commit_version",
            "hashdiff",
            "__is_current",
        ]
        _select_payload_columns(
            df, ["customer_id"], ["name", "email"], include_meta=False
        )
        df.select.assert_called_once()
        selected = df.select.call_args[0]
        selected_set = set(selected)
        assert "customer_id" in selected_set
        assert "name" in selected_set
        assert "email" in selected_set

    def test_includes_meta_when_requested(self):
        df = MagicMock()
        df.columns = ["customer_id", "name", "_change_type", "_commit_version"]
        _select_payload_columns(df, ["customer_id"], ["name"], include_meta=True)
        df.select.assert_called_once()

    def test_includes_effective_at(self):
        df = MagicMock()
        df.columns = ["customer_id", "name", "updated_at"]
        _select_payload_columns(
            df,
            ["customer_id"],
            ["name"],
            include_meta=False,
            effective_at_column="updated_at",
        )
        df.select.assert_called_once()


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
