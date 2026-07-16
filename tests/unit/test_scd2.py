"""Tests for SCD2 two-phase dispatch and rebuild_history."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.processing.scd2 import (
    _has_multiple_versions_per_key,
    _merge_classic,
    _merge_current,
    _rebuild_history,
    _select_payload_columns,
    merge_scd2,
)


@pytest.fixture(autouse=True)
def _patch_spark_fns():
    """Patch Spark functions that require an active SparkContext."""
    with patch("kimball.processing.scd2.lit", return_value=MagicMock()), \
         patch("kimball.processing.scd2.col", return_value=MagicMock()), \
         patch("kimball.processing.scd2.current_timestamp", return_value=MagicMock()), \
         patch("kimball.processing.scd2.expr", return_value=MagicMock()):
        yield


class TestHasMultipleVersionsPerKey:
    def test_no_change_type_returns_false(self):
        df = MagicMock()
        df.columns = ["customer_id", "name"]
        assert _has_multiple_versions_per_key(df, ["customer_id"]) is False

    def test_no_order_col_returns_false(self):
        df = MagicMock()
        df.columns = ["customer_id", "_change_type"]
        assert _has_multiple_versions_per_key(df, ["customer_id"]) is False

    def test_uses_effective_at_column_for_version_detection(self):
        df = MagicMock()
        df.columns = ["customer_id", "_commit_version"]
        grouped = MagicMock()
        grouped.agg.return_value = grouped
        grouped.filter.return_value = grouped
        grouped.limit.return_value = grouped
        grouped.count.return_value = 1
        df.groupBy.return_value = grouped

        assert _has_multiple_versions_per_key(df, ["customer_id"]) is True


class TestSelectPayloadColumns:
    def test_keeps_history_and_keys(self):
        df = MagicMock()
        df.columns = ["customer_id", "name", "email", "_change_type", "_commit_version", "hashdiff", "__is_current"]
        result = _select_payload_columns(
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
        _select_payload_columns(
            df, ["customer_id"], ["name"], include_meta=True
        )
        df.select.assert_called_once()

    def test_includes_effective_at(self):
        df = MagicMock()
        df.columns = ["customer_id", "name", "updated_at"]
        _select_payload_columns(
            df, ["customer_id"], ["name"], include_meta=False, effective_at_column="updated_at"
        )
        df.select.assert_called_once()


class TestMergeCurrentFallback:
    def test_no_order_col_falls_back_to_classic(self):
        df = MagicMock()
        df.columns = ["customer_id", "name"]
        df.withColumn.return_value = df
        with patch("kimball.processing.scd2._merge_classic") as mock_classic:
            result = _merge_current(
                df,
                target_table_name="dim",
                join_keys=["customer_id"],
                track_history_columns=["name"],
                surrogate_key_col="sk",
                schema_evolution=False,
                effective_at_column=None,
            )
        mock_classic.assert_called_once()


class TestRebuildHistoryNoIntermediates:
    def test_returns_early_when_no_intermediates(self):
        df = MagicMock()
        df.filter.return_value.isEmpty.return_value = True
        _rebuild_history(
            df,
            target_table_name="dim",
            join_keys=["customer_id"],
            track_history_columns=["name"],
            surrogate_key_col="sk",
            effective_at_column=None,
        )
        df.filter.assert_called_once()


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

    def test_single_version_takes_classic_path(self):
        df = MagicMock()
        df.columns = ["customer_id", "name"]
        with patch("kimball.processing.scd2._has_multiple_versions_per_key", return_value=False), \
             patch("kimball.processing.scd2._merge_classic") as mock_classic:
            merge_scd2(
                df,
                target_table_name="dim",
                join_keys=["customer_id"],
                track_history_columns=["name"],
                surrogate_key_col="sk",
            )
        mock_classic.assert_called_once()

    def test_multi_version_takes_single_pass(self):
        df = MagicMock()
        df.columns = ["customer_id", "name", "_change_type", "_commit_version"]
        df.withColumn.return_value = df
        with patch("kimball.processing.scd2._has_multiple_versions_per_key", return_value=True), \
             patch("kimball.processing.scd2._merge_single_pass") as mock_sp:
            merge_scd2(
                df,
                target_table_name="dim",
                join_keys=["customer_id"],
                track_history_columns=["name"],
                surrogate_key_col="sk",
            )
        mock_sp.assert_called_once()