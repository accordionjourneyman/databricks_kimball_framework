"""Tests for merge_helpers functions that are currently untested."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, LongType, StructType, StructField

from kimball.processing.merge_helpers import (
    PYSPARK_EXCEPTION_BASE,
    build_expire_set,
    build_insert_values,
    build_merge_condition,
    dedup_cdf,
    filter_cdf_deletes,
    generate_keys,
    get_current_df,
    get_validity_col,
)


@pytest.fixture(autouse=True)
def _patch_spark_fns():
    with patch("kimball.processing.merge_helpers.col", return_value=MagicMock()), \
         patch("kimball.processing.merge_helpers.current_timestamp", return_value=MagicMock()), \
         patch("kimball.processing.merge_helpers.lit", return_value=MagicMock()), \
         patch("kimball.processing.merge_helpers.when", return_value=MagicMock()):
        yield


class TestDedupCdf:
    def test_no_change_type_returns_unchanged(self):
        df = MagicMock()
        df.columns = ["customer_id", "name"]
        result = dedup_cdf(df, ["customer_id"])
        assert result is df

    def test_no_ordering_column_raises(self):
        df = MagicMock()
        df.columns = ["customer_id", "_change_type"]
        with pytest.raises(ValueError, match="ordering column"):
            dedup_cdf(df, ["customer_id"])

    def test_dedup_accepts_explicit_order_column(self):
        df = MagicMock()
        df.columns = ["customer_id", "updated_at"]
        df.withColumn.return_value = df
        df.filter.return_value = df
        df.drop.return_value = df
        with patch("kimball.processing.merge_helpers.Window"), \
             patch("kimball.processing.merge_helpers.row_number"), \
             patch("kimball.processing.merge_helpers.col"), \
             patch("kimball.processing.merge_helpers.when", return_value=MagicMock()), \
             patch("kimball.processing.merge_helpers.lit", return_value=MagicMock()):
            result = dedup_cdf(df, ["customer_id"], order_col="updated_at")
        assert result is df

    def test_dedup_with_commit_version(self):
        df = MagicMock()
        df.columns = ["customer_id", "_change_type", "_commit_version"]
        df.withColumn.return_value = df
        df.filter.return_value = df
        df.drop.return_value = df
        with patch("kimball.processing.merge_helpers.Window"), \
             patch("kimball.processing.merge_helpers.row_number"), \
             patch("kimball.processing.merge_helpers.col"), \
             patch("kimball.processing.merge_helpers.when", return_value=MagicMock()), \
             patch("kimball.processing.merge_helpers.lit", return_value=MagicMock()):
            result = dedup_cdf(df, ["customer_id"])
        assert result is df


class TestFilterCdfDeletes:
    def test_no_change_type_returns_all_none(self):
        df = MagicMock()
        df.columns = ["id"]
        result, deletes = filter_cdf_deletes(df)
        assert result is df
        assert deletes is None

    def test_splits_deletes(self):
        df = MagicMock()
        df.columns = ["id", "_change_type"]
        df.filter.return_value = df
        result, deletes = filter_cdf_deletes(df)
        assert result is df
        assert deletes is df
        assert df.filter.call_count == 2


class TestBuildMergeCondition:
    def test_single_key(self):
        cond = build_merge_condition(["customer_id"])
        assert cond == "target.customer_id <=> source.customer_id"

    def test_composite_key(self):
        cond = build_merge_condition(["region", "customer_id"])
        assert "target.region <=> source.region" in cond
        assert "target.customer_id <=> source.customer_id" in cond

    def test_current_only_adds_filter(self):
        cond = build_merge_condition(["customer_id"], current_only=True)
        assert "target.__is_current = true" in cond


class TestGenerateKeys:
    def test_scd1_hashes_natural_keys_only(self):
        df = MagicMock()
        df.columns = ["customer_id", "name"]
        df.withColumn.return_value = df
        with patch("kimball.processing.merge_helpers.HashKeyGenerator") as mock_gen:
            mock_gen.return_value.generate_keys.return_value = df
            result = generate_keys(df, ["customer_id"], "sk", scd_type=1)
            call_args = mock_gen.call_args
            assert call_args[0][0] == ["customer_id"]
            assert call_args[1]["version_column"] is None

    def test_scd2_with_effective_at(self):
        df = MagicMock()
        df.columns = ["customer_id", "updated_at"]
        df.withColumn.return_value = df
        with patch("kimball.processing.merge_helpers.HashKeyGenerator") as mock_gen:
            mock_gen.return_value.generate_keys.return_value = df
            result = generate_keys(df, ["customer_id"], "sk", scd_type=2, effective_at_column="updated_at")
            call_args = mock_gen.call_args
            assert call_args[1]["version_column"] == "updated_at"

    def test_scd2_fallback_to_etl_processed_at(self):
        df = MagicMock()
        df.columns = ["customer_id"]
        df.withColumn.return_value = df
        with patch("kimball.processing.merge_helpers.HashKeyGenerator") as mock_gen:
            mock_gen.return_value.generate_keys.return_value = df
            result = generate_keys(df, ["customer_id"], "sk", scd_type=2)
            call_args = mock_gen.call_args
            assert call_args[1]["version_column"] == "__etl_processed_at"
            df.withColumn.assert_called()


class TestGetValidityCol:
    def test_uses_effective_at_when_present(self):
        df = MagicMock()
        df.columns = ["customer_id", "updated_at"]
        col, note = get_validity_col("updated_at", df, "dim_customer")
        assert col == "source.updated_at"
        assert "business time" in note

    def test_warns_when_no_effective_at(self):
        df = MagicMock()
        df.columns = ["customer_id"]
        with pytest.warns(UserWarning, match="processing time"):
            col, note = get_validity_col(None, df, "dim_customer")
        assert col == "source.__etl_processed_at"
        assert "processing time" in note


class TestGetCurrentDf:
    def test_delta_table_filter(self):
        from delta.tables import DeltaTable
        filtered = MagicMock()
        dt = MagicMock(spec=DeltaTable)
        dt.toDF.return_value = MagicMock()
        dt.toDF.return_value.filter.return_value = filtered
        result = get_current_df(dt)
        assert result is filtered

    def test_dataframe_filter(self):
        filtered = MagicMock()
        df = MagicMock()
        df.filter.return_value = filtered
        result = get_current_df(df)
        assert result is filtered


class TestBuildExpireSet:
    def test_returns_correct_keys(self):
        result = build_expire_set("source.updated_at")
        assert result["__is_current"] == "false"
        assert result["__valid_to"] == "source.updated_at"
        assert result["__etl_processed_at"] == "current_timestamp()"


class TestBuildInsertValues:
    def test_maps_source_columns(self):
        df = MagicMock()
        df.columns = ["customer_id", "name", "_change_type", "__etl_processed_at"]
        result = build_insert_values(df, ["customer_id"], "sk", "source.updated_at")
        assert result["customer_id"] == "source.__orig_customer_id"
        assert result["name"] == "source.name"
        assert result["__is_current"] == "true"
        assert result["__is_skeleton"] == "false"
        assert "_change_type" not in result

    def test_include_history_false(self):
        df = MagicMock()
        df.columns = ["customer_id"]
        result = build_insert_values(df, ["customer_id"], "sk", "source.updated_at", include_history=False)
        assert "__is_skeleton" not in result


class TestIsConcurrentException:
    def test_concurrent_append_string_match(self):
        from kimball.processing.dispatcher import _is_concurrent_exception
        exc = Exception("io.delta.exceptions.ConcurrentAppendException: conflict")
        assert _is_concurrent_exception(exc) is True

    def test_write_conflict_string_match(self):
        from kimball.processing.dispatcher import _is_concurrent_exception
        exc = Exception("WriteConflictException: table modified")
        assert _is_concurrent_exception(exc) is True

    def test_non_concurrent_returns_false(self):
        from kimball.processing.dispatcher import _is_concurrent_exception
        exc = ValueError("some other error")
        assert _is_concurrent_exception(exc) is False