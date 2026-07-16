"""Tests for merge_helpers functions that are currently untested."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, LongType, StructType, StructField

from kimball.processing.merge_helpers import (
    PYSPARK_EXCEPTION_BASE,
    apply_schema_evolution,
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
        df.columns = ["customer_id", "_commit_version"]
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

    def test_falls_back_when_no_effective_at(self):
        df = MagicMock()
        df.columns = ["customer_id"]
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
        del df.toDF
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


class TestApplySchemaEvolution:
    """Tests for apply_schema_evolution (lines 65-92)."""

    @patch("kimball.processing.merge_helpers.get_spark")
    @patch("kimball.processing.merge_helpers.DeltaTable")
    @patch("kimball.processing.merge_helpers.quote_table_name", return_value="`catalog`.`schema`.`table`")
    def test_disabled_returns_early(self, mock_quote, mock_dt, mock_get_spark):
        apply_schema_evolution("test.table", enabled=False)
        mock_get_spark.assert_not_called()
        mock_dt.forName.assert_not_called()

    @patch("kimball.processing.merge_helpers.get_spark")
    @patch("kimball.processing.merge_helpers.DeltaTable")
    @patch("kimball.processing.merge_helpers.quote_table_name", return_value="`catalog`.`schema`.`table`")
    def test_enabled_sets_tblproperties_and_returns_when_source_none(self, mock_quote, mock_dt, mock_get_spark):
        apply_schema_evolution("test.table", enabled=True, source_df=None)
        mock_get_spark.return_value.sql.assert_called_once_with(
            "ALTER TABLE `catalog`.`schema`.`table` SET TBLPROPERTIES ('delta.schema.autoMerge.enabled' = 'true')"
        )
        mock_dt.forName.assert_not_called()

    @patch("kimball.processing.merge_helpers.get_spark")
    @patch("kimball.processing.merge_helpers.DeltaTable")
    @patch("kimball.processing.merge_helpers.quote_table_name", return_value="`catalog`.`schema`.`table`")
    def test_tblproperties_failure_logs_warning(self, mock_quote, mock_dt, mock_get_spark, caplog):
        from pyspark.errors import PySparkException
        mock_get_spark.return_value.sql.side_effect = PySparkException("permission denied")
        apply_schema_evolution("test.table", enabled=True, source_df=None)
        assert "Could not enable schema auto-merge" in caplog.text

    @patch("kimball.processing.merge_helpers.get_spark")
    @patch("kimball.processing.merge_helpers.DeltaTable")
    @patch("kimball.processing.merge_helpers.quote_table_name", return_value="`catalog`.`schema`.`table`")
    def test_adds_new_columns(self, mock_quote, mock_dt, mock_get_spark):
        source_df = MagicMock()
        source_df.schema.fields = [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
        ]
        target_schema = StructType([
            StructField("id", LongType(), True),
        ])
        target_df = MagicMock()
        target_df.schema = target_schema
        mock_dt.forName.return_value.toDF.return_value = target_df

        apply_schema_evolution("test.table", enabled=True, source_df=source_df)

        calls = mock_get_spark.return_value.sql.call_args_list
        assert len(calls) == 2
        assert calls[0][0][0] == (
            "ALTER TABLE `catalog`.`schema`.`table` SET TBLPROPERTIES ('delta.schema.autoMerge.enabled' = 'true')"
        )
        assert calls[1][0][0] == (
            "ALTER TABLE `catalog`.`schema`.`table` ADD COLUMNS (name string, email string)"
        )

    @patch("kimball.processing.merge_helpers.get_spark")
    @patch("kimball.processing.merge_helpers.DeltaTable")
    @patch("kimball.processing.merge_helpers.quote_table_name", return_value="`catalog`.`schema`.`table`")
    def test_skips_special_columns(self, mock_quote, mock_dt, mock_get_spark):
        source_df = MagicMock()
        source_df.schema.fields = [
            StructField("id", LongType(), True),
            StructField("__internal", StringType(), True),
            StructField("_change_type", StringType(), True),
            StructField("hashdiff", StringType(), True),
            StructField("__merge_action", StringType(), True),
            StructField("name", StringType(), True),
        ]
        target_schema = StructType([StructField("id", LongType(), True)])
        target_df = MagicMock()
        target_df.schema = target_schema
        mock_dt.forName.return_value.toDF.return_value = target_df

        apply_schema_evolution("test.table", enabled=True, source_df=source_df)

        calls = mock_get_spark.return_value.sql.call_args_list
        add_cols_call = calls[1][0][0]
        assert "name string" in add_cols_call
        assert "__internal" not in add_cols_call
        assert "_change_type" not in add_cols_call
        assert "hashdiff" not in add_cols_call
        assert "__merge_action" not in add_cols_call

    @patch("kimball.processing.merge_helpers.get_spark")
    @patch("kimball.processing.merge_helpers.DeltaTable")
    @patch("kimball.processing.merge_helpers.quote_table_name", return_value="`catalog`.`schema`.`table`")
    def test_no_new_columns_skips_add_columns(self, mock_quote, mock_dt, mock_get_spark):
        source_df = MagicMock()
        source_df.schema.fields = [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
        ]
        target_schema = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
        ])
        target_df = MagicMock()
        target_df.schema = target_schema
        mock_dt.forName.return_value.toDF.return_value = target_df

        apply_schema_evolution("test.table", enabled=True, source_df=source_df)

        assert mock_get_spark.return_value.sql.call_count == 1  # only SET TBLPROPERTIES

    @patch("kimball.processing.merge_helpers.get_spark")
    @patch("kimball.processing.merge_helpers.DeltaTable")
    @patch("kimball.processing.merge_helpers.quote_table_name", return_value="`catalog`.`schema`.`table`")
    def test_add_columns_failure_logs_warning(self, mock_quote, mock_dt, mock_get_spark, caplog):
        from pyspark.errors import PySparkException
        source_df = MagicMock()
        source_df.schema.fields = [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
        ]
        target_schema = StructType([StructField("id", LongType(), True)])
        target_df = MagicMock()
        target_df.schema = target_schema
        mock_dt.forName.return_value.toDF.return_value = target_df
        mock_get_spark.return_value.sql.side_effect = [None, PySparkException("add column failed")]

        apply_schema_evolution("test.table", enabled=True, source_df=source_df)

        assert "Schema evolution check failed" in caplog.text


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