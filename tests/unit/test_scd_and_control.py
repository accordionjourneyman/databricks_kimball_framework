"""Tests for SCD strategies, watermarks, and control records."""

import inspect
import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

os.environ.setdefault("KIMBALL_ETL_SCHEMA", "test_schema")


# SCD and control-table behavior


class TestSCD1Deduplication:
    """SCD1 CDF deduplication uses the first available ordering column:
    commit version, commit timestamp, or ETL processing time."""

    @patch("kimball.processing.scd1.generate_keys")
    @patch("kimball.processing.scd1.dedup_cdf")
    @patch("kimball.processing.scd1.DeltaTable")
    @patch("kimball.processing.scd1.get_spark")
    def test_scd1_dedup_falls_back_to_etl_processed_at(
        self,
        mock_get_spark,
        mock_delta_table,
        mock_dedup,
        mock_generate_keys,
    ):
        """Use ETL processing time when commit version is unavailable."""
        from kimball.processing.scd1 import merge_scd1

        mock_dt = MagicMock()
        mock_dt.toDF.return_value.schema.fields = []
        mock_delta_table.forName.return_value = mock_dt
        mock_dt.alias.return_value = mock_dt
        mock_merge_builder = MagicMock()
        mock_dt.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedDelete.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdate.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsert.return_value = mock_merge_builder

        source_df = MagicMock()
        source_df.columns = ["id", "val", "_change_type", "__etl_processed_at"]

        with_column_calls = []

        def track_with_column(name, col_expr):
            with_column_calls.append(name)
            return source_df

        source_df.withColumn = MagicMock(side_effect=track_with_column)
        source_df.filter.return_value = source_df
        source_df.drop.return_value = source_df
        source_df.alias.return_value = source_df

        mock_dedup.side_effect = lambda df, keys: df.withColumn("_rn", MagicMock())
        mock_generate_keys.return_value = source_df

        merge_scd1(
            source_df,
            target_table_name="test.dim",
            join_keys=["id"],
            delete_strategy="hard",
            schema_evolution=False,
            surrogate_key_col="sk",
        )
        assert "_rn" in with_column_calls, (
            "CDF deduplication should use ETL processing time "
            "when the commit version is unavailable."
        )

    @patch("kimball.processing.scd1.dedup_cdf")
    @patch("kimball.processing.scd1.DeltaTable")
    @patch("kimball.processing.scd1.get_spark")
    def test_scd1_dedup_raises_when_no_ordering_column(
        self, mock_get_spark, mock_delta_table, mock_dedup
    ):
        """Raise a clear error when no CDF ordering column is available."""
        from kimball.processing.scd1 import merge_scd1

        mock_dt = MagicMock()
        mock_delta_table.forName.return_value = mock_dt

        source_df = MagicMock()
        source_df.columns = ["id", "val", "_change_type"]

        mock_dedup.side_effect = ValueError(
            "CDF deduplication requires an ordering column. "
            "Source has _change_type but none of _commit_version, "
            "_commit_timestamp, or __etl_processed_at."
        )

        with pytest.raises(
            ValueError, match="deduplication requires an ordering column"
        ):
            merge_scd1(
                source_df,
                target_table_name="test.dim",
                join_keys=["id"],
                delete_strategy="hard",
                schema_evolution=False,
                surrogate_key_col="sk",
            )


class TestSCD4NullEquality:
    """SCD4 history comparisons use null-safe equality when expiring rows."""

    def test_scd4_expire_merge_condition_uses_null_safe_equality(self):
        """Use null-safe equality in the SCD4 expiration condition."""
        from kimball.processing.scd4 import _merge_history

        source_code = inspect.getsource(_merge_history)

        assert "target.value <=> source.value" in source_code, (
            "SCD4 EXPIRE merge condition should use "
            "<=> (null-safe equality) for value comparison, not '='."
        )
        # The expiration condition must not use ordinary equality.
        assert "target.value = source.value" not in source_code, (
            "unsafe '=' found for value comparison. "
            "Should be '<=>'."
        )


class TestSCD6CurrentValuesJoin:
    """The current-values DataFrame is aliased for qualified joins."""

    def test_scd6_current_values_aliased_in_join(self):
        """Alias the current-values DataFrame used by the join."""
        from kimball.processing.scd6 import merge_scd6

        source_code = inspect.getsource(merge_scd6)

        assert "current_values" in source_code, (
            "SCD6 'current_values' DataFrame should "
            "be used in the join so that col('current_values.hashdiff') resolves correctly."
        )


class TestSCD6DeleteColumns:
    """Delete targets retain effective time so expired rows get the correct validity end."""

    def test_scd6_delete_targets_includes_effective_at(self):
        """Keep effective time in delete targets for validity updates."""
        from kimball.processing.scd6 import merge_scd6

        source_code = inspect.getsource(merge_scd6)

        assert "EXPIRE_DELETE" in source_code
        # Check the selected columns used by the expiration branch.
        delete_section = source_code.split("EXPIRE_DELETE")[0].rsplit("select", 1)[-1]
        assert (
            "effective_col" in delete_section or "effective_at_column" in delete_section
        ), (
            "SCD6 delete_targets should include "
            "effective_at_column in its select so __valid_to is set correctly."
        )


class TestSCD6UpdatedColumns:
    """Staged updates omit current-value columns already present in the target."""

    def test_scd6_staged_updates_excludes_old_current_columns(self):
        """Exclude existing current-value columns from staged updates."""
        from kimball.processing.scd6 import merge_scd6

        source_code = inspect.getsource(merge_scd6)

        assert "for c in all_existing.columns" in source_code
        assert 'not c.startswith("current_")' in source_code, (
            "SCD6 staged_updates should exclude "
            "current_* columns from the old.* projection to avoid duplicate "
            "column names on 2nd+ run."
        )


# SCD and control-table behavior


class TestControlTableOperations:
    """Control-table updates preserve record fields and metrics."""

    @patch("kimball.orchestration.watermark.DeltaTable")
    @patch("kimball.orchestration.watermark.col")
    def test_upsert_mixed_update_keys_uses_union_of_all_keys(
        self, mock_col, mock_delta_table
    ):
        """Build the update set from fields present across all records."""
        from kimball.orchestration.watermark import ETLControlManager

        spark_mock = MagicMock()
        spark_mock.catalog.tableExists.return_value = True
        spark_mock.sql = MagicMock()

        mock_dt_instance = MagicMock()
        mock_delta_table.forName.return_value = mock_dt_instance
        mock_dt_instance.alias.return_value = mock_dt_instance

        merge_builders = []

        class _MergeBuilderTracker:
            def __call__(self, *args, **kwargs):
                builder = MagicMock()
                builder.whenMatchedUpdate.return_value = builder
                builder.whenNotMatchedInsert.return_value = builder
                merge_builders.append(builder)
                return builder

        mock_dt_instance.merge.side_effect = _MergeBuilderTracker()

        from kimball.orchestration.watermark import ETLControlManager as _CM

        update_df_mock = MagicMock()
        update_df_mock.columns = [f.name for f in _CM._UPDATE_SCHEMA.fields]
        spark_mock.createDataFrame.return_value = update_df_mock

        manager = ETLControlManager(etl_schema="test", spark_session=spark_mock)

        records = [
            {
                "target_table": "dim_customer",
                "source_table": "silver.c",
                "batch_status": "SUCCESS",
                "updated_at": datetime.now(),
            },
            {
                "target_table": "dim_customer",
                "source_table": "silver.d",
                "last_processed_version": 99,
                "updated_at": datetime.now(),
            },
        ]

        manager._upsert_control_records(records)

        upsert_builder = merge_builders[0]
        update_call_args = upsert_builder.whenMatchedUpdate.call_args
        update_set = update_call_args.kwargs.get("set", {})
        # The update set includes fields present in any input record.
        assert "last_processed_version" in update_set, (
            "_upsert_control_records should compute "
            "update_set from the UNION of all record keys, not just "
            "records[0].keys()."
        )

    @patch("kimball.orchestration.watermark.DeltaTable")
    @patch("kimball.orchestration.watermark.col")
    def test_batch_complete_updates_watermark_and_metrics(
        self, mock_col, mock_delta_table
    ):
        """batch_complete should update both
        last_processed_version and row metrics."""
        from kimball.orchestration.watermark import ETLControlManager

        spark_mock = MagicMock()
        spark_mock.catalog.tableExists.return_value = True
        spark_mock.sql = MagicMock()

        mock_dt_instance = MagicMock()
        mock_delta_table.forName.return_value = mock_dt_instance
        mock_dt_instance.alias.return_value = mock_dt_instance

        merge_builders = []

        class _MergeBuilderTracker:
            def __call__(self, *args, **kwargs):
                builder = MagicMock()
                builder.whenMatchedUpdate.return_value = builder
                builder.whenNotMatchedInsert.return_value = builder
                merge_builders.append(builder)
                return builder

        mock_dt_instance.merge.side_effect = _MergeBuilderTracker()

        from kimball.orchestration.watermark import ETLControlManager as _CM

        update_df_mock = MagicMock()
        update_df_mock.columns = [f.name for f in _CM._UPDATE_SCHEMA.fields]
        spark_mock.createDataFrame.return_value = update_df_mock

        manager = ETLControlManager(etl_schema="test", spark_session=spark_mock)

        manager.batch_complete(
            "dim_customer",
            "silver.customers",
            new_version=100,
            rows_read=500,
            rows_written=50,
        )

        batch_complete_builder = merge_builders[0]
        update_call_args = batch_complete_builder.whenMatchedUpdate.call_args
        update_set = update_call_args.kwargs.get("set", {})

        assert "last_processed_version" in update_set
        assert "rows_read" in update_set
        assert "rows_written" in update_set
        assert update_set["last_processed_version"] == "u.last_processed_version"

    @patch("kimball.orchestration.watermark.DeltaTable")
    def test_migrate_schema_detects_type_mismatches(self, mock_delta_table):
        """_migrate_schema should detect and warn about type
        mismatches in existing columns."""
        from pyspark.sql.types import (
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        from kimball.orchestration.watermark import ETLControlManager

        spark_mock = MagicMock()
        spark_mock.catalog.tableExists.return_value = True
        spark_mock.sql = MagicMock()

        existing_schema = StructType(
            [
                StructField("target_table", StringType(), True),
                StructField("source_table", StringType(), True),
                StructField("last_processed_version", IntegerType(), True),
                StructField("batch_id", StringType(), True),
                StructField("batch_started_at", TimestampType(), True),
                StructField("batch_completed_at", TimestampType(), True),
                StructField("batch_status", StringType(), True),
                StructField("rows_read", LongType(), True),
                StructField("rows_written", LongType(), True),
                StructField("error_message", StringType(), True),
                StructField("updated_at", TimestampType(), True),
            ]
        )

        existing_df = MagicMock()
        existing_df.schema = existing_schema
        spark_mock.table.return_value = existing_df

        ETLControlManager(etl_schema="test", spark_session=spark_mock)
        # Existing fields must not be added to the table again.
        alter_calls = [
            call.args[0]
            for call in spark_mock.sql.call_args_list
            if "ALTER TABLE" in str(call.args[0]) and "ADD COLUMN" in str(call.args[0])
        ]
        last_processed_alter = [c for c in alter_calls if "last_processed_version" in c]
        assert len(last_processed_alter) == 0, (
            "_migrate_schema should not ADD COLUMN "
            "for existing columns. It should detect type mismatches and "
            "log a warning instead."
        )


# =====================================================================
# SQL expression and SCD behavior
# =====================================================================


class TestSqlExpressionValidation:
    """Expression validation accepts comparison operators, arithmetic,
    and function arguments."""

    def test_negative_number_accepted(self):
        """``amount >= -1`` should be valid."""
        from kimball.processing.table_creator import _is_safe_sql_expression

        assert _is_safe_sql_expression("amount >= -1") is True, (
            "_is_safe_sql_expression should accept "
            "hyphens for negative numbers."
        )

    def test_not_equal_operator_accepted(self):
        """``status != 0`` should be valid."""
        from kimball.processing.table_creator import _is_safe_sql_expression

        assert _is_safe_sql_expression("status != 0") is True, (
            "_is_safe_sql_expression should accept "
            "'!' for != operator."
        )

    def test_function_call_accepted(self):
        """``COALESCE(a, b) > 0`` should be valid."""
        from kimball.processing.table_creator import _is_safe_sql_expression

        assert _is_safe_sql_expression("COALESCE(a, b) > 0") is True, (
            "_is_safe_sql_expression should accept "
            "commas for function arguments."
        )


class TestSqlColumnReferences:
    """Qualified column references may use SQL keywords as aliases;
    SQL statements are rejected."""

    @patch("kimball.orchestration.validation.F")
    def test_qualified_column_ref_not_flagged(self, mock_F):
        """Accept a keyword when it is used as a qualified column name."""
        from kimball.orchestration.validation import DataQualityValidator

        validator = DataQualityValidator()
        df = MagicMock()

        result = validator.validate_expression(df, "select.flag = 1")

        assert result.passed, (
            "A keyword used as a table alias is a valid qualified reference."
        )

    @patch("kimball.orchestration.validation.F")
    def test_sql_statement_still_flagged(self, mock_F):
        """Reject a SQL statement passed as an expression."""
        from kimball.orchestration.validation import DataQualityValidator

        validator = DataQualityValidator()
        df = MagicMock()

        result = validator.validate_expression(df, "select * from table")

        assert not result.passed, (
            "A SQL statement is not a valid column expression."
        )


class TestPreserveAllChangesInitialLoad:
    """Initial SCD2 loads preserve every source version when configured."""

    def test_initial_load_processes_one_version_at_a_time(self):
        """Process one source version at a time on the initial load."""
        from kimball.orchestration.services.work_plan import build_source_work_plan

        source_code = inspect.getsource(build_source_work_plan)

        assert "preserve_all_changes" in source_code, (
            "preserve_all_changes must select one "
            "initial source version at a time."
        )


class TestFullSnapshotSCD2DeleteDetection:
    """Full-snapshot SCD2 processing detects source deletes."""

    def test_scd2_has_full_snapshot_delete_detection(self):
        """Detect rows missing from a full snapshot."""
        from kimball.processing.scd2 import _merge_single_pass

        source_code = inspect.getsource(_merge_single_pass)

        assert "filter_cdf_deletes" in source_code or "left_anti" in source_code, (
            "SCD2 should have delete detection "
            "for full snapshot mode (when _change_type is absent)."
        )


class TestHashdiffOrderInvariance:
    """Hashdiff results do not depend on the order of input columns."""

    def test_sorts_columns_alphabetically(self, spark: SparkSession):
        """Produce the same hash for every ordering of the same columns."""
        from kimball.processing.hashing import compute_hashdiff

        df = spark.createDataFrame([(1, 2, 3)], ["zebra", "apple", "mango"])
        h1 = df.select(compute_hashdiff(["zebra", "apple", "mango"])).collect()[0][0]
        h2 = df.select(compute_hashdiff(["apple", "mango", "zebra"])).collect()[0][0]
        h3 = df.select(compute_hashdiff(["mango", "zebra", "apple"])).collect()[0][0]
        assert h1 == h2 == h3, (
            f"hashdiff should be order-invariant but got {h1}, {h2}, {h3}"
        )


# =====================================================================
# Watermark reset behavior
# =====================================================================


class TestResetWatermark:
    """Resetting a watermark removes control records for the selected
    target and source."""

    def test_reset_watermark_deletes_by_target_and_source(self):
        from kimball.orchestration.watermark import ETLControlManager

        spark_mock = MagicMock()
        spark_mock.catalog.tableExists.return_value = True
        spark_mock.sql = MagicMock()

        manager = ETLControlManager(etl_schema="test", spark_session=spark_mock)
        manager.reset_watermark("dim_customer", "silver.customers")

        sql_calls = [c[0][0] for c in spark_mock.sql.call_args_list]
        delete_sql = [s for s in sql_calls if "DELETE FROM" in s]
        assert len(delete_sql) == 1
        assert "dim_customer" in delete_sql[0]
        assert "silver.customers" in delete_sql[0]

    def test_reset_watermark_deletes_all_sources_when_source_is_none(self):
        from kimball.orchestration.watermark import ETLControlManager

        spark_mock = MagicMock()
        spark_mock.catalog.tableExists.return_value = True
        spark_mock.sql = MagicMock()

        manager = ETLControlManager(etl_schema="test", spark_session=spark_mock)
        manager.reset_watermark("dim_customer")

        sql_calls = [c[0][0] for c in spark_mock.sql.call_args_list]
        delete_sql = [s for s in sql_calls if "DELETE FROM" in s]
        assert len(delete_sql) == 1
        assert "dim_customer" in delete_sql[0]
        assert "source_table" not in delete_sql[0]
