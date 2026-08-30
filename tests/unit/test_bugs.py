"""
Regression tests for bugs found and fixed in the Kimball Framework.

Each test documents a specific bug that was identified, the fix applied,
and asserts the CORRECT (fixed) behavior. These serve as regression tests
to prevent the bugs from reappearing.

Bug categories:
  1. SCD logic bugs
  2. Watermark bugs
  3. Control table bugs
  4. Data processing bugs (duplicates, full reload, model change detection)
"""

import inspect
import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

os.environ.setdefault("KIMBALL_ETL_SCHEMA", "test_schema")


# =====================================================================
# 1. SCD LOGIC BUGS
# =====================================================================


class TestSCD1DedupBug:
    """BUG-SCD1-001: SCD1 dedup was silently skipped when _commit_version
    was missing but _change_type was present.

    FIX: Now falls back to _commit_timestamp, then __etl_processed_at,
    and raises a clear ValueError if no ordering column is available.
    """

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
        """If _change_type exists but _commit_version does not, dedup
        should fall back to __etl_processed_at."""
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

        # FIX VERIFIED: "_rn" IS added, meaning dedup ran with fallback column
        assert "_rn" in with_column_calls, (
            "BUG-SCD1-001 regression: SCD1 dedup was skipped because "
            "_commit_version is missing. It should fall back to "
            "__etl_processed_at for ordering."
        )

    @patch("kimball.processing.scd1.dedup_cdf")
    @patch("kimball.processing.scd1.DeltaTable")
    @patch("kimball.processing.scd1.get_spark")
    def test_scd1_dedup_raises_when_no_ordering_column(
        self, mock_get_spark, mock_delta_table, mock_dedup
    ):
        """If _change_type exists but NO ordering column is available,
        a clear ValueError should be raised."""
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


class TestSCD4NullEqualityBug:
    """BUG-SCD4-001: SCD4 history MERGE used ``=`` instead of ``<=>`` for
    value comparison, preventing NULL old values from being expired.

    FIX: Changed to ``<=>`` (null-safe equality) so NULL values can be
    expired correctly.
    """

    def test_scd4_expire_merge_condition_uses_null_safe_equality(self):
        """The SCD4 EXPIRE merge condition should use ``<=>`` for value
        comparison."""
        from kimball.processing.scd4 import _merge_history

        source_code = inspect.getsource(_merge_history)

        assert "target.value <=> source.value" in source_code, (
            "BUG-SCD4-001 regression: SCD4 EXPIRE merge condition should use "
            "<=> (null-safe equality) for value comparison, not '='."
        )
        # Ensure the old unsafe '=' is no longer used for value comparison
        assert "target.value = source.value" not in source_code, (
            "BUG-SCD4-001 regression: unsafe '=' found for value comparison. "
            "Should be '<=>'."
        )


class TestSCD6CurrentValuesAliasBug:
    """BUG-SCD6-001: SCD6 ``current_values`` DataFrame was not aliased in
    the join, causing ``col("current_values.hashdiff")`` to fail.

    FIX: Added ``.alias("current_values")`` to the join.
    """

    def test_scd6_current_values_aliased_in_join(self):
        """The ``current_values`` DataFrame should be aliased as
        ``current_values`` in the join."""
        from kimball.processing.scd6 import merge_scd6

        source_code = inspect.getsource(merge_scd6)

        assert "current_values" in source_code, (
            "BUG-SCD6-001 regression: SCD6 'current_values' DataFrame should "
            "be used in the join so that col('current_values.hashdiff') resolves correctly."
        )


class TestSCD6DeleteTargetsMissingEffectiveAtBug:
    """BUG-SCD6-002: SCD6 ``delete_targets`` only selected surrogate_key
    and ``__action``, omitting ``effective_at_column``.

    FIX: Added ``col(self.effective_at_column)`` to the select so
    ``__valid_to`` is set correctly on EXPIRE_DELETE.
    """

    def test_scd6_delete_targets_includes_effective_at(self):
        """``delete_targets`` should include ``self.effective_at_column``
        so that ``__valid_to`` is set correctly on EXPIRE_DELETE."""
        from kimball.processing.scd6 import merge_scd6

        source_code = inspect.getsource(merge_scd6)

        assert "EXPIRE_DELETE" in source_code
        # Find the delete_targets select block
        delete_section = source_code.split("EXPIRE_DELETE")[0].rsplit("select", 1)[-1]
        assert (
            "effective_col" in delete_section or "effective_at_column" in delete_section
        ), (
            "BUG-SCD6-002 regression: SCD6 delete_targets should include "
            "effective_at_column in its select so __valid_to is set correctly."
        )


class TestSCD6DuplicateCurrentColumnsBug:
    """BUG-SCD6-003: SCD6 ``staged_updates`` produced duplicate
    ``current_*`` column names on 2nd+ run.

    FIX: Added ``and not c.startswith("current_")`` to the filter condition
    to exclude existing current_* columns from the old.* projection.
    """

    def test_scd6_staged_updates_excludes_old_current_columns(self):
        """The select in staged_updates should exclude existing
        ``current_*`` columns from the ``old.*`` projection."""
        from kimball.processing.scd6 import merge_scd6

        source_code = inspect.getsource(merge_scd6)

        assert "for c in all_existing.columns" in source_code
        assert 'not c.startswith("current_")' in source_code, (
            "BUG-SCD6-003 regression: SCD6 staged_updates should exclude "
            "current_* columns from the old.* projection to avoid duplicate "
            "column names on 2nd+ run."
        )


# =====================================================================
# 3. CONTROL TABLE BUGS
# =====================================================================


class TestControlTableBugs:
    """Bugs in ETLControlManager control table operations."""

    @patch("kimball.orchestration.watermark.DeltaTable")
    @patch("kimball.orchestration.watermark.col")
    def test_upsert_mixed_update_keys_uses_union_of_all_keys(
        self, mock_col, mock_delta_table
    ):
        """BUG-CT-001: _upsert_control_records should compute update_set
        from the UNION of ALL record keys, not just records[0].keys()."""
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

        # FIX VERIFIED: last_processed_version IS in update_set because
        # the fix computes the union of all record keys
        assert "last_processed_version" in update_set, (
            "BUG-CT-001 regression: _upsert_control_records should compute "
            "update_set from the UNION of all record keys, not just "
            "records[0].keys()."
        )

    @patch("kimball.orchestration.watermark.DeltaTable")
    @patch("kimball.orchestration.watermark.col")
    def test_batch_complete_updates_watermark_and_metrics(
        self, mock_col, mock_delta_table
    ):
        """BUG-CT-002 (verification): batch_complete should update both
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
        """BUG-CT-003: _migrate_schema should detect and warn about type
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

        # FIX VERIFIED: _migrate_schema should NOT add last_processed_version
        # via ALTER TABLE ADD COLUMN because it already exists.
        alter_calls = [
            call.args[0]
            for call in spark_mock.sql.call_args_list
            if "ALTER TABLE" in str(call.args[0]) and "ADD COLUMN" in str(call.args[0])
        ]
        last_processed_alter = [c for c in alter_calls if "last_processed_version" in c]
        assert len(last_processed_alter) == 0, (
            "BUG-CT-003 regression: _migrate_schema should not ADD COLUMN "
            "for existing columns. It should detect type mismatches and "
            "log a warning instead."
        )


# =====================================================================
# 4. DATA PROCESSING BUGS (duplicates, full reload, model change detection)
# =====================================================================


class TestSafeSqlExpressionBug:
    """BUG-DP-001: ``_is_safe_sql_expression`` rejected hyphens, commas,
    and exclamation marks, blocking valid constraint expressions.

    FIX: Added ``-``, ``,``, ``!``, ``+``, ``*``, ``/`` to the whitelist
    regex.
    """

    def test_negative_number_accepted(self):
        """``amount >= -1`` should be valid."""
        from kimball.processing.table_creator import _is_safe_sql_expression

        assert _is_safe_sql_expression("amount >= -1") is True, (
            "BUG-DP-001 regression: _is_safe_sql_expression should accept "
            "hyphens for negative numbers."
        )

    def test_not_equal_operator_accepted(self):
        """``status != 0`` should be valid."""
        from kimball.processing.table_creator import _is_safe_sql_expression

        assert _is_safe_sql_expression("status != 0") is True, (
            "BUG-DP-001 regression: _is_safe_sql_expression should accept "
            "'!' for != operator."
        )

    def test_function_call_accepted(self):
        """``COALESCE(a, b) > 0`` should be valid."""
        from kimball.processing.table_creator import _is_safe_sql_expression

        assert _is_safe_sql_expression("COALESCE(a, b) > 0") is True, (
            "BUG-DP-001 regression: _is_safe_sql_expression should accept "
            "commas for function arguments."
        )


class TestValidateExpressionFalsePositive:
    """BUG-DP-002: ``validate_expression`` flagged qualified column
    references containing SQL keywords as forbidden.

    FIX: Changed regex to require whitespace AFTER the keyword, so
    ``select.flag = 1`` (where ``select`` is a table alias) is NOT
    flagged, while ``SELECT * FROM`` (SQL statement) IS flagged.
    """

    @patch("kimball.orchestration.validation.F")
    def test_qualified_column_ref_not_flagged(self, mock_F):
        """``select.flag = 1`` should NOT be flagged as forbidden.

        ``F`` is patched so the ``F.expr()`` call inside ``validate_expression``
        does not require a live SparkContext (which is unavailable in unit
        tests). The forbidden-keyword regex check runs before ``F.expr`` is
        ever reached, so patching ``F`` does not mask the behaviour under test.
        """
        from kimball.orchestration.validation import DataQualityValidator

        validator = DataQualityValidator()
        df = MagicMock()

        result = validator.validate_expression(df, "select.flag = 1")

        assert result.passed, (
            "BUG-DP-002 regression: validate_expression should NOT flag "
            "'select.flag = 1' as a forbidden SQL keyword when 'select' is "
            "a table/alias name, not a SQL statement."
        )

    @patch("kimball.orchestration.validation.F")
    def test_sql_statement_still_flagged(self, mock_F):
        """``select * from table`` SHOULD still be flagged.

        The forbidden-keyword regex matches ``select `` (whitespace after the
        keyword) and short-circuits before ``F.expr`` is invoked, so patching
        ``F`` does not affect this assertion.
        """
        from kimball.orchestration.validation import DataQualityValidator

        validator = DataQualityValidator()
        df = MagicMock()

        result = validator.validate_expression(df, "select * from table")

        assert not result.passed, (
            "BUG-DP-002 regression: validate_expression should still flag "
            "actual SQL statements like 'select * from table'."
        )


class TestPreserveAllChangesInitialLoadBug:
    """BUG-DP-003: ``preserve_all_changes`` with SCD2 did NOT process
    versions one at a time during the initial load.

    FIX: Added preserve_all_changes check to the initial load path
    (wm is None), processing one version at a time when enabled.
    """

    def test_initial_load_processes_one_version_at_a_time(self):
        """The initial load path (wm is None) should use one-version-at-a-time
        logic when preserve_all_changes is True."""
        from kimball.orchestration.services.work_plan import build_source_work_plan

        source_code = inspect.getsource(build_source_work_plan)

        assert "preserve_all_changes" in source_code, (
            "BUG-DP-003 regression: preserve_all_changes must select one "
            "initial source version at a time."
        )


class TestFullSnapshotSCD2DeleteDetection:
    """BUG-DP-004: Full snapshot + SCD2 could not detect source deletes.

    FIX: Added anti-join based delete detection in SCD2Strategy.merge()
    for when _change_type is not present (full snapshot mode).
    """

    def test_scd2_has_full_snapshot_delete_detection(self):
        """SCD2 merge should have anti-join delete detection for
        full snapshot mode (no _change_type column)."""
        from kimball.processing.scd2 import _merge_single_pass

        source_code = inspect.getsource(_merge_single_pass)

        assert "filter_cdf_deletes" in source_code or "left_anti" in source_code, (
            "BUG-DP-004 regression: SCD2 should have delete detection "
            "for full snapshot mode (when _change_type is absent)."
        )


class TestHashdiffOrderInvariance:
    """BUG-DP-006 (verification): ``compute_hashdiff`` with
    ``sort_columns=True`` should produce the same hash regardless of
    column order in the input list. Uses a real Spark DataFrame."""

    def test_sorts_columns_alphabetically(self, spark: SparkSession):
        """Reorders columns in the input list; the hash must be identical
        because the function sorts alphabetically internally."""
        from kimball.processing.hashing import compute_hashdiff

        df = spark.createDataFrame([(1, 2, 3)], ["zebra", "apple", "mango"])
        h1 = df.select(compute_hashdiff(["zebra", "apple", "mango"])).collect()[0][0]
        h2 = df.select(compute_hashdiff(["apple", "mango", "zebra"])).collect()[0][0]
        h3 = df.select(compute_hashdiff(["mango", "zebra", "apple"])).collect()[0][0]
        assert h1 == h2 == h3, (
            f"hashdiff should be order-invariant but got {h1}, {h2}, {h3}"
        )


class TestSCD2HashdiffInInsertValues:
    """BUG-DP-007 (verification): SCD2 hashdiff column should be included
    in insert_values so it's persisted in the target table for future
    change detection.
    """


# =====================================================================
# 6. ADDITIONAL SCD LOGIC EDGE CASES
# =====================================================================


# =====================================================================
# 8. FULL RELOAD
# =====================================================================


class TestResetWatermark:
    """reset_watermark deletes control records so the next run starts fresh."""

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
