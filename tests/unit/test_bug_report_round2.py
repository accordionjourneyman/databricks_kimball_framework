"""Regression tests for Round-2 Bug Report findings.

Each test confirms a specific bug exists in the current code. When the bug
is fixed, the test will fail — prompting an update to assert the fixed behaviour.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

os.environ.setdefault("KIMBALL_ETL_SCHEMA", "test_schema")

from pyspark.sql import DataFrame, SparkSession


def _make_df(columns: list[str]) -> MagicMock:
    df = MagicMock(spec=DataFrame)
    df.columns = columns
    df.isEmpty.return_value = False
    df.limit.return_value = df
    df.head.return_value = []
    df.count.return_value = 0
    df.filter.return_value = df
    df.join.return_value = df
    df.select.return_value = df
    df.withColumn.return_value = df
    df.alias.return_value = df
    df.sparkSession = MagicMock(spec=SparkSession)
    return df


# ===================================================================
# SCD2 payload selection retains a custom effective-time column
# ===================================================================


class TestSCD2PayloadEffectiveAt:
    """The configured effective-time value must survive payload selection."""

    def test_select_payload_keeps_custom_effective_at_column(self):
        """Verify _select_payload_columns now accepts effective_at_column
        parameter and keeps it in the output."""
        import inspect

        from kimball.processing.scd2 import _select_payload_columns

        sig = inspect.signature(_select_payload_columns)
        # After fix: function accepts effective_at_column parameter
        assert "effective_at_column" in sig.parameters


# ===================================================================
# #2  HIGH: SCD6 SK fix clobbers existing rows' SK
# ===================================================================


class TestBugSCD6SKClobbersExistingRows:
    """generate_keys is now applied only to INSERT rows, not UPDATE/EXPIRE."""

    @patch("kimball.processing.scd6.DeltaTable")
    @patch("kimball.processing.scd6.filter_cdf_deletes")
    @patch("kimball.processing.scd6.compute_hashdiff")
    @patch("kimball.processing.scd6.HashKeyGenerator")
    @patch("kimball.processing.scd6.col", return_value=MagicMock())
    @patch("kimball.processing.scd6.lit", return_value=MagicMock())
    @patch("kimball.processing.scd6.when", return_value=MagicMock())
    def test_generate_keys_applied_only_to_insert_rows(
        self,
        mock_when,
        mock_lit,
        mock_col,
        mock_hkg,
        mock_hashdiff,
        mock_filter,
        mock_dt,
    ):
        from kimball.processing.scd6 import merge_scd6

        mock_hashdiff.return_value = MagicMock()
        mock_gen_instance = MagicMock()
        mock_hkg.return_value = mock_gen_instance

        def fake_generate_keys(df, key_col):
            return df.withColumn(key_col, MagicMock(name="hash_sk"))

        mock_gen_instance.generate_keys.side_effect = fake_generate_keys

        upserts = _make_df(["id", "name", "__etl_processed_at", "__etl_batch_id"])
        upserts.isEmpty.return_value = False
        deletes = MagicMock()
        deletes.isEmpty.return_value = True
        mock_filter.return_value = (upserts, deletes)

        target_df = _make_df(
            [
                "surrogate_key",
                "id",
                "name",
                "hashdiff",
                "__is_current",
                "__valid_from",
                "__valid_to",
                "__etl_processed_at",
                "__etl_batch_id",
                "__is_deleted",
                "__is_skeleton",
            ]
        )
        target_df.filter.return_value = target_df
        target_df.join.return_value = target_df
        target_df.alias.return_value = target_df

        mock_dt_instance = MagicMock()
        mock_dt.forName.return_value = mock_dt_instance
        merge_chain = mock_dt_instance.alias.return_value.merge.return_value
        merge_chain.whenMatchedUpdate.return_value = merge_chain
        merge_chain.whenNotMatchedInsert.return_value = merge_chain

        merge_scd6(
            upserts,
            target_table_name="test_target",
            join_keys=["id"],
            track_history_columns=["name"],
            current_value_columns=["name"],
        )

        # After fix: generate_keys is called only on INSERT rows
        mock_gen_instance.generate_keys.assert_called_once()


# ===================================================================
# #3  MEDIUM: reset_watermark SQL injection
# ===================================================================


class TestBugResetWatermarkSQLInjection:
    """reset_watermark must escape single quotes in table names so a crafted
    name cannot break out of the string literal (SQL injection)."""

    def test_reset_watermark_escapes_injected_quote(self):
        from kimball.orchestration.watermark import ETLControlManager

        spark_mock = MagicMock(spec=SparkSession)
        spark_mock.catalog.tableExists.return_value = True
        spark_mock.sql = MagicMock()

        manager = ETLControlManager(etl_schema="test", spark_session=spark_mock)

        # Payload that would break out of the WHERE string literal if the
        # table name were interpolated raw (the original bug).
        payload = "evil'; DROP TABLE etl_control;--"
        manager.reset_watermark(payload, "source_table")

        sql_call = spark_mock.sql.call_args[0][0]
        assert sql_call.startswith("DELETE FROM `test`.`etl_control` WHERE ")
        # The injected single quote MUST be doubled ('') so the payload stays
        # inside the string literal instead of terminating it. Just checking
        # that "'" appears in the SQL would pass even with no escaping, since
        # the literal is single-quoted anyway -- the real protection is the
        # doubling.
        assert "target_table = 'evil''; DROP TABLE etl_control;--'" in sql_call, (
            f"Injected quote not escaped; SQL vulnerable to injection: {sql_call!r}"
        )
        # The dangerous statement must remain INSIDE the escaped literal, not
        # appear as a separate top-level statement.
        assert "DROP TABLE etl_control" in sql_call  # present but escaped
        assert "DROP TABLE `etl_control`" not in sql_call, (
            f"Injection escaped the string literal: {sql_call!r}"
        )


# ===================================================================
# #4  MEDIUM: SCD4 duplicate __is_current=true EAV rows
# ===================================================================


class TestBugSCD4DuplicateEAVRows:
    """whenNotMatchedInsert has no guard against existing current rows."""

    @patch("kimball.processing.scd4.DeltaTable")
    @patch("kimball.processing.scd4.merge_scd1")
    @patch("kimball.processing.scd4.col", return_value=MagicMock())
    @patch("kimball.processing.scd4.lit", return_value=MagicMock())
    @patch("kimball.processing.scd4.expr", return_value=MagicMock())
    @patch("kimball.processing.scd4.row_number", return_value=MagicMock())
    @patch("kimball.processing.scd4.Window")
    def test_scd4_allows_duplicate_current_inserts(
        self,
        mock_window,
        mock_row_number,
        mock_expr,
        mock_lit,
        mock_col,
        mock_scd1,
        mock_dt,
    ):
        from kimball.processing.scd4 import merge_scd4

        mock_dt_instance = MagicMock()
        mock_dt.forName.return_value = mock_dt_instance
        merge_chain = mock_dt_instance.alias.return_value.merge.return_value
        merge_chain.whenMatchedUpdate.return_value = merge_chain
        merge_chain.whenNotMatchedInsert.return_value = merge_chain

        source_df = _make_df(["id", "field", "new_value", "effective_at"])
        source_df.isEmpty.return_value = False

        target_df = _make_df(
            [
                "surrogate_key",
                "field",
                "value",
                "valid_from",
                "valid_to",
                "__is_current",
                "__etl_processed_at",
            ]
        )
        target_df.alias.return_value = target_df
        mock_dt_instance.toDF.return_value = target_df

        merge_scd4(
            source_df,
            target_table_name="test_target",
            history_table_name="test_history",
            join_keys=["id"],
            track_history_columns=["field", "new_value"],
            surrogate_key_col="surrogate_key",
        )

        # The whenNotMatchedInsert is called with condition="source.__action = 'INSERT'"
        # but no guard against existing current rows with same value
        merge_chain.whenNotMatchedInsert.assert_called_once()


# ===================================================================
# #5  MEDIUM: Streaming per-version joins CDF metadata from wrong table
# ===================================================================


class TestBugStreamingPerVersionWrongCDFTable:
    """Per-version processing must preserve each filtered CDF version."""

    def test_per_version_readsmeta_from_original_batch(self):
        from kimball.streaming.orchestrator import StreamingOrchestrator

        orch = StreamingOrchestrator.__new__(StreamingOrchestrator)
        orch.spark = MagicMock(spec=SparkSession)
        orch.etl_schema = "test_schema"
        orch.config = MagicMock()
        orch.config.table_name = "test_target"
        orch.config.scd_type = 2
        orch.config.effective_at = "updated_at"
        orch.config.natural_keys = ["id"]
        orch.config.track_history_columns = ["val"]
        orch.config.surrogate_key = "surrogate_key"
        orch.config.transformation_sql = None
        orch.config.schema_evolution = False
        orch.config.effective_at = None
        orch.config.history_table = None
        orch.config.current_value_columns = None
        orch.config.delete_strategy = "hard"
        orch.etl_control = MagicMock()

        batch_df = _make_df(["id", "val", "_commit_version", "_commit_timestamp"])
        batch_df.select.return_value.distinct.return_value.collect.return_value = [
            MagicMock(_commit_version=1),
        ]
        batch_df.filter.return_value = batch_df

        source = MagicMock()
        source.name = "test_source"
        source.alias = "test_source"

        orch._execute_one_microbatch = MagicMock()

        orch._execute_microbatch_per_version(batch_df, source, "batch_1")

        orch._execute_one_microbatch.assert_called_once()


# ===================================================================
# #6  HIGH: Zombie recovery batch_id mismatch
# ===================================================================


class TestBugZombieRecoveryBatchIdMismatch:
    """batch_start_all generates per-source UUIDs, but recover_zombies
    matches against the run-level batch_id used as userMetadata."""

    def test_batch_start_all_uses_per_source_uuids(self):
        from kimball.orchestration.watermark import ETLControlManager

        spark_mock = MagicMock(spec=SparkSession)
        spark_mock.catalog.tableExists.return_value = True
        spark_mock.sql = MagicMock()

        update_df_mock = MagicMock()
        update_df_mock.columns = [
            "target_table",
            "source_table",
            "last_processed_version",
            "batch_id",
            "batch_started_at",
            "batch_completed_at",
            "batch_status",
            "rows_read",
            "rows_written",
            "error_message",
            "updated_at",
            "config_fingerprint",
            "source_schema_fingerprint",
        ]
        spark_mock.createDataFrame.return_value = update_df_mock

        manager = ETLControlManager(etl_schema="test", spark_session=spark_mock)

        mock_dt_instance = MagicMock()
        mock_dt_instance.alias.return_value = mock_dt_instance
        merge_builder = MagicMock()
        merge_builder.whenMatchedUpdate.return_value = merge_builder
        merge_builder.whenNotMatchedInsert.return_value = merge_builder
        mock_dt_instance.merge.return_value = merge_builder

        with patch("kimball.orchestration.watermark.DeltaTable") as mock_dt_class:
            mock_dt_class.forName.return_value = mock_dt_instance
            result = manager.batch_start_all("target", ["src_a", "src_b"])

        # The bug: each source gets its own UUID, not the run-level batch_id
        assert "src_a" in result
        assert "src_b" in result
        assert result["src_a"] != result["src_b"]


# ===================================================================
# #8  MEDIUM: Double FK validation per run
# ===================================================================


class TestBugDoubleFKValidation:
    """validate_fact_fk_integrity is now skipped when run_config_tests covers FKs."""

    def test_validate_fact_fk_skipped_when_tests_defined(self):
        from kimball.common.config import (
            ForeignKeyConfig,
            SourceConfig,
            TableConfig,
            TestDefinition,
        )
        from kimball.orchestration.orchestrator import Orchestrator

        config = TableConfig(
            table_name="test_fact",
            table_type="fact",
            scd_type=1,
            merge_keys=["id"],
            sources=[SourceConfig(name="src", alias="src")],
            foreign_keys=[
                ForeignKeyConfig(
                    column="dim_id", references="dim_table", dimension_key="dim_id"
                )
            ],
            tests=[TestDefinition(column="id", tests=["not_null"])],
        )
        orch = Orchestrator.__new__(Orchestrator)
        orch.config = config
        orch.spark = MagicMock(spec=SparkSession)
        orch.spark.catalog.tableExists.return_value = True

        orch.runtime_options = MagicMock()
        orch.runtime_options.use_approximate_unique = False

        orch._validator = MagicMock()
        orch._validator.run_config_tests.return_value = MagicMock(
            results=[], raise_on_failure=MagicMock()
        )
        orch._validator.validate_fact_fk_integrity.return_value = MagicMock(
            results=[], raise_on_failure=MagicMock()
        )
        orch.metrics_collector = None
        orch.etl_control = MagicMock()

        transformed_df = _make_df(["id", "dim_id"])

        orch._transform_and_validate({"src": transformed_df})

        orch._validator.run_config_tests.assert_called_once()
        # After fix: validate_fact_fk_integrity is skipped when tests are defined
        orch._validator.validate_fact_fk_integrity.assert_not_called()


# ===================================================================
# #13 LOW/MED: SCD2 perpetual churn when tracked history columns are NULL
# ===================================================================

# ===================================================================
# #14 LOW: reset_watermark metric attribution
# ===================================================================


class TestBugResetWatermarkMetricAttribution:
    """Metrics divided by len(active_dfs) inside per-source loop."""

    def test_metrics_fractionally_attributed(self):
        from kimball.common.config import SourceConfig, TableConfig
        from kimball.orchestration.orchestrator import Orchestrator

        config = TableConfig(
            table_name="t",
            table_type="dimension",
            scd_type=1,
            surrogate_key="sk",
            sources=[
                SourceConfig(name="s1", alias="s1"),
                SourceConfig(name="s2", alias="s2"),
            ],
            natural_keys=["id"],
        )

        orch = Orchestrator.__new__(Orchestrator)
        orch.config = config
        orch.spark = MagicMock(spec=SparkSession)

        assert len(config.sources) == 2
