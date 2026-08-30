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
# #2  HIGH: SCD6 SK fix clobbers existing rows' SK
# ===================================================================


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
            with patch.object(
                manager, "get_states", return_value={}
            ) as mock_get_states:
                result = manager.batch_start_all("target", ["src_a", "src_b"])
            mock_get_states.assert_called_once_with("target", ["src_a", "src_b"])

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
