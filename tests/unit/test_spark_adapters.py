"""Unit tests for kimball.ops.spark_adapters (mock-backed, no JVM needed)."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from unittest.mock import MagicMock, patch

from kimball.ops.spark_adapters import (
    SparkDeltaHistoryProvider,
    SparkETLControlStore,
    SparkSourceMetadataProvider,
)


def _mk_ctl() -> Any:
    ctl = MagicMock()
    ctl.fq_table = "etl.etl_control"
    return ctl


class TestSparkETLControlStore:
    def test_control_table_exists_true(self):
        ctl = _mk_ctl()
        ctl.spark.catalog.tableExists.return_value = True
        assert SparkETLControlStore(ctl).control_table_exists() is True

    def test_control_table_exists_false_on_error(self):
        ctl = _mk_ctl()
        ctl.spark.catalog.tableExists.side_effect = RuntimeError("boom")
        assert SparkETLControlStore(ctl).control_table_exists() is False

    def test_get_target_state_when_table_missing(self):
        ctl = _mk_ctl()
        ctl.spark.catalog.tableExists.return_value = False
        state = SparkETLControlStore(ctl).get_target_state("t1")
        assert state.control_table_exists is False
        assert state.batches == ()

    def test_get_target_state_maps_batches(self):
        ctl = _mk_ctl()
        ctl.spark.catalog.tableExists.return_value = True
        row = {
            "batch_id": "b-1",
            "source_table": "orders",
            "batch_status": "SUCCESS",
            "last_processed_version": 7,
            "batch_started_at": None,
            "previous_success_watermark": 3,
            "batch_completed_at": None,
            "error_message": None,
            "config_fingerprint": "cfp",
            "source_schema_fingerprint": "sfp",
        }
        rows_df = MagicMock()
        rows_df.collect.return_value = [row]
        ctl.spark.sql.return_value = rows_df
        state = SparkETLControlStore(ctl).get_target_state("t1")
        assert state.control_table_exists is True
        assert len(state.batches) == 1
        batch = state.batches[0]
        assert batch.batch_id == "b-1"
        assert batch.status == "SUCCESS"
        assert batch.last_processed_version == 7
        assert batch.previous_success_watermark == 3
        assert batch.config_fingerprint == "cfp"

    def test_set_batch_failed_delegates(self):
        ctl = _mk_ctl()
        SparkETLControlStore(ctl).set_batch_failed("t1", "orders", "err")
        ctl.batch_fail.assert_called_once_with("t1", "orders", "err")

    def test_rewind_watermark_none_resets(self):
        ctl = _mk_ctl()
        SparkETLControlStore(ctl).rewind_watermark("t1", "orders", None)
        ctl.reset_watermark.assert_called_once_with("t1", "orders")

    def test_rewind_watermark_version_rewinds(self):
        ctl = _mk_ctl()
        SparkETLControlStore(ctl).rewind_watermark("t1", "orders", 5)
        ctl.rewind_to_version.assert_called_once_with("t1", "orders", 5)


class TestSparkDeltaHistoryProvider:
    def test_missing_table_returns_not_exists(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        state = SparkDeltaHistoryProvider(spark).get_target_delta_state("t1")
        assert state.table_exists is False
        assert state.current_version is None
        assert not state.commits

    def test_history_rows_become_commits(self):

        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        from datetime import datetime

        commit = {
            "version": 3,
            "timestamp": datetime(2025, 1, 1),
            "operation": "MERGE",
            "userMetadata": "b-2",
        }
        hist = MagicMock()
        hist.collect.return_value = [commit]
        with patch("delta.tables.DeltaTable.forName") as for_name:
            for_name.return_value.history.return_value = hist
            state = SparkDeltaHistoryProvider(spark).get_target_delta_state("t1")
        assert state.table_exists is True
        assert state.current_version == 3
        assert len(state.commits) == 1
        assert state.commits[0].batch_id == "b-2"

    def test_extract_batch_id_compound(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        commit = {
            "version": 5,
            "timestamp": None,
            "operation": "MERGE",
            "userMetadata": "run=abc; batch_id=b-9",
        }
        hist = MagicMock()
        hist.collect.return_value = [commit]
        with patch("delta.tables.DeltaTable.forName") as for_name:
            for_name.return_value.history.return_value = hist
            state = SparkDeltaHistoryProvider(spark).get_target_delta_state("t1")
        assert state.commits[0].batch_id == "b-9"

    def test_restore_to_version_executes_sql(self):
        spark = MagicMock()
        SparkDeltaHistoryProvider(spark).restore_to_version("t1", 4)
        assert "VERSION AS OF 4" in spark.sql.call_args.args[0]

    def test_restore_to_timestamp_executes_sql(self):

        spark = MagicMock()
        SparkDeltaHistoryProvider(spark).restore_to_timestamp(
            "t1", datetime(2025, 6, 1)
        )
        assert "TIMESTAMP AS OF" in spark.sql.call_args.args[0]


class TestSparkSourceMetadataProvider:
    def test_missing_table_reports_not_exists(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        report = SparkSourceMetadataProvider(spark).get_source_health("orders", 4)
        assert report.exists is False
        assert report.watermark_version == 4
        assert report.cdf_enabled is None

    def test_existing_table_reports_cdf_and_fingerprint(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        df = MagicMock()
        df.columns = ["id", "amount", "_change_type"]
        df.schema.json.return_value = '{"a":"long"}'
        detail = MagicMock()
        detail.collect.return_value = [
            {"key": "delta.enableChangeDataFeed", "value": "true"}
        ]
        spark.sql.return_value = detail
        spark.table.return_value = df
        report = SparkSourceMetadataProvider(spark).get_source_health("orders", 4)
        assert report.exists is True
        assert report.cdf_enabled is True
        assert report.current_schema_fingerprint is not None
