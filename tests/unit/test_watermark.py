"""Unit tests for ETLControlManager watermark and batch lifecycle behavior."""

import os
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row, SparkSession

os.environ["KIMBALL_ETL_SCHEMA"] = "test_schema"

from kimball.orchestration.watermark import ETLControlManager


@pytest.fixture
def spark_mock():
    mock = MagicMock(spec=SparkSession)
    mock.catalog.tableExists.return_value = True
    mock.sql = MagicMock()
    return mock


@pytest.fixture
def manager(spark_mock):
    return ETLControlManager(etl_schema="test_schema", spark_session=spark_mock)


def test_ensure_table_exists_creates_table(spark_mock):
    spark_mock.catalog.tableExists.return_value = False

    ETLControlManager(etl_schema="test_schema", spark_session=spark_mock)

    assert spark_mock.sql.call_count >= 1


@patch("kimball.orchestration.watermark.col")
def test_get_watermark_returns_value(mock_col, manager, spark_mock):
    mock_df = MagicMock()
    spark_mock.table.return_value = mock_df
    mock_df.filter.return_value.select.return_value.first.return_value = Row(
        last_processed_version=100
    )

    assert manager.get_watermark("fact_sales", "dim_customer") == 100


@patch("kimball.orchestration.watermark.col")
def test_get_watermark_returns_none(mock_col, manager, spark_mock):
    mock_df = MagicMock()
    spark_mock.table.return_value = mock_df
    mock_df.filter.return_value.select.return_value.first.return_value = None

    assert manager.get_watermark("fact_sales", "dim_customer") is None


def test_env_var_schema(spark_mock):
    os.environ["KIMBALL_ETL_SCHEMA"] = "from_env"

    manager = ETLControlManager(spark_session=spark_mock)

    assert manager.schema == "from_env"

    os.environ["KIMBALL_ETL_SCHEMA"] = "test_schema"


def test_batch_complete_updates_watermark_and_metrics(manager):
    manager._upsert_control_record = MagicMock()

    manager.batch_complete(
        "target", "source", new_version=42, rows_read=10, rows_written=4
    )

    manager._upsert_control_record.assert_called_once()
    args, kwargs = manager._upsert_control_record.call_args
    updates = kwargs["updates"] if "updates" in kwargs else args[2]
    assert updates["last_processed_version"] == 42
    assert updates["batch_status"] == "SUCCESS"
    assert updates["rows_read"] == 10
    assert updates["rows_written"] == 4


def test_batch_fail_records_failure_without_updating_watermark(manager):
    manager._upsert_control_record = MagicMock()

    manager.batch_fail("target", "source", "boom")

    manager._upsert_control_record.assert_called_once()
    args, kwargs = manager._upsert_control_record.call_args
    updates = kwargs["updates"] if "updates" in kwargs else args[2]
    assert updates["batch_status"] == "FAILED"
    assert updates["error_message"] == "boom"
    assert "last_processed_version" not in updates


def test_batch_complete_all_uses_one_control_merge(manager):
    manager._upsert_control_records = MagicMock()

    manager.batch_complete_all(
        "target",
        [
            {
                "source_table": "source_a",
                "new_version": 4,
                "rows_read": 10,
                "rows_written": 5,
                "config_fingerprint": "cfg",
                "source_schema_fingerprint": "schema-a",
            },
            {
                "source_table": "source_b",
                "new_version": 9,
                "rows_read": 10,
                "rows_written": 5,
                "config_fingerprint": "cfg",
                "source_schema_fingerprint": "schema-b",
            },
        ],
    )

    manager._upsert_control_records.assert_called_once()
    records = manager._upsert_control_records.call_args.args[0]
    assert {record["last_processed_version"] for record in records} == {4, 9}
    assert all(record["batch_status"] == "SUCCESS" for record in records)


def test_batch_start_all_records_every_source(manager):
    manager._upsert_control_records = MagicMock()

    batch_ids = manager.batch_start_all("target", ["source_a", "source_b"])

    assert set(batch_ids) == {"source_a", "source_b"}
    manager._upsert_control_records.assert_called_once()
    records = manager._upsert_control_records.call_args.args[0]
    assert len(records) == 2
    assert {record["source_table"] for record in records} == {"source_a", "source_b"}
    assert all(record["target_table"] == "target" for record in records)
    assert all(record["batch_status"] == "RUNNING" for record in records)


@patch("kimball.orchestration.watermark.F")
@patch("kimball.orchestration.watermark.current_timestamp")
@patch("kimball.orchestration.watermark.col")
def test_get_running_batches_filters_only_running_records(
    mock_col, mock_cts, mock_F, manager, spark_mock
):
    cts_result = MagicMock()
    cts_result.__sub__ = MagicMock(return_value=cts_result)
    mock_cts.return_value = cts_result
    mock_F.expr.return_value = MagicMock()

    col_result = MagicMock()
    col_result.__eq__ = MagicMock(return_value=col_result)
    col_result.__lt__ = MagicMock(return_value=col_result)
    col_result.__and__ = MagicMock(return_value=col_result)
    mock_col.return_value = col_result

    table_mock = MagicMock()
    table_mock.filter.return_value = table_mock
    table_mock.select.return_value = table_mock
    table_mock.collect.return_value = [
        Row(batch_id="batch-1", source_table="source_a"),
        Row(batch_id=None, source_table="source_b"),
    ]
    spark_mock.table.return_value = table_mock

    result = manager.get_running_batches("target")

    assert len(result) == 1
    assert result[0] == {"batch_id": "batch-1", "source_table": "source_a"}
    spark_mock.table.return_value.filter.assert_called_once()
    spark_mock.table.return_value.select.assert_called_once_with(
        "batch_id", "source_table"
    )


@patch("kimball.orchestration.watermark.DeltaTable.forName")
def test_upsert_control_records_uses_union_of_update_keys(
    mock_for_name, manager, spark_mock
):
    delta_table = MagicMock()
    mock_for_name.return_value = delta_table

    merge_builder = MagicMock()
    delta_table.alias.return_value.merge.return_value = merge_builder
    merge_builder.whenMatchedUpdate.return_value = merge_builder
    merge_builder.whenNotMatchedInsert.return_value = merge_builder

    update_df = MagicMock()
    update_df.alias.return_value = update_df
    update_df.columns = [
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
    ]
    spark_mock.createDataFrame.return_value = update_df

    manager._upsert_control_records(
        [
            {"target_table": "t", "source_table": "s", "batch_id": "b1"},
            {
                "target_table": "t",
                "source_table": "s",
                "rows_read": 10,
                "rows_written": 5,
            },
        ]
    )

    update_kwargs = merge_builder.whenMatchedUpdate.call_args.kwargs
    assert update_kwargs["set"]["batch_id"] == "u.batch_id"
    assert update_kwargs["set"]["rows_read"] == "u.rows_read"
    assert update_kwargs["set"]["rows_written"] == "u.rows_written"


def test_batch_start_all_preserves_previous_success_watermark(manager):
    manager._upsert_control_records = MagicMock()
    manager.get_states = MagicMock(
        return_value={"source_a": {"last_processed_version": 11}}
    )

    manager.batch_start_all("target", ["source_a", "source_b"])

    records = manager._upsert_control_records.call_args.args[0]
    by_source = {record["source_table"]: record for record in records}
    assert by_source["source_a"]["previous_success_watermark"] == 11
    assert by_source["source_b"]["previous_success_watermark"] is None


@patch("kimball.orchestration.watermark.F")
@patch("kimball.orchestration.watermark.current_timestamp")
@patch("kimball.orchestration.watermark.col")
def test_get_running_batches_without_ttl_includes_fresh_batches(
    mock_col, mock_current_timestamp, mock_F, manager, spark_mock
):
    predicate = MagicMock()
    predicate.__and__ = MagicMock(return_value=predicate)
    mock_col.return_value.__eq__.return_value = predicate

    table = MagicMock()
    table.filter.return_value.select.return_value.collect.return_value = [
        Row(batch_id="fresh-batch", source_table="source_a")
    ]
    spark_mock.table.return_value = table

    assert manager.get_running_batches("target", ttl_minutes=None) == [
        {"batch_id": "fresh-batch", "source_table": "source_a"}
    ]
    mock_current_timestamp.assert_not_called()
    mock_F.expr.assert_not_called()
