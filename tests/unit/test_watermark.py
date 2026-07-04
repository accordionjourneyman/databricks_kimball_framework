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


def test_batch_start_records_running_state_and_returns_uuid(manager):
    manager._upsert_control_record = MagicMock()

    batch_id = manager.batch_start("target", "source")

    assert len(batch_id) > 0
    manager._upsert_control_record.assert_called_once()
    args, kwargs = manager._upsert_control_record.call_args
    updates = kwargs["updates"] if "updates" in kwargs else args[2]
    assert updates["batch_status"] == "RUNNING"
    assert updates["batch_id"] == batch_id
    assert updates["rows_read"] is None
    assert updates["rows_written"] is None


def test_batch_complete_updates_watermark_and_metrics(manager):
    manager._upsert_control_record = MagicMock()

    manager.batch_complete("target", "source", new_version=42, rows_read=10, rows_written=4)

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


def test_get_batch_status_returns_current_state(manager, spark_mock):
    spark_mock.table.return_value.filter.return_value.first.return_value = Row(
        batch_id="batch-1",
        batch_status="RUNNING",
        batch_started_at="2024-01-01 00:00:00",
        batch_completed_at=None,
        last_processed_version=7,
        rows_read=100,
        rows_written=50,
        error_message=None,
    )

    status = manager.get_batch_status("target", "source")

    assert status is not None
    assert status["batch_id"] == "batch-1"
    assert status["batch_status"] == "RUNNING"
    assert status["last_processed_version"] == 7
    assert status["rows_read"] == 100
    assert status["rows_written"] == 50


def test_get_running_batches_filters_only_running_records(manager, spark_mock):
    spark_mock.table.return_value.filter.return_value.select.return_value.collect.return_value = [
        Row(batch_id="batch-1", source_table="source_a"),
        Row(batch_id=None, source_table="source_b"),
    ]

    assert manager.get_running_batches("target") == [
        {"batch_id": "batch-1", "source_table": "source_a"}
    ]


@patch("kimball.orchestration.watermark.DeltaTable.forName")
def test_control_table_supports_independent_concurrent_updates(mock_for_name, manager, spark_mock):
    store = {}

    class FakeDataFrame:
        def __init__(self, rows, schema):
            self._rows = rows
            self.columns = [field.name for field in schema.fields]

        def alias(self, *_args, **_kwargs):
            return self

    class FakeMergeBuilder:
        def __init__(self, rows, store):
            self._rows = rows
            self._store = store

        def whenMatchedUpdate(self, set=None):
            self._set = set
            return self

        def whenNotMatchedInsert(self, values=None):
            self._values = values
            return self

        def execute(self):
            for row in self._rows:
                key = (row["target_table"], row["source_table"])
                if key in self._store:
                    self._store[key].update({k: v for k, v in row.items() if v is not None})
                else:
                    self._store[key] = dict(row)

    class FakeDeltaTable:
        def __init__(self, store):
            self._store = store

        def alias(self, *_args, **_kwargs):
            return self

        def merge(self, update_df, _join_condition):
            return FakeMergeBuilder(update_df._rows, self._store)

    mock_for_name.return_value = FakeDeltaTable(store)
    spark_mock.createDataFrame.side_effect = lambda rows, schema=None: FakeDataFrame(rows, schema)

    manager.update_watermark("dim_customer", "orders", 10)
    manager.update_watermark("dim_product", "orders", 20)

    assert len(store) == 2
    assert store[("dim_customer", "orders")]["last_processed_version"] == 10
    assert store[("dim_product", "orders")]["last_processed_version"] == 20


def test_control_table_keeps_separate_status_for_each_concurrent_job(manager, spark_mock):
    spark_mock.table.return_value.filter.return_value.first.side_effect = [
        Row(batch_id="batch-1", batch_status="RUNNING", batch_started_at=None, batch_completed_at=None, last_processed_version=None, rows_read=None, rows_written=None, error_message=None),
        Row(batch_id="batch-2", batch_status="RUNNING", batch_started_at=None, batch_completed_at=None, last_processed_version=None, rows_read=None, rows_written=None, error_message=None),
    ]

    status_a = manager.get_batch_status("dim_customer", "orders")
    status_b = manager.get_batch_status("dim_product", "orders")

    assert status_a["batch_id"] == "batch-1"
    assert status_b["batch_id"] == "batch-2"
    assert status_a["batch_status"] == "RUNNING"
    assert status_b["batch_status"] == "RUNNING"


@patch("kimball.orchestration.watermark.DeltaTable.forName")
def test_upsert_control_records_uses_union_of_update_keys(mock_for_name, manager, spark_mock):
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
            {"target_table": "t", "source_table": "s", "rows_read": 10, "rows_written": 5},
        ]
    )

    update_kwargs = merge_builder.whenMatchedUpdate.call_args.kwargs
    assert update_kwargs["set"]["batch_id"] == "u.batch_id"
    assert update_kwargs["set"]["rows_read"] == "u.rows_read"
    assert update_kwargs["set"]["rows_written"] == "u.rows_written"
