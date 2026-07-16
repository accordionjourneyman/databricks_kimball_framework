from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from kimball.processing.scd1 import merge_scd1


@pytest.fixture
def spark_mock():
    return MagicMock(spec=SparkSession)


def _wire_delta(mock_delta_table):
    """Configure the patched DeltaTable so its builder chain is capturable.

    Every chained method (alias/merge/whenMatched*/whenNotMatched*/execute)
    returns the same instance, mirroring DeltaTable's real fluent builder.
    Returns the instance whose calls the tests assert against.
    """
    mock_dt = MagicMock()
    mock_delta_table.forName.return_value = mock_dt
    mock_dt.alias.return_value = mock_dt
    mock_dt.merge.return_value = mock_dt
    # Keep the entire fluent builder on the same instance so every stage's
    # call args are capturable on mock_dt (otherwise chained calls land on
    # auto-created child mocks and the assertions miss them).
    mock_dt.whenMatchedDelete.return_value = mock_dt
    mock_dt.whenMatchedUpdate.return_value = mock_dt
    mock_dt.whenNotMatchedInsert.return_value = mock_dt
    mock_dt.toDF.return_value.schema.fields = []
    return mock_dt


def _make_source_df(columns):
    mock_df = MagicMock()
    mock_df.columns = columns
    mock_df.withColumn.return_value = mock_df
    mock_df.alias.return_value = mock_df
    mock_df.isEmpty.return_value = False
    return mock_df


@patch("kimball.processing.scd1.generate_keys")
@patch("kimball.processing.scd1.dedup_cdf")
@patch("kimball.processing.scd1.DeltaTable")
@patch("kimball.processing.scd1.get_spark")
def test_merge_uses_correct_merge_condition(
    mock_get_spark, mock_delta_table, mock_dedup, mock_generate_keys
):
    """The MERGE must join target<=>source on the configured join keys."""
    mock_dt = _wire_delta(mock_delta_table)
    mock_df = _make_source_df(["id", "val"])
    mock_dedup.return_value = mock_df
    mock_generate_keys.return_value = mock_df

    merge_scd1(mock_df, target_table_name="target", join_keys=["id"], delete_strategy="hard")

    merge_args = mock_dt.merge.call_args
    condition = merge_args.args[1]
    assert condition == "target.id <=> source.id"


@patch("kimball.processing.scd1.generate_keys")
@patch("kimball.processing.scd1.dedup_cdf")
@patch("kimball.processing.scd1.DeltaTable")
@patch("kimball.processing.scd1.get_spark")
def test_hard_delete_emits_when_matched_delete(
    mock_get_spark, mock_delta_table, mock_dedup, mock_generate_keys
):
    """Hard delete strategy must emit a whenMatchedDelete for delete changes."""
    mock_dt = _wire_delta(mock_delta_table)
    mock_df = _make_source_df(["id", "val", "_change_type", "__etl_processed_at"])
    mock_dedup.return_value = mock_df
    mock_generate_keys.return_value = mock_df

    merge_scd1(mock_df, target_table_name="target", join_keys=["id"], delete_strategy="hard")

    mock_dt.whenMatchedDelete.assert_called_once_with(condition="source._change_type = 'delete'")


@patch("kimball.processing.scd1.generate_keys")
@patch("kimball.processing.scd1.dedup_cdf")
@patch("kimball.processing.scd1.DeltaTable")
@patch("kimball.processing.scd1.get_spark")
def test_update_and_insert_maps_mark_not_deleted(
    mock_get_spark, mock_delta_table, mock_dedup, mock_generate_keys
):
    """Matched updates and inserts must map data columns and reset __is_deleted."""
    mock_dt = _wire_delta(mock_delta_table)
    mock_df = _make_source_df(["id", "val", "_change_type", "__etl_processed_at"])
    mock_dedup.return_value = mock_df
    mock_generate_keys.return_value = mock_df

    merge_scd1(mock_df, target_table_name="target", join_keys=["id"], delete_strategy="hard")

    update_call = mock_dt.whenMatchedUpdate.call_args
    assert update_call.kwargs["condition"] == "source._change_type != 'delete'"
    update_set = update_call.kwargs["set"]
    assert update_set["val"] == "source.val"
    assert update_set["__is_deleted"] == "false"
    assert update_set["__etl_processed_at"] == "current_timestamp()"
    assert "_change_type" not in update_set  # cdf metadata excluded from updates

    insert_call = mock_dt.whenNotMatchedInsert.call_args
    assert insert_call.kwargs["condition"] == "source._change_type != 'delete'"
    insert_values = insert_call.kwargs["values"]
    assert insert_values["val"] == "source.val"
    assert insert_values["__is_deleted"] == "false"
    assert insert_values["__etl_processed_at"] == "current_timestamp()"

    # The whole fluent chain must terminate in execute().
    mock_dt.execute.assert_called_once()


@patch("kimball.processing.scd1.generate_keys")
@patch("kimball.processing.scd1.dedup_cdf")
@patch("kimball.processing.scd1.DeltaTable")
@patch("kimball.processing.scd1.get_spark")
def test_soft_delete_emits_update_not_delete(
    mock_get_spark, mock_delta_table, mock_dedup, mock_generate_keys
):
    """Soft delete strategy must tombstone (update __is_deleted) not hard-delete."""
    mock_dt = _wire_delta(mock_delta_table)
    mock_df = _make_source_df(["id", "val", "_change_type", "__etl_processed_at"])
    mock_dedup.return_value = mock_df
    mock_generate_keys.return_value = mock_df

    merge_scd1(mock_df, target_table_name="target", join_keys=["id"], delete_strategy="soft")

    mock_dt.whenMatchedDelete.assert_not_called()
    # Soft delete calls whenMatchedUpdate twice: first the tombstone (delete
    # changes -> __is_deleted=true), then the regular update. call_args holds
    # the LAST call, so the tombstone is the first entry of call_args_list.
    soft_delete_call = mock_dt.whenMatchedUpdate.call_args_list[0]
    assert soft_delete_call.kwargs["condition"] == "source._change_type = 'delete'"
    assert soft_delete_call.kwargs["set"]["__is_deleted"] == "true"


@patch("kimball.processing.scd1.generate_keys")
@patch("kimball.processing.scd1.dedup_cdf")
@patch("kimball.processing.scd1.DeltaTable")
@patch("kimball.processing.scd1.get_spark")
def test_no_change_type_skips_delete_clause(
    mock_get_spark, mock_delta_table, mock_dedup, mock_generate_keys
):
    """Without _change_type there is no delete path and no update condition."""
    mock_dt = _wire_delta(mock_delta_table)
    mock_df = _make_source_df(["id", "val"])
    mock_dedup.return_value = mock_df
    mock_generate_keys.return_value = mock_df

    merge_scd1(mock_df, target_table_name="target", join_keys=["id"], delete_strategy="hard")

    mock_dt.whenMatchedDelete.assert_not_called()
    update_call = mock_dt.whenMatchedUpdate.call_args
    assert update_call.kwargs["condition"] is None  # no delete filter without cdf


def test_missing_join_keys_raises():
    with pytest.raises(ValueError, match="join_keys must be provided"):
        merge_scd1(MagicMock(), target_table_name="target", join_keys=[])