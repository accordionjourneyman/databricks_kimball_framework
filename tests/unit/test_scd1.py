from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.processing.scd1 import merge_scd1


@pytest.fixture
def source_df():
    df = MagicMock()
    df.columns = ["id", "name", "_change_type"]
    df.alias.return_value = df
    df.withColumn.return_value = df
    return df


def _merge_builder():
    builder = MagicMock()
    builder.whenMatchedDelete.return_value = builder
    builder.whenMatchedUpdate.return_value = builder
    builder.whenNotMatchedInsert.return_value = builder
    return builder


def test_requires_join_keys(source_df) -> None:
    with pytest.raises(ValueError, match="join_keys must be provided"):
        merge_scd1(source_df, target_table_name="t", join_keys=[])


def test_uses_one_conditional_merge_without_hash_preflight(source_df) -> None:
    table = MagicMock()
    builder = _merge_builder()
    table.alias.return_value.merge.return_value = builder
    table.toDF.return_value.columns = ["id", "name", "__is_deleted"]

    with (
        patch("kimball.processing.scd1.get_spark", return_value=MagicMock()),
        patch("kimball.processing.scd1.DeltaTable.forName", return_value=table),
        patch("kimball.processing.scd1.dedup_cdf", return_value=source_df),
        patch("kimball.processing.scd1.apply_schema_evolution"),
        patch("kimball.processing.scd1.build_merge_condition", return_value="cond"),
    ):
        merge_scd1(source_df, target_table_name="t", join_keys=["id"])

    builder.execute.assert_called_once()
    condition = builder.whenMatchedUpdate.call_args.kwargs["condition"]
    assert "NOT (target.name <=> source.name)" in condition
    assert not hasattr(source_df, "isEmpty") or source_df.isEmpty.call_count == 0


def test_soft_cdf_delete_is_a_conditional_merge(source_df) -> None:
    table = MagicMock()
    builder = _merge_builder()
    table.alias.return_value.merge.return_value = builder
    table.toDF.return_value.columns = ["id", "name", "__is_deleted"]

    with (
        patch("kimball.processing.scd1.get_spark", return_value=MagicMock()),
        patch("kimball.processing.scd1.DeltaTable.forName", return_value=table),
        patch("kimball.processing.scd1.dedup_cdf", return_value=source_df),
        patch("kimball.processing.scd1.apply_schema_evolution"),
        patch("kimball.processing.scd1.build_merge_condition", return_value="cond"),
    ):
        merge_scd1(
            source_df,
            target_table_name="t",
            join_keys=["id"],
            delete_strategy="soft",
        )

    assert builder.whenMatchedUpdate.call_count == 2
    soft_delete = builder.whenMatchedUpdate.call_args_list[0].kwargs
    assert soft_delete["set"]["__etl_batch_id"] == "source.__etl_batch_id"
    builder.execute.assert_called_once()


def test_append_only_writes_without_delta_merge(source_df) -> None:
    writer = MagicMock()
    source_df.write = writer
    writer.format.return_value.mode.return_value.saveAsTable.return_value = None

    with (
        patch("kimball.processing.scd1.dedup_cdf", return_value=source_df),
        patch("kimball.processing.scd1.DeltaTable.forName") as delta_table,
    ):
        merge_scd1(
            source_df,
            target_table_name="t",
            join_keys=["id"],
            append_only=True,
        )

    delta_table.assert_not_called()
    writer.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
        "t"
    )
