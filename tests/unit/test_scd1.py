from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.processing.scd1 import merge_scd1


@pytest.fixture
def mock_df():
    df = MagicMock()
    df.columns = ["id", "name", "_change_type", "__etl_processed_at"]
    df.withColumn.return_value = df
    df.alias.return_value = df
    df.isEmpty.return_value = False
    return df


class TestMergeSCD1:
    def test_raises_without_join_keys(self, mock_df):
        with pytest.raises(ValueError, match="join_keys must be provided"):
            merge_scd1(mock_df, target_table_name="t", join_keys=[])

    def test_skips_merge_when_hashes_match(self, mock_df):
        mock_df.isEmpty.return_value = False
        mock_df.columns = ["id", "name"]
        delta_table = MagicMock()
        target_df = MagicMock()
        target_df.columns = ["id", "name"]
        delta_table.toDF.return_value = target_df
        source_hashed = MagicMock()
        source_hashed.columns = ["id", "name", "_hash"]
        target_hashed = MagicMock()
        target_hashed.columns = ["id", "name", "_hash"]
        mismatched = MagicMock()
        mismatched.limit.return_value.count.return_value = 0
        source_hashed.join.return_value.filter.return_value = mismatched
        mock_df.withColumn.return_value = source_hashed
        target_df.withColumn.return_value = target_hashed

        with (
            patch("kimball.processing.scd1.get_spark", return_value=MagicMock()),
            patch(
                "kimball.processing.scd1.DeltaTable.forName", return_value=delta_table
            ),
            patch("kimball.processing.scd1.dedup_cdf", return_value=mock_df),
            patch("kimball.processing.scd1.build_merge_condition", return_value="cond"),
            patch("kimball.processing.scd1.compute_hashdiff", return_value=MagicMock()),
        ):
            merge_scd1(mock_df, target_table_name="t", join_keys=["id"])
        delta_table.alias.assert_not_called()

    def test_executes_merge_when_hashes_differ(self, mock_df):
        mock_df.isEmpty.return_value = False
        mock_df.columns = ["id", "name"]
        delta_table = MagicMock()
        merge_builder = MagicMock()
        delta_table.alias.return_value.merge.return_value = merge_builder
        merge_builder.whenMatchedUpdate.return_value = merge_builder
        merge_builder.whenNotMatchedInsert.return_value = merge_builder
        target_df = MagicMock()
        target_df.columns = ["id", "name"]
        delta_table.toDF.return_value = target_df
        source_hashed = MagicMock()
        source_hashed.columns = ["id", "name", "_hash"]
        target_hashed = MagicMock()
        target_hashed.columns = ["id", "name", "_hash"]
        mismatched = MagicMock()
        mismatched.limit.return_value.count.return_value = 1
        source_hashed.join.return_value.filter.return_value = mismatched
        mock_df.withColumn.return_value = source_hashed
        target_df.withColumn.return_value = target_hashed

        with (
            patch("kimball.processing.scd1.get_spark", return_value=MagicMock()),
            patch(
                "kimball.processing.scd1.DeltaTable.forName", return_value=delta_table
            ),
            patch("kimball.processing.scd1.dedup_cdf", return_value=mock_df),
            patch("kimball.processing.scd1.build_merge_condition", return_value="cond"),
            patch("kimball.processing.scd1.compute_hashdiff", return_value=MagicMock()),
            patch("kimball.processing.scd1.apply_schema_evolution"),
            patch("kimball.processing.scd1.generate_keys", return_value=mock_df),
        ):
            merge_scd1(mock_df, target_table_name="t", join_keys=["id"])
        merge_builder.execute.assert_called_once()

    def test_handles_pyspark_exception_in_hash_check(self, mock_df):
        from pyspark.errors import PySparkException

        mock_df.isEmpty.return_value = False
        mock_df.columns = ["id", "name"]
        delta_table = MagicMock()
        delta_table.toDF.side_effect = PySparkException("spark error")
        merge_builder = MagicMock()
        delta_table.alias.return_value.merge.return_value = merge_builder
        merge_builder.whenMatchedUpdate.return_value = merge_builder
        merge_builder.whenNotMatchedInsert.return_value = merge_builder

        with (
            patch("kimball.processing.scd1.get_spark", return_value=MagicMock()),
            patch(
                "kimball.processing.scd1.DeltaTable.forName", return_value=delta_table
            ),
            patch("kimball.processing.scd1.dedup_cdf", return_value=mock_df),
            patch("kimball.processing.scd1.build_merge_condition", return_value="cond"),
            patch("kimball.processing.scd1.apply_schema_evolution"),
            patch("kimball.processing.scd1.generate_keys", return_value=mock_df),
        ):
            merge_scd1(mock_df, target_table_name="t", join_keys=["id"])
        merge_builder.execute.assert_called_once()

    def test_handles_cdf_delete_strategy_soft(self, mock_df):
        mock_df.columns = ["id", "name", "_change_type"]
        delta_table = MagicMock()
        merge_builder = MagicMock()
        delta_table.alias.return_value.merge.return_value = merge_builder
        merge_builder.whenMatchedUpdate.return_value = merge_builder
        merge_builder.whenNotMatchedInsert.return_value = merge_builder

        with (
            patch("kimball.processing.scd1.get_spark", return_value=MagicMock()),
            patch(
                "kimball.processing.scd1.DeltaTable.forName", return_value=delta_table
            ),
            patch("kimball.processing.scd1.dedup_cdf", return_value=mock_df),
            patch("kimball.processing.scd1.build_merge_condition", return_value="cond"),
            patch("kimball.processing.scd1.apply_schema_evolution"),
            patch("kimball.processing.scd1.generate_keys", return_value=mock_df),
        ):
            merge_scd1(
                mock_df, target_table_name="t", join_keys=["id"], delete_strategy="soft"
            )
        merge_builder.execute.assert_called_once()

    def test_skips_merge_when_deduped_cdf_is_empty(self, mock_df):
        empty_df = MagicMock()
        empty_df.columns = ["id", "name", "_change_type"]
        empty_df.isEmpty.return_value = True
        empty_df.alias.return_value = empty_df
        delta_table = MagicMock()

        with (
            patch("kimball.processing.scd1.get_spark", return_value=MagicMock()),
            patch(
                "kimball.processing.scd1.DeltaTable.forName", return_value=delta_table
            ),
            patch("kimball.processing.scd1.dedup_cdf", return_value=empty_df),
            patch("kimball.processing.scd1.build_merge_condition", return_value="cond"),
        ):
            merge_scd1(mock_df, target_table_name="t", join_keys=["id"])
        delta_table.alias.assert_not_called()

    def test_executes_merge_when_deduped_cdf_has_rows(self, mock_df):
        mock_df.columns = ["id", "name", "_change_type"]
        mock_df.isEmpty.return_value = False
        delta_table = MagicMock()
        merge_builder = MagicMock()
        delta_table.alias.return_value.merge.return_value = merge_builder
        merge_builder.whenMatchedDelete.return_value = merge_builder
        merge_builder.whenMatchedUpdate.return_value = merge_builder
        merge_builder.whenNotMatchedInsert.return_value = merge_builder

        with (
            patch("kimball.processing.scd1.get_spark", return_value=MagicMock()),
            patch(
                "kimball.processing.scd1.DeltaTable.forName", return_value=delta_table
            ),
            patch("kimball.processing.scd1.dedup_cdf", return_value=mock_df),
            patch("kimball.processing.scd1.build_merge_condition", return_value="cond"),
            patch("kimball.processing.scd1.apply_schema_evolution"),
            patch("kimball.processing.scd1.generate_keys", return_value=mock_df),
        ):
            merge_scd1(mock_df, target_table_name="t", join_keys=["id"])
        merge_builder.execute.assert_called_once()


class TestMergeSCD1AppendOnly:
    def test_append_only_uses_insert_into(self, mock_df):
        mock_df.columns = ["id", "name", "_change_type"]
        writer = MagicMock()
        mock_df.write = writer
        writer.format.return_value.mode.return_value.saveAsTable.return_value = None

        with (
            patch("kimball.processing.scd1.get_spark", return_value=MagicMock()),
            patch("kimball.processing.scd1.DeltaTable.forName") as mock_delta_for_name,
            patch("kimball.processing.scd1.dedup_cdf", return_value=mock_df),
        ):
            merge_scd1(
                mock_df, target_table_name="t", join_keys=["id"], append_only=True
            )
        mock_delta_for_name.assert_not_called()
        writer.format.assert_called_once_with("delta")
        writer.format.return_value.mode.assert_called_once_with("append")
        writer.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
            "t"
        )

    def test_append_only_skips_empty_source(self, mock_df):
        empty_df = MagicMock()
        empty_df.columns = ["id", "name"]
        empty_df.isEmpty.return_value = True
        empty_df.write = MagicMock()
        empty_df.alias.return_value = empty_df

        with (
            patch("kimball.processing.scd1.get_spark", return_value=MagicMock()),
            patch("kimball.processing.scd1.DeltaTable.forName") as mock_delta_for_name,
            patch("kimball.processing.scd1.dedup_cdf", return_value=empty_df),
        ):
            merge_scd1(
                mock_df, target_table_name="t", join_keys=["id"], append_only=True
            )
        mock_delta_for_name.assert_not_called()
        empty_df.write.format.assert_not_called()

    def test_not_append_only_uses_merge(self, mock_df):
        mock_df.columns = ["id", "name"]
        mock_df.isEmpty.return_value = False
        delta_table = MagicMock()
        merge_builder = MagicMock()
        delta_table.alias.return_value.merge.return_value = merge_builder
        merge_builder.whenMatchedUpdate.return_value = merge_builder
        merge_builder.whenNotMatchedInsert.return_value = merge_builder

        with (
            patch("kimball.processing.scd1.get_spark", return_value=MagicMock()),
            patch(
                "kimball.processing.scd1.DeltaTable.forName", return_value=delta_table
            ),
            patch("kimball.processing.scd1.dedup_cdf", return_value=mock_df),
            patch("kimball.processing.scd1.build_merge_condition", return_value="cond"),
            patch("kimball.processing.scd1.apply_schema_evolution"),
        ):
            merge_scd1(
                mock_df, target_table_name="t", join_keys=["id"], append_only=False
            )
        merge_builder.execute.assert_called_once()
        mock_df.write.format.assert_not_called()
