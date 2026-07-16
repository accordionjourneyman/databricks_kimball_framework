from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from kimball.processing.late_arriving_dimension import LateArrivingDimensionProcessor


@pytest.fixture
def spark():
    return MagicMock()


@pytest.fixture
def processor(spark):
    return LateArrivingDimensionProcessor(spark)


class TestUpdateSkeletonsWithRealData:
    def test_skips_when_table_not_exists(self, processor, spark):
        spark.catalog.tableExists.return_value = False
        result = processor.update_skeletons_with_real_data("dim", MagicMock(), ["key"])
        assert result == 0

    def test_skips_when_no_skeleton_column(self, processor, spark):
        spark.catalog.tableExists.return_value = True
        delta_table = MagicMock()
        dim_df = MagicMock()
        dim_df.columns = ["id", "name"]
        delta_table.toDF.return_value = dim_df
        with patch("kimball.processing.late_arriving_dimension.DeltaTable.forName", return_value=delta_table):
            result = processor.update_skeletons_with_real_data("dim", MagicMock(), ["key"])
        assert result == 0

    def test_skips_when_no_skeleton_rows(self, processor, spark):
        spark.catalog.tableExists.return_value = True
        delta_table = MagicMock()
        dim_df = MagicMock()
        dim_df.columns = ["__is_skeleton", "id"]
        dim_df.filter.return_value.isEmpty.return_value = True
        delta_table.toDF.return_value = dim_df
        # col("__is_skeleton") is a real pyspark call that needs a SparkContext;
        # patch it (as the sibling update-path tests do) so this unit test does
        # not depend on a live session.
        with (
            patch("kimball.processing.late_arriving_dimension.DeltaTable.forName", return_value=delta_table),
            patch("kimball.processing.late_arriving_dimension.col", return_value=MagicMock()),
        ):
            result = processor.update_skeletons_with_real_data("dim", MagicMock(), ["key"])
        assert result == 0

    def test_updates_skeletons_and_returns_count(self, processor, spark):
        spark.catalog.tableExists.return_value = True
        delta_table = MagicMock()
        dim_df = MagicMock()
        dim_df.columns = ["__is_skeleton", "id"]
        dim_df.filter.return_value.isEmpty.return_value = False
        delta_table.toDF.return_value = dim_df
        merge_builder = MagicMock()
        merge_builder.whenMatchedUpdate.return_value = merge_builder
        delta_table.alias.return_value.merge.return_value = merge_builder
        history = MagicMock()
        history.select.return_value.first.return_value = SimpleNamespace(
            operationMetrics={"numTargetRowsUpdated": 5}
        )
        delta_table.history.return_value = history
        source_df = MagicMock()
        source_df.columns = ["id", "name"]

        with (
            patch("kimball.processing.late_arriving_dimension.DeltaTable.forName", return_value=delta_table),
            patch("kimball.processing.late_arriving_dimension.col", return_value=MagicMock()),
        ):
            result = processor.update_skeletons_with_real_data("dim", source_df, ["id"])
        assert result == 5

    def test_handles_history_exception(self, processor, spark):
        spark.catalog.tableExists.return_value = True
        delta_table = MagicMock()
        dim_df = MagicMock()
        dim_df.columns = ["__is_skeleton", "id"]
        dim_df.filter.return_value.isEmpty.return_value = False
        delta_table.toDF.return_value = dim_df
        merge_builder = MagicMock()
        merge_builder.whenMatchedUpdate.return_value = merge_builder
        delta_table.alias.return_value.merge.return_value = merge_builder
        delta_table.history.side_effect = Exception("history error")
        source_df = MagicMock()
        source_df.columns = ["id", "name"]

        with (
            patch("kimball.processing.late_arriving_dimension.DeltaTable.forName", return_value=delta_table),
            patch("kimball.processing.late_arriving_dimension.col", return_value=MagicMock()),
        ):
            result = processor.update_skeletons_with_real_data("dim", source_df, ["id"])
        assert result == 0


class TestReconcileFactForeignKeys:
    def test_skips_when_tables_not_exist(self, processor, spark):
        spark.catalog.tableExists.return_value = False
        result = processor.reconcile_fact_foreign_keys("fact", "dim", "fk_col", "sk", ["nk"])
        assert result == 0

    def test_reconciles_successfully(self, processor, spark):
        spark.catalog.tableExists.return_value = True
        delta_table = MagicMock()
        merge_builder = MagicMock()
        merge_builder.whenMatchedUpdate.return_value = merge_builder
        delta_table.alias.return_value.merge.return_value = merge_builder
        with patch("kimball.processing.late_arriving_dimension.DeltaTable.forName", return_value=delta_table):
            result = processor.reconcile_fact_foreign_keys("fact", "dim", "fk_col", "sk", ["nk"])
        assert result == 1

    def test_handles_exception(self, processor, spark):
        spark.catalog.tableExists.return_value = True
        with patch("kimball.processing.late_arriving_dimension.DeltaTable.forName", side_effect=Exception("merge failed")):
            result = processor.reconcile_fact_foreign_keys("fact", "dim", "fk_col", "sk", ["nk"])
        assert result == 0


class TestProcessLateArrivingDimension:
    def test_returns_stats(self, processor, spark):
        processor.update_skeletons_with_real_data = MagicMock(return_value=3)
        processor.reconcile_fact_foreign_keys = MagicMock(return_value=1)
        result = processor.process_late_arriving_dimension(
            "dim", MagicMock(), ["nk"],
            fact_tables=[{"table": "fact", "fk_column": "fk", "dimension_sk_column": "sk"}],
        )
        assert result["skeletons_updated"] == 3
        assert result["facts_reconciled"] == 1

    def test_no_fact_tables(self, processor, spark):
        processor.update_skeletons_with_real_data = MagicMock(return_value=2)
        result = processor.process_late_arriving_dimension("dim", MagicMock(), ["nk"])
        assert result["skeletons_updated"] == 2
        assert result["facts_reconciled"] == 0
