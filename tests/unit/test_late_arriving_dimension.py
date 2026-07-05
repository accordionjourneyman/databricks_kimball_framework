from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from kimball.processing.late_arriving_dimension import LateArrivingDimensionProcessor


def test_update_skeletons_reports_merge_metrics():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True

    delta_table = MagicMock()
    dim_df = MagicMock()
    dim_df.columns = ["__is_skeleton", "customer_id"]
    dim_df.filter.return_value.isEmpty.return_value = False
    delta_table.toDF.return_value = dim_df

    history = MagicMock()
    history.select.return_value.first.return_value = SimpleNamespace(
        operationMetrics={"numTargetRowsUpdated": 3}
    )
    delta_table.history.return_value = history

    source_df = MagicMock()
    source_df.columns = ["customer_id", "name"]

    with (
        patch("kimball.processing.late_arriving_dimension.DeltaTable.forName",
              return_value=delta_table),
        patch("kimball.processing.late_arriving_dimension.col",
              return_value=MagicMock()),
    ):
        processor = LateArrivingDimensionProcessor(spark)
        result = processor.update_skeletons_with_real_data(
            dimension_table="dim_customer",
            source_df=source_df,
            natural_keys=["customer_id"],
        )

    assert result == 3
