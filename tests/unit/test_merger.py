from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from kimball.processing.merger import DeltaMerger


@pytest.fixture
def spark_mock():
    return MagicMock(spec=SparkSession)


@patch(
    "kimball.processing.merger.broadcast", lambda x: x
)  # Mock broadcast to pass-through
@patch("kimball.processing.merger.current_timestamp")
@patch("kimball.processing.merger.lit")
@patch("kimball.processing.merger.col")
@patch("kimball.processing.merger.DeltaTable")
@patch("kimball.common.spark_session.get_spark")
def test_merge_execution(
    mock_get_spark, mock_delta_table, mock_col, mock_lit, mock_curr_ts
):
    # Setup mocks
    mock_dt_instance = MagicMock()
    mock_delta_table.forName.return_value = mock_dt_instance
    mock_dt_instance.alias.return_value = mock_dt_instance
    mock_dt_instance.merge.return_value = mock_dt_instance

    merger = DeltaMerger()

    mock_df = MagicMock()
    mock_df.columns = ["id", "val", "_change_type"]

    # Mocking withColumn to return self (fluent interface)
    mock_df.withColumn.return_value = mock_df
    mock_df.alias.return_value = mock_df

    merger.merge(
        target_table_name="target",
        source_df=mock_df,
        join_keys=["id"],
        delete_strategy="hard",
    )

    # C-10: Verify DeltaTable was accessed
    mock_delta_table.forName.assert_called_once()

    # Verify merge chain was started
    mock_dt_instance.alias.assert_called()
    mock_dt_instance.merge.assert_called_once()
