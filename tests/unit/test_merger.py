from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from kimball.processing.scd1 import merge_scd1


@pytest.fixture
def spark_mock():
    return MagicMock(spec=SparkSession)


@patch("kimball.processing.scd1.generate_keys")
@patch("kimball.processing.scd1.dedup_cdf")
@patch("kimball.processing.scd1.DeltaTable")
@patch("kimball.processing.scd1.get_spark")
def test_merge_execution(
    mock_get_spark,
    mock_delta_table,
    mock_dedup,
    mock_generate_keys,
):
    mock_dt_instance = MagicMock()
    mock_delta_table.forName.return_value = mock_dt_instance
    mock_dt_instance.alias.return_value = mock_dt_instance
    mock_dt_instance.merge.return_value = mock_dt_instance
    mock_dt_instance.toDF.return_value.schema.fields = []

    mock_df = MagicMock()
    mock_df.columns = ["id", "val", "_change_type", "__etl_processed_at"]
    mock_df.withColumn.return_value = mock_df
    mock_df.alias.return_value = mock_df

    mock_dedup.return_value = mock_df
    mock_generate_keys.return_value = mock_df

    merge_scd1(
        mock_df,
        target_table_name="target",
        join_keys=["id"],
        delete_strategy="hard",
    )

    mock_delta_table.forName.assert_called_once()
    mock_dt_instance.alias.assert_called()
    mock_dt_instance.merge.assert_called_once()