from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from kimball.processing.loader import DataLoader


@pytest.fixture
def spark_mock():
    return MagicMock(spec=SparkSession)


@patch("kimball.processing.loader.spark")
def test_load_full_snapshot(mock_spark):
    loader = DataLoader()
    loader.load_full_snapshot("source_table")

    mock_spark.read.format.assert_called_with("delta")
    mock_spark.read.format.return_value.table.assert_called_with("source_table")


@patch("kimball.processing.loader.spark")
def test_load_cdf(mock_spark):
    loader = DataLoader()

    # Configure mock for method chaining: .option(...).option(...)
    mock_reader = mock_spark.read.format.return_value
    mock_reader.option.return_value = mock_reader

    loader.load_cdf("source_table", 100)

    mock_spark.read.format.assert_called_with("delta")

    # Verify both calls were made on the reader
    mock_reader.option.assert_any_call("readChangeFeed", "true")
    mock_reader.option.assert_any_call("startingVersion", 100)

    # Verify table load
    mock_reader.table.assert_called_with("source_table")
