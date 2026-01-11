"""
Unit tests for DataLoader.

Tests loading strategies with mocked SparkSession using dependency injection.
"""

from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from kimball.processing.loader import DataLoader


@pytest.fixture
def spark_mock():
    """Create a mock SparkSession for testing."""
    mock = MagicMock(spec=SparkSession)
    # Setup method chaining for reader
    mock_reader = MagicMock()
    mock_reader.option.return_value = mock_reader
    mock_reader.options.return_value = mock_reader
    mock.read.format.return_value = mock_reader
    return mock


def test_load_full_snapshot(spark_mock):
    """Test full snapshot loading with injected SparkSession."""
    loader = DataLoader(spark_session=spark_mock)
    loader.load_full_snapshot("source_table")

    spark_mock.read.format.assert_called_with("delta")
    spark_mock.read.format.return_value.table.assert_called_with("source_table")


def test_load_cdf(spark_mock):
    """Test CDF loading with injected SparkSession."""
    loader = DataLoader(spark_session=spark_mock)
    loader.load_cdf("source_table", 100)

    spark_mock.read.format.assert_called_with("delta")

    mock_reader = spark_mock.read.format.return_value
    mock_reader.option.assert_any_call("readChangeFeed", "true")
    mock_reader.option.assert_any_call("startingVersion", 100)
    mock_reader.table.assert_called_with("source_table")
