"""
Unit tests for DataLoader.

Tests loading strategies with mocked SparkSession using dependency injection.
"""

from unittest.mock import MagicMock, patch

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
    # C-07: Pass ending_version explicitly to avoid triggering get_latest_version
    loader.load_cdf("source_table", 100, ending_version=200)

    spark_mock.read.format.assert_called_with("delta")

    mock_reader = spark_mock.read.format.return_value
    mock_reader.option.assert_any_call("readChangeFeed", "true")
    mock_reader.option.assert_any_call("startingVersion", 100)
    mock_reader.option.assert_any_call("endingVersion", 200)
    mock_reader.table.assert_called_with("source_table")


@patch("kimball.processing.loader.DeltaTable")
def test_load_cdf_auto_fetches_ending_version(mock_delta_table, spark_mock):
    """Test that load_cdf auto-fetches ending_version when not provided."""
    # Setup mock for get_latest_version
    mock_dt_instance = MagicMock()
    mock_delta_table.forName.return_value = mock_dt_instance
    mock_history = MagicMock()
    mock_history.select.return_value.first.return_value = {"version": 150}
    mock_dt_instance.history.return_value = mock_history

    loader = DataLoader(spark_session=spark_mock)
    # Don't pass ending_version - should auto-fetch
    loader.load_cdf("source_table", 100)

    # Verify get_latest_version was called (via DeltaTable.forName)
    mock_delta_table.forName.assert_called_once_with(spark_mock, "source_table")
