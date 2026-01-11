"""
Unit tests for DataLoader.

Tests CDF loading, full snapshot loading, and version retrieval.
"""

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_df():
    """Create a mock DataFrame."""
    df = MagicMock()
    df.columns = ["id", "val", "_change_type", "_commit_version"]
    df.filter.return_value = df
    df.withColumn.return_value = df
    df.drop.return_value = df
    return df


@patch("kimball.loader.col")
@patch("kimball.loader.row_number")
@patch("kimball.loader.Window")
@patch("kimball.loader.spark")
@patch("kimball.loader.DeltaTable")
def test_load_full_snapshot(
    mock_delta_table, mock_spark, mock_window, mock_row_number, mock_col, mock_df
):
    """Test loading a full snapshot of a table."""
    from kimball.loader import DataLoader

    mock_spark.read.format.return_value.table.return_value = mock_df

    loader = DataLoader()  # No spark argument
    loader.load_full_snapshot("source_table")

    mock_spark.read.format.assert_called_with("delta")
    mock_spark.read.format.return_value.table.assert_called_with("source_table")


@patch("kimball.loader.col")
@patch("kimball.loader.row_number")
@patch("kimball.loader.Window")
@patch("kimball.loader.spark")
@patch("kimball.loader.DeltaTable")
def test_load_cdf(
    mock_delta_table, mock_spark, mock_window, mock_row_number, mock_col, mock_df
):
    """Test loading changes via CDF."""
    from kimball.loader import DataLoader

    # Setup mock chain
    mock_read = mock_spark.read.format.return_value
    mock_read.option.return_value = mock_read  # Allow chaining
    mock_read.table.return_value = mock_df

    loader = DataLoader()
    loader.load_cdf("source_table", 100)

    mock_spark.read.format.assert_called_with("delta")
    mock_read.option.assert_any_call("readChangeFeed", "true")
    mock_read.option.assert_any_call("startingVersion", 100)


@patch("kimball.loader.col")
@patch("kimball.loader.row_number")
@patch("kimball.loader.Window")
@patch("kimball.loader.spark")
@patch("kimball.loader.DeltaTable")
def test_load_cdf_with_deduplication(
    mock_delta_table, mock_spark, mock_window, mock_row_number, mock_col, mock_df
):
    """Test CDF loading with key-based deduplication."""
    from kimball.loader import DataLoader

    mock_read = mock_spark.read.format.return_value
    mock_read.option.return_value = mock_read
    mock_read.table.return_value = mock_df

    # Setup window mock
    mock_window.partitionBy.return_value.orderBy.return_value = MagicMock()
    mock_row_number.return_value.over.return_value = MagicMock()
    mock_col.return_value.desc.return_value = MagicMock()
    mock_col.return_value.__eq__ = MagicMock(return_value=MagicMock())

    loader = DataLoader()
    loader.load_cdf("source_table", 100, deduplicate_keys=["id"])

    # Should trigger deduplication logic via Window
    assert mock_df.withColumn.called or mock_df.filter.called


@patch("kimball.loader.col")
@patch("kimball.loader.row_number")
@patch("kimball.loader.Window")
@patch("kimball.loader.spark")
@patch("kimball.loader.DeltaTable")
def test_get_latest_version(
    mock_delta_table, mock_spark, mock_window, mock_row_number, mock_col
):
    """Test retrieving the latest version of a table."""
    from kimball.loader import DataLoader

    mock_dt = MagicMock()
    mock_delta_table.forName.return_value = mock_dt
    mock_dt.history.return_value.select.return_value.first.return_value = {
        "version": 150
    }

    loader = DataLoader()
    version = loader.get_latest_version("source_table")

    assert version == 150
    mock_delta_table.forName.assert_called_once()
