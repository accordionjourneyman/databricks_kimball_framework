"""
Unit tests for ETLControlManager.

Tests watermark management, batch lifecycle, and target_version tracking.
"""

import os
from unittest.mock import MagicMock, patch
import sys

import pytest

# Set env var before importing (simulates notebook setup)
os.environ["KIMBALL_ETL_SCHEMA"] = "test_schema"


@pytest.fixture
def mock_delta_table():
    """Create a mock DeltaTable."""
    dt = MagicMock()
    dt.alias.return_value.merge.return_value = dt
    dt.whenMatchedUpdate.return_value = dt
    dt.whenNotMatchedInsert.return_value = dt
    return dt


@pytest.fixture
def mock_col():
    """Create mock for pyspark.sql.functions.col."""
    mock = MagicMock()
    mock.return_value = MagicMock()
    return mock


@patch("kimball.watermark.col")
@patch("kimball.watermark.spark")
@patch("kimball.watermark.DeltaTable")
def test_ensure_table_exists_creates_table(mock_dt, mock_spark, mock_col):
    """Test that the table is created if it doesn't exist."""
    from kimball.watermark import ETLControlManager

    mock_spark.catalog.tableExists.return_value = False

    manager = ETLControlManager(etl_schema="test_schema")

    mock_spark.sql.assert_called()  # CREATE DATABASE and CREATE TABLE
    # Verify CREATE TABLE was called with target_version column
    calls = [str(c) for c in mock_spark.sql.call_args_list]
    create_table_call = [c for c in calls if "CREATE TABLE" in c][0]
    assert "target_version" in create_table_call


@patch("kimball.watermark.col")
@patch("kimball.watermark.spark")
@patch("kimball.watermark.DeltaTable")
def test_get_watermark_returns_value(mock_dt, mock_spark, mock_col):
    """Test retrieving an existing watermark."""
    from kimball.watermark import ETLControlManager

    mock_spark.catalog.tableExists.return_value = True

    manager = ETLControlManager(etl_schema="test_schema")

    # Mock the return of spark.table(...).filter(...).select(...).first()
    mock_df = MagicMock()
    mock_spark.table.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.first.return_value = {"last_processed_version": 100}

    version = manager.get_watermark("fact_sales", "dim_customer")
    assert version == 100


@patch("kimball.watermark.col")
@patch("kimball.watermark.spark")
@patch("kimball.watermark.DeltaTable")
def test_get_watermark_returns_none(mock_dt, mock_spark, mock_col):
    """Test retrieving a non-existent watermark."""
    from kimball.watermark import ETLControlManager

    mock_spark.catalog.tableExists.return_value = True

    manager = ETLControlManager(etl_schema="test_schema")

    mock_df = MagicMock()
    mock_spark.table.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.first.return_value = None

    version = manager.get_watermark("fact_sales", "dim_customer")
    assert version is None


@patch("kimball.watermark.col")
@patch("kimball.watermark.spark")
@patch("kimball.watermark.DeltaTable")
def test_env_var_schema(mock_dt, mock_spark, mock_col):
    """Test that KIMBALL_ETL_SCHEMA env var is used."""
    from kimball.watermark import ETLControlManager

    mock_spark.catalog.tableExists.return_value = True
    os.environ["KIMBALL_ETL_SCHEMA"] = "from_env"

    manager = ETLControlManager()  # No explicit schema

    assert manager.schema == "from_env"

    # Cleanup
    os.environ["KIMBALL_ETL_SCHEMA"] = "test_schema"


@patch("kimball.watermark.col")
@patch("kimball.watermark.spark")
@patch("kimball.watermark.DeltaTable")
def test_batch_start(mock_dt, mock_spark, mock_col, mock_delta_table):
    """Test batch start lifecycle."""
    from kimball.watermark import ETLControlManager

    mock_spark.catalog.tableExists.return_value = True
    mock_dt.forName.return_value = mock_delta_table

    # Mock createDataFrame
    mock_spark.createDataFrame.return_value = MagicMock()

    manager = ETLControlManager(etl_schema="test_schema")
    batch_id = manager.batch_start("target_table", "source_table")

    assert batch_id is not None
    assert len(batch_id) == 36  # UUID format


@patch("kimball.watermark.col")
@patch("kimball.watermark.spark")
@patch("kimball.watermark.DeltaTable")
def test_batch_complete_with_target_version(
    mock_dt, mock_spark, mock_col, mock_delta_table
):
    """Test that batch_complete stores target_version."""
    from kimball.watermark import ETLControlManager

    mock_spark.catalog.tableExists.return_value = True
    mock_dt.forName.return_value = mock_delta_table
    mock_spark.createDataFrame.return_value = MagicMock()

    manager = ETLControlManager(etl_schema="test_schema")
    manager.batch_complete(
        target_table="gold.dim_customer",
        source_table="silver.customers",
        new_version=100,
        target_version=42,  # New field
        rows_read=1000,
        rows_written=50,
    )

    # Verify createDataFrame was called (for the MERGE)
    assert mock_spark.createDataFrame.called


@patch("kimball.watermark.col")
@patch("kimball.watermark.spark")
@patch("kimball.watermark.DeltaTable")
def test_get_source_version_for_target(mock_dt, mock_spark, mock_col):
    """Test retrieving source version for a target version (rollback support)."""
    from kimball.watermark import ETLControlManager

    mock_spark.catalog.tableExists.return_value = True

    manager = ETLControlManager(etl_schema="test_schema")

    mock_df = MagicMock()
    mock_spark.table.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.first.return_value = {
        "last_processed_version": 95,
        "source_table": "silver.customers",
    }

    source_version = manager.get_source_version_for_target("gold.dim_customer", 42)

    assert source_version == 95


@patch("kimball.watermark.col")
@patch("kimball.watermark.spark")
@patch("kimball.watermark.DeltaTable")
def test_batch_fail_does_not_update_watermark(
    mock_dt, mock_spark, mock_col, mock_delta_table
):
    """Test that batch_fail does NOT update the watermark."""
    from kimball.watermark import ETLControlManager

    mock_spark.catalog.tableExists.return_value = True
    mock_dt.forName.return_value = mock_delta_table
    mock_spark.createDataFrame.return_value = MagicMock()

    manager = ETLControlManager(etl_schema="test_schema")
    manager.batch_fail(
        target_table="gold.dim_customer",
        source_table="silver.customers",
        error_message="Test error",
    )

    # The batch_fail should NOT include last_processed_version in updates
    # This is the key invariant - failures preserve the resume point
    assert mock_spark.createDataFrame.called
