"""
Unit tests for ETLControlManager watermark functionality.

Tests watermark management with mocked SparkSession using dependency injection.
"""

import os
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row, SparkSession

# Set env var before importing (simulates notebook setup)
os.environ["KIMBALL_ETL_SCHEMA"] = "test_schema"

from kimball.orchestration.watermark import ETLControlManager


@pytest.fixture
def spark_mock():
    """Create a mock SparkSession for testing."""
    mock = MagicMock(spec=SparkSession)
    mock.catalog.tableExists.return_value = False
    mock.sql = MagicMock()
    return mock


def test_ensure_table_exists_creates_table(spark_mock):
    """Test that the table is created if it doesn't exist."""
    spark_mock.catalog.tableExists.return_value = False

    _ = ETLControlManager(etl_schema="test_schema", spark_session=spark_mock)

    spark_mock.sql.assert_called()  # CREATE DATABASE and CREATE TABLE


@patch("kimball.orchestration.watermark.col")
def test_get_watermark_returns_value(mock_col, spark_mock):
    """Test retrieving an existing watermark."""
    spark_mock.catalog.tableExists.return_value = True

    manager = ETLControlManager(etl_schema="test_schema", spark_session=spark_mock)

    # Mock the return of spark.table(...).filter(...).select(...).first()
    mock_df = MagicMock()
    spark_mock.table.return_value = mock_df
    mock_df.filter.return_value.select.return_value.first.return_value = Row(
        last_processed_version=100
    )

    version = manager.get_watermark("fact_sales", "dim_customer")
    assert version == 100


@patch("kimball.orchestration.watermark.col")
def test_get_watermark_returns_none(mock_col, spark_mock):
    """Test retrieving a non-existent watermark."""
    spark_mock.catalog.tableExists.return_value = True

    manager = ETLControlManager(etl_schema="test_schema", spark_session=spark_mock)

    mock_df = MagicMock()
    spark_mock.table.return_value = mock_df
    mock_df.filter.return_value.select.return_value.first.return_value = None

    version = manager.get_watermark("fact_sales", "dim_customer")
    assert version is None


def test_env_var_schema(spark_mock):
    """Test that KIMBALL_ETL_SCHEMA env var is used."""
    spark_mock.catalog.tableExists.return_value = True
    os.environ["KIMBALL_ETL_SCHEMA"] = "from_env"

    manager = ETLControlManager(spark_session=spark_mock)  # No explicit schema

    assert manager.schema == "from_env"

    # Cleanup
    os.environ["KIMBALL_ETL_SCHEMA"] = "test_schema"
