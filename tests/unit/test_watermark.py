import os
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row, SparkSession

# Set env var before importing (simulates notebook setup)
os.environ["KIMBALL_ETL_SCHEMA"] = "test_schema"

from kimball.watermark import ETLControlManager


@pytest.fixture
def spark_mock():
    spark = MagicMock(spec=SparkSession)
    spark.catalog.tableExists.return_value = False
    return spark


@patch("kimball.watermark.spark")
def test_ensure_table_exists_creates_table(mock_spark):
    """Test that the table is created if it doesn't exist."""
    mock_spark.catalog.tableExists.return_value = False

    manager = ETLControlManager(etl_schema="test_schema")

    mock_spark.sql.assert_called()  # CREATE DATABASE and CREATE TABLE


@patch("kimball.watermark.spark")
def test_get_watermark_returns_value(mock_spark):
    """Test retrieving an existing watermark."""
    mock_spark.catalog.tableExists.return_value = True

    manager = ETLControlManager(etl_schema="test_schema")

    # Mock the return of spark.table(...).filter(...).select(...).collect()
    mock_df = MagicMock()
    mock_spark.table.return_value = mock_df
    mock_df.filter.return_value.select.return_value.collect.return_value = [
        Row(last_processed_version=100)
    ]

    version = manager.get_watermark("fact_sales", "dim_customer")
    assert version == 100


@patch("kimball.watermark.spark")
def test_get_watermark_returns_none(mock_spark):
    """Test retrieving a non-existent watermark."""
    mock_spark.catalog.tableExists.return_value = True

    manager = ETLControlManager(etl_schema="test_schema")

    mock_df = MagicMock()
    mock_spark.table.return_value = mock_df
    mock_df.filter.return_value.select.return_value.collect.return_value = []

    version = manager.get_watermark("fact_sales", "dim_customer")
    assert version is None


@patch("kimball.watermark.spark")
def test_env_var_schema(mock_spark):
    """Test that KIMBALL_ETL_SCHEMA env var is used."""
    mock_spark.catalog.tableExists.return_value = True
    os.environ["KIMBALL_ETL_SCHEMA"] = "from_env"

    manager = ETLControlManager()  # No explicit schema

    assert manager.schema == "from_env"

    # Cleanup
    os.environ["KIMBALL_ETL_SCHEMA"] = "test_schema"
