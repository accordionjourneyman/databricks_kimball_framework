import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from kimball.watermark import WatermarkManager

@pytest.fixture
def spark_mock():
    spark = MagicMock(spec=SparkSession)
    spark.catalog.tableExists.return_value = False
    return spark

def test_ensure_table_exists_creates_table(spark_mock):
    """Test that the table is created if it doesn't exist."""
    manager = WatermarkManager(spark_mock, "test_watermarks")
    
    spark_mock.catalog.tableExists.assert_called_with("test_watermarks")
    spark_mock.createDataFrame.assert_called()
    spark_mock.createDataFrame.return_value.write.format.return_value.mode.return_value.saveAsTable.assert_called_with("test_watermarks")

def test_get_watermark_returns_value(spark_mock):
    """Test retrieving an existing watermark."""
    manager = WatermarkManager(spark_mock, "test_watermarks")
    
    # Mock the return of spark.table(...).filter(...).select(...).collect()
    mock_df = MagicMock()
    spark_mock.table.return_value = mock_df
    mock_df.filter.return_value.select.return_value.collect.return_value = [Row(last_processed_version=100)]
    
    version = manager.get_watermark("fact_sales", "dim_customer")
    assert version == 100

def test_get_watermark_returns_none(spark_mock):
    """Test retrieving a non-existent watermark."""
    manager = WatermarkManager(spark_mock, "test_watermarks")
    
    mock_df = MagicMock()
    spark_mock.table.return_value = mock_df
    mock_df.filter.return_value.select.return_value.collect.return_value = []
    
    version = manager.get_watermark("fact_sales", "dim_customer")
    assert version is None
