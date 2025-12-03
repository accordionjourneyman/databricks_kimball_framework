import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession, Row
from kimball.loader import DataLoader

@pytest.fixture
def spark_mock():
    return MagicMock(spec=SparkSession)

def test_load_full_snapshot(spark_mock):
    loader = DataLoader(spark_mock)
    loader.load_full_snapshot("source_table")
    
    spark_mock.read.format.assert_called_with("delta")
    spark_mock.read.format.return_value.table.assert_called_with("source_table")

def test_load_cdf(spark_mock):
    loader = DataLoader(spark_mock)
    loader.load_cdf("source_table", 100)
    
    spark_mock.read.format.assert_called_with("delta")
    spark_mock.read.format.return_value.option.assert_any_call("readChangeFeed", "true")
    spark_mock.read.format.return_value.option.assert_any_call("startingVersion", 100)
    spark_mock.read.format.return_value.option.return_value.option.return_value.table.assert_called_with("source_table")
