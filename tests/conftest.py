import importlib
import importlib.util
import sys
import tempfile
from typing import Any, cast
from unittest.mock import MagicMock

# Mock databricks.sdk.runtime to allow local testing without credentials
# Always mock it, regardless of whether package exists - the real SDK tries to auth
# at import time which fails without Databricks environment
mock_db_sdk = MagicMock()
mock_db_sdk.spark = MagicMock()
mock_db_sdk.dbutils = MagicMock()
sys.modules["databricks.sdk.runtime"] = mock_db_sdk

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession with Delta Lake support for testing."""
    builder = (
        SparkSession.builder.appName("KimballFrameworkTest")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.ansi.enabled", "true"
        )  # Enable ANSI mode to ensure Spark 4 Readiness and future-proof code
        # Use in-memory catalog/warehouse if possible, or let it default to target/
        .config(
            "spark.sql.warehouse.dir",
            tempfile.mkdtemp(prefix="spark-warehouse-kimball-tests-"),
        )
    )
    spark = builder.getOrCreate()

    # Inject the local SparkSession into the mocked databricks.sdk.runtime
    # This allows application code (which imports spark from there) to use our local engine
    if "databricks.sdk.runtime" in sys.modules:
        cast(Any, sys.modules["databricks.sdk.runtime"]).spark = spark

    yield spark
    spark.stop()
