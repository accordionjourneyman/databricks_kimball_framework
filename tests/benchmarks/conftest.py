import pytest
import tempfile


def pytest_addoption(parser):
    parser.addoption(
        "--scale",
        action="store",
        default="tiny",
        help="Benchmark scale tier: tiny, small, medium",
    )


@pytest.fixture
def scale(request):
    return request.config.getoption("--scale")


def pytest_configure(config):
    """Increase Spark memory for benchmarks."""
    pass


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession with more memory for benchmarks."""
    import sys, types
    from unittest.mock import MagicMock
    if "databricks.sdk.runtime" not in sys.modules:
        mock_db_sdk = types.ModuleType("databricks.sdk.runtime")
        mock_db_sdk.spark = MagicMock()
        mock_db_sdk.dbutils = MagicMock()
        sys.modules["databricks.sdk.runtime"] = mock_db_sdk

    from pyspark.sql import SparkSession
    session = (
        SparkSession.builder.appName("KimballBenchmark")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.ansi.enabled", "false")
        .config(
            "spark.sql.warehouse.dir",
            tempfile.mkdtemp(prefix="spark-warehouse-bench-"),
        )
        .getOrCreate()
    )
    if "databricks.sdk.runtime" in sys.modules:
        import types
        mock_runtime = types.ModuleType("databricks.sdk.runtime")
        mock_runtime.spark = session
        mock_runtime.dbutils = MagicMock()
        sys.modules["databricks.sdk.runtime"] = mock_runtime
    yield session
    session.stop()


@pytest.fixture
def bench_db(spark):
    """Override bench_db to use the benchmark spark fixture."""
    import os
    import uuid
    db = f"kimball_bench_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    os.environ["KIMBALL_ETL_SCHEMA"] = db
    yield db
    spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
