import pytest


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


def _is_databricks_runtime() -> bool:
    """Return True if running inside a Databricks cluster (Spark Connect or DBR)."""
    import os

    return bool(
        os.environ.get("SPARK_REMOTE")
        or os.environ.get("DATABRICKS_RUNTIME_VERSION")
        or os.environ.get("DATABRICKS_HOST")
    )


@pytest.fixture(scope="session")
def spark():
    """Provide a SparkSession for benchmarks.

    On Databricks: use the active Spark Connect session (do NOT create local).
    Locally: create a new local[2] session with 4GB driver memory.
    """
    import sys
    import types
    from unittest.mock import MagicMock

    if "databricks.sdk.runtime" not in sys.modules:
        mock_db_sdk = types.ModuleType("databricks.sdk.runtime")
        mock_db_sdk.spark = MagicMock()
        mock_db_sdk.dbutils = MagicMock()
        sys.modules["databricks.sdk.runtime"] = mock_db_sdk

    if _is_databricks_runtime():
        from pyspark.sql import SparkSession

        session = SparkSession.builder.getOrCreate()
    else:
        import tempfile

        from pyspark.sql import SparkSession

        builder = (
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
        )
        # Ensure the delta-spark JARs are on the classpath when running
        # outside Databricks (e.g. GitHub Actions, local dev).
        try:
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)
        except ImportError:
            pass  # delta-spark not installed — tests that need it will fail
        session = builder.getOrCreate()

    if "databricks.sdk.runtime" in sys.modules:
        import types

        mock_runtime = types.ModuleType("databricks.sdk.runtime")
        mock_runtime.spark = session
        mock_runtime.dbutils = MagicMock()
        sys.modules["databricks.sdk.runtime"] = mock_runtime
    yield session
    if not _is_databricks_runtime():
        session.stop()


@pytest.fixture
def bench_db(spark):
    import os
    import uuid

    db = f"kimball_bench_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    os.environ["KIMBALL_ETL_SCHEMA"] = db
    yield db
    spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
