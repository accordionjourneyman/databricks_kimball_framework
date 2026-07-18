import pytest

SCALE_PARAMS = {
    "tiny": {"products": 1_000, "changed": 300, "deleted": 100},
    "small": {"products": 100_000, "changed": 30_000, "deleted": 10_000},
    "medium": {
        "products": 1_000_000,
        "changed": 300_000,
        "deleted": 100_000,
    },
}


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


@pytest.fixture
def scale_params(scale):
    return SCALE_PARAMS[scale]


@pytest.fixture
def benchmark_rounds():
    import os

    return int(os.environ.get("KIMBALL_BENCHMARK_ROUNDS", "5"))


@pytest.fixture
def benchmark_warmups():
    import os

    return int(os.environ.get("KIMBALL_BENCHMARK_WARMUPS", "2"))


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

        warehouse_dir = tempfile.mkdtemp(prefix="spark-warehouse-bench-")
        import os
        from pathlib import Path

        event_log_dir = os.environ.get("KIMBALL_BENCHMARK_EVENTLOG_DIR")
        if event_log_dir:
            Path(event_log_dir).mkdir(parents=True, exist_ok=True)
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
            .config("spark.sql.warehouse.dir", warehouse_dir)
        )
        if event_log_dir:
            builder = builder.config("spark.eventLog.enabled", "true").config(
                "spark.eventLog.dir", Path(event_log_dir).as_uri()
            )

        if event_log_dir:
            builder = builder.config("spark.eventLog.compress", "false")
            builder = builder.config("spark.eventLog.rolling.enabled", "false")

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
        import shutil

        shutil.rmtree(warehouse_dir, ignore_errors=True)


@pytest.fixture
def bench_db(spark):
    import os
    import uuid

    db = f"kimball_bench_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    os.environ["KIMBALL_ETL_SCHEMA"] = db
    yield db
    spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
