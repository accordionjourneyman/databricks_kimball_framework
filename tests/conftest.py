import os
import sys
import tempfile
import uuid
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

# Load .env from the repository root if present.
# This lets tests pick up DATABRICKS_HOST / DATABRICKS_TOKEN without hard-coding secrets.
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_dotenv_path = os.path.join(_repo_root, ".env")
if os.path.exists(_dotenv_path):
    try:
        from dotenv import load_dotenv

        load_dotenv(_dotenv_path, override=False)
    except ImportError:
        pass  # python-dotenv is only required for remote integration runs


# Mock databricks.sdk.runtime to allow local testing without credentials.
# Import the real databricks package first so sys.modules["databricks"]
# points to the real package, not a synthetic one created by the mock.
# Then only mock the sdk.runtime submodule (which is not importable
# outside Databricks runtime).
import databricks  # noqa: F401, E402

mock_db_sdk = MagicMock()
mock_db_sdk.spark = MagicMock()
mock_db_sdk.dbutils = MagicMock()
sys.modules["databricks.sdk.runtime"] = mock_db_sdk


def _is_remote_spark_requested() -> bool:
    """Return True if Databricks Connect credentials are configured."""
    if os.environ.get("KIMBALL_TEST_TARGET", "").lower() == "local":
        return False
    if os.environ.get("KIMBALL_TARGET", "").lower() == "local":
        return False
    return bool(
        os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN")
    )


def _is_databricks_runtime() -> bool:
    """Return True if this process is already running inside Databricks.

    In serverless/notebook tasks, SPARK_REMOTE is set to a local Spark Connect
    socket and the environment provides a real SparkSession via dbconnect.
    """
    return bool(
        os.environ.get("SPARK_REMOTE") or os.environ.get("DATABRICKS_RUNTIME_VERSION")
    )


def _create_databricks_runtime_spark_session() -> SparkSession:
    """Use the SparkSession already provided by the Databricks runtime."""
    return SparkSession.builder.getOrCreate()


def _create_remote_spark_session() -> SparkSession:
    """Create a Spark Connect session against a Databricks cluster."""
    try:
        from databricks.connect import DatabricksSession
    except ImportError as exc:
        raise RuntimeError(
            "databricks-connect is not installed. Install it to run tests with "
            "DATABRICKS_HOST/DATABRICKS_TOKEN, or unset those variables and "
            "set KIMBALL_TARGET=local to run locally."
        ) from exc

    cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
    builder = DatabricksSession.builder
    if cluster_id:
        builder = builder.clusterId(cluster_id)
    else:
        builder = builder.serverless()

    return builder.getOrCreate()


def _is_remote_only() -> bool:
    """Check whether pyspark is in remote-only mode (databricks-connect installed)."""
    try:
        from pyspark.rdd import is_remote_only

        return is_remote_only()
    except ImportError:
        return False


def _create_local_spark_session(warehouse_dir: str | None = None) -> SparkSession:
    """Create a local SparkSession with Delta Lake support.

    Args:
        warehouse_dir: Path for spark.sql.warehouse.dir. If None, a temp dir
                       is created (caller must clean up).
    """
    builder = SparkSession.builder.appName("KimballFrameworkTest")
    if _is_remote_only():
        # pyspark 4.x with databricks-connect: only Spark Connect sessions are
        # allowed.  .remote("local[...]") starts an embedded Connect server.
        builder = builder.remote("local[*]")
    else:
        builder = (
            builder.master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        # Ensure the delta-spark JARs are on the classpath when running
        # outside Databricks (e.g. GitHub Actions, local dev).
        try:
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)
        except ImportError:
            pass  # delta-spark not installed — tests that need it will fail
    return (
        builder.config("spark.sql.ansi.enabled", "true")
        .config(
            "spark.sql.warehouse.dir",
            warehouse_dir or tempfile.mkdtemp(prefix="spark-warehouse-kimball-tests-"),
        )
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def spark():
    """
    Provide a SparkSession for tests.

    Behavior:
      - If DATABRICKS_HOST and DATABRICKS_TOKEN are set, connect to a real
        Databricks cluster via Databricks Connect v2.
      - If already running inside Databricks (serverless/notebook task), use the
        runtime SparkSession directly.
      - Otherwise, fall back to a local SparkSession (requires Java).

    For remote runs, set DATABRICKS_CLUSTER_ID to skip interactive cluster selection.

    The local Spark warehouse directory is created as a temp dir and
    automatically cleaned up when the session ends.
    """
    if _is_databricks_runtime():
        spark = _create_databricks_runtime_spark_session()
    elif _is_remote_spark_requested():
        spark = _create_remote_spark_session()
    else:
        # Use TemporaryDirectory so the warehouse dir is cleaned up on exit
        warehouse_dir = tempfile.mkdtemp(prefix="spark-warehouse-kimball-tests-")
        spark = _create_local_spark_session(warehouse_dir)

    # Inject the active SparkSession into the mocked databricks.sdk.runtime
    # so application code that imports `spark` from there uses the right session.
    if "databricks.sdk.runtime" in sys.modules:
        cast(Any, sys.modules["databricks.sdk.runtime"]).spark = spark

    # Also register it with spark_session.set_active_spark so get_spark()
    # returns the same instance even when the runtime mock is bypassed.
    from kimball.common.spark_session import set_active_spark

    set_active_spark(spark)

    yield spark

    # Do not stop remote or runtime sessions; their lifecycle is managed separately.
    if not _is_remote_spark_requested() and not _is_databricks_runtime():
        spark.stop()
        # Clean up the warehouse directory after stopping the session
        import shutil

        shutil.rmtree(warehouse_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def test_catalog() -> str:
    """
    Return the catalog to use for integration tests.

    Defaults to the Databricks workspace default catalog, or 'spark_catalog'
    for local runs. Override with KIMBALL_TEST_CATALOG.
    """
    return os.environ.get("KIMBALL_TEST_CATALOG", "spark_catalog")


# ---------------------------------------------------------------------------
# Shared fixtures for integration tests
# ---------------------------------------------------------------------------


@pytest.fixture
def test_db(spark: SparkSession, request: pytest.FixtureRequest):
    """
    Create a unique test database (schema) for each test function and drop it
    on teardown.

    The database name is derived from the test module and a random suffix so
    that parallel test runs do not collide.  The database is dropped with
    CASCADE on teardown regardless of test outcome.

    Usage in an integration test module::

        pytestmark = pytest.mark.usefixtures("spark")

        def test_something(spark, test_db):
            spark.sql(f"CREATE TABLE {test_db}.my_table ...")
    """
    # Derive a short prefix from the test module name
    module_name = request.module.__name__ if request.module else "kimball"
    # Take the last component of the module path, strip "test_"
    prefix = module_name.split(".")[-1].replace("test_", "")[:12]
    db_name = f"kimball_{prefix}_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    os.environ["KIMBALL_ETL_SCHEMA"] = db_name
    yield db_name
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")


@pytest.fixture
def config_loader():
    """Provide a ConfigLoader instance for integration tests."""
    from kimball.common.config import ConfigLoader

    return ConfigLoader()


@pytest.fixture
def tmp_config(tmp_path):
    """
    Write a config YAML string to a temp file and return its path.

    Usage::

        config_path = tmp_config(f\"\"\"
        table_name: {test_db}.dim_customer
        table_type: dimension
        ...
        \"\"\")
    """
    configs_written: list[str] = []

    def _write(content: str) -> str:
        path = tmp_path / f"test_config_{uuid.uuid4().hex[:8]}.yml"
        path.write_text(content, encoding="utf-8")
        configs_written.append(str(path))
        return str(path)

    yield _write

    # Clean up config files after the test
    for p in configs_written:
        try:
            os.remove(p)
        except OSError:
            pass
