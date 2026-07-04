import os
import sys
import tempfile
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
# Always mock it: the real SDK tries to authenticate at import time, which fails
# outside Databricks. The mock is replaced with a real remote session when
# integration tests run against Databricks Connect.
mock_db_sdk = MagicMock()
mock_db_sdk.spark = MagicMock()
mock_db_sdk.dbutils = MagicMock()
sys.modules["databricks.sdk.runtime"] = mock_db_sdk


def _is_remote_spark_requested() -> bool:
    """Return True if Databricks Connect credentials are configured."""
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
    from databricks.connect import DatabricksSession

    cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
    builder = DatabricksSession.builder
    if cluster_id:
        builder = builder.clusterId(cluster_id)

    return builder.getOrCreate()


def _create_local_spark_session() -> SparkSession:
    """Create a local SparkSession with Delta Lake support if available."""
    builder = (
        SparkSession.builder.appName("KimballFrameworkTest")
        .master("local[*]")
        .config(
            "spark.sql.warehouse.dir",
            tempfile.mkdtemp(prefix="spark-warehouse-kimball-tests-"),
        )
    )
    try:
        import importlib
        importlib.import_module("delta")
        session = (
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.ansi.enabled", "true")
            .getOrCreate()
        )
        session.range(1).write.format("delta").mode("overwrite").save(
            tempfile.mkdtemp() + "/_delta_test"
        )
        return session
    except Exception:
        pass
    return (
        builder
        .config("spark.sql.ansi.enabled", "true")
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
    """
    if _is_databricks_runtime():
        spark = _create_databricks_runtime_spark_session()
    elif _is_remote_spark_requested():
        spark = _create_remote_spark_session()
    else:
        spark = _create_local_spark_session()

    # Inject the active SparkSession into the mocked databricks.sdk.runtime
    # so application code that imports `spark` from there uses the right session.
    if "databricks.sdk.runtime" in sys.modules:
        cast(Any, sys.modules["databricks.sdk.runtime"]).spark = spark

    yield spark

    # Do not stop remote or runtime sessions; their lifecycle is managed separately.
    if not _is_remote_spark_requested() and not _is_databricks_runtime():
        spark.stop()


@pytest.fixture(scope="session")
def test_catalog() -> str:
    """
    Return the catalog to use for integration tests.

    Defaults to the Databricks workspace default catalog, or 'spark_catalog'
    for local runs. Override with KIMBALL_TEST_CATALOG.
    """
    return os.environ.get("KIMBALL_TEST_CATALOG", "spark_catalog")
