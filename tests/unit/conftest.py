"""Shared fixtures for unit tests.

Provides a real local Spark session for behavior-level tests that must
verify actual DataFrame output (hashing, dedup, PII masking) rather than
mock call counts. Skipped on Databricks and when Java is unavailable, so
the suite still runs in environments without a JVM.
"""

from __future__ import annotations

import os
import shutil

import pytest
from pyspark.sql import SparkSession


def _has_java() -> bool:
    return shutil.which("java") is not None or bool(os.environ.get("JAVA_HOME"))


def _is_remote_only() -> bool:
    try:
        from pyspark.rdd import is_remote_only

        return is_remote_only()
    except ImportError:
        return False


@pytest.fixture(scope="session")
def spark():
    """A real local Spark session for behavior-level unit tests."""
    if os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("SPARK_REMOTE"):
        pytest.skip("Skipping local-Spark unit test on Databricks")
    if not _has_java():
        pytest.skip("Java is not available — skipping Spark-dependent unit test")
    if _is_remote_only():
        pytest.skip("Databricks Connect is remote-only; local Spark is unavailable")
    builder = SparkSession.builder.appName("KimballUnit").master("local[2]")
    try:
        return builder.getOrCreate()
    except RuntimeError as exc:
        if "Only remote Spark sessions" in str(exc):
            pytest.skip("Databricks Connect cannot create a local Spark session")
        raise
