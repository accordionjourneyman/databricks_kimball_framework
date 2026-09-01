"""Shared fixtures for unit tests.

Provides a real local Spark session for behavior-level tests that must
verify actual DataFrame output (hashing, dedup, PII masking) rather than
mock call counts. Skipped on Databricks and when Java is unavailable, so
the suite still runs in environments without a JVM.
"""

from __future__ import annotations

import contextlib
import importlib
import os
import shutil
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession


def _has_java() -> bool:
    return shutil.which("java") is not None or bool(os.environ.get("JAVA_HOME"))


def _is_remote_only() -> bool:
    try:
        from pyspark.rdd import is_remote_only

        return bool(is_remote_only())
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
    builder = builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    ).config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:
        pass  # delta-spark not installed — tests that need it will fail
    try:
        spark = builder.getOrCreate()
    except RuntimeError as exc:
        if "Only remote Spark sessions" in str(exc):
            pytest.skip("Databricks Connect cannot create a local Spark session")
        raise
    from kimball.common.spark_session import set_active_spark

    set_active_spark(spark)
    return spark


@pytest.fixture
def _clear_active_spark():
    """Temporarily clear the global active Spark session, then restore it.

    Tests that exercise ``get_spark()`` fallback semantics must mutate
    ``spark_session._active_spark``; without restore, every later consumer
    of the global (including the session-scoped ``spark`` fixture) sees a
    cleared session — an order-dependent failure mode.
    """
    from kimball.common import spark_session

    saved = spark_session._active_spark
    spark_session._active_spark = None
    yield
    spark_session._active_spark = saved


@pytest.fixture(autouse=True)
def _isolate_pyspark_constructors(request: pytest.FixtureRequest):
    """Patch eager PySpark-4.x function constructors for mock-based tests.

    PySpark 4 resolves ``col()``, ``current_timestamp()``, ``count()`` and
    friends against the active SparkContext *at expression-construction
    time* (PySpark 3 kept them lazy). Mock-only unit tests that build these
    expressions fail with SESSION_OR_CONTEXT_NOT_EXISTS unless a real JVM
    happens to be running from an earlier test -- which made the suite
    order-dependent (pass alone, fail after other modules, or vice versa).

    Tests that request the ``spark`` fixture get the real constructors so
    behavior-level assertions keep working; everything else runs against
    MagicMock namespaces for the modules listed below.
    """
    if "spark" in request.fixturenames:
        yield
        return

    from unittest.mock import MagicMock

    def _comparable_mock() -> MagicMock:
        """MagicMock that tolerates comparison/lazy-chaining operators.

        ``F.col("x") > 1``, ``~cond``, ``cond | cond`` and friends must not
        raise on a MagicMock: the validator builds them before touching the
        mocked DataFrame. ``configure_mock`` supports the dunder protocol
        entries directly.
        """
        m = MagicMock()
        m.__gt__ = MagicMock(return_value=MagicMock())
        m.__lt__ = MagicMock(return_value=MagicMock())
        m.__ge__ = MagicMock(return_value=MagicMock())
        m.__le__ = MagicMock(return_value=MagicMock())
        m.__eq__ = MagicMock(return_value=MagicMock())
        m.__ne__ = MagicMock(return_value=MagicMock())
        m.__invert__ = MagicMock(return_value=MagicMock())
        m.__or__ = MagicMock(return_value=MagicMock())
        m.__and__ = MagicMock(return_value=MagicMock())
        m.__rand__ = MagicMock(return_value=MagicMock())
        m.__ror__ = MagicMock(return_value=MagicMock())
        return m

    with contextlib.ExitStack() as stack:
        for module, names in (
            ("kimball.orchestration.validation", ("F",)),
            ("kimball.orchestration.watermark", ("col", "current_timestamp")),
            ("kimball.observability.resilience", ("col", "current_timestamp", "desc")),
            ("kimball.observability.unresolved_keys", ("F",)),
            ("kimball.processing.scd1", ("current_timestamp", "lit")),
        ):
            target = importlib.import_module(module)
            for name in names:
                if name == "F":
                    mock_F = MagicMock()
                    mock_F.col.return_value = _comparable_mock()
                    mock_F.lit.return_value = _comparable_mock()
                    stack.enter_context(patch.object(target, name, mock_F, create=True))
                elif name in ("col", "current_timestamp", "desc", "lit"):
                    stack.enter_context(
                        patch.object(
                            target, name, return_value=_comparable_mock(), create=True
                        )
                    )
                else:
                    stack.enter_context(patch.object(target, name, create=True))
        yield
