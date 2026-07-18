"""Microbenchmarks for current framework hot paths."""

from __future__ import annotations

import os
import shutil

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from kimball.common.config import ConfigLoader, TableConfig, TestDefinition
from kimball.processing.hashing import compute_hashdiff
from kimball.validation import DataQualityValidator, TestSeverity


def _remote_only() -> bool:
    try:
        from pyspark.rdd import is_remote_only

        return is_remote_only()
    except ImportError:
        return False


@pytest.fixture(scope="module")
def spark():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("SPARK_REMOTE"):
        pytest.skip("Local-only microbenchmark")
    if _remote_only():
        pytest.skip("Databricks Connect cannot create a local Spark session")
    if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
        pytest.skip("Java is required for Spark microbenchmarks")
    session = (
        SparkSession.builder.appName("KimballMicrobenchmarks")
        .master("local[2]")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_hashdiff_10_columns_1k(benchmark, spark):
    frame = spark.range(1_000).select(
        *(F.col("id").cast("int").alias(f"column_{index}") for index in range(10))
    )
    expression = compute_hashdiff([f"column_{index}" for index in range(10)])
    result = benchmark(lambda: frame.select(expression.alias("hashdiff")).count())
    assert result == 1_000


def test_hashdiff_3_columns_10k(benchmark, spark):
    frame = spark.range(10_000).select(
        F.col("id").cast("int").alias("a"),
        F.col("id").cast("string").alias("b"),
        F.col("id").cast("int").alias("c"),
    )
    expression = compute_hashdiff(["a", "b", "c"])
    result = benchmark(lambda: frame.select(expression.alias("hashdiff")).count())
    assert result == 10_000


@pytest.mark.parametrize("rows", [1_000, 100_000])
def test_validate_unique(benchmark, spark, rows):
    frame = spark.range(rows).select(F.col("id").cast("int").alias("key"))
    validator = DataQualityValidator(spark=spark)
    result = benchmark(
        lambda: validator.validate_unique(frame, ["key"], severity=TestSeverity.ERROR)
    )
    assert result.passed


def test_validate_unique_approximate_1m(benchmark, spark):
    frame = spark.range(1_000_000).select(F.col("id").cast("int").alias("key"))
    validator = DataQualityValidator(spark=spark)
    result = benchmark(
        lambda: validator.validate_unique_approximate(
            frame, ["key"], severity=TestSeverity.WARN
        )
    )
    assert result.total_rows == 1_000_000
    assert result.severity == TestSeverity.WARN


def test_validate_not_null_1k(benchmark, spark):
    frame = spark.range(1_000).select(F.col("id").cast("int").alias("value"))
    validator = DataQualityValidator(spark=spark)
    result = benchmark(
        lambda: validator.validate_not_null(
            frame, ["value"], severity=TestSeverity.ERROR
        )
    )
    assert result.passed


@pytest.mark.parametrize("with_tests", [False, True])
def test_config_fingerprint(benchmark, with_tests):
    tests = (
        [
            TestDefinition(column="id", tests=["unique", "not_null"]),
            TestDefinition(column="name", tests=["not_null"]),
        ]
        if with_tests
        else []
    )
    config = TableConfig(
        table_name="catalog.schema.dim_test",
        table_type="dimension",
        surrogate_key="id_sk",
        natural_keys=["id"],
        sources=[],
        scd_type=2,
        effective_at="updated_at",
        track_history_columns=["name", "value"],
        transformation_sql="SELECT id, name, value FROM source",
        tests=tests,
    )
    result = benchmark(lambda: ConfigLoader().compute_fingerprint(config))
    assert len(result) == 16
