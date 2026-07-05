"""
Unit-level performance benchmarks using pytest-benchmark.

Tracks microbenchmarks for hot paths:
- hashdiff computation (SHA-256 over columns)
- validation predicates (unique, not_null)
- config fingerprinting

Run with:
    pytest tests/unit/test_performance.py -v --benchmark-only
    pytest tests/unit/test_performance.py -v --benchmark-only --benchmark-compare=20240101
"""

import hashlib
import os
import time

import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from kimball.common.config import ConfigLoader, TestDefinition, TableConfig
from kimball.processing.hashing import compute_hashdiff
from kimball.validation import DataQualityValidator, TestSeverity


@pytest.fixture(scope="module")
def small_spark():
    """Local Spark session for benchmark tests (skipped on Databricks)."""
    if os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("SPARK_REMOTE"):
        pytest.skip("Skipping local-only benchmark on Databricks")
    return SparkSession.builder.appName("BenchUnit").master("local[2]").getOrCreate()


@pytest.fixture(scope="module")
def medium_spark():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("SPARK_REMOTE"):
        pytest.skip("Skipping local-only benchmark on Databricks")
    return SparkSession.builder.appName("BenchUnit2").master("local[2]").getOrCreate()


class TestHashdiffBench:
    """Hashdiff is called on every SCD2 row — this is the most performance-critical path."""

    def test_hashdiff_10_columns_1k(self, benchmark, small_spark):
        from pyspark.sql.functions import col, lit, struct, when
        df = small_spark.range(1000).select(
            *(col(f"id").cast("int").alias(f"col_{i}") for i in range(10))
        )
        hashdiff = compute_hashdiff([f"col_{i}" for i in range(10)])
        result = benchmark(lambda: df.select(hashdiff.alias("h")).count())
        assert result > 0

    def test_hashdiff_3_columns_10k(self, benchmark, small_spark):
        from pyspark.sql.functions import col
        df = small_spark.range(10000).select(
            col("id").cast("int").alias("a"),
            col("id").cast("string").alias("b"),
            col("id").cast("int").alias("c"),
        )
        hashdiff = compute_hashdiff(["a", "b", "c"])
        result = benchmark(lambda: df.select(hashdiff.alias("h")).count())
        assert result > 0


class TestValidationBench:
    """Validation predicates run on every pipeline iteration."""

    def test_validate_unique_1k(self, benchmark, small_spark):
        from pyspark.sql.functions import col
        df = small_spark.range(1000).select(col("id").cast("int").alias("k"))
        validator = DataQualityValidator(spark_session=small_spark)
        result = benchmark(
            lambda: validator.validate_unique(df, ["k"], severity=TestSeverity.ERROR)
        )
        assert result is not None

    def test_validate_unique_100k(self, benchmark, medium_spark):
        from pyspark.sql.functions import col
        df = medium_spark.range(100000).select(col("id").cast("int").alias("k"))
        validator = DataQualityValidator(spark_session=medium_spark)
        result = benchmark(
            lambda: validator.validate_unique(df, ["k"], severity=TestSeverity.ERROR)
        )
        assert result is not None

    def test_validate_unique_approximate_1m(self, benchmark, medium_spark):
        from pyspark.sql.functions import col
        df = medium_spark.range(1_000_000).select(col("id").cast("int").alias("k"))
        validator = DataQualityValidator(spark_session=medium_spark)
        result = benchmark(
            lambda: validator.validate_unique_approximate(
                df, ["k"], severity=TestSeverity.WARN
            )
        )
        assert result is not None

    def test_validate_not_null_1k(self, benchmark, small_spark):
        from pyspark.sql.functions import col, lit
        df = small_spark.range(1000).select(col("id").cast("int").alias("v"))
        validator = DataQualityValidator(spark_session=small_spark)
        result = benchmark(
            lambda: validator.validate_not_null(df, ["v"], severity=TestSeverity.ERROR)
        )
        assert result is not None


class TestFingerprintBench:
    """Config fingerprinting gates the state-aware validation skip."""

    def test_fingerprint_small_config(self, benchmark):
        loader = ConfigLoader()
        config = TableConfig(
            table_name="catalog.schema.dim_test",
            table_type="dimension",
            surrogate_key="id_sk",
            natural_keys=["id"],
            sources=[],
            scd_type=2,
            track_history_columns=["name", "value"],
            transformation_sql="SELECT id, name, value FROM source",
        )
        result = benchmark(lambda: loader.compute_fingerprint(config))
        assert len(result) == 16

    def test_fingerprint_with_tests(self, benchmark):
        loader = ConfigLoader()
        config = TableConfig(
            table_name="catalog.schema.dim_test",
            table_type="dimension",
            surrogate_key="id_sk",
            natural_keys=["id"],
            sources=[],
            scd_type=2,
            track_history_columns=["name", "value"],
            transformation_sql="SELECT id, name, value FROM source",
            tests=[
                TestDefinition(column="id", tests=["unique", "not_null"]),
                TestDefinition(column="name", tests=["not_null"]),
            ],
        )
        result = benchmark(lambda: loader.compute_fingerprint(config))
        assert len(result) == 16


class TestSchemaFingerprintBench:
    """Source schema fingerprint enables cheap state-aware skipping."""

    def test_hash_10_columns(self, benchmark):
        schema_repr = ",".join(
            f"{f.name}:{f.dataType.simpleString()}" for f in [
                type("F", (), {"name": f"col_{i}", "dataType": type("D", (), {"simpleString": lambda self: "int"})()})()
                for i in range(10)
            ]
        )
        result = benchmark(lambda: hashlib.sha256(schema_repr.encode("utf-8")).hexdigest()[:16])
        assert len(result) == 16
