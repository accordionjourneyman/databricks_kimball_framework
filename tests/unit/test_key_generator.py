"""Tests for HashKeyGenerator using real Spark DataFrames.

These tests verify actual key generation behavior rather than mock call patterns.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from kimball.processing.key_generator import HashKeyGenerator, type7_key_columns


def test_generate_keys_produces_deterministic_keys(spark: SparkSession):
    """Same input should produce the same surrogate key across calls."""
    df = spark.createDataFrame([("a", 1), ("b", 2)], ["key", "val"])
    gen = HashKeyGenerator(["key"])
    result = gen.generate_keys(df, "sk")
    keys = [r["sk"] for r in result.select("sk").collect()]
    assert len(keys) == 2
    assert keys[0] != keys[1], "different natural keys should get different SKs"

    # Deterministic: same input produces same keys
    df2 = spark.createDataFrame([("a", 1)], ["key", "val"])
    result2 = gen.generate_keys(df2, "sk")
    key_a = result2.select("sk").collect()[0][0]
    assert key_a == keys[0], "same natural key should produce the same SK"


def test_generate_keys_preserves_input_columns(spark: SparkSession):
    """generate_keys should add the SK column without dropping existing columns."""
    df = spark.createDataFrame([(1, "hello")], ["id", "val"])
    gen = HashKeyGenerator(["id"])
    result = gen.generate_keys(df, "sk")
    assert "sk" in result.columns
    assert "val" in result.columns
    assert result.select("val").collect()[0][0] == "hello"


def test_type7_key_columns_returns_correct_columns(spark: SparkSession):
    """type7_key_columns should return the expected set of column expressions."""
    columns = type7_key_columns(["source_system", "customer_id"], "updated_at")
    names = {str(c) for c in columns}
    assert "durable_key" in names, f"expected durable_key in {names}"
    assert "row_key" in names, f"expected row_key in {names}"
    assert "durable_fingerprint" in names, f"expected durable_fingerprint in {names}"
    assert "row_fingerprint" in names, f"expected row_fingerprint in {names}"
