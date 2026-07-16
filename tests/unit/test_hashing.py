"""
Unit tests for hashing utilities.

Verifies the actual hashdiff behavior produced by ``compute_hashdiff``
against a real Spark session, rather than asserting on mock call counts
(which would pass even if the underlying hash function or null handling
changed). These tests guard the SCD2 change-detection contract:

- a changed tracked column changes the hash
- an unchanged row keeps the hash
- column order in config does not affect the hash (sorted)
- NULLs are handled (sentinel), and differ from a real value
- untracked columns do not affect the hash
"""
from __future__ import annotations

from kimball.processing.hashing import compute_hashdiff


def test_hashdiff_distinguishes_changed_rows(spark):
    """A changed tracked column must produce a different hashdiff."""
    df = spark.createDataFrame([("a", "x"), ("a", "y")], ["name", "city"])
    rows = df.withColumn("h", compute_hashdiff(["name", "city"])).select("h").collect()
    assert rows[0]["h"] != rows[1]["h"]


def test_hashdiff_stable_for_identical_rows(spark):
    """Identical rows must hash to the same value."""
    df = spark.createDataFrame([("a", "x"), ("a", "x")], ["name", "city"])
    rows = df.withColumn("h", compute_hashdiff(["name", "city"])).select("h").collect()
    assert rows[0]["h"] == rows[1]["h"]


def test_hashdiff_independent_of_column_order(spark):
    """Config-order independence: sorted columns hash the same regardless of input order."""
    base = spark.createDataFrame([("a", "x")], ["name", "city"])
    left = base.withColumn("h", compute_hashdiff(["name", "city"])).select("h").head()
    right = base.withColumn("h", compute_hashdiff(["city", "name"])).select("h").head()
    assert left["h"] == right["h"]


def test_hashdiff_handles_nulls(spark):
    """A NULL tracked column must not crash and must differ from a non-null value."""
    df = spark.createDataFrame([("a", None), ("a", "x")], ["name", "city"])
    rows = df.withColumn("h", compute_hashdiff(["name", "city"])).select("h").collect()
    assert rows[0]["h"] != rows[1]["h"]  # null sentinel != "x"


def test_hashdiff_only_tracks_requested_columns(spark):
    """Changes to an untracked column must NOT change the hashdiff."""
    df = spark.createDataFrame([("a", "x", 1), ("a", "x", 2)], ["name", "city", "phone"])
    rows = df.withColumn("h", compute_hashdiff(["name", "city"])).select("h").collect()
    assert rows[0]["h"] == rows[1]["h"]  # phone not tracked -> same hash


def test_hashdiff_returns_bigint_column(spark):
    """The hashdiff column is a 64-bit integer (xxhash64), not a string."""
    from pyspark.sql.types import LongType

    df = spark.createDataFrame([("a", "x")], ["name", "city"])
    field = df.withColumn("h", compute_hashdiff(["name", "city"])).schema.fieldNames().index("h")
    assert isinstance(df.withColumn("h", compute_hashdiff(["name", "city"])).schema.fields[field].dataType, LongType)