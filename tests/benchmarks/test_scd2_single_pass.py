"""Benchmark single-pass vs two-phase SCD2 MERGE on real Spark.

Runs on Databricks Connect (requires DATABRICKS_HOST + DATABRICKS_TOKEN).
Creates a Delta table, runs both approaches, and compares timing + output.
"""
from __future__ import annotations

import os
import sys
import time
import uuid
from unittest.mock import MagicMock
from typing import Any

from databricks.connect import DatabricksSession
_spark = DatabricksSession.builder.serverless().getOrCreate()
mock_runtime = MagicMock()
mock_runtime.spark = _spark
sys.modules["databricks.sdk.runtime"] = mock_runtime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from kimball.processing.scd2 import _merge_single_pass, merge_scd2

TABLE = "dim_test"

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("updated_at", StringType(), False),
    StructField("_change_type", StringType(), True),
    StructField("_commit_version", IntegerType(), True),
])


def _create_schema(spark: SparkSession, run_id: str) -> str:
    schema = f"kimball_scd2_bench_{run_id}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    return schema


def _drop_schema(spark: SparkSession, schema: str) -> None:
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def _create_target_table(spark: SparkSession, schema: str) -> None:
    spark.sql(f"""
        CREATE TABLE {schema}.{TABLE} (
            customer_sk BIGINT,
            customer_id INT,
            name STRING,
            email STRING,
            hashdiff BIGINT,
            __is_current BOOLEAN,
            __valid_from TIMESTAMP,
            __valid_to TIMESTAMP,
            __is_deleted BOOLEAN,
            __is_skeleton BOOLEAN,
            __etl_processed_at TIMESTAMP
        )
        USING DELTA
    """)


def _insert_default(spark: SparkSession, schema: str) -> None:
    spark.sql(f"""
        INSERT INTO {schema}.{TABLE}
        SELECT -1, NULL, 'N/A', 'N/A', 0, false,
               '1900-01-01'::timestamp, '2099-12-31'::timestamp,
               false, false, current_timestamp()
    """)


def _make_source(spark: SparkSession, data: list[dict[str, Any]]) -> DataFrame:
    rows = []
    for i, row in enumerate(data):
        r = dict(row)
        r.setdefault("_change_type", "insert")
        r.setdefault("_commit_version", i + 1)
        rows.append(r)
    return spark.createDataFrame(rows, CUSTOMER_SCHEMA)


def _count_rows(spark: SparkSession, schema: str) -> int:
    return spark.sql(f"SELECT COUNT(*) as cnt FROM {schema}.{TABLE}").first()["cnt"]


def _get_non_default_rows(spark: SparkSession, schema: str) -> list[dict]:
    rows = spark.sql(f"""
        SELECT customer_id, name, email, __is_current, __is_deleted, __is_skeleton
        FROM {schema}.{TABLE}
        WHERE customer_id IS NOT NULL
        ORDER BY customer_id, __valid_from
    """).toLocalIterator()
    return [row.asDict() for row in rows]


def _reset_table(spark: SparkSession, schema: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {schema}.{TABLE}")
    _create_target_table(spark, schema)
    _insert_default(spark, schema)


def _run_single_pass(spark: SparkSession, schema: str, source: DataFrame) -> None:
    _merge_single_pass(
        source, target_table_name=f"{schema}.{TABLE}",
        join_keys=["customer_id"], track_history_columns=["name", "email"],
        surrogate_key_col="customer_sk", effective_at_column="updated_at",
        schema_evolution=False,
    )


def _run_two_phase(spark: SparkSession, schema: str, source: DataFrame) -> None:
    merge_scd2(
        source, target_table_name=f"{schema}.{TABLE}",
        join_keys=["customer_id"], track_history_columns=["name", "email"],
        surrogate_key_col="customer_sk", effective_at_column="updated_at",
        schema_evolution=False,
    )


# Test data generators
def _gen_single_version():
    return _make_source(_spark, [
        {"customer_id": 1, "name": "Alice", "email": "alice@x.com", "updated_at": "2024-01-01"},
    ])


def _gen_two_versions():
    return _make_source(_spark, [
        {"customer_id": 1, "name": "Alice", "email": "alice@x.com", "updated_at": "2024-01-01"},
        {"customer_id": 1, "name": "Alice", "email": "alice@y.com", "updated_at": "2024-06-01"},
    ])


def _gen_three_versions():
    return _make_source(_spark, [
        {"customer_id": 1, "name": "Alice", "email": "alice@x.com", "updated_at": "2024-01-01"},
        {"customer_id": 1, "name": "Alice", "email": "alice@y.com", "updated_at": "2024-06-01"},
        {"customer_id": 1, "name": "Alice", "email": "alice@z.com", "updated_at": "2024-09-01"},
    ])


def _gen_mixed_keys():
    return _make_source(_spark, [
        {"customer_id": 1, "name": "Alice", "email": "alice@x.com", "updated_at": "2024-01-01"},
        {"customer_id": 1, "name": "Alice", "email": "alice@y.com", "updated_at": "2024-06-01"},
        {"customer_id": 2, "name": "Bob", "email": "bob@x.com", "updated_at": "2024-01-01"},
        {"customer_id": 3, "name": "Carol", "email": "carol@x.com", "updated_at": "2024-01-01"},
        {"customer_id": 3, "name": "Carol", "email": "carol@y.com", "updated_at": "2024-06-01"},
        {"customer_id": 3, "name": "Carol", "email": "carol@z.com", "updated_at": "2024-09-01"},
    ])


def _gen_large_batch(n_keys: int = 100, max_versions: int = 5):
    import random
    random.seed(42)
    data = []
    for key_id in range(1, n_keys + 1):
        n_versions = random.randint(1, max_versions)
        for v in range(n_versions):
            month = (v % 12) + 1
            year = 2024 + (v // 12)
            data.append({
                "customer_id": key_id,
                "name": f"Customer_{key_id}",
                "email": f"customer_{key_id}_v{v}@x.com",
                "updated_at": f"{year}-{month:02d}-01",
            })
    return _make_source(_spark, data)


SCENARIOS = [
    ("1 version/key", _gen_single_version),
    ("2 versions/key", _gen_two_versions),
    ("3 versions/key", _gen_three_versions),
    ("mixed keys (6 rows)", _gen_mixed_keys),
    ("100 keys, up to 5 versions", lambda: _gen_large_batch(100, 5)),
]


def test_benchmark(spark: SparkSession) -> None:
    run_id = uuid.uuid4().hex[:8]
    schema = _create_schema(spark, run_id)
    results = []

    try:
        for name, gen_source in SCENARIOS:
            source = gen_source()
            n_source_rows = source.count()

            # --- Single-pass ---
            _reset_table(spark, schema)
            t0 = time.time()
            _run_single_pass(spark, schema, source)
            sp_time = time.time() - t0
            sp_rows = _get_non_default_rows(spark, schema)
            sp_count = _count_rows(spark, schema)

            # --- Two-phase ---
            _reset_table(spark, schema)
            t0 = time.time()
            try:
                _run_two_phase(spark, schema, source)
                tp_time = time.time() - t0
                tp_rows = _get_non_default_rows(spark, schema)
                tp_count = _count_rows(spark, schema)
                tp_error = None
            except Exception as e:
                tp_time = 0
                tp_rows = []
                tp_count = -1
                tp_error = str(e)[:120]

            # Compare outputs
            match = sp_rows == tp_rows

            print(f"\n{'='*60}")
            print(f"Scenario: {name} ({n_source_rows} source rows)")
            print(f"  Single-pass: {sp_time:.2f}s, {sp_count} rows")
            if tp_error:
                print(f"  Two-phase:    ERROR ({tp_error})")
            else:
                print(f"  Two-phase:    {tp_time:.2f}s, {tp_count} rows")
                print(f"  Output match: {'YES' if match else 'NO'}")
                if not match:
                    print(f"    SP: {sp_rows}")
                    print(f"    TP: {tp_rows}")
            ratio = f"{tp_time/sp_time:.2f}x" if sp_time > 0 and tp_time > 0 else "N/A"
            print(f"  Speed ratio:  {ratio} (two-phase / single-pass)")

            results.append({
                "scenario": name,
                "source_rows": n_source_rows,
                "sp_time": sp_time,
                "tp_time": tp_time,
                "match": match,
                "tp_error": tp_error,
            })

        # Summary
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        print(f"{'Scenario':<30} {'SP (s)':>8} {'TP (s)':>8} {'Ratio':>8} {'Match':>6}")
        print("-" * 60)
        for r in results:
            ratio = f"{r['tp_time']/r['sp_time']:.2f}x" if r['sp_time'] > 0 and r['tp_time'] > 0 else "N/A"
            match = "YES" if r["match"] else "ERR" if r["tp_error"] else "NO"
            print(f"{r['scenario']:<30} {r['sp_time']:>8.2f} {r['tp_time']:>8.2f} {ratio:>8} {match:>6}")

    finally:
        _drop_schema(spark, schema)


if __name__ == "__main__":
    test_benchmark(_spark)
    _spark.stop()