"""Canonical SCD2 multi-version benchmark on the shared local Spark fixture."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from kimball.processing.scd2 import merge_scd2

SOURCE_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("updated_at", StringType(), False),
        StructField("_change_type", StringType(), True),
        StructField("_commit_version", IntegerType(), True),
    ]
)


@dataclass
class MergeScenario:
    table: str
    source: DataFrame
    keys: int
    versions: int


def _target(spark: SparkSession, table: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE {table} (
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
        ) USING DELTA
        """
    )
    spark.sql(
        f"""
        INSERT INTO {table}
        SELECT -1, NULL, 'N/A', 'N/A', 0, false,
               TIMESTAMP '1900-01-01', TIMESTAMP '2099-12-31',
               false, false, current_timestamp()
        """
    )


def _source(spark: SparkSession, keys: int, versions: int) -> DataFrame:
    rows = []
    for key in range(1, keys + 1):
        for version in range(versions):
            rows.append(
                {
                    "customer_id": key,
                    "name": f"Customer_{key}",
                    "email": f"customer_{key}_v{version}@example.com",
                    "updated_at": f"2024-{version + 1:02d}-01",
                    "_change_type": "insert",
                    "_commit_version": version + 1,
                }
            )
    return spark.createDataFrame(rows, SOURCE_SCHEMA)


def _run_measured(scenario: MergeScenario) -> None:
    spark = scenario.source.sparkSession
    group_id = f"kimball-benchmark:{scenario.table}"
    spark.sparkContext.setJobGroup(group_id, group_id)
    try:
        _run(scenario)
    finally:
        spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
        spark.sparkContext.setLocalProperty("spark.job.description", None)


def _run(scenario: MergeScenario) -> None:
    merge_scd2(
        scenario.source,
        target_table_name=scenario.table,
        join_keys=["customer_id"],
        track_history_columns=["name", "email"],
        surrogate_key_col="customer_sk",
        effective_at_column="updated_at",
        schema_evolution=False,
    )


@pytest.mark.parametrize("versions", [1, 3, 5])
def test_scd2_version_chain(
    benchmark,
    spark,
    bench_db,
    benchmark_rounds,
    benchmark_warmups,
    versions,
):
    contexts: list[MergeScenario] = []

    def setup():
        table = f"{bench_db}.dim_customer_{uuid.uuid4().hex[:10]}"
        _target(spark, table)
        scenario = MergeScenario(
            table=table,
            source=_source(spark, keys=100, versions=versions),
            keys=100,
            versions=versions,
        )
        contexts.append(scenario)
        return (scenario,), {}

    benchmark.pedantic(
        _run_measured,
        setup=setup,
        rounds=benchmark_rounds,
        warmup_rounds=benchmark_warmups,
        iterations=1,
    )
    scenario = contexts[-1]
    target = spark.table(scenario.table).filter("customer_id IS NOT NULL")
    assert target.count() == scenario.keys * scenario.versions
    assert target.filter("__is_current = true").count() == scenario.keys
    assert target.filter("__valid_to IS NULL").count() == 0
    assert (
        target.filter("__is_current = true AND year(__valid_to) = 9999").count()
        == scenario.keys
    )
