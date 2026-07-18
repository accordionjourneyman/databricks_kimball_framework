"""Current-code end-to-end Spark/Delta benchmarks.

Each measured round gets fresh source and target tables. Data generation,
initial-state preparation, and correctness assertions are outside the timed
region; pytest-benchmark owns warmups, samples, and JSON serialization.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from kimball.common.config import ForeignKeyConfig, NullPolicyConfig
from kimball.common.constants import DEFAULT_VALID_TO
from kimball.orchestration.orchestrator import Orchestrator
from kimball.processing.key_broker import KeyBroker
from kimball.processing.key_generator import HashKeyGenerator


@dataclass
class Scenario:
    config_path: str
    database: str
    source_table: str
    target_table: str
    rows: int
    changed: int
    deleted: int


def _source(
    spark: SparkSession,
    table: str,
    rows: int,
    *,
    extra_columns: int = 0,
) -> None:
    columns = [
        F.col("id").cast("int").alias("product_id"),
        F.concat(F.lit("Product_"), F.col("id")).alias("name"),
        ((F.col("id") % 10_000) / F.lit(100)).cast("decimal(12,2)").alias("price"),
        (F.col("id") % 100).cast("int").alias("category_id"),
        F.lit("2024-01-01 00:00:00").cast("timestamp").alias("updated_at"),
    ]
    columns.extend(
        F.concat(F.lit(f"extra_{index}_"), F.col("id")).alias(f"extra_{index}")
        for index in range(extra_columns)
    )
    spark.range(rows).select(*columns).write.format("delta").mode(
        "overwrite"
    ).saveAsTable(table)


def _config(
    path: Path,
    *,
    source_table: str,
    target_table: str,
    scd_type: int,
    schema_evolution: bool = False,
    validation: bool = False,
) -> str:
    effective_at = "effective_at: updated_at" if scd_type in (2, 7) else ""
    durable_key = "durable_key: product_dk" if scd_type == 7 else ""
    tests = (
        """
tests:
  - column: product_id
    tests: [unique, not_null]
    severity: error
  - column: price
    tests: [not_null]
    severity: error
"""
        if validation
        else ""
    )
    path.write_text(
        f"""
table_name: {target_table}
table_type: dimension
scd_type: {scd_type}
{effective_at}
surrogate_key: product_sk
{durable_key}
natural_keys: [product_id]
track_history_columns: [name, price, category_id]
schema_evolution: {str(schema_evolution).lower()}
{tests}
sources:
  - name: {source_table}
    alias: p
    cdc_strategy: full
transformation_sql: |
  SELECT product_id, name, price, category_id, updated_at FROM p
""",
        encoding="utf-8",
    )
    return str(path)


def _prepare(
    spark: SparkSession,
    database: str,
    tmp_path: Path,
    params: dict[str, int],
    *,
    scd_type: int,
    phase: str,
    schema_evolution: bool = False,
    validation: bool = False,
) -> Scenario:
    token = uuid.uuid4().hex[:10]
    source_table = f"{database}.products_{token}"
    target_table = f"{database}.dim_products_{token}"
    _source(
        spark,
        source_table,
        params["products"],
        extra_columns=20 if schema_evolution else 0,
    )
    config_path = _config(
        tmp_path / f"{token}.yml",
        source_table=source_table,
        target_table=target_table,
        scd_type=scd_type,
        schema_evolution=schema_evolution,
        validation=validation,
    )
    scenario = Scenario(
        config_path=config_path,
        database=database,
        source_table=source_table,
        target_table=target_table,
        rows=params["products"],
        changed=params["changed"],
        deleted=params["deleted"],
    )
    if phase != "initial":
        initial = _run(spark, scenario)
        assert initial["status"] == "SUCCESS"
    if phase == "changed":
        spark.sql(
            f"""
            UPDATE {source_table}
            SET price = price + 1,
                updated_at = TIMESTAMP '2024-02-01 00:00:00'
            WHERE product_id < {scenario.changed}
            """
        )
    elif phase == "deleted":
        spark.sql(f"DELETE FROM {source_table} WHERE product_id < {scenario.deleted}")
    return scenario


def _run(spark: SparkSession, scenario: Scenario) -> dict[str, Any]:
    return Orchestrator(
        scenario.config_path,
        spark=spark,
        etl_schema=scenario.database,
    ).run()


def _run_measured(spark: SparkSession, scenario: Scenario) -> dict[str, Any]:
    group_id = f"kimball-benchmark:{scenario.target_table}"
    spark.sparkContext.setJobGroup(group_id, group_id)
    try:
        return _run(spark, scenario)
    finally:
        spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
        spark.sparkContext.setLocalProperty("spark.job.description", None)


def _measure(
    benchmark,
    spark: SparkSession,
    bench_db: str,
    tmp_path: Path,
    scale_params: dict[str, int],
    benchmark_rounds: int,
    benchmark_warmups: int,
    *,
    scd_type: int,
    phase: str,
    schema_evolution: bool = False,
    validation: bool = False,
) -> tuple[dict[str, Any], Scenario]:
    contexts: list[Scenario] = []

    def setup():
        scenario = _prepare(
            spark,
            bench_db,
            tmp_path,
            scale_params,
            scd_type=scd_type,
            phase=phase,
            schema_evolution=schema_evolution,
            validation=validation,
        )
        contexts.append(scenario)
        return (spark, scenario), {}

    result = benchmark.pedantic(
        _run_measured,
        setup=setup,
        rounds=benchmark_rounds,
        warmup_rounds=benchmark_warmups,
        iterations=1,
    )
    assert result["status"] == "SUCCESS"
    return result, contexts[-1]


@pytest.mark.parametrize(
    ("name", "scd_type", "phase"),
    [
        ("scd1_initial", 1, "initial"),
        ("scd1_changed", 1, "changed"),
        ("scd1_noop", 1, "noop"),
        ("scd2_changed", 2, "changed"),
        ("scd2_full_snapshot_delete", 2, "deleted"),
        ("scd7_changed", 7, "changed"),
    ],
    ids=lambda value: str(value),
)
def test_pipeline_scenarios(
    benchmark,
    spark,
    bench_db,
    tmp_path,
    scale_params,
    benchmark_rounds,
    benchmark_warmups,
    name,
    scd_type,
    phase,
):
    benchmark.group = "pipeline"
    benchmark.name = name
    _, scenario = _measure(
        benchmark,
        spark,
        bench_db,
        tmp_path,
        scale_params,
        benchmark_rounds,
        benchmark_warmups,
        scd_type=scd_type,
        phase=phase,
    )
    target = spark.table(scenario.target_table).filter("product_id IS NOT NULL")
    target = target.filter("product_id >= 0")
    if scd_type == 1:
        assert target.count() == scenario.rows
    else:
        assert target.filter("__is_current = true").count() == (
            scenario.rows - (scenario.deleted if phase == "deleted" else 0)
        )
        if phase == "changed":
            assert target.count() == scenario.rows + scenario.changed
        if scd_type == 7:
            assert target.filter("product_dk IS NULL").count() == 0


def test_type7_key_broker_range_lookup(
    benchmark,
    spark,
    bench_db,
    scale_params,
):
    token = uuid.uuid4().hex[:10]
    dimension_table = f"{bench_db}.dim_customer_broker_{token}"
    row_count = scale_params["products"]
    base = spark.range(row_count).select(
        F.col("id").cast("string").alias("customer_id"),
        F.lit("2024-01-01").cast("timestamp").alias("updated_at"),
        F.lit("2024-01-01").cast("timestamp").alias("__valid_from"),
        F.lit(DEFAULT_VALID_TO).cast("timestamp").alias("__valid_to"),
    )
    (
        HashKeyGenerator(["customer_id"], version_column="updated_at")
        .generate_type7_keys(base, "customer_sk", "customer_dk")
        .write.format("delta")
        .saveAsTable(dimension_table)
    )
    facts = spark.range(row_count).select(
        F.col("id").alias("order_id"),
        F.col("id").cast("string").alias("customer_id"),
        F.lit("2025-01-01").cast("timestamp").alias("order_at"),
    )
    fk = ForeignKeyConfig(
        column="customer_sk",
        references=dimension_table,
        dimension_key="customer_sk",
        relationship="type7",
        durable_column="customer_dk",
        durable_dimension_key="customer_dk",
        lookup={
            "source_columns": ["customer_id"],
            "event_time": "order_at",
            "early_arriving": "error",
        },
    )

    def measured():
        return (
            KeyBroker(spark)
            .resolve_fact_keys(
                facts,
                [fk],
                batch_id="benchmark",
                null_policy=NullPolicyConfig(),
            )
            .agg(
                F.sum("customer_sk").alias("sk_checksum"),
                F.sum("customer_dk").alias("dk_checksum"),
            )
            .collect()
        )

    benchmark.group = "key_broker"
    result = benchmark(measured)
    assert result[0]["sk_checksum"] is not None
    assert result[0]["dk_checksum"] is not None


def test_wide_source_adaptive_pruning(
    benchmark,
    spark,
    bench_db,
    tmp_path,
    scale_params,
    benchmark_rounds,
    benchmark_warmups,
):
    _, scenario = _measure(
        benchmark,
        spark,
        bench_db,
        tmp_path,
        scale_params,
        benchmark_rounds,
        benchmark_warmups,
        scd_type=2,
        phase="initial",
        schema_evolution=True,
    )
    columns = set(spark.table(scenario.target_table).columns)
    assert {"name", "price", "category_id"} <= columns
    assert not any(column.startswith("extra_") for column in columns)


@pytest.mark.parametrize("approximate", [False, True], ids=["exact", "approximate"])
def test_validation(
    benchmark,
    spark,
    bench_db,
    tmp_path,
    scale_params,
    benchmark_rounds,
    benchmark_warmups,
    monkeypatch,
    approximate,
):
    monkeypatch.setenv("KIMBALL_USE_APPROXIMATE_UNIQUE", "1" if approximate else "0")
    _measure(
        benchmark,
        spark,
        bench_db,
        tmp_path,
        scale_params,
        benchmark_rounds,
        benchmark_warmups,
        scd_type=2,
        phase="changed",
        validation=True,
    )
