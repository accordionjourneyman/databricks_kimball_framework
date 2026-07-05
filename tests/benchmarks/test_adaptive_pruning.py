"""
Benchmark adaptive column pruning: measure I/O savings when
schema_evolution=True but the target already has the needed columns.
"""

import json
import os
import time
import uuid
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, date_add, lit, rand, when

from kimball.orchestration.orchestrator import Orchestrator


def _generate_products_wide(
    spark: SparkSession, n: int, db: str, extra_cols: int = 20
) -> None:
    """Generate n products with many extra columns to make pruning worthwhile."""
    spark.sql(f"DROP TABLE IF EXISTS {db}.products_src")
    extra_col_defs = ", ".join(
        f"extra_{i} STRING" for i in range(extra_cols)
    )
    spark.sql(f"""
        CREATE TABLE {db}.products_src (
            product_id INT, name STRING, price DECIMAL(10,2), category_id INT,
            brand STRING, color STRING, size STRING, weight_g INT,
            in_stock BOOLEAN, launch_date DATE, rating DOUBLE,
            {extra_col_defs}
        ) USING DELTA
    """)
    select_cols = [
        col("id").cast("int").alias("product_id"),
        concat(lit("Product_"), col("id").cast("string")).alias("name"),
        (rand() * 1000).cast("decimal(10,2)").alias("price"),
        (rand() * 100).cast("int").alias("category_id"),
        concat(lit("Brand_"), (rand() * 50).cast("int").cast("string")).alias("brand"),
        when(rand() < 0.3, lit("red"))
        .when(rand() < 0.6, lit("blue"))
        .otherwise(lit("green"))
        .alias("color"),
        when(rand() < 0.5, lit("S"))
        .when(rand() < 0.8, lit("M"))
        .otherwise(lit("L"))
        .alias("size"),
        (rand() * 5000).cast("int").alias("weight_g"),
        (rand() > 0.2).alias("in_stock"),
        date_add(lit("2024-01-01").cast("date"), (rand() * 1000).cast("int")).alias("launch_date"),
        (rand() * 5).alias("rating"),
    ]
    for i in range(extra_cols):
        select_cols.append(
            concat(lit(f"val_"), (rand() * 1000).cast("int").cast("string")).alias(f"extra_{i}")
        )
    df = spark.range(n).select(*select_cols)
    df.write.format("delta").mode("overwrite").saveAsTable(f"{db}.products_src")


SCALE_PARAMS = {
    "tiny": {"products": 5_000, "n_changed": 1_500, "extra_cols": 20},
    "small": {"products": 100_000, "n_changed": 30_000, "extra_cols": 20},
}


def _make_config_pruning(db: str, schema_evolution: bool) -> str:
    se = "true" if schema_evolution else "false"
    return f"""
table_name: {db}.dim_product_prune
table_type: dimension
scd_type: 2
keys:
  surrogate_key: product_sk
  natural_keys: [product_id]
surrogate_key_strategy: identity
track_history_columns: [name, price, category_id, brand, color, size, weight_g, in_stock, launch_date, rating]
schema_evolution: {se}
sources:
  - name: {db}.products_src
    alias: p
    cdc_strategy: full
transformation_sql: |
  SELECT product_id, name, price, category_id, brand, color, size,
         weight_g, in_stock, launch_date, rating
  FROM p
"""


def _write(content: str, tmp_path) -> str:
    path = tmp_path / f"p_{uuid.uuid4().hex[:8]}.yml"
    path.write_text(content, encoding="utf-8")
    return str(path)


def _save(scenario: str, scale: str, **kwargs):
    out_dir = Path(__file__).parent.parent.parent / "tools" / "benchmarks" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    data = {"scenario": scenario, "scale": scale, **kwargs, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")}
    out = out_dir / f"{scenario}_{scale}.json"
    with open(out, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  [{scenario} @ {scale}] {kwargs}")


class TestAdaptivePruning:
    """Measure I/O savings from adaptive column pruning with wide source tables."""

    def test_no_pruning_baseline(
        self, spark: SparkSession, bench_db: str, scale, tmp_path
    ):
        """Baseline: schema_evolution=true (old behavior = no pruning at all)."""
        params = SCALE_PARAMS[scale]
        _generate_products_wide(spark, params["products"], bench_db, params["extra_cols"])
        config_path = _write(_make_config_pruning(bench_db, schema_evolution=True), tmp_path)

        start = time.time()
        result = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        first_ms = (time.time() - start) * 1000

        spark.sql(f"""
            UPDATE {bench_db}.products_src
            SET price = price * 1.15
            WHERE product_id < {params['n_changed']}
        """)

        start = time.time()
        result2 = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        second_ms = (time.time() - start) * 1000

        assert result["status"] == "SUCCESS"
        assert result2["status"] == "SUCCESS"

        _save("pruning_baseline", scale, first_ms=first_ms, second_ms=second_ms, params=params)

    def test_adaptive_pruning(
        self, spark: SparkSession, bench_db: str, scale, tmp_path
    ):
        """New adaptive pruning: drops unused extra columns even with schema_evolution=true."""
        params = SCALE_PARAMS[scale]
        _generate_products_wide(spark, params["products"], bench_db, params["extra_cols"])
        config_path = _write(_make_config_pruning(bench_db, schema_evolution=True), tmp_path)

        start = time.time()
        result = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        first_ms = (time.time() - start) * 1000

        spark.sql(f"""
            UPDATE {bench_db}.products_src
            SET price = price * 1.15
            WHERE product_id < {params['n_changed']}
        """)

        start = time.time()
        result2 = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        second_ms = (time.time() - start) * 1000

        assert result["status"] == "SUCCESS"
        assert result2["status"] == "SUCCESS"

        target = spark.table(f"{bench_db}.dim_product_prune")
        target_cols = {f.name for f in target.schema.fields}
        tracked_in_target = {"name", "price", "category_id"} <= target_cols
        extras_in_target = any(c.startswith("extra_") for c in target_cols)

        _save(
            "pruning_adaptive",
            scale,
            first_ms=first_ms,
            second_ms=second_ms,
            params=params,
            tracked_columns_in_target=tracked_in_target,
            extra_columns_in_target=extras_in_target,
        )
