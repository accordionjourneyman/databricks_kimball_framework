"""
dbt-style validation benchmarks: compare full validation vs state-aware skip
vs approximate uniqueness.
"""

import json
import os
import time
import uuid
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, date_add, lit, rand, when

from kimball.orchestration.orchestrator import Orchestrator


def _generate_products(spark: SparkSession, n: int, db: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {db}.products_src")
    spark.sql(f"""
        CREATE TABLE {db}.products_src (
            product_id INT, name STRING, price DECIMAL(10,2), category_id INT,
            brand STRING, color STRING, size STRING, weight_g INT,
            in_stock BOOLEAN, launch_date DATE, rating DOUBLE
        ) USING DELTA
    """)
    df = (
        spark.range(n)
        .select(
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
        )
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{db}.products_src")


SCALE_PARAMS = {
    "tiny": {"products": 5_000, "n_changed": 1_500, "n_deleted": 500, "n_new": 500},
    "small": {"products": 100_000, "n_changed": 30_000, "n_deleted": 10_000, "n_new": 10_000},
}


def _make_config(db: str) -> str:
    return f"""
table_name: {db}.dim_product_v
table_type: dimension
scd_type: 2
keys:
  surrogate_key: product_sk
  natural_keys: [product_id]
surrogate_key_strategy: identity
track_history_columns: [name, price, category_id, brand, color, size, weight_g, in_stock, launch_date, rating]
tests:
  - column: product_id
    tests: [unique, not_null]
    severity: error
  - column: price
    tests: [not_null]
    severity: warn
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
    path = tmp_path / f"v_{uuid.uuid4().hex[:8]}.yml"
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


class TestValidationOptimizations:
    """Compare full validation vs state-aware skip vs approximate unique."""

    def test_full_validation_baseline(
        self, spark: SparkSession, bench_db: str, scale, tmp_path
    ):
        """Baseline: run with all validations enabled (no optimizations)."""
        params = SCALE_PARAMS[scale]
        _generate_products(spark, params["products"], bench_db)
        config_path = _write(_make_config(bench_db), tmp_path)

        os.environ.pop("KIMBALL_SKIP_VALIDATION_IF_UNCHANGED", None)
        os.environ.pop("KIMBALL_USE_APPROXIMATE_UNIQUE", None)

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

        _save("validation_full", scale, first_ms=first_ms, second_ms=second_ms,
              params=params)

    def test_state_aware_skip(
        self, spark: SparkSession, bench_db: str, scale, tmp_path
    ):
        """dbt state:modified+ equivalent: skip validation when fingerprints unchanged."""
        params = SCALE_PARAMS[scale]
        _generate_products(spark, params["products"], bench_db)
        config_path = _write(_make_config(bench_db), tmp_path)

        os.environ["KIMBALL_SKIP_VALIDATION_IF_UNCHANGED"] = "1"

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

        _save("validation_state_aware", scale, first_ms=first_ms, second_ms=second_ms,
              params=params)

    def test_approximate_unique(
        self, spark: SparkSession, bench_db: str, scale, tmp_path
    ):
        """Use HLL-based approx_count_distinct instead of exact groupBy."""
        params = SCALE_PARAMS[scale]
        _generate_products(spark, params["products"], bench_db)
        config_path = _write(_make_config(bench_db), tmp_path)

        os.environ.pop("KIMBALL_SKIP_VALIDATION_IF_UNCHANGED", None)
        os.environ["KIMBALL_USE_APPROXIMATE_UNIQUE"] = "1"

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

        _save("validation_approximate", scale, first_ms=first_ms, second_ms=second_ms,
              params=params)

    def test_combined_optimizations(
        self, spark: SparkSession, bench_db: str, scale, tmp_path
    ):
        """Both optimizations: state-aware skip + approximate unique."""
        params = SCALE_PARAMS[scale]
        _generate_products(spark, params["products"], bench_db)
        config_path = _write(_make_config(bench_db), tmp_path)

        os.environ["KIMBALL_SKIP_VALIDATION_IF_UNCHANGED"] = "1"
        os.environ["KIMBALL_USE_APPROXIMATE_UNIQUE"] = "1"

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

        _save("validation_combined", scale, first_ms=first_ms, second_ms=second_ms,
              params=params)
