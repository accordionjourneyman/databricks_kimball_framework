"""
Performance benchmarks using the existing conftest infrastructure.

These tests exercise the framework at multiple scale tiers and collect
timing metrics. Run with:
    python -m pytest tests/benchmarks/ -v -s
    python -m pytest tests/benchmarks/ -v -s --scale medium
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


def _generate_products(spark: SparkSession, n: int, db: str) -> None:
    """Generate n synthetic product rows."""
    spark.sql(f"DROP TABLE IF EXISTS {db}.products_src")
    spark.sql(f"""
        CREATE TABLE {db}.products_src (
            product_id INT, name STRING, price DECIMAL(10,2), category_id INT,
            brand STRING, color STRING, size STRING, weight_g INT,
            in_stock BOOLEAN, launch_date DATE, rating DOUBLE
        ) USING DELTA
    """)
    df = spark.range(n).select(
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
        date_add(lit("2024-01-01").cast("date"), (rand() * 1000).cast("int")).alias(
            "launch_date"
        ),
        (rand() * 5).alias("rating"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(f"{db}.products_src")


SCALE_TIERS = {
    "tiny": {"products": 1_000, "n_changed": 300, "n_deleted": 100, "n_new": 100},
    "small": {
        "products": 100_000,
        "n_changed": 30_000,
        "n_deleted": 10_000,
        "n_new": 10_000,
    },
    "medium": {
        "products": 1_000_000,
        "n_changed": 300_000,
        "n_deleted": 100_000,
        "n_new": 100_000,
    },
}


def pytest_addoption(parser):
    parser.addoption(
        "--scale",
        action="store",
        default="tiny",
        help="Scale tier: tiny, small, medium",
    )


@pytest.fixture
def scale(request):
    return request.config.getoption("--scale")


@pytest.fixture
def bench_db(spark: SparkSession):
    """Create a unique benchmark database."""
    db = f"kimball_bench_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    os.environ["KIMBALL_ETL_SCHEMA"] = db
    yield db
    spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")


def _make_config(
    db: str, table_suffix: str, scd_type: int = 1, track_cols: list[str] | None = None
) -> str:
    """Generate a config YAML for a products dimension."""
    track = track_cols or [
        "name",
        "price",
        "category_id",
        "brand",
        "color",
        "size",
        "weight_g",
        "in_stock",
        "launch_date",
        "rating",
    ]
    track_yaml = "[" + ", ".join(track) + "]" if scd_type == 2 else "[]"
    return f"""
table_name: {db}.dim_product{table_suffix}
table_type: dimension
scd_type: {scd_type}
keys:
  surrogate_key: product_sk
  natural_keys: [product_id]
surrogate_key_strategy: identity
track_history_columns: {track_yaml}
sources:
  - name: {db}.products_src
    alias: p
    cdc_strategy: full
transformation_sql: |
  SELECT product_id, name, price, category_id, brand, color, size,
         weight_g, in_stock, launch_date, rating
  FROM p
"""


def _write_config(content: str, tmp_path) -> str:
    path = tmp_path / f"bench_{uuid.uuid4().hex[:8]}.yml"
    path.write_text(content, encoding="utf-8")
    return str(path)


class TestBenchmarkSCD1:
    """SCD1 baseline: no history tracking, just upsert."""

    def test_scd1_baseline(self, spark: SparkSession, bench_db: str, scale, tmp_path):
        params = SCALE_TIERS[scale]
        _generate_products(spark, params["products"], bench_db)

        config_path = _write_config(
            _make_config(bench_db, "_scd1", scd_type=1), tmp_path
        )

        start = time.time()
        result = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        first_ms = (time.time() - start) * 1000

        assert result["status"] == "SUCCESS", f"First run failed: {result}"

        # Mutate: change prices
        spark.sql(f"""
            UPDATE {bench_db}.products_src
            SET price = price * 1.15
            WHERE product_id < {params["n_changed"]}
        """)

        start = time.time()
        result2 = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        second_ms = (time.time() - start) * 1000

        assert result2["status"] == "SUCCESS", f"Second run failed: {result2}"

        _save_result("scd1_baseline", scale, first_ms, second_ms, params)


class TestBenchmarkSCD2ChangeDetection:
    """SCD2 change detection: hashdiff comparison, row classification."""

    def test_scd2_change_detection(
        self, spark: SparkSession, bench_db: str, scale, tmp_path
    ):
        params = SCALE_TIERS[scale]
        _generate_products(spark, params["products"], bench_db)

        config_path = _write_config(
            _make_config(bench_db, "_scd2", scd_type=2), tmp_path
        )

        start = time.time()
        result = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        first_ms = (time.time() - start) * 1000

        assert result["status"] == "SUCCESS", f"First run failed: {result}"

        # Mutate: change some prices (triggers SCD2 versioning)
        spark.sql(f"""
            UPDATE {bench_db}.products_src
            SET price = price * 1.15
            WHERE product_id < {params["n_changed"]}
        """)

        start = time.time()
        result2 = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        second_ms = (time.time() - start) * 1000

        assert result2["status"] == "SUCCESS", f"Second run failed: {result2}"

        _save_result("scd2_change_detection", scale, first_ms, second_ms, params)


class TestBenchmarkSCD2FullCDCDelete:
    """SCD2 with full CDC: anti-join to detect deletes."""

    def test_scd2_full_cdc_delete(
        self, spark: SparkSession, bench_db: str, scale, tmp_path
    ):
        params = SCALE_TIERS[scale]
        _generate_products(spark, params["products"], bench_db)

        config_path = _write_config(
            _make_config(bench_db, "_scd2_del", scd_type=2), tmp_path
        )

        start = time.time()
        result = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        first_ms = (time.time() - start) * 1000

        assert result["status"] == "SUCCESS", f"First run failed: {result}"

        # Delete some products (triggers full CDC delete detection)
        spark.sql(
            f"DELETE FROM {bench_db}.products_src WHERE product_id < {params['n_deleted']}"
        )

        start = time.time()
        result2 = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        second_ms = (time.time() - start) * 1000

        assert result2["status"] == "SUCCESS", f"Second run failed: {result2}"

        _save_result("scd2_full_cdc_delete", scale, first_ms, second_ms, params)


class TestBenchmarkValidation:
    """Validation overhead: NK uniqueness + FK integrity checks."""

    def test_validation_overhead(
        self, spark: SparkSession, bench_db: str, scale, tmp_path
    ):
        params = SCALE_TIERS[scale]
        _generate_products(spark, params["products"], bench_db)

        config_path = _write_config(
            _make_config(bench_db, "_validated", scd_type=2), tmp_path
        )

        start = time.time()
        result = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        first_ms = (time.time() - start) * 1000

        assert result["status"] == "SUCCESS", f"First run failed: {result}"

        spark.sql(f"""
            UPDATE {bench_db}.products_src
            SET price = price * 1.10
            WHERE product_id < {params["n_changed"]}
        """)

        start = time.time()
        result2 = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        second_ms = (time.time() - start) * 1000

        assert result2["status"] == "SUCCESS", f"Second run failed: {result2}"

        _save_result("validation_overhead", scale, first_ms, second_ms, params)


def _save_result(
    scenario: str, scale: str, first_ms: float, second_ms: float, params: dict
):
    """Write benchmark result to JSON for the analyzer to read."""
    output_dir = (
        Path(__file__).parent.parent.parent / "tools" / "benchmarks" / "results"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    result = {
        "scenario": scenario,
        "scale": scale,
        "params": params,
        "first_run_total_ms": first_ms,
        "second_run_total_ms": second_ms,
        "first_run_status": "SUCCESS",
        "second_run_status": "SUCCESS",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    outfile = output_dir / f"{scenario}_{scale}.json"
    with open(outfile, "w") as f:
        json.dump(result, f, indent=2)
    print(
        f"\n  [{scenario} @ {scale}] first={first_ms:.0f}ms  second={second_ms:.0f}ms"
    )
