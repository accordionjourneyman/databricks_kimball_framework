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


class TestBenchmarkSCD2NoOp:
    """SCD2 no-op: re-run with no source changes.

    Measures the overhead of the change-detection machinery (hashdiff
    compute, left join to target, target scan) when zero rows actually
    change.  A well-optimised merge should skip the write entirely.
    """

    def test_scd2_noop(self, spark: SparkSession, bench_db: str, scale, tmp_path):
        params = SCALE_TIERS[scale]
        _generate_products(spark, params["products"], bench_db)

        config_path = _write_config(
            _make_config(bench_db, "_noop", scd_type=2), tmp_path
        )

        # Initial load
        result = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        assert result["status"] == "SUCCESS"

        # Second run — no changes at all
        start = time.time()
        result2 = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        noop_ms = (time.time() - start) * 1000

        assert result2["status"] == "SUCCESS", f"No-op run failed: {result2}"

        # Compare to a change run for context
        spark.sql(f"""
            UPDATE {bench_db}.products_src
            SET price = price * 1.15
            WHERE product_id < {params["n_changed"]}
        """)
        start = time.time()
        result3 = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        change_ms = (time.time() - start) * 1000
        assert result3["status"] == "SUCCESS"

        _save_result(
            "scd2_noop",
            scale,
            noop_ms,
            change_ms,
            {**params, "noop_ms": noop_ms, "change_ms": change_ms},
        )


class TestBenchmarkSCD2MultiVersion:
    """SCD2 two-phase merge: multiple versions per key in one batch.

    Measures the overhead of the two-phase algorithm (phase 1 latest
    merge + phase 2 history back-fill) vs a single-version classic merge
    with the same total row count.
    """

    def test_scd2_multiversion(
        self, spark: SparkSession, bench_db: str, scale, tmp_path
    ):
        params = SCALE_TIERS[scale]
        _generate_products(spark, params["products"], bench_db)

        config_path = _write_config(_make_config(bench_db, "_mv", scd_type=2), tmp_path)

        # Initial load
        result = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        assert result["status"] == "SUCCESS"

        # Apply N separate UPDATE transactions so the full-snapshot reload
        # sees multiple versions per key (forces two-phase path).
        n_versions = 3
        for i in range(n_versions):
            spark.sql(f"""
                UPDATE {bench_db}.products_src
                SET price = price * 1.{10 + i}
                WHERE product_id < {params["n_changed"]}
            """)

        start = time.time()
        result2 = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        multiversion_ms = (time.time() - start) * 1000
        assert result2["status"] == "SUCCESS", f"Multi-version run failed: {result2}"

        # Compare to a single-change run with the same number of changed rows
        _generate_products(spark, params["products"], bench_db)
        config_path2 = _write_config(
            _make_config(bench_db, "_sv", scd_type=2), tmp_path
        )
        result = Orchestrator(config_path2, spark=spark, etl_schema=bench_db).run()
        assert result["status"] == "SUCCESS"

        spark.sql(f"""
            UPDATE {bench_db}.products_src
            SET price = price * 1.15
            WHERE product_id < {params["n_changed"]}
        """)
        start = time.time()
        result3 = Orchestrator(config_path2, spark=spark, etl_schema=bench_db).run()
        singleversion_ms = (time.time() - start) * 1000
        assert result3["status"] == "SUCCESS"

        _save_result(
            "scd2_multiversion",
            scale,
            multiversion_ms,
            singleversion_ms,
            {
                **params,
                "n_versions": n_versions,
                "multiversion_ms": multiversion_ms,
                "singleversion_ms": singleversion_ms,
            },
        )


class TestBenchmarkSCD1NoOp:
    """SCD1 no-op: re-run with no source changes.

    SCD1 has no hashdiff comparison — it always overwrites.  This
    measures the bare overhead of the SCD1 merge (dedup, join,
    overwrite) when nothing changed.
    """

    def test_scd1_noop(self, spark: SparkSession, bench_db: str, scale, tmp_path):
        params = SCALE_TIERS[scale]
        _generate_products(spark, params["products"], bench_db)

        config_path = _write_config(
            _make_config(bench_db, "_s1noop", scd_type=1), tmp_path
        )

        result = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        assert result["status"] == "SUCCESS"

        start = time.time()
        result2 = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        noop_ms = (time.time() - start) * 1000
        assert result2["status"] == "SUCCESS"

        spark.sql(f"""
            UPDATE {bench_db}.products_src
            SET price = price * 1.15
            WHERE product_id < {params["n_changed"]}
        """)
        start = time.time()
        result3 = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        change_ms = (time.time() - start) * 1000
        assert result3["status"] == "SUCCESS"

        _save_result("scd1_noop", scale, noop_ms, change_ms, params)


class TestBenchmarkWideTable:
    """Wide table: 50+ columns to stress hashdiff and adaptive pruning.

    The SCD2 hashdiff hashes all track_history_columns.  With 50
    columns the hash compute is more expensive and the join carries
    more data.  This benchmark isolates that cost vs the 10-column
    baseline.
    """

    WIDE_COLS = [f"col_{i}" for i in range(50)]

    def test_wide_scd2(self, spark: SparkSession, bench_db: str, scale, tmp_path):
        params = SCALE_TIERS[scale]
        n = params["products"]

        spark.sql(f"DROP TABLE IF EXISTS {bench_db}.wide_src")
        col_defs = ", ".join(f"col_{i} STRING" for i in range(50))
        spark.sql(f"""
            CREATE TABLE {bench_db}.wide_src (
                product_id INT, {col_defs}
            ) USING DELTA
        """)

        df = spark.range(n).select(
            col("id").cast("int").alias("product_id"),
            *[
                concat(lit("v_"), col("id").cast("string"), lit(f"_{i}")).alias(
                    f"col_{i}"
                )
                for i in range(50)
            ],
        )
        df.write.format("delta").mode("overwrite").saveAsTable(f"{bench_db}.wide_src")

        track_yaml = "[" + ", ".join(f"col_{i}" for i in range(50)) + "]"
        config_yaml = f"""
table_name: {bench_db}.dim_wide
table_type: dimension
scd_type: 2
keys:
  surrogate_key: product_sk
  natural_keys: [product_id]
track_history_columns: {track_yaml}
grain_validation: skip
sources:
  - name: {bench_db}.wide_src
    alias: w
    cdc_strategy: full
transformation_sql: |
  SELECT product_id, {", ".join(f"col_{i}" for i in range(50))} FROM w
"""
        config_path = _write_config(config_yaml, tmp_path)

        start = time.time()
        result = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        first_ms = (time.time() - start) * 1000
        assert result["status"] == "SUCCESS"

        spark.sql(f"""
            UPDATE {bench_db}.wide_src
            SET col_0 = concat(col_0, '_changed')
            WHERE product_id < {params["n_changed"]}
        """)
        start = time.time()
        result2 = Orchestrator(config_path, spark=spark, etl_schema=bench_db).run()
        second_ms = (time.time() - start) * 1000
        assert result2["status"] == "SUCCESS"

        _save_result(
            "wide_scd2",
            scale,
            first_ms,
            second_ms,
            {**params, "n_columns": 50},
        )
