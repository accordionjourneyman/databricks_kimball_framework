"""Compare physical plans for xxhash64 vs sha256 hashdiff in SCD2 merge.

Run inside Docker:
    docker-compose run --rm kimball-tests python tools/benchmarks/compare_hash_plans.py
"""

import tempfile
import time

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, sha2, when, xxhash64

_NULL_SENTINEL = "__NULL_SENTINEL_12345678123456781234567812345678__"


def build_spark() -> tuple[SparkSession, str]:
    """Build a local SparkSession and return (session, warehouse_dir)."""
    warehouse_dir = tempfile.mkdtemp(prefix="spark-warehouse-")
    builder = (
        SparkSession.builder.appName("HashPlanCompare")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.warehouse.dir", warehouse_dir)
    )
    return configure_spark_with_delta_pip(builder).getOrCreate(), warehouse_dir


def compute_hashdiff_xxhash64_str(columns):
    ordered = sorted(columns)
    normalized = [
        when(col(c).isNull(), lit(_NULL_SENTINEL)).otherwise(col(c).cast("string"))
        for c in ordered
    ]
    return xxhash64(concat_ws("\x00", *normalized)).cast("string")


def compute_hashdiff_sha256(columns):
    ordered = sorted(columns)
    normalized = [
        when(col(c).isNull(), lit(_NULL_SENTINEL)).otherwise(col(c).cast("string"))
        for c in ordered
    ]
    return sha2(concat_ws("\x00", *normalized), 256)


def main():
    spark, warehouse_dir = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    db = "kimball_hash_compare"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    # Create source table with 10K rows
    spark.sql(f"DROP TABLE IF EXISTS {db}.products_src")
    spark.sql(f"""
        CREATE TABLE {db}.products_src (
            product_id INT, name STRING, price DOUBLE, category_id INT
        ) USING DELTA
    """)
    spark.sql(f"""
        INSERT INTO {db}.products_src
        SELECT id, CONCAT('Product_', CAST(id AS STRING)), RAND() * 100, CAST(RAND() * 10 AS INT)
        FROM RANGE(10000)
    """)

    # Create target SCD2 table (hashdiff is STRING to match both algorithms)
    spark.sql(f"DROP TABLE IF EXISTS {db}.dim_product")
    spark.sql(f"""
        CREATE TABLE {db}.dim_product (
            product_sk BIGINT, product_id INT, name STRING, price DOUBLE, category_id INT,
            hashdiff STRING, __is_current BOOLEAN, __valid_from TIMESTAMP, __valid_to TIMESTAMP,
            __etl_processed_at TIMESTAMP, __is_deleted BOOLEAN
        ) USING DELTA
    """)
    spark.sql(f"""
        INSERT INTO {db}.dim_product
        SELECT id, id, CONCAT('Product_', CAST(id AS STRING)), RAND() * 100, CAST(RAND() * 10 AS INT),
        'abc123', true, TIMESTAMP('2024-01-01'), TIMESTAMP('9999-12-31'),
        current_timestamp(), false
        FROM RANGE(5000)
    """)

    track_cols = ["name", "price", "category_id"]
    join_keys = ["product_id"]

    source = spark.table(f"{db}.products_src")

    # --- Plan 1: xxhash64 (cast to string) ---
    print("\n" + "=" * 80)
    print("PLAN 1: xxhash64 cast to STRING (no comparison-time cast)")
    print("=" * 80)

    source_xx = source.withColumn("hashdiff", compute_hashdiff_xxhash64_str(track_cols))
    target_xx = spark.table(f"{db}.dim_product").filter("__is_current = true")
    source_keys = source_xx.select(*join_keys).distinct()
    target_pruned = target_xx.join(source_keys, join_keys, "semi")

    joined_xx = (
        source_xx.alias("s")
        .join(
            target_pruned.alias("t"),
            col("s.product_id").eqNullSafe(col("t.product_id")),
            "left",
        )
        .select(
            col("s.*"),
            col("t.hashdiff").alias("target_hashdiff"),
        )
    )
    changed_xx = joined_xx.filter(
        col("target_hashdiff").isNull() | (col("hashdiff") != col("target_hashdiff"))
    )

    changed_xx.explain(mode="formatted")

    # --- Plan 2: sha256 ---
    print("\n" + "=" * 80)
    print("PLAN 2: sha256 (string hashdiff)")
    print("=" * 80)

    source_sha = source.withColumn("hashdiff", compute_hashdiff_sha256(track_cols))
    target_sha = spark.table(f"{db}.dim_product").filter("__is_current = true")
    source_keys2 = source_sha.select(*join_keys).distinct()
    target_pruned2 = target_sha.join(source_keys2, join_keys, "semi")

    joined_sha = (
        source_sha.alias("s")
        .join(
            target_pruned2.alias("t"),
            col("s.product_id").eqNullSafe(col("t.product_id")),
            "left",
        )
        .select(
            col("s.*"),
            col("t.hashdiff").alias("target_hashdiff"),
        )
    )
    changed_sha = joined_sha.filter(
        col("target_hashdiff").isNull() | (col("hashdiff") != col("target_hashdiff"))
    )

    changed_sha.explain(mode="formatted")

    # --- Fair microbenchmark: warm cache, 100K rows, 10 cols ---
    print("\n" + "=" * 80)
    print("MICROBENCHMARK: hash compute only (100K rows, 10 cols, warm cache)")
    print("=" * 80)

    spark.sql(f"DROP TABLE IF EXISTS {db}.wide_src")
    col_defs = ", ".join(f"col_{i} STRING" for i in range(10))
    spark.sql(f"""
        CREATE TABLE {db}.wide_src (
            id INT, {col_defs}
        ) USING DELTA
    """)
    spark.sql(f"""
        INSERT INTO {db}.wide_src
        SELECT id, {", ".join(f"CONCAT('v_', CAST(id AS STRING), '_{i}')" for i in range(10))}
        FROM RANGE(100000)
    """)

    wide = spark.table(f"{db}.wide_src")
    wide_cols = [f"col_{i}" for i in range(10)]

    # Warm the file cache by reading the table once
    wide.cache()
    wide.count()

    # Run 3 rounds, alternating, to eliminate warm/cold bias
    results = {"xxhash64_str": [], "sha256": []}
    for _round_num in range(3):
        spark.catalog.clearCache()
        wide.cache()
        wide.count()  # re-warm

        # xxhash64
        t0 = time.time()
        df_xx = wide.withColumn("hashdiff", compute_hashdiff_xxhash64_str(wide_cols))
        df_xx.cache()
        df_xx.count()
        t_xx = time.time() - t0
        results["xxhash64_str"].append(t_xx)
        df_xx.unpersist()

        spark.catalog.clearCache()
        wide.cache()
        wide.count()  # re-warm

        # sha256
        t0 = time.time()
        df_sha = wide.withColumn("hashdiff", compute_hashdiff_sha256(wide_cols))
        df_sha.cache()
        df_sha.count()
        t_sha = time.time() - t0
        results["sha256"].append(t_sha)
        df_sha.unpersist()

    print("  Round  | xxhash64_str | sha256       | ratio")
    print("  -------|--------------|--------------|------")
    for i in range(3):
        xx = results["xxhash64_str"][i]
        sha = results["sha256"][i]
        print(
            f"  {i + 1}      | {xx * 1000:>10.0f}ms | {sha * 1000:>10.0f}ms | {sha / xx:.2f}x"
        )

    avg_xx = sum(results["xxhash64_str"]) / 3
    avg_sha = sum(results["sha256"]) / 3
    print("  -------|--------------|--------------|------")
    print(
        f"  avg    | {avg_xx * 1000:>10.0f}ms | {avg_sha * 1000:>10.0f}ms | {avg_sha / avg_xx:.2f}x"
    )

    # Cleanup
    wide.unpersist()
    spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
    spark.stop()
    import shutil

    shutil.rmtree(warehouse_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
