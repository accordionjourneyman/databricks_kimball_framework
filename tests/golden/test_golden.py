"""
Golden end-to-end tests for the Kimball Framework.

This test replicates the ETL DAG from examples/Kimball_Demo.py:
  1. Create CDF-enabled silver source tables.
  2. Ingest Day 1 CSV data via MERGE (producing initial CDF versions).
  3. Run dimension pipelines (SCD1 product, SCD2 customer).
  4. Run fact pipeline with FK lookups.
  5. Ingest Day 2 CSV data via MERGE (producing CDF updates).
  6. Re-run the same pipelines; watermarks ensure only incremental changes
     are processed.
  7. Assert expected row counts, SCD2 history, and fact linkage.

The test runs locally (if Java/Spark is available) or remotely via Databricks
Connect / Databricks Runtime.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from kimball import PipelineExecutor
from kimball.common.config import ConfigLoader

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
GOLDEN_DIR = Path(__file__).resolve().parent


CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("address", StringType(), False),
        StructField("updated_at", StringType(), False),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("unit_cost", DoubleType(), False),
        StructField("updated_at", StringType(), False),
    ]
)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("order_date", StringType(), False),
        StructField("status", StringType(), False),
        StructField("updated_at", StringType(), False),
    ]
)

ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("order_item_id", IntegerType(), False),
        StructField("order_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("sales_amount", DoubleType(), False),
    ]
)


def _catalog(spark: SparkSession) -> str:
    """Return the integration-test catalog."""
    return os.environ.get("KIMBALL_TEST_CATALOG", "spark_catalog")


def _full_table(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def _render_config(raw_config_path: Path, catalog: str) -> str:
    """Render Jinja2 placeholders in a config file and return the rendered path."""
    raw_content = raw_config_path.read_text(encoding="utf-8")
    rendered = raw_content.replace("{{ catalog }}", catalog)
    rendered_path = raw_config_path.parent / f"_{raw_config_path.name}"
    rendered_path.write_text(rendered, encoding="utf-8")
    return str(rendered_path)


def _read_csv(spark: SparkSession, csv_path: Path, schema: StructType):
    """Read a CSV with a known schema (string timestamps to keep demo parity)."""
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .schema(schema)
        .csv(str(csv_path))
    )


def _merge_into_silver(
    spark: SparkSession,
    catalog: str,
    schema_name: str,
    table_name: str,
    df,
    merge_keys: list[str],
) -> None:
    """Merge CSV data into a CDF-enabled silver table, creating it if needed.

    This mirrors the demo notebook's ingest_silver helper so that the CDF
    feed produces real insert/update/delete changes for the orchestrator.
    """
    full_name = _full_table(catalog, schema_name, table_name)

    if not spark.catalog.tableExists(full_name):
        df.write.format("delta").mode("overwrite").option(
            "delta.enableChangeDataFeed", "true"
        ).saveAsTable(full_name)
        return

    delta_table = DeltaTable.forName(spark, full_name)
    merge_condition = " AND ".join([f"t.`{k}` = s.`{k}`" for k in merge_keys])
    delta_table.alias("t").merge(
        df.alias("s"), merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


@pytest.fixture(scope="module")
def golden_catalog() -> str:
    """Return the integration-test catalog (module-level constant)."""
    return os.environ.get("KIMBALL_TEST_CATALOG", "spark_catalog")


@pytest.fixture(scope="module")
def golden_schemas(spark: SparkSession, golden_catalog: str):
    """Create raw and golden schemas, dropping any prior tables for a clean run."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {golden_catalog}.kimball_golden_raw")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {golden_catalog}.kimball_golden")

    for schema in ("kimball_golden_raw", "kimball_golden"):
        rows = spark.sql(f"SHOW TABLES IN {golden_catalog}.{schema}").collect()
        for row in rows:
            spark.sql(f"DROP TABLE IF EXISTS {golden_catalog}.{schema}.{row.tableName}")

    yield golden_catalog

    # Optional cleanup: uncomment to drop schemas after tests
    # spark.sql(f"DROP SCHEMA IF EXISTS {golden_catalog}.kimball_golden_raw CASCADE")
    # spark.sql(f"DROP SCHEMA IF EXISTS {golden_catalog}.kimball_golden CASCADE")


@pytest.fixture(scope="module")
def rendered_configs(golden_catalog: str) -> list[str]:
    """Return paths to rendered config files."""
    return [
        _render_config(GOLDEN_DIR / "dim_customer.yml", golden_catalog),
        _render_config(GOLDEN_DIR / "dim_product.yml", golden_catalog),
        _render_config(GOLDEN_DIR / "fact_sales.yml", golden_catalog),
    ]


@pytest.fixture(scope="module")
def day1_data(spark: SparkSession, golden_schemas: str) -> str:
    """Load day 1 CSV data into CDF-enabled silver tables."""
    day1 = GOLDEN_DIR / "data" / "day1"
    _merge_into_silver(
        spark,
        golden_schemas,
        "kimball_golden_raw",
        "customers",
        _read_csv(spark, day1 / "customers.csv", CUSTOMERS_SCHEMA),
        ["customer_id"],
    )
    _merge_into_silver(
        spark,
        golden_schemas,
        "kimball_golden_raw",
        "products",
        _read_csv(spark, day1 / "products.csv", PRODUCTS_SCHEMA),
        ["product_id"],
    )
    _merge_into_silver(
        spark,
        golden_schemas,
        "kimball_golden_raw",
        "orders",
        _read_csv(spark, day1 / "orders.csv", ORDERS_SCHEMA),
        ["order_id"],
    )
    _merge_into_silver(
        spark,
        golden_schemas,
        "kimball_golden_raw",
        "order_items",
        _read_csv(spark, day1 / "order_items.csv", ORDER_ITEMS_SCHEMA),
        ["order_item_id"],
    )
    return golden_schemas


@pytest.fixture(scope="module")
def day2_data(spark: SparkSession, day1_data: str) -> str:
    """Merge day 2 CSV data into silver tables to generate CDF updates."""
    day2 = GOLDEN_DIR / "data" / "day2"
    _merge_into_silver(
        spark,
        day1_data,
        "kimball_golden_raw",
        "customers",
        _read_csv(spark, day2 / "customers.csv", CUSTOMERS_SCHEMA),
        ["customer_id"],
    )
    _merge_into_silver(
        spark,
        day1_data,
        "kimball_golden_raw",
        "products",
        _read_csv(spark, day2 / "products.csv", PRODUCTS_SCHEMA),
        ["product_id"],
    )
    _merge_into_silver(
        spark,
        day1_data,
        "kimball_golden_raw",
        "orders",
        _read_csv(spark, day2 / "orders.csv", ORDERS_SCHEMA),
        ["order_id"],
    )
    _merge_into_silver(
        spark,
        day1_data,
        "kimball_golden_raw",
        "order_items",
        _read_csv(spark, day2 / "order_items.csv", ORDER_ITEMS_SCHEMA),
        ["order_item_id"],
    )
    return day1_data


def _run_pipelines(rendered_configs: list[str], catalog: str) -> None:
    """Execute the pipeline executor and raise on any failure."""
    os.environ["KIMBALL_ETL_SCHEMA"] = f"{catalog}.kimball_golden"
    executor = PipelineExecutor(
        rendered_configs, etl_schema=f"{catalog}.kimball_golden"
    )
    summary = executor.run()
    assert summary.failed == 0, f"Pipelines failed: {summary.results}"


def test_config_renders(rendered_configs: list[str]) -> None:
    """Smoke test: all golden configs load and validate."""
    loader = ConfigLoader(env_vars={"catalog": "spark_catalog"})
    for path in rendered_configs:
        config = loader.load_config(path)
        assert config.table_name
        assert config.table_type in ("dimension", "fact")


def test_day1_pipeline(
    spark: SparkSession,
    day1_data: str,
    rendered_configs: list[str],
) -> None:
    """Run day 1 dimensions and facts, then assert expected state."""
    _run_pipelines(rendered_configs, day1_data)

    customer_count = spark.table(f"{day1_data}.kimball_golden.dim_customer").count()
    product_count = spark.table(f"{day1_data}.kimball_golden.dim_product").count()
    fact_count = spark.table(f"{day1_data}.kimball_golden.fact_sales").count()

    # 2 real customers + 3 seeded defaults (-1, -2, -3) = 5
    assert customer_count == 5, f"Expected 5 customer rows, got {customer_count}"
    # 2 real products + 3 seeded defaults = 5
    assert product_count == 5, f"Expected 5 product rows, got {product_count}"
    # 2 order items
    assert fact_count == 2, f"Expected 2 fact rows, got {fact_count}"

    # Alice and Bob should each have 1 current row
    alice = spark.sql(
        f"""
        SELECT * FROM {day1_data}.kimball_golden.dim_customer
        WHERE customer_id = 1 AND __is_current = true
        """
    ).collect()
    assert len(alice) == 1
    assert alice[0].address == "123 Apple St, NY"


def test_day2_pipeline(
    spark: SparkSession,
    day2_data: str,
    rendered_configs: list[str],
) -> None:
    """Run day 2 incremental load and assert SCD2 + SCD1 behavior."""
    _run_pipelines(rendered_configs, day2_data)

    # Alice should now have 2 history rows: old NY (expired) and new LA (current)
    alice_history = spark.sql(
        f"""
        SELECT customer_sk, address, __is_current
        FROM {day2_data}.kimball_golden.dim_customer
        WHERE customer_id = 1
        ORDER BY __valid_from
        """
    ).collect()
    assert len(alice_history) == 2, (
        f"Expected 2 rows for Alice, got {len(alice_history)}"
    )
    assert alice_history[-1].address == "789 Cherry Ln, LA"
    assert alice_history[-1]["__is_current"] is True

    # Charlie (new customer) should have 1 current row
    charlie = spark.sql(
        f"""
        SELECT * FROM {day2_data}.kimball_golden.dim_customer
        WHERE customer_id = 3 AND __is_current = true
        """
    ).collect()
    assert len(charlie) == 1
    assert charlie[0]["first_name"] == "Charlie"

    # Laptop price should be updated to 900 (SCD1)
    laptop = spark.sql(
        f"""
        SELECT unit_cost FROM {day2_data}.kimball_golden.dim_product
        WHERE product_id = 101
        """
    ).collect()[0]
    assert laptop.unit_cost == 900.0, f"Expected 900.0, got {laptop.unit_cost}"

    # Fact count should be 4 total order items
    fact_count = spark.table(f"{day2_data}.kimball_golden.fact_sales").count()
    assert fact_count == 4, f"Expected 4 fact rows, got {fact_count}"

    # Alice's day 1 order should still link to her NY SK
    # Alice's day 2 order should link to her LA SK
    links = spark.sql(
        f"""
        SELECT
          o.order_id,
          o.order_date,
          c.address AS linked_customer_address
        FROM {day2_data}.kimball_golden.fact_sales f
        JOIN {day2_data}.kimball_golden.dim_customer c ON f.customer_sk = c.customer_sk
        JOIN {day2_data}.kimball_golden_raw.orders o ON f.order_id = o.order_id
        WHERE o.customer_id = 1
        ORDER BY o.order_date
        """
    ).collect()
    assert len(links) == 2
    assert links[0].linked_customer_address == "123 Apple St, NY"
    assert links[1].linked_customer_address == "789 Cherry Ln, LA"
