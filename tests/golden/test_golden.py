"""
Golden end-to-end tests for the Kimball Framework.

This test exercises the full pipeline against a real Spark session:
- Load deterministic CSV data into silver source tables.
- Run dimension pipelines (SCD1 product, SCD2 customer).
- Run a fact pipeline with FK lookups.
- Assert expected row counts, SCD2 history, and fact linkage.

The test is designed to run:
  - locally (if Java/Spark is available), or
  - remotely via Databricks Connect when DATABRICKS_HOST + DATABRICKS_TOKEN are set.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from kimball import PipelineExecutor
from kimball.common.config import ConfigLoader

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
GOLDEN_DIR = Path(__file__).resolve().parent


def _catalog(spark: SparkSession) -> str:
    """Return the integration-test catalog."""
    return os.environ.get("KIMBALL_TEST_CATALOG", "spark_catalog")


def _load_csv(
    spark: SparkSession, catalog: str, table_name: str, csv_path: Path
) -> None:
    """Load a CSV into a managed Delta table in the raw schema."""
    full_table_name = f"{catalog}.kimball_golden_raw.{table_name}"
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(str(csv_path))
    )
    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(full_table_name)


def _render_config(raw_config_path: Path, catalog: str) -> str:
    """Render Jinja2 placeholders in a config file and return the rendered path."""
    raw_content = raw_config_path.read_text(encoding="utf-8")
    rendered = raw_content.replace("{{ catalog }}", catalog)
    rendered_path = raw_config_path.parent / f"_{raw_config_path.name}"
    rendered_path.write_text(rendered, encoding="utf-8")
    return str(rendered_path)


@pytest.fixture(scope="module")
def golden_schema(spark: SparkSession):
    """Set up raw and golden schemas, return the catalog."""
    catalog = _catalog(spark)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.kimball_golden_raw")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.kimball_golden")
    yield catalog
    # Optional cleanup: uncomment to drop schemas after tests
    # spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.kimball_golden_raw CASCADE")
    # spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.kimball_golden CASCADE")


@pytest.fixture(scope="module")
def day1_data(spark: SparkSession, golden_schema: str):
    """Load day 1 CSV data into raw tables."""
    day1 = GOLDEN_DIR / "data" / "day1"
    _load_csv(spark, golden_schema, "customers", day1 / "customers.csv")
    _load_csv(spark, golden_schema, "products", day1 / "products.csv")
    _load_csv(spark, golden_schema, "orders", day1 / "orders.csv")
    _load_csv(spark, golden_schema, "order_items", day1 / "order_items.csv")
    return golden_schema


@pytest.fixture(scope="module")
def day2_data(spark: SparkSession, day1_data: str):
    """Load day 2 CSV data into raw tables."""
    day2 = GOLDEN_DIR / "data" / "day2"
    _load_csv(spark, day1_data, "customers", day2 / "customers.csv")
    _load_csv(spark, day1_data, "products", day2 / "products.csv")
    _load_csv(spark, day1_data, "orders", day2 / "orders.csv")
    _load_csv(spark, day1_data, "order_items", day2 / "order_items.csv")
    return day1_data


@pytest.fixture(scope="module")
def rendered_configs(golden_schema: str) -> list[str]:
    """Return paths to rendered config files."""
    configs = [
        _render_config(GOLDEN_DIR / "dim_customer.yml", golden_schema),
        _render_config(GOLDEN_DIR / "dim_product.yml", golden_schema),
        _render_config(GOLDEN_DIR / "fact_sales.yml", golden_schema),
    ]
    return configs


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
    os.environ["KIMBALL_ETL_SCHEMA"] = f"{day1_data}.kimball_golden"

    executor = PipelineExecutor(
        rendered_configs, etl_schema=f"{day1_data}.kimball_golden"
    )
    summary = executor.run()
    assert summary.failed == 0, f"Pipelines failed: {summary.results}"

    # Day 1 expectations
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
    os.environ["KIMBALL_ETL_SCHEMA"] = f"{day2_data}.kimball_golden"

    executor = PipelineExecutor(
        rendered_configs, etl_schema=f"{day2_data}.kimball_golden"
    )
    summary = executor.run()
    assert summary.failed == 0, f"Pipelines failed: {summary.results}"

    # Alice should now have 2 history rows: old NY and new LA
    alice_history = spark.sql(
        f"""
        SELECT customer_sk, address, __is_current
        FROM {day2_data}.kimball_golden.dim_customer
        WHERE customer_id = 1
        ORDER BY __valid_from
        """
    ).collect()
    assert len(alice_history) == 3, (
        f"Expected 3 rows for Alice, got {len(alice_history)}"
    )
    assert alice_history[-1].address == "789 Cherry Ln, LA"
    assert alice_history[-1].__is_current is True

    # Charlie (new customer) should have 1 current row
    charlie = spark.sql(
        f"""
        SELECT * FROM {day2_data}.kimball_golden.dim_customer
        WHERE customer_id = 3 AND __is_current = true
        """
    ).collect()
    assert len(charlie) == 1
    assert charlie[0].first_name == "Charlie"

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
