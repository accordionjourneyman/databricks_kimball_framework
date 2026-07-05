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
import uuid
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
    """Return the integration-test catalog.

    For local Spark (the default), ``spark_catalog`` is the implicit V1 catalog
    and 3-part table names like ``spark_catalog.schema.table`` raise a parse
    error in the Delta Java APIs. In that case we return an empty string so
    table names collapse to ``schema.table`` form.
    """
    catalog = os.environ.get("KIMBALL_TEST_CATALOG", "spark_catalog")
    return "" if catalog == "spark_catalog" else catalog


def _full_table(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}" if catalog else f"{schema}.{table}"


def _render_config(raw_config_path: Path, catalog: str, run_id: str) -> str:
    """Render Jinja2 placeholders in a config file and return the rendered path.

    Each config may reference ``{{ catalog }}``, ``{{ raw_schema }}`` and
    ``{{ out_schema }}``; the latter two are derived from ``run_id`` so multiple
    runs in the same Spark session don't collide on table names.

    When ``catalog`` is empty (local Spark default), we still need *something*
    in place of ``{{ catalog }}.`` in the rendered file; the orchestrator then
    treats it as a relative 2-part name ``schema.table``.
    """
    raw_content = raw_config_path.read_text(encoding="utf-8")
    catalog_prefix = f"{catalog}." if catalog else ""
    rendered = raw_content.replace("{{ catalog }}.", catalog_prefix)
    rendered = rendered.replace(
        "{{ raw_schema }}", f"kimball_golden_raw_{run_id}"
    )
    rendered = rendered.replace("{{ out_schema }}", f"kimball_golden_{run_id}")
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
def run_id() -> str:
    """Unique ID per test run so schemas do not collide with prior runs."""
    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="module")
def golden_catalog(spark: SparkSession) -> str:
    """Return the integration-test catalog (empty for local Spark default)."""
    return _catalog(spark)


@pytest.fixture(scope="module")
def golden_schemas(spark: SparkSession, golden_catalog: str, run_id: str):
    """Create unique raw and golden schemas for this run; drop them on teardown.

    Unique names per run prevent collisions when the same Spark session is reused
    across modules (which is the case when this file is collected alongside the
    integration tests).
    """
    raw_schema = f"kimball_golden_raw_{run_id}"
    out_schema = f"kimball_golden_{run_id}"

    full_raw = f"{golden_catalog}.{raw_schema}" if golden_catalog else raw_schema
    full_out = f"{golden_catalog}.{out_schema}" if golden_catalog else out_schema

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_raw}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_out}")

    yield raw_schema, out_schema

    spark.sql(f"DROP SCHEMA IF EXISTS {full_raw} CASCADE")
    spark.sql(f"DROP SCHEMA IF EXISTS {full_out} CASCADE")


@pytest.fixture(scope="module")
def rendered_configs(spark: SparkSession, run_id: str) -> list[str]:
    """Return paths to rendered config files (catalog + unique schemas baked in)."""
    catalog = _catalog(spark)
    return [
        _render_config(
            GOLDEN_DIR / "dim_customer.yml", catalog, run_id
        ),
        _render_config(
            GOLDEN_DIR / "dim_product.yml", catalog, run_id
        ),
        _render_config(
            GOLDEN_DIR / "fact_sales.yml", catalog, run_id
        ),
    ]


@pytest.fixture(scope="module")
def day1_data(spark: SparkSession, golden_schemas: tuple[str, str]) -> str:
    """Load day 1 CSV data into CDF-enabled silver tables."""
    raw_schema, _ = golden_schemas
    day1 = GOLDEN_DIR / "data" / "day1"
    catalog = _catalog(spark)
    _merge_into_silver(
        spark,
        catalog,
        raw_schema,
        "customers",
        _read_csv(spark, day1 / "customers.csv", CUSTOMERS_SCHEMA),
        ["customer_id"],
    )
    _merge_into_silver(
        spark,
        catalog,
        raw_schema,
        "products",
        _read_csv(spark, day1 / "products.csv", PRODUCTS_SCHEMA),
        ["product_id"],
    )
    _merge_into_silver(
        spark,
        catalog,
        raw_schema,
        "orders",
        _read_csv(spark, day1 / "orders.csv", ORDERS_SCHEMA),
        ["order_id"],
    )
    _merge_into_silver(
        spark,
        catalog,
        raw_schema,
        "order_items",
        _read_csv(spark, day1 / "order_items.csv", ORDER_ITEMS_SCHEMA),
        ["order_item_id"],
    )
    return catalog


@pytest.fixture(scope="module")
def day2_data(spark: SparkSession, day1_data: str, golden_schemas: tuple[str, str]) -> str:
    """Merge day 2 CSV data into silver tables to generate CDF updates."""
    raw_schema, _ = golden_schemas
    day2 = GOLDEN_DIR / "data" / "day2"
    _merge_into_silver(
        spark,
        day1_data,
        raw_schema,
        "customers",
        _read_csv(spark, day2 / "customers.csv", CUSTOMERS_SCHEMA),
        ["customer_id"],
    )
    _merge_into_silver(
        spark,
        day1_data,
        raw_schema,
        "products",
        _read_csv(spark, day2 / "products.csv", PRODUCTS_SCHEMA),
        ["product_id"],
    )
    _merge_into_silver(
        spark,
        day1_data,
        raw_schema,
        "orders",
        _read_csv(spark, day2 / "orders.csv", ORDERS_SCHEMA),
        ["order_id"],
    )
    _merge_into_silver(
        spark,
        day1_data,
        raw_schema,
        "order_items",
        _read_csv(spark, day2 / "order_items.csv", ORDER_ITEMS_SCHEMA),
        ["order_item_id"],
    )
    return day1_data


def _run_pipelines(
    rendered_configs: list[str], catalog: str, out_schema: str
) -> None:
    """Execute the pipeline executor and raise on any failure."""
    etl_target = f"{catalog}.{out_schema}" if catalog else out_schema
    os.environ["KIMBALL_ETL_SCHEMA"] = etl_target
    executor = PipelineExecutor(rendered_configs, etl_schema=etl_target)
    summary = executor.run()
    assert summary.failed == 0, f"Pipelines failed: {summary.results}"


def _q(catalog: str, schema: str, table: str) -> str:
    """Quote a possibly-empty catalog prefix around a schema.table name."""
    return f"{catalog}.{schema}.{table}" if catalog else f"{schema}.{table}"


def test_config_renders(rendered_configs: list[str]) -> None:
    """Smoke test: all golden configs load and validate."""
    loader = ConfigLoader(env_vars={"catalog": ""})
    for path in rendered_configs:
        config = loader.load_config(path)
        assert config.table_name
        assert config.table_type in ("dimension", "fact")


def test_day1_pipeline(
    spark: SparkSession,
    day1_data: str,
    golden_schemas: tuple[str, str],
    rendered_configs: list[str],
) -> None:
    """Run day 1 dimensions and facts, then assert expected state."""
    _, out_schema = golden_schemas
    _run_pipelines(rendered_configs, day1_data, out_schema)

    customer_count = spark.table(_q(day1_data, out_schema, "dim_customer")).count()
    product_count = spark.table(_q(day1_data, out_schema, "dim_product")).count()
    fact_count = spark.table(_q(day1_data, out_schema, "fact_sales")).count()

    # 2 real customers + 3 seeded defaults (-1, -2, -3) = 5
    assert customer_count == 5, f"Expected 5 customer rows, got {customer_count}"
    # 2 real products + 3 seeded defaults = 5
    assert product_count == 5, f"Expected 5 product rows, got {product_count}"
    # 2 order items
    assert fact_count == 2, f"Expected 2 fact rows, got {fact_count}"

    # Alice and Bob should each have 1 current row
    alice = spark.sql(
        f"""
        SELECT * FROM {_q(day1_data, out_schema, "dim_customer")}
        WHERE customer_id = 1 AND __is_current = true
        """
    ).collect()
    assert len(alice) == 1
    assert alice[0].address == "123 Apple St, NY"


def test_day2_pipeline(
    spark: SparkSession,
    day2_data: str,
    golden_schemas: tuple[str, str],
    rendered_configs: list[str],
) -> None:
    """Run day 2 incremental load and assert SCD2 + SCD1 behavior."""
    _, out_schema = golden_schemas
    _run_pipelines(rendered_configs, day2_data, out_schema)

    # Alice should now have 2 history rows: old NY (expired) and new LA (current)
    alice_history = spark.sql(
        f"""
        SELECT customer_sk, address, __is_current
        FROM {_q(day2_data, out_schema, "dim_customer")}
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
        SELECT * FROM {_q(day2_data, out_schema, "dim_customer")}
        WHERE customer_id = 3 AND __is_current = true
        """
    ).collect()
    assert len(charlie) == 1
    assert charlie[0]["first_name"] == "Charlie"

    # Laptop price should be updated to 900 (SCD1)
    laptop = spark.sql(
        f"""
        SELECT unit_cost FROM {_q(day2_data, out_schema, "dim_product")}
        WHERE product_id = 101
        """
    ).collect()[0]
    assert laptop.unit_cost == 900.0, f"Expected 900.0, got {laptop.unit_cost}"

    # Fact count should be 4 total order items
    fact_count = spark.table(_q(day2_data, out_schema, "fact_sales")).count()
    assert fact_count == 4, f"Expected 4 fact rows, got {fact_count}"

    # Alice's day 1 order should still link to her NY SK
    # Alice's day 2 order should link to her LA SK
    raw_schema, _ = golden_schemas
    links = spark.sql(
        f"""
        SELECT
          o.order_id,
          o.order_date,
          c.address AS linked_customer_address
        FROM {_q(day2_data, out_schema, "fact_sales")} f
        JOIN {_q(day2_data, out_schema, "dim_customer")} c ON f.customer_sk = c.customer_sk
        JOIN {_q(day2_data, raw_schema, "orders")} o ON f.order_id = o.order_id
        WHERE o.customer_id = 1
        ORDER BY o.order_date
        """
    ).collect()
    assert len(links) == 2
    assert links[0].linked_customer_address == "123 Apple St, NY"
    assert links[1].linked_customer_address == "789 Cherry Ln, LA"
