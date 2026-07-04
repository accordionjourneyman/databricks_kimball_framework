"""
Online Retail II (UCI) dataset integration tests.

Models the Kimball challenges from the UCI Online Retail II dataset:
  - Returns / cancellations: invoices with prefix 'C' are returns, should
    be soft-deleted in the customer dimension or flagged in the fact table
  - SCD1 on products: stockcode/description corrections overwrite
  - SCD2 on customers: country can change, customer can be reclassified
  - Full CDC customer dimension: customer_id may not exist yet (new customer)
  - Negative quantities / prices: returns create negative line items

Reference: https://archive.ics.uci.edu/dataset/502/online+retail+ii
These tests use small synthetic samples (3-5 rows per table) that exercise
the same Kimball patterns as the full 1M-row dataset.
"""

import os
import uuid

import pytest
from pyspark.sql import SparkSession

from kimball.common.config import ConfigLoader
from kimball.orchestration.orchestrator import Orchestrator

pytestmark = pytest.mark.usefixtures("spark")


@pytest.fixture
def test_db(spark: SparkSession):
    db_name = f"kimball_retail_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    os.environ["KIMBALL_ETL_SCHEMA"] = db_name
    yield db_name
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")


@pytest.fixture
def config_loader():
    return ConfigLoader()


@pytest.fixture
def tmp_config(tmp_path, config_loader):
    def _write(content: str) -> str:
        path = tmp_path / f"retail_{uuid.uuid4().hex[:8]}.yml"
        path.write_text(content, encoding="utf-8")
        return str(path)
    return _write


class TestOnlineRetailSCD1Product:
    """Test SCD1 on products: description corrections overwrite."""

    def test_product_description_correction_overwrites(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.products (
                stock_code STRING, description STRING, unit_price DOUBLE
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.products VALUES
            ('S001', 'WHITE HANGING HEART',     2.55),
            ('S002', 'RED WOOLLY HOTTIE',       3.39)
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.dim_product
table_type: dimension
scd_type: 1
keys:
  surrogate_key: product_sk
  natural_keys: [stock_code]
surrogate_key_strategy: identity
sources:
  - name: {test_db}.products
    alias: p
    cdc_strategy: full
transformation_sql: |
  SELECT stock_code, description, unit_price FROM p
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        spark.sql(f"""
            UPDATE {test_db}.products
            SET description = 'WHITE HANGING HEART T-LIGHT HOLDER'
            WHERE stock_code = 'S001'
        """)

        orchestrator2 = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result2 = orchestrator2.run()
        assert result2["status"] == "SUCCESS"

        rows = spark.table(f"{test_db}.dim_product").filter("stock_code = 'S001'").collect()
        assert len(rows) == 1, "SCD1 should overwrite, not version"
        assert "T-LIGHT HOLDER" in rows[0]["description"]

        for t in [f"{test_db}.dim_product", f"{test_db}.products"]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")


class TestOnlineRetailSCD2Customer:
    """Test SCD2 on customers: country change creates a new version."""

    def test_customer_country_change_creates_new_version(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.customers (
                customer_id INT, country STRING, segment STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.customers VALUES
            (1, 'United Kingdom', 'retail'),
            (2, 'Germany',        'wholesale')
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.dim_customer
table_type: dimension
scd_type: 2
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
surrogate_key_strategy: identity
track_history_columns: [country, segment]
sources:
  - name: {test_db}.customers
    alias: c
    cdc_strategy: full
transformation_sql: |
  SELECT customer_id, country, segment FROM c
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        spark.sql(f"""
            UPDATE {test_db}.customers
            SET country = 'France'
            WHERE customer_id = 1
        """)

        orchestrator2 = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result2 = orchestrator2.run()
        assert result2["status"] == "SUCCESS"

        c1_rows = (
            spark.table(f"{test_db}.dim_customer")
            .filter("customer_id = 1")
            .orderBy("customer_sk")
            .collect()
        )
        assert len(c1_rows) == 2
        old, new = c1_rows[0], c1_rows[1]
        assert not old["__is_current"]
        assert old["country"] == "United Kingdom"
        assert new["__is_current"]
        assert new["country"] == "France"

        for t in [f"{test_db}.dim_customer", f"{test_db}.customers"]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")


class TestOnlineRetailReturnsAsSoftDelete:
    """Test that cancelled invoices (returns) are soft-deleted in the customer dim."""

    def test_missing_customer_after_return_is_soft_deleted(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.customers (
                customer_id INT, country STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.customers VALUES
            (1, 'United Kingdom'),
            (2, 'Germany'),
            (3, 'France')
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.dim_customer
table_type: dimension
scd_type: 2
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
surrogate_key_strategy: identity
track_history_columns: [country]
delete_strategy: soft
sources:
  - name: {test_db}.customers
    alias: c
    cdc_strategy: full
transformation_sql: |
  SELECT customer_id, country FROM c
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        active = spark.table(f"{test_db}.dim_customer").filter("__is_current = true AND customer_id > 0").collect()
        assert len(active) == 3

        spark.sql(f"DELETE FROM {test_db}.customers WHERE customer_id = 3")

        orchestrator2 = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result2 = orchestrator2.run()
        assert result2["status"] == "SUCCESS"

        c3_rows = spark.table(f"{test_db}.dim_customer").filter("customer_id = 3").collect()
        assert len(c3_rows) == 1
        assert not c3_rows[0]["__is_current"]
        assert c3_rows[0]["__is_deleted"]

        still_active = spark.table(f"{test_db}.dim_customer").filter("__is_current = true AND customer_id > 0").collect()
        assert len(still_active) == 2

        for t in [f"{test_db}.dim_customer", f"{test_db}.customers"]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")


class TestOnlineRetailFactWithReturns:
    """Test a fact table that includes both sales and returns (negative quantities)."""

    def test_fact_invoice_lines_with_returns(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.invoice_lines (
                invoice_no STRING, stock_code STRING, customer_id INT,
                quantity INT, unit_price DOUBLE, invoice_date TIMESTAMP
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.invoice_lines VALUES
            ('I001', 'S001', 1,  6, 2.55, TIMESTAMP '2024-01-15 10:00:00'),
            ('I001', 'S002', 1, 12, 3.39, TIMESTAMP '2024-01-15 10:00:00'),
            ('C001', 'S001', 1, -3, 2.55, TIMESTAMP '2024-02-01 14:00:00'),
            ('I002', 'S001', 2,  4, 2.55, TIMESTAMP '2024-01-20 11:00:00')
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.fact_invoice_lines
table_type: fact
merge_keys: [invoice_no, stock_code]
sources:
  - name: {test_db}.invoice_lines
    alias: l
    cdc_strategy: full
transformation_sql: |
  SELECT
    invoice_no, stock_code, customer_id, quantity, unit_price,
    quantity * unit_price AS line_revenue,
    invoice_date
  FROM l
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        rows = spark.table(f"{test_db}.fact_invoice_lines").collect()
        assert len(rows) == 4

        returns = [r for r in rows if r.invoice_no.startswith("C")]
        assert len(returns) == 1
        assert returns[0]["quantity"] == -3
        assert returns[0]["line_revenue"] < 0

        sales_revenue = sum(r.line_revenue for r in rows if not r.invoice_no.startswith("C"))
        returns_revenue = sum(r.line_revenue for r in rows if r.invoice_no.startswith("C"))
        net = sales_revenue + returns_revenue
        assert net < sales_revenue, "Returns should reduce net revenue"

        for t in [f"{test_db}.fact_invoice_lines", f"{test_db}.invoice_lines"]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")
