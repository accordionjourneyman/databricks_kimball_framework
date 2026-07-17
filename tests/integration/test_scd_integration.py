"""
Integration tests for the Kimball Framework using real Delta tables.

These tests run against a local Spark+Delta environment (default) or a
remote Databricks cluster (when KIMBALL_TARGET=databricks).

Each test follows the full pipeline lifecycle:
  initial load -> incremental -> delete -> schema evolution -> 2nd run

Requirements:
  - Java installed (for local Spark)
  - pyspark + delta-spark installed
  - Run via: python tools/run_tests.py -t local --integration
"""

import pytest
from pyspark.sql import SparkSession

from kimball.orchestration.orchestrator import Orchestrator

pytestmark = pytest.mark.usefixtures("spark")


# =====================================================================
# SCD1 Integration Tests
# =====================================================================


class TestSCD1Integration:
    """End-to-end SCD1 tests with real Delta tables."""

    def test_scd1_initial_load_and_update(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        """SCD1: Initial load inserts rows, second run updates existing."""
        # Create source table
        spark.sql(f"""
            CREATE TABLE {test_db}.customers_src (
                customer_id INT,
                name STRING,
                city STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.customers_src VALUES
            (1, 'Alice', 'Lisbon'),
            (2, 'Bob', 'Porto')
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.dim_customer
table_type: dimension
scd_type: 1
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
delete_strategy: hard
sources:
  - name: {test_db}.customers_src
    alias: c
    cdc_strategy: full
transformation_sql: |
  SELECT customer_id, name, city FROM c
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        # Verify initial load (2 data rows; default rows may also be seeded)
        rows = (
            spark.table(f"{test_db}.dim_customer")
            .filter("customer_id > 0")
            .orderBy("customer_id")
            .collect()
        )
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[0]["city"] == "Lisbon"

        # Update source data
        spark.sql(f"""
            UPDATE {test_db}.customers_src SET city = 'Faro' WHERE customer_id = 1
        """)

        # Second run - should update existing row
        orchestrator2 = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result2 = orchestrator2.run()
        assert result2["status"] == "SUCCESS"

        rows2 = (
            spark.table(f"{test_db}.dim_customer")
            .filter("customer_id > 0")
            .orderBy("customer_id")
            .collect()
        )
        assert len(rows2) == 2
        # Alice's city should be updated
        alice = [r for r in rows2 if r["customer_id"] == 1][0]
        assert alice["city"] == "Faro"
        # SK should be preserved (identity, not re-generated)
        assert alice["customer_sk"] == rows[0]["customer_sk"]

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {test_db}.dim_customer")
        spark.sql(f"DROP TABLE IF EXISTS {test_db}.customers_src")


# =====================================================================
# SCD2 Integration Tests
# =====================================================================


class TestSCD2Integration:
    """End-to-end SCD2 tests with real Delta tables."""

    def test_scd2_initial_load_and_change(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        """SCD2: Initial load, then change a tracked column -> new version."""
        spark.sql(f"""
            CREATE TABLE {test_db}.products_src (
                product_id INT,
                name STRING,
                price DOUBLE,
                updated_at STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.products_src VALUES
            (100, 'Widget', 9.99, '2024-01-01'),
            (200, 'Gadget', 19.99, '2024-01-01')
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.dim_product
table_type: dimension
scd_type: 2
keys:
  surrogate_key: product_sk
  natural_keys: [product_id]
track_history_columns: [name, price]
effective_at: updated_at
sources:
  - name: {test_db}.products_src
    alias: p
    cdc_strategy: full
transformation_sql: |
  SELECT product_id, name, price, updated_at FROM p
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        # Verify initial load (2 data rows + 3 default rows = 5)
        rows = spark.table(f"{test_db}.dim_product").filter("product_id > 0").collect()
        assert len(rows) == 2
        widget = [r for r in rows if r["product_id"] == 100][0]
        assert widget["__is_current"]
        assert widget["price"] == 9.99

        # Change a tracked column and advance effective_at so the new version
        # gets a distinct valid_from (SCD2 versions must not share valid_from).
        spark.sql(f"""
            UPDATE {test_db}.products_src SET price = 14.99, updated_at = '2024-06-01' WHERE product_id = 100
        """)

        # Second run - should create a new version
        orchestrator2 = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result2 = orchestrator2.run()
        assert result2["status"] == "SUCCESS"

        all_rows = (
            spark.table(f"{test_db}.dim_product").filter("product_id = 100").collect()
        )

        # Should have 2 rows: old (expired) + new (current)
        assert len(all_rows) == 2
        old_row = [r for r in all_rows if not r["__is_current"]][0]
        new_row = [r for r in all_rows if r["__is_current"]][0]
        assert old_row["price"] == 9.99
        assert new_row["price"] == 14.99
        # SKs should be different (new version gets new SK)
        assert old_row["product_sk"] != new_row["product_sk"]

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {test_db}.dim_product")
        spark.sql(f"DROP TABLE IF EXISTS {test_db}.products_src")

    def test_scd2_full_snapshot_delete_detection(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        """SCD2 with full snapshot should detect deletes via anti-join."""
        spark.sql(f"""
            CREATE TABLE {test_db}.customers_del_src (
                customer_id INT,
                name STRING,
                updated_at STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.customers_del_src VALUES
            (1, 'Alice', '2024-01-01'),
            (2, 'Bob', '2024-01-01'),
            (3, 'Charlie', '2024-01-01')
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.dim_customer_del
table_type: dimension
scd_type: 2
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
track_history_columns: [name]
effective_at: updated_at
sources:
  - name: {test_db}.customers_del_src
    alias: c
    cdc_strategy: full
transformation_sql: |
  SELECT customer_id, name, updated_at FROM c
""")

        # Initial load
        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        # Verify 3 active rows
        active = (
            spark.table(f"{test_db}.dim_customer_del")
            .filter("__is_current = true AND customer_id > 0")
            .collect()
        )
        assert len(active) == 3

        # Delete Charlie from source
        spark.sql(f"DELETE FROM {test_db}.customers_del_src WHERE customer_id = 3")

        # Second run - should detect delete via anti-join and expire Charlie
        orchestrator2 = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result2 = orchestrator2.run()
        assert result2["status"] == "SUCCESS"

        # Charlie should be expired
        charlie_rows = (
            spark.table(f"{test_db}.dim_customer_del")
            .filter("customer_id = 3")
            .collect()
        )
        assert len(charlie_rows) == 1
        assert not charlie_rows[0]["__is_current"]
        assert charlie_rows[0]["__is_deleted"]

        # Alice and Bob should still be active
        still_active = (
            spark.table(f"{test_db}.dim_customer_del")
            .filter("__is_current = true AND customer_id > 0")
            .collect()
        )
        assert len(still_active) == 2

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {test_db}.dim_customer_del")
        spark.sql(f"DROP TABLE IF EXISTS {test_db}.customers_del_src")


# =====================================================================
# Schema Evolution Integration Tests
# =====================================================================


class TestSchemaEvolutionIntegration:
    """End-to-end schema evolution tests."""

    def test_scd2_add_new_column(self, spark: SparkSession, test_db: str, tmp_config):
        """Adding a new column to source + config should evolve target schema."""
        spark.sql(f"""
            CREATE TABLE {test_db}.evolve_src (
                entity_id INT,
                val_a STRING,
                updated_at STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.evolve_src VALUES (1, 'hello', '2024-01-01'), (2, 'world', '2024-01-01')
        """)

        config_v1 = tmp_config(f"""
table_name: {test_db}.dim_evolve
table_type: dimension
scd_type: 2
keys:
  surrogate_key: entity_sk
  natural_keys: [entity_id]
track_history_columns: [val_a]
effective_at: updated_at
sources:
  - name: {test_db}.evolve_src
    alias: e
    cdc_strategy: full
transformation_sql: |
  SELECT entity_id, val_a, updated_at FROM e
""")

        # Initial load
        orchestrator = Orchestrator(config_v1, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        # Add column to source
        spark.sql(f"ALTER TABLE {test_db}.evolve_src ADD COLUMN val_b STRING")
        spark.sql(
            f"UPDATE {test_db}.evolve_src SET val_b = 'extra', updated_at = '2024-06-01' WHERE entity_id = 1"
        )

        # Evolved config with new column + schema_evolution enabled
        config_v2 = tmp_config(f"""
table_name: {test_db}.dim_evolve
table_type: dimension
scd_type: 2
keys:
  surrogate_key: entity_sk
  natural_keys: [entity_id]
track_history_columns: [val_a, val_b]
effective_at: updated_at
schema_evolution: true
sources:
  - name: {test_db}.evolve_src
    alias: e
    cdc_strategy: full
transformation_sql: |
  SELECT entity_id, val_a, val_b, updated_at FROM e
""")

        orchestrator2 = Orchestrator(config_v2, spark=spark, etl_schema=test_db)
        result2 = orchestrator2.run()
        assert result2["status"] == "SUCCESS"

        # Verify new column exists in target
        schema = spark.table(f"{test_db}.dim_evolve").schema
        field_names = [f.name for f in schema.fields]
        assert "val_b" in field_names, (
            f"val_b should be in schema after evolution: {field_names}"
        )

        # Cleanup
        spark.sql(f"DROP TABLE IF EXISTS {test_db}.dim_evolve")
        spark.sql(f"DROP TABLE IF EXISTS {test_db}.evolve_src")
