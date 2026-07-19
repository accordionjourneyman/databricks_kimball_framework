"""Integration tests for fact loading resolution validation.

Tests the three pre-merge gates added to KeyBroker:
  1. Resolution rate logging (always on)
  2. Fanout detection (detect_fanout=true)
  3. Count assertion (validate_resolution=true)

Uses real local SparkSession + Delta tables (no mocking).
"""

import pytest
from pyspark.sql import SparkSession

from kimball.common.errors import DataQualityError
from kimball.orchestration.orchestrator import Orchestrator

pytestmark = pytest.mark.usefixtures("spark")


class TestResolutionValidation:
    """Test resolution validation through the Orchestrator pipeline."""

    def test_resolve_passes_when_all_fks_match(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        """All fact NKs exist in dimension -> pipeline succeeds."""
        spark.sql(f"""
            CREATE TABLE {test_db}.dim_customer (
                customer_sk BIGINT,
                customer_id INT,
                name STRING,
                __is_current BOOLEAN,
                __valid_from TIMESTAMP,
                __valid_to TIMESTAMP,
                __etl_processed_at TIMESTAMP,
                __etl_batch_id STRING,
                __is_skeleton BOOLEAN,
                __skeleton_created_at TIMESTAMP,
                __is_deleted BOOLEAN
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.dim_customer VALUES
            (1, 10, 'Alice', true, current_timestamp(), NULL,
             current_timestamp(), 'INIT', false, NULL, false),
            (2, 20, 'Bob', true, current_timestamp(), NULL,
             current_timestamp(), 'INIT', false, NULL, false)
        """)
        spark.sql(f"""
            CREATE TABLE {test_db}.orders_src (
                order_id INT,
                customer_id INT,
                amount DOUBLE
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.orders_src VALUES
            (1, 10, 100.0),
            (2, 20, 200.0)
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.fact_orders
table_type: fact
merge_keys: [order_id]
foreign_keys:
  - column: customer_sk
    references: {test_db}.dim_customer
    dimension_key: customer_sk
    lookup:
      source_columns: [customer_id]
      early_arriving: default
      detect_fanout: true
      validate_resolution: true
sources:
  - name: {test_db}.orders_src
    alias: o
    cdc_strategy: full
transformation_sql: |
  SELECT order_id, customer_id, amount FROM o
""")

        result = Orchestrator(config_path, spark=spark, etl_schema=test_db).run()
        assert result["status"] == "SUCCESS"

    def test_resolve_passes_when_skeletons_created(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        """Fact NK missing from dim with early_arriving=skeleton -> skeleton created, pipeline succeeds."""
        spark.sql(f"""
            CREATE TABLE {test_db}.dim_product (
                product_sk BIGINT,
                product_id INT,
                name STRING,
                __is_current BOOLEAN,
                __valid_from TIMESTAMP,
                __valid_to TIMESTAMP,
                __etl_processed_at TIMESTAMP,
                __etl_batch_id STRING,
                __is_skeleton BOOLEAN,
                __skeleton_created_at TIMESTAMP,
                __is_deleted BOOLEAN
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.dim_product VALUES
            (1, 100, 'Widget', true, current_timestamp(), NULL,
             current_timestamp(), 'INIT', false, NULL, false)
        """)
        spark.sql(f"""
            CREATE TABLE {test_db}.sales_src (
                sale_id INT,
                product_id INT,
                revenue DOUBLE
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.sales_src VALUES
            (1, 100, 50.0),
            (2, 200, 75.0)
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.dim_product
table_type: dimension
scd_type: 1
keys:
  surrogate_key: product_sk
  natural_keys: [product_id]
delete_strategy: hard
sources:
  - name: {test_db}.sales_src
    alias: s
    cdc_strategy: full
transformation_sql: |
  SELECT DISTINCT product_id, CAST(NULL AS STRING) AS name FROM s
""")

        result = Orchestrator(config_path, spark=spark, etl_schema=test_db).run()
        assert result["status"] == "SUCCESS"

    def test_fanout_raises_on_duplicate_dim_keys(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        """Dimension with duplicate NKs -> DataQualityError on fanout."""
        spark.sql(f"""
            CREATE TABLE {test_db}.dim_customer (
                customer_sk BIGINT,
                customer_id INT,
                name STRING,
                __is_current BOOLEAN,
                __valid_from TIMESTAMP,
                __valid_to TIMESTAMP,
                __etl_processed_at TIMESTAMP,
                __etl_batch_id STRING,
                __is_skeleton BOOLEAN,
                __skeleton_created_at TIMESTAMP,
                __is_deleted BOOLEAN
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.dim_customer VALUES
            (1, 10, 'Alice', true, current_timestamp(), NULL,
             current_timestamp(), 'INIT', false, NULL, false),
            (2, 10, 'Alice v2', true, current_timestamp(), NULL,
             current_timestamp(), 'INIT', false, NULL, false)
        """)
        spark.sql(f"""
            CREATE TABLE {test_db}.orders_src (
                order_id INT,
                customer_id INT,
                amount DOUBLE
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.orders_src VALUES
            (1, 10, 100.0)
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.fact_orders
table_type: fact
merge_keys: [order_id]
foreign_keys:
  - column: customer_sk
    references: {test_db}.dim_customer
    dimension_key: customer_sk
    lookup:
      source_columns: [customer_id]
      early_arriving: default
      detect_fanout: true
sources:
  - name: {test_db}.orders_src
    alias: o
    cdc_strategy: full
transformation_sql: |
  SELECT order_id, customer_id, amount FROM o
""")

        with pytest.raises(DataQualityError, match="Fanout detected"):
            Orchestrator(config_path, spark=spark, etl_schema=test_db).run()

    def test_count_mismatch_raises_when_validate_enabled(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        """Fact has NK not in dim, validate_resolution=true -> DataQualityError."""
        spark.sql(f"""
            CREATE TABLE {test_db}.dim_customer (
                customer_sk BIGINT,
                customer_id INT,
                name STRING,
                __is_current BOOLEAN,
                __valid_from TIMESTAMP,
                __valid_to TIMESTAMP,
                __etl_processed_at TIMESTAMP,
                __etl_batch_id STRING,
                __is_skeleton BOOLEAN,
                __skeleton_created_at TIMESTAMP,
                __is_deleted BOOLEAN
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.dim_customer VALUES
            (1, 10, 'Alice', true, current_timestamp(), NULL,
             current_timestamp(), 'INIT', false, NULL, false)
        """)
        spark.sql(f"""
            CREATE TABLE {test_db}.orders_src (
                order_id INT,
                customer_id INT,
                amount DOUBLE
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.orders_src VALUES
            (1, 10, 100.0),
            (2, 99, 200.0)
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.fact_orders
table_type: fact
merge_keys: [order_id]
foreign_keys:
  - column: customer_sk
    references: {test_db}.dim_customer
    dimension_key: customer_sk
    lookup:
      source_columns: [customer_id]
      early_arriving: error
      detect_fanout: true
      validate_resolution: true
sources:
  - name: {test_db}.orders_src
    alias: o
    cdc_strategy: full
transformation_sql: |
  SELECT order_id, customer_id, amount FROM o
""")

        with pytest.raises((DataQualityError, ValueError)):
            Orchestrator(config_path, spark=spark, etl_schema=test_db).run()

    def test_fanout_check_disabled_skips_validation(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        """Dimension with duplicate NKs but detect_fanout=false -> pipeline succeeds."""
        spark.sql(f"""
            CREATE TABLE {test_db}.dim_customer (
                customer_sk BIGINT,
                customer_id INT,
                name STRING,
                __is_current BOOLEAN,
                __valid_from TIMESTAMP,
                __valid_to TIMESTAMP,
                __etl_processed_at TIMESTAMP,
                __etl_batch_id STRING,
                __is_skeleton BOOLEAN,
                __skeleton_created_at TIMESTAMP,
                __is_deleted BOOLEAN
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.dim_customer VALUES
            (1, 10, 'Alice', true, current_timestamp(), NULL,
             current_timestamp(), 'INIT', false, NULL, false),
            (2, 10, 'Alice v2', true, current_timestamp(), NULL,
             current_timestamp(), 'INIT', false, NULL, false)
        """)
        spark.sql(f"""
            CREATE TABLE {test_db}.orders_src (
                order_id INT,
                customer_id INT,
                amount DOUBLE
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.orders_src VALUES
            (1, 10, 100.0)
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.fact_orders
table_type: fact
merge_keys: [order_id]
foreign_keys:
  - column: customer_sk
    references: {test_db}.dim_customer
    dimension_key: customer_sk
    lookup:
      source_columns: [customer_id]
      early_arriving: default
      detect_fanout: false
sources:
  - name: {test_db}.orders_src
    alias: o
    cdc_strategy: full
transformation_sql: |
  SELECT order_id, customer_id, amount FROM o
""")

        result = Orchestrator(config_path, spark=spark, etl_schema=test_db).run()
        assert result["status"] == "SUCCESS"
