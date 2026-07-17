"""Integration tests for skeleton generation and hydration.

Tests the full lifecycle:
  1. Skeleton rows are created when fact keys are missing from the dimension
  2. Skeleton rows are hydrated when real data arrives later
  3. SCD2 merge works correctly with skeleton rows present
  4. Edge cases: missing table, missing __is_skeleton column

Uses real local SparkSession + Delta tables (no mocking).
"""

import pytest
from pyspark.sql import SparkSession

from kimball.orchestration.orchestrator import Orchestrator
from kimball.processing.late_arriving_dimension import LateArrivingDimensionProcessor
from kimball.processing.skeleton_generator import SkeletonGenerator

pytestmark = pytest.mark.usefixtures("spark")


# =====================================================================
# Direct SkeletonGenerator tests (real Spark, no Orchestrator)
# =====================================================================


class TestSkeletonGeneratorDirect:
    """Test SkeletonGenerator with real Spark tables."""

    def test_creates_skeletons_for_missing_keys(self, spark: SparkSession, test_db):
        """Fact has keys not in dimension -> skeleton rows created."""
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

        # Fact references customer_id=20 which is NOT in the dimension
        fact_data = [(10,), (20,)]
        fact_df = spark.createDataFrame(fact_data, ["customer_id"])

        gen = SkeletonGenerator(spark)
        gen.generate_skeletons(
            fact_df=fact_df,
            dim_table_name=f"{test_db}.dim_customer",
            fact_join_key="customer_id",
            dim_join_key="customer_id",
            surrogate_key_col="customer_sk",
            batch_id="test-batch-001",
        )

        rows = spark.table(f"{test_db}.dim_customer").orderBy("customer_id").collect()
        assert len(rows) == 2

        alice = [r for r in rows if r["customer_id"] == 10][0]
        assert not alice["__is_skeleton"]

        skeleton = [r for r in rows if r["customer_id"] == 20][0]
        assert skeleton["__is_skeleton"]
        assert skeleton["__is_current"]
        assert skeleton["name"] is None
        assert skeleton["__etl_batch_id"] == "test-batch-001"

    def test_no_skeletons_when_all_keys_exist(self, spark: SparkSession, test_db):
        """All fact keys already in dimension -> no skeleton rows."""
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
             current_timestamp(), 'INIT', false, NULL, false),
            (2, 200, 'Gadget', true, current_timestamp(), NULL,
             current_timestamp(), 'INIT', false, NULL, false)
        """)

        # All fact keys exist in dimension
        fact_data = [(100,), (200,)]
        fact_df = spark.createDataFrame(fact_data, ["product_id"])

        gen = SkeletonGenerator(spark)
        gen.generate_skeletons(
            fact_df=fact_df,
            dim_table_name=f"{test_db}.dim_product",
            fact_join_key="product_id",
            dim_join_key="product_id",
            surrogate_key_col="product_sk",
            batch_id="test-batch-002",
        )

        rows = spark.table(f"{test_db}.dim_product").collect()
        assert len(rows) == 2
        assert all(not r["__is_skeleton"] for r in rows)

    def test_skips_when_table_not_exists(self, spark: SparkSession):
        """Non-existent dimension table -> no error."""
        fact_df = spark.createDataFrame([(1,)], ["id"])
        gen = SkeletonGenerator(spark)
        # Should not raise
        gen.generate_skeletons(
            fact_df=fact_df,
            dim_table_name="nonexistent_table_xyz",
            fact_join_key="id",
            dim_join_key="id",
            surrogate_key_col="sk",
        )

    def test_skips_when_no_skeleton_column(self, spark: SparkSession, test_db):
        """Dimension without __is_skeleton -> no error."""
        spark.sql(f"""
            CREATE TABLE {test_db}.dim_plain (
                sk BIGINT,
                entity_id INT,
                name STRING
            ) USING DELTA
        """)
        spark.sql(f"INSERT INTO {test_db}.dim_plain VALUES (1, 10, 'Alice')")

        fact_df = spark.createDataFrame([(20,)], ["entity_id"])
        gen = SkeletonGenerator(spark)
        # Should not raise
        gen.generate_skeletons(
            fact_df=fact_df,
            dim_table_name=f"{test_db}.dim_plain",
            fact_join_key="entity_id",
            dim_join_key="entity_id",
            surrogate_key_col="sk",
        )

        rows = spark.table(f"{test_db}.dim_plain").collect()
        assert len(rows) == 1  # No skeleton added


# =====================================================================
# Skeleton hydration tests (LateArrivingDimensionProcessor)
# =====================================================================


class TestSkeletonHydration:
    """Test that skeleton rows are hydrated with real data."""

    def test_hydration_updates_skeleton_with_real_data(
        self, spark: SparkSession, test_db
    ):
        """Skeleton row should be updated when real data arrives."""
        spark.sql(f"""
            CREATE TABLE {test_db}.dim_customer (
                customer_sk BIGINT,
                customer_id INT,
                name STRING,
                city STRING,
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
        # Insert a skeleton row (as SkeletonGenerator would create it)
        spark.sql(f"""
            INSERT INTO {test_db}.dim_customer VALUES
            (1, 20, NULL, NULL, true, timestamp('1800-01-01'), NULL,
             current_timestamp(), 'SKELETON_GEN', true, current_timestamp(), false)
        """)

        # Real data arrives for customer_id=20
        real_data = [(20, "Bob", "Porto")]
        source_df = spark.createDataFrame(real_data, ["customer_id", "name", "city"])

        processor = LateArrivingDimensionProcessor(spark)
        updated = processor.update_skeletons_with_real_data(
            dimension_table=f"{test_db}.dim_customer",
            source_df=source_df,
            natural_keys=["customer_id"],
        )

        assert updated == 1, f"Expected exactly 1 skeleton hydrated, got {updated}"

        rows = spark.table(f"{test_db}.dim_customer").collect()
        assert len(rows) == 1
        bob = rows[0]
        assert bob["customer_id"] == 20
        assert bob["name"] == "Bob"
        assert bob["city"] == "Porto"
        assert not bob["__is_skeleton"]

    def test_hydration_preserves_existing_non_skeleton_rows(
        self, spark: SparkSession, test_db
    ):
        """Hydration should not touch non-skeleton rows."""
        spark.sql(f"""
            CREATE TABLE {test_db}.dim_cust2 (
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
            INSERT INTO {test_db}.dim_cust2 VALUES
            (1, 10, 'Alice', true, current_timestamp(), NULL,
             current_timestamp(), 'INIT', false, NULL, false),
            (2, 20, NULL, true, timestamp('1800-01-01'), NULL,
             current_timestamp(), 'SKELETON_GEN', true, current_timestamp(), false)
        """)

        # Real data arrives for customer_id=20 only
        source_df = spark.createDataFrame([(20, "Bob")], ["customer_id", "name"])

        processor = LateArrivingDimensionProcessor(spark)
        processor.update_skeletons_with_real_data(
            dimension_table=f"{test_db}.dim_cust2",
            source_df=source_df,
            natural_keys=["customer_id"],
        )

        rows = spark.table(f"{test_db}.dim_cust2").orderBy("customer_id").collect()
        assert len(rows) == 2
        alice = rows[0]
        assert alice["name"] == "Alice"
        assert not alice["__is_skeleton"]
        bob = rows[1]
        assert bob["name"] == "Bob"
        assert not bob["__is_skeleton"]

    def test_no_hydration_when_no_skeletons_exist(self, spark: SparkSession, test_db):
        """If no skeleton rows exist, hydration is a no-op."""
        spark.sql(f"""
            CREATE TABLE {test_db}.dim_no_skel (
                sk BIGINT,
                entity_id INT,
                name STRING,
                __is_skeleton BOOLEAN
            ) USING DELTA
        """)
        spark.sql(f"INSERT INTO {test_db}.dim_no_skel VALUES (1, 10, 'Alice', false)")

        source_df = spark.createDataFrame([(10, "Bob")], ["entity_id", "name"])

        processor = LateArrivingDimensionProcessor(spark)
        updated = processor.update_skeletons_with_real_data(
            dimension_table=f"{test_db}.dim_no_skel",
            source_df=source_df,
            natural_keys=["entity_id"],
        )

        assert updated == 0
        rows = spark.table(f"{test_db}.dim_no_skel").collect()
        assert rows[0]["name"] == "Alice"  # Unchanged


# =====================================================================
# Full Orchestrator pipeline with skeleton generation
# =====================================================================


class TestSkeletonOrchestratorIntegration:
    """Test skeleton generation through the Orchestrator pipeline."""

    def test_initial_load_creates_skeletons_for_missing_keys(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        """Orchestrator creates skeleton rows when fact has keys missing from dim."""
        # Create a dimension table with only customer_id=10
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

        # Create fact source with customer_id=10 AND customer_id=20
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
table_name: {test_db}.dim_customer
table_type: dimension
scd_type: 1
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
delete_strategy: hard
sources:
  - name: {test_db}.orders_src
    alias: o
    cdc_strategy: full
transformation_sql: |
  SELECT DISTINCT customer_id, CAST(NULL AS STRING) AS name FROM o
early_arriving_facts:
  - dimension_table: {test_db}.dim_customer
    fact_join_key: customer_id
    dimension_join_key: customer_id
    surrogate_key_col: customer_sk
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        rows = (
            spark.table(f"{test_db}.dim_customer")
            .filter("customer_id > 0")
            .orderBy("customer_id")
            .collect()
        )
        # Should have Alice (10) + skeleton for Bob (20)
        assert len(rows) == 2
        customer_ids = [r["customer_id"] for r in rows]
        assert 10 in customer_ids
        assert 20 in customer_ids

        skeleton = [r for r in rows if r["customer_id"] == 20][0]
        assert skeleton["__is_skeleton"]

    def test_skeleton_hydration_via_late_arriving_processor(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        """Full lifecycle: create skeleton -> hydrate with real data."""
        # Step 1: Create dimension with skeleton for customer_id=20
        spark.sql(f"""
            CREATE TABLE {test_db}.dim_cust_hydration (
                customer_sk BIGINT,
                customer_id INT,
                name STRING,
                city STRING,
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
            INSERT INTO {test_db}.dim_cust_hydration VALUES
            (1, 10, 'Alice', 'Lisbon', true, current_timestamp(), NULL,
             current_timestamp(), 'INIT', false, NULL, false),
            (2, 20, NULL, NULL, true, timestamp('1800-01-01'), NULL,
             current_timestamp(), 'SKELETON_GEN', true, current_timestamp(), false)
        """)

        # Step 2: Real data arrives for customer_id=20
        real_data = [(20, "Bob", "Porto")]
        source_df = spark.createDataFrame(real_data, ["customer_id", "name", "city"])

        processor = LateArrivingDimensionProcessor(spark)
        updated = processor.update_skeletons_with_real_data(
            dimension_table=f"{test_db}.dim_cust_hydration",
            source_df=source_df,
            natural_keys=["customer_id"],
        )

        assert updated == 1, f"Expected exactly 1 skeleton hydrated, got {updated}"

        # Step 3: Verify final state
        rows = (
            spark.table(f"{test_db}.dim_cust_hydration")
            .orderBy("customer_id")
            .collect()
        )
        assert len(rows) == 2

        alice = [r for r in rows if r["customer_id"] == 10][0]
        assert alice["name"] == "Alice"
        assert not alice["__is_skeleton"]

        bob = [r for r in rows if r["customer_id"] == 20][0]
        assert bob["name"] == "Bob"
        assert bob["city"] == "Porto"
        assert not bob["__is_skeleton"]

    def test_scd2_skeleton_merge_and_hydration(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        """SCD2: skeleton row gets hydrated and preserves history."""
        # Create SCD2 dimension with a skeleton row
        spark.sql(f"""
            CREATE TABLE {test_db}.dim_product_scd2 (
                product_sk BIGINT,
                product_id INT,
                name STRING,
                price DOUBLE,
                updated_at STRING,
                __is_current BOOLEAN,
                __valid_from TIMESTAMP,
                __valid_to TIMESTAMP,
                __etl_processed_at TIMESTAMP,
                __etl_batch_id STRING,
                hashdiff BIGINT,
                __is_skeleton BOOLEAN,
                __skeleton_created_at TIMESTAMP,
                __is_deleted BOOLEAN
            ) USING DELTA
        """)
        # Real product
        spark.sql(f"""
            INSERT INTO {test_db}.dim_product_scd2 VALUES
            (1, 100, 'Widget', 9.99, '2024-01-01T00:00:00', true, current_timestamp(), NULL,
             current_timestamp(), 'INIT', 12345, false, NULL, false)
        """)
        # Skeleton for product_id=200
        spark.sql(f"""
            INSERT INTO {test_db}.dim_product_scd2 VALUES
            (2, 200, NULL, NULL, NULL, true, timestamp('1800-01-01'), NULL,
             current_timestamp(), 'SKELETON_GEN', NULL, true, current_timestamp(), false)
        """)

        # Now real data arrives for product_id=200
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
            (200, 'Gadget', 19.99, '2024-01-01T00:00:00')
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.dim_product_scd2
table_type: dimension
scd_type: 2
effective_at: updated_at
keys:
  surrogate_key: product_sk
  natural_keys: [product_id]
track_history_columns: [name, price]
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

        rows = (
            spark.table(f"{test_db}.dim_product_scd2")
            .filter("product_id = 200")
            .collect()
        )
        # The skeleton should have been hydrated IN PLACE (SK preserved, no
        # duplicate row created). Exactly one row for product_id=200 proves
        # in-place hydration; a second row would mean a new version was
        # inserted instead of the skeleton being filled in.
        assert len(rows) == 1, (
            f"Expected 1 hydrated row for product_id=200, got {len(rows)}: {rows}"
        )
        hydrated = rows[0]
        assert hydrated["name"] == "Gadget"
        assert hydrated["price"] == 19.99
        assert not hydrated["__is_skeleton"]
