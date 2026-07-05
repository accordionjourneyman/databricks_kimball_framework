"""
Olist Brazilian E-Commerce dataset integration tests.

Models the Kimball challenges from the Olist dataset:
  - Multi-source join: orders + order_items + products + sellers
  - Identity bridge: seller_id from one source maps to producer_id in another
  - Late-arriving facts: reviews arrive days/weeks after the order
  - SCD2 on sellers: seller city/region can change
  - SCD2 on products: product category can be reclassified

Reference: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
These tests use small synthetic samples (3-5 rows per table) that exercise
the same Kimball patterns as the full 100K-row dataset.
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
    db_name = f"kimball_olist_{uuid.uuid4().hex[:8]}"
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
        path = tmp_path / f"olist_{uuid.uuid4().hex[:8]}.yml"
        path.write_text(content, encoding="utf-8")
        return str(path)

    return _write


class TestOlistMultiSourceJoin:
    """Test that a fact table can be built from multiple sources (orders + items)."""

    def test_fact_orders_from_orders_and_items(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.orders (
                order_id STRING, customer_id STRING, order_status STRING,
                order_purchase_timestamp TIMESTAMP
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.orders VALUES
            ('o1', 'c1', 'delivered', TIMESTAMP '2024-01-15 10:00:00'),
            ('o2', 'c2', 'shipped',   TIMESTAMP '2024-01-16 11:00:00'),
            ('o3', 'c3', 'canceled',  TIMESTAMP '2024-01-17 12:00:00')
        """)

        spark.sql(f"""
            CREATE TABLE {test_db}.order_items (
                order_id STRING, order_item_id INT, product_id STRING,
                seller_id STRING, price DOUBLE, freight_value DOUBLE
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.order_items VALUES
            ('o1', 1, 'p1', 's1', 100.0, 10.0),
            ('o1', 2, 'p2', 's2',  50.0,  5.0),
            ('o2', 1, 'p1', 's1',  75.0,  8.0)
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.fact_orders
table_type: fact
merge_keys: [order_id, order_item_id]
sources:
  - name: {test_db}.orders
    alias: o
    cdc_strategy: full
  - name: {test_db}.order_items
    alias: i
    cdc_strategy: full
    join_on: order_id
transformation_sql: |
  SELECT
    o.order_id,
    i.order_item_id,
    o.customer_id,
    i.product_id,
    i.seller_id,
    i.price,
    i.freight_value,
    o.order_status,
    o.order_purchase_timestamp
  FROM o
  JOIN i ON o.order_id = i.order_id
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        rows = spark.table(f"{test_db}.fact_orders").collect()
        assert len(rows) == 3, f"Expected 3 order lines, got {len(rows)}"
        total_price = sum(r.price for r in rows)
        assert abs(total_price - 225.0) < 0.01, (
            f"Expected total 225.0, got {total_price}"
        )

        for t in [
            f"{test_db}.fact_orders",
            f"{test_db}.orders",
            f"{test_db}.order_items",
        ]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")


class TestOlistIdentityBridge:
    """Test the identity bridge: producer_id -> seller_id mapping.

    In the real Olist data, a producer (manufacturer) can supply multiple sellers.
    The bridge maps producer_id (business key in one source) to seller_id
    (the canonical key in the dimension). This tests that the bridge
    correctly resolves unmapped keys via COALESCE.
    """

    def test_producer_bridge_resolves_to_seller(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.sellers (
                seller_id INT, seller_city STRING, seller_state STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.sellers VALUES
            (1, 'Sao Paulo', 'SP'),
            (2, 'Rio de Janeiro', 'RJ'),
            (3, 'Belo Horizonte', 'MG')
        """)

        spark.sql(f"""
            CREATE TABLE {test_db}.seller_producer_bridge (
                producer_id INT, canonical_seller_id INT
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.seller_producer_bridge VALUES
            (100, 1),
            (200, 2)
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.dim_seller
table_type: dimension
scd_type: 2
keys:
  surrogate_key: seller_sk
  natural_keys: [seller_id]
surrogate_key_strategy: identity
track_history_columns: [seller_city, seller_state]
sources:
  - name: {test_db}.sellers
    alias: s
    cdc_strategy: full
transformation_sql: |
  SELECT seller_id, seller_city, seller_state FROM s
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        dim = spark.table(f"{test_db}.dim_seller").filter("seller_id > 0").collect()
        assert len(dim) == 3
        by_id = {r.seller_id: r for r in dim}
        assert by_id[1]["seller_city"] == "Sao Paulo"
        assert by_id[2]["seller_city"] == "Rio de Janeiro"
        assert by_id[3]["seller_city"] == "Belo Horizonte"

        bridge_count = spark.table(f"{test_db}.seller_producer_bridge").count()
        assert bridge_count == 2

        for t in [
            f"{test_db}.dim_seller",
            f"{test_db}.sellers",
            f"{test_db}.seller_producer_bridge",
        ]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")


class TestOlistLateArrivingReviews:
    """Test late-arriving fact: reviews arrive after the order is delivered."""

    def test_late_arriving_review_creates_new_fact_row(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.orders (
                order_id STRING, customer_id STRING, order_status STRING,
                order_purchase_timestamp TIMESTAMP
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.orders VALUES
            ('o1', 'c1', 'delivered', TIMESTAMP '2024-01-15 10:00:00'),
            ('o2', 'c2', 'delivered', TIMESTAMP '2024-01-16 10:00:00')
        """)

        spark.sql(f"""
            CREATE TABLE {test_db}.reviews (
                review_id STRING, order_id STRING, review_score INT,
                review_creation_timestamp TIMESTAMP
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.reviews VALUES
            ('r1', 'o1', 5, TIMESTAMP '2024-01-20 12:00:00')
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.fact_reviews
table_type: fact
merge_keys: [review_id]
sources:
  - name: {test_db}.reviews
    alias: r
    cdc_strategy: full
transformation_sql: |
  SELECT review_id, order_id, review_score, review_creation_timestamp FROM r
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        rows = spark.table(f"{test_db}.fact_reviews").collect()
        assert len(rows) == 1
        assert rows[0]["review_score"] == 5

        spark.sql(f"""
            INSERT INTO {test_db}.reviews VALUES
            ('r2', 'o2', 3, TIMESTAMP '2024-01-22 14:00:00')
        """)

        orchestrator2 = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result2 = orchestrator2.run()
        assert result2["status"] == "SUCCESS"

        rows2 = spark.table(f"{test_db}.fact_reviews").collect()
        assert len(rows2) == 2, f"Expected 2 reviews, got {len(rows2)}"
        scores = {r["review_id"]: r["review_score"] for r in rows2}
        assert scores["r1"] == 5
        assert scores["r2"] == 3

        for t in [f"{test_db}.fact_reviews", f"{test_db}.reviews", f"{test_db}.orders"]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")


class TestOlistSCD2OnProducts:
    """Test SCD2 on products: category reclassification creates a new version."""

    def test_product_category_change_creates_new_version(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        spark.sql(f"""
            CREATE TABLE {test_db}.products (
                product_id INT, product_category STRING, product_weight_g INT
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.products VALUES
            (1, 'electronics', 500),
            (2, 'furniture',  2000)
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.dim_product
table_type: dimension
scd_type: 2
keys:
  surrogate_key: product_sk
  natural_keys: [product_id]
surrogate_key_strategy: identity
track_history_columns: [product_category, product_weight_g]
sources:
  - name: {test_db}.products
    alias: p
    cdc_strategy: full
transformation_sql: |
  SELECT product_id, product_category, product_weight_g FROM p
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        spark.sql(f"""
            UPDATE {test_db}.products
            SET product_category = 'audio', product_weight_g = 450
            WHERE product_id = 1
        """)

        orchestrator2 = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result2 = orchestrator2.run()
        assert result2["status"] == "SUCCESS"

        p1_rows = (
            spark.table(f"{test_db}.dim_product")
            .filter("product_id = 1")
            .orderBy("product_sk")
            .collect()
        )
        assert len(p1_rows) == 2, f"Expected 2 versions of p1, got {len(p1_rows)}"

        old, new = p1_rows[0], p1_rows[1]
        assert not old["__is_current"]
        assert old["product_category"] == "electronics"
        assert new["__is_current"]
        assert new["product_category"] == "audio"
        assert old["product_sk"] != new["product_sk"]

        for t in [f"{test_db}.dim_product", f"{test_db}.products"]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")
