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

import pytest
from pyspark.sql import SparkSession

from kimball.orchestration.orchestrator import Orchestrator

pytestmark = pytest.mark.usefixtures("spark")


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
    """Test the identity bridge end-to-end: producer_id -> seller_id mapping.

    In the real Olist data, a producer (manufacturer) is identified by
    producer_id in the source, but the canonical dimension key is seller_id.
    The identity bridge maps producer_id -> canonical_seller_id so the
    dimension is keyed by the resolved seller_id. This test configures the
    bridge and asserts resolution actually happens through a real run() -- if
    the bridge is never applied in the pipeline, the dimension would be keyed
    by producer_id (100/200/300) and the assertions below would fail.
    """

    def test_producer_bridge_resolves_to_seller(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        # Source is keyed by the ALTERNATE business key (producer_id).
        spark.sql(f"""
            CREATE TABLE {test_db}.producers (
                producer_id INT, seller_city STRING, seller_state STRING,
                updated_at STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.producers VALUES
            (100, 'Sao Paulo', 'SP', '2024-01-01T00:00:00'),
            (200, 'Rio de Janeiro', 'RJ', '2024-01-01T00:00:00'),
            (300, 'Belo Horizonte', 'MG', '2024-01-01T00:00:00')
        """)

        # Bridge maps the alternate key to the canonical key.
        spark.sql(f"""
            CREATE TABLE {test_db}.seller_producer_bridge (
                producer_id INT, canonical_seller_id INT
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.seller_producer_bridge VALUES
            (100, 1),
            (200, 2),
            (300, 3)
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.dim_seller
table_type: dimension
scd_type: 2
effective_at: updated_at
keys:
  surrogate_key: seller_sk
  natural_keys: [producer_id]
track_history_columns: [seller_city, seller_state]
identity_bridge:
  table: {test_db}.seller_producer_bridge
  join_on: producer_id
  target_column: canonical_seller_id
sources:
  - name: {test_db}.producers
    alias: p
    cdc_strategy: full
transformation_sql: |
  SELECT producer_id, seller_city, seller_state, updated_at FROM p
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        # After bridge resolution the natural key column (producer_id) holds
        # the CANONICAL seller ids (1/2/3), not the producer ids (100/200/300).
        dim = spark.table(f"{test_db}.dim_seller").filter("producer_id > 0").collect()
        assert len(dim) == 3, f"Expected 3 sellers, got {len(dim)}: {dim}"
        by_id = {r.producer_id: r for r in dim}
        # If the bridge never ran, these keys would be 100/200/300, not 1/2/3.
        assert 1 in by_id, f"Bridge did not resolve producer 100 -> seller 1: {by_id}"
        assert 2 in by_id, f"Bridge did not resolve producer 200 -> seller 2: {by_id}"
        assert 3 in by_id, f"Bridge did not resolve producer 300 -> seller 3: {by_id}"
        assert by_id[1]["seller_city"] == "Sao Paulo"
        assert by_id[2]["seller_city"] == "Rio de Janeiro"
        assert by_id[3]["seller_city"] == "Belo Horizonte"

        for t in [
            f"{test_db}.dim_seller",
            f"{test_db}.producers",
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
                product_id INT, product_category STRING, product_weight_g INT,
                updated_at STRING
            ) USING DELTA
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.products VALUES
            (1, 'electronics', 500,  '2024-01-01T00:00:00'),
            (2, 'furniture',  2000, '2024-01-01T00:00:00')
        """)

        config_path = tmp_config(f"""
table_name: {test_db}.dim_product
table_type: dimension
scd_type: 2
effective_at: updated_at
keys:
  surrogate_key: product_sk
  natural_keys: [product_id]
track_history_columns: [product_category, product_weight_g]
sources:
  - name: {test_db}.products
    alias: p
    cdc_strategy: full
transformation_sql: |
  SELECT product_id, product_category, product_weight_g, updated_at FROM p
""")

        orchestrator = Orchestrator(config_path, spark=spark, etl_schema=test_db)
        result = orchestrator.run()
        assert result["status"] == "SUCCESS"

        spark.sql(f"""
            UPDATE {test_db}.products
            SET product_category = 'audio', product_weight_g = 450, updated_at = '2024-06-01T00:00:00'
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
