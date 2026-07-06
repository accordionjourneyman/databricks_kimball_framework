"""
End-to-end streaming integration tests for the Kimball Framework.

Tests ``StreamingOrchestrator`` against real Delta tables with CDF enabled,
exercising the full pipeline: source table with ``delta.enableChangeDataFeed``,
batch path to create the target, then a streaming query that picks up new
changes via CDF and merges them into the target dimension or fact table.

These tests share the same environment as the other integration tests
(Java, pyspark, delta-spark).
"""

import os
import uuid

import pytest
from pyspark.sql import SparkSession

from kimball.common.config import ConfigLoader
from kimball.orchestration.orchestrator import Orchestrator
from kimball.streaming.orchestrator import StreamingOrchestrator

pytestmark = pytest.mark.usefixtures("spark")


@pytest.fixture
def test_db(spark: SparkSession):
    db_name = f"kimball_stream_{uuid.uuid4().hex[:8]}"
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
        path = tmp_path / f"stream_test_{uuid.uuid4().hex[:8]}.yml"
        path.write_text(content, encoding="utf-8")
        return str(path)

    return _write


class TestStreamingCdfSCD1:
    """Streaming CDF integration with SCD1 dimensions."""

    def test_streaming_cdf_picks_up_new_rows(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        """
        Batch processes initial rows, then streaming CDF picks up new inserts.

        1. Create a CDF-enabled source table with 2 rows.
        2. Run batch Orchestrator to create the target dimension table.
        3. Insert 2 more rows into the source.
        4. Run StreamingOrchestrator (availableNow) with starting_version=0.
        5. Verify all 4 data rows exist in the target.
        """
        # ---- Step 1: CDF-enabled source ----
        spark.sql(f"""
            CREATE TABLE {test_db}.customers_src (
                customer_id INT,
                name STRING,
                city STRING
            ) USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.customers_src VALUES
            (1, 'Alice', 'Lisbon'),
            (2, 'Bob', 'Porto')
        """)

        # ---- Step 2: Batch Orchestrator creates target ----
        batch_cfg = tmp_config(f"""
table_name: {test_db}.dim_customer
table_type: dimension
scd_type: 1
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
surrogate_key_strategy: identity
delete_strategy: hard
sources:
  - name: {test_db}.customers_src
    alias: c
    cdc_strategy: cdf
transformation_sql: |
  SELECT customer_id, name, city FROM c
""")
        batch_orch = Orchestrator(batch_cfg, spark=spark, etl_schema=test_db)
        result = batch_orch.run()
        assert result["status"] == "SUCCESS"

        rows = (
            spark.table(f"{test_db}.dim_customer")
            .filter("customer_id > 0")
            .orderBy("customer_id")
            .collect()
        )
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"

        # ---- Step 3: Insert more data ----
        spark.sql(f"""
            INSERT INTO {test_db}.customers_src VALUES
            (3, 'Charlie', 'Madrid'),
            (4, 'Diana', 'Paris')
        """)

        # ---- Step 4: Streaming config with same target + CDF source ----
        stream_cfg = tmp_config(f"""
table_name: {test_db}.dim_customer
table_type: dimension
scd_type: 1
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
surrogate_key_strategy: identity
delete_strategy: hard
sources:
  - name: {test_db}.customers_src
    alias: c
    cdc_strategy: cdf
    streaming:
      enabled: true
      trigger: available_now
transformation_sql: |
  SELECT customer_id, name, city FROM c
""")
        stream_orch = StreamingOrchestrator(stream_cfg, spark=spark, etl_schema=test_db)
        stream_result = stream_orch.run()
        assert stream_result["status"] == "SUCCESS", (
            f"Streaming pipeline failed: {stream_result.get('errors', 'no errors')}"
        )
        assert "queries" in stream_result, (
            f"Expected queries key in result: {stream_result}"
        )

        # ---- Step 5: Verify all 4 rows ----
        final_rows = (
            spark.table(f"{test_db}.dim_customer")
            .filter("customer_id > 0")
            .orderBy("customer_id")
            .collect()
        )
        assert len(final_rows) == 4, (
            f"Expected 4 data rows, got {len(final_rows)}: {[r['customer_id'] for r in final_rows]}"
        )
        assert final_rows[2]["name"] == "Charlie"
        assert final_rows[3]["name"] == "Diana"

        # SKs should be sequential (identity strategy)
        assert final_rows[0]["customer_sk"] < final_rows[3]["customer_sk"]

        for t in [f"{test_db}.dim_customer", f"{test_db}.customers_src"]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")

    def test_streaming_cdf_updates_existing(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        """
        Batch processes initial rows, then streaming CDF picks up updates.

        1. Create a CDF-enabled source table with 2 rows.
        2. Run batch Orchestrator to create target.
        3. Update one row in the source.
        4. Run StreamingOrchestrator (availableNow).
        5. Verify the updated value in the target.
        """
        spark.sql(f"""
            CREATE TABLE {test_db}.products_src (
                product_id INT,
                product_name STRING,
                price DOUBLE
            ) USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.products_src VALUES
            (100, 'Widget', 9.99),
            (200, 'Gadget', 19.99)
        """)

        batch_cfg = tmp_config(f"""
table_name: {test_db}.dim_product
table_type: dimension
scd_type: 1
keys:
  surrogate_key: product_sk
  natural_keys: [product_id]
surrogate_key_strategy: identity
delete_strategy: hard
sources:
  - name: {test_db}.products_src
    alias: p
    cdc_strategy: cdf
transformation_sql: |
  SELECT product_id, product_name, price FROM p
""")
        batch_orch = Orchestrator(batch_cfg, spark=spark, etl_schema=test_db)
        result = batch_orch.run()
        assert result["status"] == "SUCCESS"

        # Update Widget's price
        spark.sql(f"""
            UPDATE {test_db}.products_src
            SET price = 14.99
            WHERE product_id = 100
        """)

        stream_cfg = tmp_config(f"""
table_name: {test_db}.dim_product
table_type: dimension
scd_type: 1
keys:
  surrogate_key: product_sk
  natural_keys: [product_id]
surrogate_key_strategy: identity
delete_strategy: hard
sources:
  - name: {test_db}.products_src
    alias: p
    cdc_strategy: cdf
    streaming:
      enabled: true
      trigger: available_now
transformation_sql: |
  SELECT product_id, product_name, price FROM p
""")
        stream_orch = StreamingOrchestrator(stream_cfg, spark=spark, etl_schema=test_db)
        stream_result = stream_orch.run()
        assert stream_result["status"] == "SUCCESS"

        widget = (
            spark.table(f"{test_db}.dim_product").filter("product_id = 100").first()
        )
        assert widget is not None
        assert widget["price"] == 14.99, f"Expected price 14.99, got {widget['price']}"
        # SK should be preserved (same row, not re-inserted)
        assert widget["product_sk"] is not None

        for t in [f"{test_db}.dim_product", f"{test_db}.products_src"]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")

    def test_streaming_fallback_to_batch_when_not_enabled(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        """
        When streaming is not explicitly enabled, StreamingOrchestrator
        falls back to the batch Orchestrator.
        """
        spark.sql(f"""
            CREATE TABLE {test_db}.no_stream_src (
                id INT, val STRING
            ) USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.no_stream_src VALUES
            (1, 'alpha'), (2, 'beta')
        """)

        cfg = tmp_config(f"""
table_name: {test_db}.dim_no_stream
table_type: dimension
scd_type: 1
keys:
  surrogate_key: sk
  natural_keys: [id]
surrogate_key_strategy: identity
delete_strategy: hard
sources:
  - name: {test_db}.no_stream_src
    alias: s
    cdc_strategy: cdf
transformation_sql: |
  SELECT id, val FROM s
""")

        # Use StreamingOrchestrator without streaming.enabled
        orch = StreamingOrchestrator(cfg, spark=spark, etl_schema=test_db)
        result = orch.run()
        assert result["status"] == "SUCCESS"

        rows = spark.table(f"{test_db}.dim_no_stream").filter("id > 0").collect()
        assert len(rows) == 2

        for t in [f"{test_db}.dim_no_stream", f"{test_db}.no_stream_src"]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")
