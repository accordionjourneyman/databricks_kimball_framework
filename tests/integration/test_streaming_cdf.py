"""
End-to-end streaming integration tests for the Kimball Framework.

Tests ``StreamingOrchestrator`` against real Delta tables with CDF enabled,
exercising the full pipeline: source table with ``delta.enableChangeDataFeed``,
batch path to create the target, then a streaming query that picks up new
changes via CDF and merges them into the target dimension or fact table.

These tests share the same environment as the other integration tests
(Java, pyspark, delta-spark).
"""

import pytest
from pyspark.sql import SparkSession

from kimball.orchestration.orchestrator import Orchestrator
from kimball.streaming.orchestrator import StreamingOrchestrator

pytestmark = pytest.mark.usefixtures("spark")


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

        # SKs are deterministic hash surrogate keys (xxhash64), so they have no
        # ordering relationship to customer_id. Verify they are present and unique
        # instead — the real invariant for a surrogate key column.
        sk_values = [r["customer_sk"] for r in final_rows]
        assert all(sk is not None for sk in sk_values), (
            f"Expected non-null surrogate keys, got: {sk_values}"
        )
        assert len(set(sk_values)) == len(sk_values), (
            f"Expected unique surrogate keys, got duplicates in: {sk_values}"
        )

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

        # Capture Widget's surrogate key BEFORE the update so we can prove the
        # streaming merge preserved it (in-place update) rather than re-inserting
        # the row with a new SK.
        original_sk = (
            spark.table(f"{test_db}.dim_product")
            .filter("product_id = 100")
            .first()["product_sk"]
        )

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
        # SK must be preserved (in-place update), not a freshly generated key.
        # A non-null check alone would pass even if the row were re-inserted.
        assert widget["product_sk"] == original_sk, (
            f"Expected SK preserved={original_sk}, got {widget['product_sk']}"
        )

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


class TestStreamingCdfSCD2:
    """Streaming CDF integration with SCD2 dimensions and per-version correctness."""

    def test_streaming_scd2_preserves_multiple_versions_in_one_microbatch(
        self, spark: SparkSession, test_db: str, tmp_config
    ):
        """
        When multiple Delta versions for the same key land in one streaming
        micro-batch, per_version processing must create a full SCD2 history.

        1. Create CDF-enabled source and seed Alice (version 0).
        2. Run batch Orchestrator to create SCD2 target.
        3. Commit TWO separate UPDATE transactions for Alice.
        4. Run StreamingOrchestrator with availableNow.
        5. Assert Alice has 3 rows: original + 2 updates.
        """
        spark.sql(f"""
            CREATE TABLE {test_db}.customers_src (
                customer_id INT,
                address STRING,
                updated_at STRING
            ) USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)
        spark.sql(f"""
            INSERT INTO {test_db}.customers_src VALUES
            (1, '123 Apple St', '2025-01-01T10:00:00')
        """)

        batch_cfg = tmp_config(f"""
table_name: {test_db}.dim_customer
table_type: dimension
scd_type: 2
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
effective_at: updated_at
track_history_columns:
  - address
sources:
  - name: {test_db}.customers_src
    alias: c
    cdc_strategy: cdf
    primary_keys: [customer_id]
transformation_sql: |
  SELECT customer_id, address, updated_at, _change_type FROM c
""")
        batch_orch = Orchestrator(batch_cfg, spark=spark, etl_schema=test_db)
        assert batch_orch.run()["status"] == "SUCCESS"

        # Two separate Delta transactions updating Alice -> two CDF versions.
        spark.sql(f"""
            UPDATE {test_db}.customers_src
            SET address = '456 Banana Blvd', updated_at = '2025-01-02T10:00:00'
            WHERE customer_id = 1
        """)
        spark.sql(f"""
            UPDATE {test_db}.customers_src
            SET address = '789 Cherry Ln', updated_at = '2025-01-03T10:00:00'
            WHERE customer_id = 1
        """)

        stream_cfg = tmp_config(f"""
table_name: {test_db}.dim_customer
table_type: dimension
scd_type: 2
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
effective_at: updated_at
track_history_columns:
  - address
sources:
  - name: {test_db}.customers_src
    alias: c
    cdc_strategy: cdf
    primary_keys: [customer_id]
    streaming:
      enabled: true
      trigger: available_now
transformation_sql: |
  SELECT customer_id, address, updated_at, _change_type FROM c
""")
        stream_orch = StreamingOrchestrator(stream_cfg, spark=spark, etl_schema=test_db)
        stream_result = stream_orch.run()
        assert stream_result["status"] == "SUCCESS", stream_result.get("errors")

        alice_history = (
            spark.table(f"{test_db}.dim_customer")
            .filter("customer_id = 1")
            .orderBy("__valid_from")
            .collect()
        )
        assert len(alice_history) == 3, (
            f"Expected 3 Alice rows (seed + 2 updates), got {len(alice_history)}: "
            + str(alice_history)
        )
        addresses = [r["address"] for r in alice_history]
        assert addresses == ["123 Apple St", "456 Banana Blvd", "789 Cherry Ln"]
        assert alice_history[-1]["__is_current"] is True

        for t in [f"{test_db}.dim_customer", f"{test_db}.customers_src"]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")
