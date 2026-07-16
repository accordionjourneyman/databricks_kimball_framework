import pytest
from pyspark.sql import SparkSession

from kimball.orchestration.orchestrator import Orchestrator

pytestmark = pytest.mark.usefixtures("spark")


def test_schema_evolution_with_new_column(spark: SparkSession, test_db: str, tmp_config):
    """
    Integration test for schema evolution: Add a new column to source and verify
    SCD2 merge handles it gracefully with hash-based surrogate keys.
    """
    initial_config = f"""
table_name: {test_db}.dim_customer
table_type: dimension
scd_type: 2
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
track_history_columns: [first_name, last_name]
sources:
  - name: {test_db}.customers
    alias: c
    cdc_strategy: full
transformation_sql: |
  SELECT
    c.customer_id,
    c.first_name,
    c.last_name
  FROM c
"""

    spark.sql(f"""
    CREATE TABLE {test_db}.customers (
        customer_id INT,
        first_name STRING,
        last_name STRING
    ) USING DELTA
    """)

    spark.sql(f"""
    INSERT INTO {test_db}.customers VALUES
    (1, 'John', 'Doe'),
    (2, 'Jane', 'Smith')
    """)

    config_path = tmp_config(initial_config)

    orchestrator = Orchestrator(config_path, spark=spark)
    result = orchestrator.run()
    assert result["status"] == "SUCCESS"

    ddl_rows = spark.sql(f"DESCRIBE EXTENDED {test_db}.dim_customer").collect()
    ddl_text = "\n".join([row[0] for row in ddl_rows])
    assert "customer_sk" in ddl_text, (
        f"Expected customer_sk column, got:\n{ddl_text}"
    )

    evolved_config = f"""
table_name: {test_db}.dim_customer
table_type: dimension
scd_type: 2
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
track_history_columns: [first_name, last_name, email]
schema_evolution: true
sources:
  - name: {test_db}.customers
    alias: c
    cdc_strategy: full
transformation_sql: |
  SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email
  FROM c
"""
    spark.sql(f"""
    ALTER TABLE {test_db}.customers ADD COLUMN email STRING
    """)

    spark.sql(f"""
    UPDATE {test_db}.customers SET email = 'john@example.com' WHERE customer_id = 1
    """)

    spark.sql(f"""
    UPDATE {test_db}.customers SET email = 'jane@example.com' WHERE customer_id = 2
    """)

    config_path = tmp_config(evolved_config)

    orchestrator = Orchestrator(config_path, spark=spark)
    result = orchestrator.run()
    assert result["status"] == "SUCCESS"

    evolved_schema = spark.table(f"{test_db}.dim_customer").schema
    email_field = next(
        (f for f in evolved_schema.fields if f.name == "email"), None
    )
    assert email_field is not None

    data = spark.table(f"{test_db}.dim_customer").collect()
    sk_values = [row.customer_sk for row in data]
    assert len(set(sk_values)) == len(sk_values)

    spark.sql(f"DROP TABLE {test_db}.dim_customer")
    spark.sql(f"DROP TABLE {test_db}.customers")