"""End-to-end contract checks and durable finding events."""

import pytest
from pyspark.sql import SparkSession

from kimball.orchestration.orchestrator import Orchestrator
from kimball.orchestration.services.contracts import ContractValidationError

pytestmark = pytest.mark.usefixtures("spark")


def test_contract_events_are_persisted_and_breaking_schema_blocks(
    spark: SparkSession, test_db: str, tmp_config
) -> None:
    spark.sql(f"""
        CREATE TABLE {test_db}.customers (
            customer_id INT, name STRING, updated_at STRING
        ) USING DELTA
    """)
    spark.sql(
        f"INSERT INTO {test_db}.customers VALUES (1, 'Ada', '2024-01-01T00:00:00')"
    )
    valid_config = tmp_config(f"""
table_name: {test_db}.dim_customer
table_type: dimension
scd_type: 1
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
sources:
  - name: {test_db}.customers
    alias: c
    cdc_strategy: full
    primary_keys: [customer_id]
    contract:
      id: test.customer
      version: "1.0.0"
      schema:
        customer_id: {{type: int, nullable: true}}
        name: {{type: string, nullable: true}}
        updated_at: {{type: string, nullable: true}}
      quality:
        - rule: not_null
          column: customer_id
          severity: error
transformation_sql: |
  SELECT customer_id, name, updated_at FROM c
""")

    result = Orchestrator(valid_config, spark=spark, etl_schema=test_db).run()
    assert result["status"] == "SUCCESS"
    events = spark.table(f"{test_db}.etl_data_quality_events")
    assert events.filter("contract_id = 'test.customer'").count() > 0
    assert events.filter("status = 'PASSED'").count() > 0

    invalid_config = tmp_config(f"""
table_name: {test_db}.dim_should_not_exist
table_type: dimension
scd_type: 1
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
sources:
  - name: {test_db}.customers
    alias: c
    cdc_strategy: full
    contract:
      id: test.customer
      version: "2.0.0"
      schema:
        customer_id: {{type: bigint, nullable: true}}
transformation_sql: |
  SELECT customer_id FROM c
""")
    with pytest.raises(ContractValidationError, match="type changed"):
        Orchestrator(invalid_config, spark=spark, etl_schema=test_db).run()
    assert not spark.catalog.tableExists(f"{test_db}.dim_should_not_exist")
    assert (
        spark.table(f"{test_db}.etl_data_quality_events")
        .filter("contract_version = '2.0.0' AND status = 'FAILED'")
        .count()
        > 0
    )

    for table in [
        f"{test_db}.dim_customer",
        f"{test_db}.customers",
        f"{test_db}.etl_data_quality_events",
    ]:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
