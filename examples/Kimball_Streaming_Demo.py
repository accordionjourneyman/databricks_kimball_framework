# Databricks notebook source
# DBTITLE 1,Cell 1 - Install cluster libraries
# ruff: noqa: F821, E402
# pyright: reportUndefinedVariable=false
# Kimball Framework - Streaming Demo
# This notebook demonstrates the StreamingOrchestrator for CDF-based
# streaming SCD pipelines on Databricks.

# Install from source into the cluster library environment.
# %pip installs cluster-wide libraries so streaming foreachBatch workers can
# import kimball. subprocess pip only installs into the notebook REPL.
# Update the path below to match where this repo is checked out in your workspace.

# MAGIC %pip install "pydantic<2.10"
# MAGIC %pip install "/Workspace/Users/t.diogo.marques@gmail.com/databricks_kimball_framework"

# COMMAND ----------

# DBTITLE 1,Cell 2 - ETL Configuration
# %pip restarts Python, so recompute the repo root from the notebook path.
import os

from delta.tables import DeltaTable

from kimball import Orchestrator, StreamingOrchestrator

_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
_pyspark_root = "/Workspace" + os.path.dirname(os.path.dirname(_nb_path)) + "/"
_repo_root = os.path.dirname(os.path.dirname(_pyspark_root))

os.environ["KIMBALL_ETL_SCHEMA"] = "demo_streaming_gold"

# Use lite mode for the demo (fewer optional checks, clearer output)
os.environ["KIMBALL_MODE"] = "lite"

CONFIG_PATH = f"{_repo_root}/examples/configs/streaming_demo"
dbutils.fs.mkdirs(CONFIG_PATH)

spark.sql("CREATE DATABASE IF NOT EXISTS demo_streaming_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS demo_streaming_gold")

# Clean up previous demo
for db in ["demo_streaming_silver", "demo_streaming_gold"]:
    tables = spark.sql(f"SHOW TABLES IN {db}").collect()
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {db}.{table.tableName}")

print("✓ Demo environment set up")
print(f"✓ Config path: {CONFIG_PATH}")
print(f"✓ ETL schema: {os.environ['KIMBALL_ETL_SCHEMA']}")

# COMMAND ----------

# Define the streaming dimension config.
# Only one source is marked streaming.enabled: true.
# The other sources are full-snapshot lookups.

dim_customer_streaming_yaml = """table_name: demo_streaming_gold.dim_customer
table_type: dimension
scd_type: 2
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
surrogate_key_strategy: identity
effective_at: updated_at
track_history_columns:
  - first_name
  - last_name
  - email
  - address
sources:
  - name: demo_streaming_silver.customers
    alias: c
    cdc_strategy: cdf
    starting_version: 0
    primary_keys: [customer_id]
    streaming:
      enabled: true
      trigger: available_now
      checkpoint_location: /tmp/kimball_streaming_checkpoints/customers
      starting_version: 0
transformation_sql: |
  SELECT customer_id, first_name, last_name, email, address, updated_at, _change_type FROM c
audit_columns: true
preserve_all_changes: true
"""

dim_product_yaml = """table_name: demo_streaming_gold.dim_product
table_type: dimension
scd_type: 1
keys:
  surrogate_key: product_sk
  natural_keys: [product_id]
surrogate_key_strategy: identity
sources:
  - name: demo_streaming_silver.products
    alias: p
    cdc_strategy: full
transformation_sql: |
  SELECT product_id, name, category, unit_cost, updated_at FROM p
audit_columns: true
"""

dbutils.fs.put(
    f"{CONFIG_PATH}/dim_customer.yml", dim_customer_streaming_yaml, overwrite=True
)
dbutils.fs.put(f"{CONFIG_PATH}/dim_product.yml", dim_product_yaml, overwrite=True)

print("✓ Configs written")

# COMMAND ----------

# Helper to ingest data into a CDF-enabled Silver table.


def ingest_silver(table_name, data, schema, merge_keys):
    full_table_name = f"demo_streaming_silver.{table_name}"
    df = spark.createDataFrame(data, schema=schema)

    if not spark.catalog.tableExists(full_table_name):
        print(f"Creating table {full_table_name}...")
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("delta.enableChangeDataFeed", "true")
            .saveAsTable(full_table_name)
        )
    else:
        print(f"Merging into {full_table_name}...")
        delta_table = DeltaTable.forName(spark, full_table_name)
        merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        (
            delta_table.alias("t")
            .merge(df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Initial Load (Batch)
# MAGIC
# MAGIC StreamingOrchestrator requires the target table to exist. We run the
# MAGIC batch Orchestrator once to create the dimension and seed defaults.

# COMMAND ----------

# Day 1 customers
customers_day1 = [
    (
        1,
        "Alice",
        "Smith",
        "alice@example.com",
        "123 Apple St, NY",
        "2025-01-01T10:00:00",
    ),
    (
        2,
        "Bob",
        "Jones",
        "bob@example.com",
        "456 Banana Blvd, SF",
        "2025-01-01T10:00:00",
    ),
]
customers_schema = "customer_id INT, first_name STRING, last_name STRING, email STRING, address STRING, updated_at STRING"

products_day1 = [
    (101, "Laptop", "Electronics", 1000.00, "2025-01-01T10:00:00"),
    (102, "Mouse", "Electronics", 20.00, "2025-01-01T10:00:00"),
]
products_schema = (
    "product_id INT, name STRING, category STRING, unit_cost DOUBLE, updated_at STRING"
)

ingest_silver("customers", customers_day1, customers_schema, ["customer_id"])
ingest_silver("products", products_day1, products_schema, ["product_id"])

print("✓ Day 1 data ingested")

# COMMAND ----------

# Batch load to create the target.
result = Orchestrator(
    f"{CONFIG_PATH}/dim_customer.yml",
    spark=spark,
    etl_schema="demo_streaming_gold",
).run()
print(f"Batch result: {result['status']}")

display(spark.table("demo_streaming_gold.dim_customer"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Day 2: Streaming Incremental Load
# MAGIC
# MAGIC We update Alice's address and add Charlie. Then we run
# MAGIC `StreamingOrchestrator` with `trigger: available_now` so it processes
# MAGIC the available CDF batch and stops.

# COMMAND ----------

customers_day2 = [
    (
        1,
        "Alice",
        "Smith",
        "alice@example.com",
        "789 Cherry Ln, LA",
        "2025-01-02T09:00:00",
    ),
    (
        2,
        "Bob",
        "Jones",
        "bob@example.com",
        "456 Banana Blvd, SF",
        "2025-01-01T10:00:00",
    ),
    (
        3,
        "Charlie",
        "Brown",
        "charlie@example.com",
        "321 Date Dr, TX",
        "2025-01-02T10:00:00",
    ),
]

ingest_silver("customers", customers_day2, customers_schema, ["customer_id"])
print("✓ Day 2 changes ingested")

# COMMAND ----------

# Streaming run picks up the CDF changes.
stream_result = StreamingOrchestrator(
    f"{CONFIG_PATH}/dim_customer.yml",
    spark=spark,
    etl_schema="demo_streaming_gold",
).run()
print(f"Streaming result: {stream_result['status']}")
print(f"Queries: {list(stream_result['queries'].keys())}")

display(spark.table("demo_streaming_gold.dim_customer"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify Day 2 Streaming SCD2
# MAGIC
# MAGIC Alice should now have two rows (NY → LA), Charlie should have one.

# COMMAND ----------

alice_history = spark.sql("""
    SELECT customer_id, address, __valid_from, __valid_to, __is_current
    FROM demo_streaming_gold.dim_customer
    WHERE customer_id = 1
    ORDER BY __valid_from
""").collect()

print("Alice history:")
for row in alice_history:
    print(row)

assert len(alice_history) == 2, "Alice should have 2 rows after streaming update"
assert not alice_history[0]["__is_current"]
assert alice_history[1]["__is_current"]
assert alice_history[1]["address"] == "789 Cherry Ln, LA"
print("✅ Streaming SCD2 verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Day 3: Another Streaming Increment
# MAGIC
# MAGIC Charlie changes email (SCD2), Dana is new.

# COMMAND ----------

customers_day3 = [
    (
        1,
        "Alice",
        "Smith",
        "alice@example.com",
        "789 Cherry Ln, LA",
        "2025-01-02T09:00:00",
    ),
    (
        3,
        "Charlie",
        "Brown",
        "charlie.brown@newmail.com",
        "321 Date Dr, TX",
        "2025-01-03T09:00:00",
    ),
    (4, "Dana", "White", "dana@example.com", "555 Elm St, WA", "2025-01-03T10:00:00"),
]

ingest_silver("customers", customers_day3, customers_schema, ["customer_id"])

stream_result = StreamingOrchestrator(
    f"{CONFIG_PATH}/dim_customer.yml",
    spark=spark,
    etl_schema="demo_streaming_gold",
).run()
print(f"Day 3 streaming result: {stream_result['status']}")

display(spark.table("demo_streaming_gold.dim_customer"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Full Reload
# MAGIC
# MAGIC `StreamingOrchestrator.run(full_reload=True)` drops the target, resets
# MAGIC the watermark, clears the streaming checkpoint, runs a batch full
# MAGIC # snapshot, and then resumes streaming from the fresh baseline.

# COMMAND ----------

reload_result = StreamingOrchestrator(
    f"{CONFIG_PATH}/dim_customer.yml",
    spark=spark,
    etl_schema="demo_streaming_gold",
).run(full_reload=True)

print(f"Full reload result: {reload_result['status']}")
print(f"Rows read: {reload_result.get('rows_read', 'N/A')}")
print(f"Rows written: {reload_result.get('rows_written', 'N/A')}")

display(spark.table("demo_streaming_gold.dim_customer"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify Full Reload
# MAGIC
# MAGIC After reload, the dimension should reflect the current source state
# MAGIC (no stale SCD2 history beyond what currently exists).

# COMMAND ----------

current_count = spark.table("demo_streaming_gold.dim_customer").count()
data_rows = (
    spark.table("demo_streaming_gold.dim_customer").filter("customer_id > 0").count()
)

print(f"Total rows: {current_count}")
print(f"Data rows: {data_rows}")

assert data_rows == 3, f"Expected 3 current data rows, got {data_rows}"
print("✅ Full reload verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Streaming Continues After Reload
# MAGIC
# MAGIC Add one more change and confirm the streaming query still works.

# COMMAND ----------

customers_day4 = [
    (
        1,
        "Alice",
        "Smith",
        "alice-new@example.com",
        "789 Cherry Ln, LA",
        "2025-01-04T09:00:00",
    ),
]

ingest_silver("customers", customers_day4, customers_schema, ["customer_id"])

stream_result = StreamingOrchestrator(
    f"{CONFIG_PATH}/dim_customer.yml",
    spark=spark,
    etl_schema="demo_streaming_gold",
).run()
print(f"Post-reload streaming result: {stream_result['status']}")

alice_current = spark.sql("""
    SELECT email
    FROM demo_streaming_gold.dim_customer
    WHERE customer_id = 1 AND __is_current = true
""").collect()[0]
assert alice_current["email"] == "alice-new@example.com"
print("✅ Streaming after full reload verified")
