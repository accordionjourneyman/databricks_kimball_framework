# Databricks notebook source
# Kimball Framework - Installation Cell
# Installs directly from the repo source (no pre-built wheel needed)

import subprocess
import os

# Install from source (builds and installs the package)
# Path: implementations/pyspark/examples/Kimball_Demo.py → go up 3 levels to pyspark root
_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
# Go up: examples → pyspark → implementations → repo_root
_pyspark_root = "/Workspace" + os.path.dirname(os.path.dirname(_nb_path))
_repo_root = os.path.dirname(os.path.dirname(_pyspark_root))
subprocess.check_call(["pip", "install", _pyspark_root, "-q"])
print(f"✓ Installed kimball from {_repo_root}")

# Note: If upgrading versions and seeing stale behavior, restart your cluster

# COMMAND ----------

# ALTERNATIVE: Install from a pre-built wheel
# Build the wheel locally with: python -m build
# Then upload to your preferred location and install:
#
# wheel_path = "/Workspace/Users/your.email@example.com/wheels/kimball_framework-0.1.1-py3-none-any.whl"
# subprocess.check_call(["pip", "install", wheel_path, "-q"])
# dbutils.library.restartPython()

# COMMAND ----------

# ETL Configuration
import time
from delta.tables import DeltaTable

# Benchmark metrics storage
benchmark_metrics = []

# ============================================================================
# SET ETL SCHEMA - Configure once, use everywhere
# ============================================================================
# This environment variable tells the Kimball framework where to store the
# ETL control table (watermarks, batch tracking, metrics).
# Set this ONCE at the start of your notebook - no need to pass it to every
# Orchestrator or PipelineExecutor call.
os.environ["KIMBALL_ETL_SCHEMA"] = "demo_gold"

# ============================================================================
# FEATURE FLAGS - Choose lite or full mode
# ============================================================================
# The framework runs in LITE MODE by default (minimal overhead).
#
# OPTION 1: Lite Mode (default)
# - Comment out the line below to run with no optional features
# - Core SCD1/SCD2 functionality works identically
# - Cleaner output, faster startup
#
# OPTION 2: Full Mode (enable all features)
os.environ["KIMBALL_MODE"] = "full"  # <-- Comment this line for lite mode
#
# OPTION 3: Enable specific features only
# os.environ["KIMBALL_ENABLE_CHECKPOINTS"] = "1"      # Pipeline checkpointing
# os.environ["KIMBALL_ENABLE_STAGING_CLEANUP"] = "1"  # Orphaned staging cleanup
# os.environ["KIMBALL_ENABLE_METRICS"] = "1"          # Query metrics collection
# os.environ["KIMBALL_ENABLE_AUTO_CLUSTER"] = "1"     # Auto Liquid Clustering
# ============================================================================

# Setup Paths
# - Configs: Store in repo's examples folder (reuses _repo_root from cell 1)
# - Tables: Use managed tables (no explicit paths)

CONFIG_PATH = f"{_repo_root}/examples/configs"

# Create config directory
dbutils.fs.mkdirs(CONFIG_PATH)

# Create databases for managed tables
spark.sql("CREATE DATABASE IF NOT EXISTS demo_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS demo_gold")

# Clean up previous run
print("Cleaning up previous demo...")
for db in ["demo_silver", "demo_gold"]:
    tables = spark.sql(f"SHOW TABLES IN {db}").collect()
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {db}.{table.tableName}")

mode = "FULL" if os.environ.get("KIMBALL_MODE") == "full" else "LITE"
print(f"✓ Demo environment set up ({mode} mode)")
print(f"✓ Config path: {CONFIG_PATH}")
print(f"✓ ETL schema: {os.environ['KIMBALL_ETL_SCHEMA']}")

# COMMAND ----------

# Define Configuration
# Write YAML files to workspace (file I/O allowed)

dim_customer_yaml = """table_name: demo_gold.dim_customer
table_type: dimension
scd_type: 2
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
surrogate_key_strategy: hash
track_history_columns:
  - first_name
  - last_name
  - email
  - address
sources:
  - name: demo_silver.customers
    alias: c
    cdc_strategy: cdf
    primary_keys: [customer_id]  # Required for CDF deduplication
transformation_sql: |
  SELECT customer_id, first_name, last_name, email, address, updated_at FROM c
audit_columns: true
"""

dim_product_yaml = """table_name: demo_gold.dim_product
table_type: dimension
scd_type: 1
keys:
  surrogate_key: product_sk
  natural_keys: [product_id]
surrogate_key_strategy: identity
sources:
  - name: demo_silver.products
    alias: p
    cdc_strategy: cdf
    primary_keys: [product_id]  # Required for CDF deduplication
transformation_sql: |
  SELECT product_id, name, category, unit_cost, updated_at FROM p
audit_columns: true
"""

fact_sales_yaml = """table_name: demo_gold.fact_sales
table_type: fact
merge_keys: [order_item_id]

# Kimball-proper: Explicit foreign key declarations
# Replaces the old naming convention hack (columns ending with '_sk')
foreign_keys:
  - column: customer_sk
    references: demo_gold.dim_customer
    default_value: -1  # Unknown customer
  - column: product_sk
    references: demo_gold.dim_product
    default_value: -1  # Unknown product

sources:
  - name: demo_silver.order_items
    alias: oi
    cdc_strategy: cdf
    primary_keys: [order_item_id]  # Required for CDF deduplication
  - name: demo_silver.orders
    alias: o
    cdc_strategy: cdf
    primary_keys: [order_id]  # Required for CDF deduplication
  - name: demo_gold.dim_customer
    alias: c
    cdc_strategy: full
  - name: demo_gold.dim_product
    alias: p
    cdc_strategy: full
transformation_sql: |
  SELECT
    oi.order_item_id,
    o.order_id,
    c.customer_sk,
    p.product_sk,
    o.order_date,
    oi.quantity,
    oi.sales_amount,
    (oi.sales_amount - (p.unit_cost * oi.quantity)) as net_profit
  FROM oi
  JOIN o ON oi.order_id = o.order_id
  LEFT JOIN c ON o.customer_id = c.customer_id 
             AND o.order_date >= c.__valid_from 
             AND o.order_date < c.__valid_to
  LEFT JOIN p ON oi.product_id = p.product_id
audit_columns: true
"""

# Write configs using dbutils (workspace file I/O)
dbutils.fs.put(f"{CONFIG_PATH}/dim_customer.yml", dim_customer_yaml, overwrite=True)
dbutils.fs.put(f"{CONFIG_PATH}/dim_product.yml", dim_product_yaml, overwrite=True)
dbutils.fs.put(f"{CONFIG_PATH}/fact_sales.yml", fact_sales_yaml, overwrite=True)

print("✓ Configs written to workspace")
print(f"  - {CONFIG_PATH}/dim_customer.yml")
print(f"  - {CONFIG_PATH}/dim_product.yml")
print(f"  - {CONFIG_PATH}/fact_sales.yml")

# COMMAND ----------


def ingest_silver(table_name, data, schema, merge_keys):
    """
    Ingests data into a Silver Delta table with CDF enabled.
    """
    full_table_name = f"demo_silver.{table_name}"

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
# MAGIC ## 3. Day 1: Initial Load
# MAGIC We load the initial state of our source system.
# MAGIC *   **Customers**: Alice (NY), Bob (SF)
# MAGIC *   **Products**: Laptop ($1000), Mouse ($20)
# MAGIC *   **Orders**: 2 Orders

# COMMAND ----------

# Create Database for Demo
spark.sql("CREATE DATABASE IF NOT EXISTS demo_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS demo_gold")

# --- Day 1 Data ---
customers_data = [
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

products_data = [
    (101, "Laptop", "Electronics", 1000.00, "2025-01-01T10:00:00"),
    (102, "Mouse", "Electronics", 20.00, "2025-01-01T10:00:00"),
]
products_schema = (
    "product_id INT, name STRING, category STRING, unit_cost DOUBLE, updated_at STRING"
)

orders_data = [
    (1001, 1, "2025-01-01", "Completed", "2025-01-01T12:00:00"),
    (1002, 2, "2025-01-01", "Processing", "2025-01-01T13:00:00"),
]
orders_schema = (
    "order_id INT, customer_id INT, order_date STRING, status STRING, updated_at STRING"
)

order_items_data = [(5001, 1001, 101, 1, 1200.00), (5002, 1002, 102, 2, 50.00)]
order_items_schema = (
    "order_item_id INT, order_id INT, product_id INT, quantity INT, sales_amount DOUBLE"
)

# --- Ingest Day 1 ---
_t_load_start = time.perf_counter()
ingest_silver("customers", customers_data, customers_schema, ["customer_id"])
ingest_silver("products", products_data, products_schema, ["product_id"])
ingest_silver("orders", orders_data, orders_schema, ["order_id"])
ingest_silver("order_items", order_items_data, order_items_schema, ["order_item_id"])
_day1_load_time = time.perf_counter() - _t_load_start

print(f"Day 1 Data Ingested in {_day1_load_time:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Pipeline (Day 1)
# MAGIC We run the Orchestrator for Dimensions first, then Facts.

# COMMAND ----------

from kimball import Orchestrator

# Set Environment Variable for Jinja
os.environ["env"] = "demo"

# Run Dimensions (ETL schema already configured via KIMBALL_ETL_SCHEMA env var)
_t_transform_start = time.perf_counter()
print("Running dim_customer...")
Orchestrator(f"{CONFIG_PATH}/dim_customer.yml").run()

print("Running dim_product...")
Orchestrator(f"{CONFIG_PATH}/dim_product.yml").run()

# Run Fact
print("Running fact_sales...")
Orchestrator(f"{CONFIG_PATH}/fact_sales.yml").run()
_day1_transform_time = time.perf_counter() - _t_transform_start

_day1_rows = spark.table("demo_gold.fact_sales").count()
benchmark_metrics.append(
    {
        "framework": "pyspark",
        "day": 1,
        "load_time": _day1_load_time,
        "transform_time": _day1_transform_time,
        "total_time": _day1_load_time + _day1_transform_time,
        "rows": _day1_rows,
    }
)

print(f"Day 1 Pipeline Complete in {_day1_transform_time:.2f}s ({_day1_rows} rows)")

# COMMAND ----------

# Verify Day 1 Results
display(spark.table("demo_gold.dim_customer"))
display(spark.table("demo_gold.fact_sales"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Day 2: Incremental Updates
# MAGIC Now we simulate the next day's data load.
# MAGIC *   **Alice** moves to LA (Address Change -> Should trigger SCD2 new row).
# MAGIC *   **Laptop** price drops to $900 (Cost Change -> Should update SCD1 in place).
# MAGIC *   **Charlie** joins (New Customer).
# MAGIC *   **New Orders** placed.

# COMMAND ----------

# --- Day 2 Data ---
customers_day2 = [
    (
        1,
        "Alice",
        "Smith",
        "alice@example.com",
        "789 Cherry Ln, LA",
        "2025-01-02T09:00:00",
    ),  # Updated
    (
        2,
        "Bob",
        "Jones",
        "bob@example.com",
        "456 Banana Blvd, SF",
        "2025-01-01T10:00:00",
    ),  # Same
    (
        3,
        "Charlie",
        "Brown",
        "charlie@example.com",
        "321 Date Dr, TX",
        "2025-01-02T10:00:00",
    ),  # New
]

products_day2 = [
    (101, "Laptop", "Electronics", 900.00, "2025-01-02T09:00:00"),  # Updated Cost
    (102, "Mouse", "Electronics", 20.00, "2025-01-01T10:00:00"),  # Same
    (103, "Keyboard", "Electronics", 50.00, "2025-01-02T10:00:00"),  # New
]

orders_day2 = [
    (1003, 1, "2025-01-02", "Processing", "2025-01-02T11:00:00"),  # Alice's new order
    (1004, 3, "2025-01-02", "Shipped", "2025-01-02T14:00:00"),  # Charlie's order
]

order_items_day2 = [
    (5003, 1003, 102, 1, 25.00),  # Alice buys Mouse
    (5004, 1004, 103, 1, 60.00),  # Charlie buys Keyboard
]

# --- Ingest Day 2 ---
_t_load_start = time.perf_counter()
ingest_silver("customers", customers_day2, customers_schema, ["customer_id"])
ingest_silver("products", products_day2, products_schema, ["product_id"])
ingest_silver("orders", orders_day2, orders_schema, ["order_id"])
ingest_silver("order_items", order_items_day2, order_items_schema, ["order_item_id"])
_day2_load_time = time.perf_counter() - _t_load_start

print(f"Day 2 Data Ingested in {_day2_load_time:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Pipeline (Day 2)
# MAGIC Rerunning the orchestrator will process *only* the changes (CDF).

# COMMAND ----------

# Run Dimensions
_t_transform_start = time.perf_counter()
print("Running dim_customer (Day 2)...")
Orchestrator(f"{CONFIG_PATH}/dim_customer.yml").run()

print("Running dim_product (Day 2)...")
Orchestrator(f"{CONFIG_PATH}/dim_product.yml").run()

# Run Fact
print("Running fact_sales (Day 2)...")
Orchestrator(f"{CONFIG_PATH}/fact_sales.yml").run()
_day2_transform_time = time.perf_counter() - _t_transform_start

_day2_rows = spark.table("demo_gold.fact_sales").count()
benchmark_metrics.append(
    {
        "framework": "pyspark",
        "day": 2,
        "load_time": _day2_load_time,
        "transform_time": _day2_transform_time,
        "total_time": _day2_load_time + _day2_transform_time,
        "rows": _day2_rows,
    }
)

print(f"Day 2 Pipeline Complete in {_day2_transform_time:.2f}s ({_day2_rows} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verification & Testing
# MAGIC Let's verify the SCD behavior.

# COMMAND ----------

# 1. Verify SCD2 on Customer (Alice)
# We expect 2 rows for Alice:
# - One valid from Day 1 to Day 2
# - One valid from Day 2 to NULL (Current)
alice_history = spark.sql("""
    SELECT customer_sk, address, __valid_from, __valid_to, __is_current 
    FROM demo_gold.dim_customer 
    WHERE customer_id = 1 
    ORDER BY __valid_from
""").collect()

print("Alice History:")
for row in alice_history:
    print(row)

assert len(alice_history) == 2, "Alice should have 2 history rows"
assert alice_history[0]["__is_current"] == False, "First row should be expired"
assert alice_history[1]["__is_current"] == True, "Second row should be current"
assert alice_history[1]["address"] == "789 Cherry Ln, LA", (
    "Current address should be LA"
)

print("\n✅ SCD2 Test Passed")

# COMMAND ----------

# 2. Verify SCD1 on Product (Laptop)
# We expect 1 row for Laptop with the NEW cost (900), overwriting the old one.
laptop = spark.sql("""
    SELECT unit_cost 
    FROM demo_gold.dim_product 
    WHERE product_id = 101
""").collect()[0]

print(f"Laptop Cost: {laptop.unit_cost}")
assert laptop.unit_cost == 900.0, "Laptop cost should be updated to 900 (SCD1)"

print("\n✅ SCD1 Test Passed")

# COMMAND ----------

# 3. Verify Fact Sales Links
# Order 1001 (Day 1) should link to Alice's OLD SK.
# Order 1003 (Day 2) should link to Alice's NEW SK.

sales_check = spark.sql("""
    SELECT 
        o.order_id, 
        o.order_date,
        c.address as linked_customer_address
    FROM demo_gold.fact_sales f
    JOIN demo_gold.dim_customer c ON f.customer_sk = c.customer_sk
    JOIN demo_silver.orders o ON f.order_id = o.order_id -- Joining back to source for verification
    WHERE o.customer_id = 1
    ORDER BY o.order_date
""").collect()

print("Alice's Sales Links:")
for row in sales_check:
    print(row)

assert sales_check[0]["linked_customer_address"] == "123 Apple St, NY", (
    "Day 1 order should link to NY address"
)
assert sales_check[1]["linked_customer_address"] == "789 Cherry Ln, LA", (
    "Day 2 order should link to LA address"
)

print("\n✅ Fact Linkage Test Passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Benchmark Results

# COMMAND ----------

# Print benchmark comparison table
print("\n" + "=" * 70)
print("BENCHMARK RESULTS: PySpark Kimball Framework")
print("=" * 70)
print(
    f"{'Day':<6} {'Load (s)':<12} {'Transform (s)':<15} {'Total (s)':<12} {'Rows':<8}"
)
print("-" * 70)
for m in benchmark_metrics:
    print(
        f"{m['day']:<6} {m['load_time']:<12.2f} {m['transform_time']:<15.2f} {m['total_time']:<12.2f} {m['rows']:<8}"
    )
print("=" * 70)

# Save metrics for comparison with dbt
import json

metrics_path = f"{_repo_root}/benchmark_pyspark.json"
metrics_json = json.dumps(benchmark_metrics, indent=2)

# Use dbutils.fs.put for Databricks Repos compatibility
try:
    dbutils.fs.put(metrics_path, metrics_json, overwrite=True)
    print(f"\nMetrics saved to: {metrics_path}")
except Exception as e:
    # Fallback: print metrics for manual copy
    print(f"\n⚠️ Could not save to file ({e})")
    print("Metrics JSON:")
    print(metrics_json)

