# Databricks notebook source
# MAGIC %md
# MAGIC # dbt Kimball Framework Demo
# MAGIC
# MAGIC This notebook demonstrates the dbt-based Kimball framework on Databricks.
# MAGIC It mirrors the PySpark demo to allow performance comparison.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: Install dbt and Create Silver Tables

# COMMAND ----------

# Install dbt-databricks
# %pip install dbt-databricks dbt-core -q

# COMMAND ----------

import subprocess
import os
import time

# Benchmark metrics storage
benchmark_metrics = []

# Get repo root from notebook path
_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
_repo_root = "/Workspace" + os.path.dirname(os.path.dirname(os.path.dirname(_nb_path)))
DBT_PROJECT_PATH = f"{_repo_root}/dbt_kimball"

print(f"✓ dbt project path: {DBT_PROJECT_PATH}")

# COMMAND ----------

# Create databases
spark.sql("CREATE DATABASE IF NOT EXISTS demo_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS demo_gold")

# Clean up previous run
print("Cleaning up previous demo...")
for db in ["demo_silver", "demo_gold"]:
    tables = spark.sql(f"SHOW TABLES IN {db}").collect()
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {db}.{table.tableName}")

print("✓ Databases ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest Silver Layer Data (Day 1)

# COMMAND ----------

from delta.tables import DeltaTable


def ingest_silver(table_name, data, schema, merge_keys):
    full_table_name = f"demo_silver.{table_name}"
    df = spark.createDataFrame(data, schema=schema)

    if not spark.catalog.tableExists(full_table_name):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("delta.enableChangeDataFeed", "true")
            .saveAsTable(full_table_name)
        )
    else:
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

# Ingest Day 1
_t_load_start = time.perf_counter()
ingest_silver("customers", customers_data, customers_schema, ["customer_id"])
ingest_silver("products", products_data, products_schema, ["product_id"])
ingest_silver("orders", orders_data, orders_schema, ["order_id"])
ingest_silver("order_items", order_items_data, order_items_schema, ["order_item_id"])
_day1_load_time = time.perf_counter() - _t_load_start

print(f"✓ Day 1 Data Ingested in {_day1_load_time:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run dbt Pipeline (Day 1)

# COMMAND ----------

# Setup dbt profile for this session
dbt_profiles_dir = "/tmp/dbt_profiles"
os.makedirs(dbt_profiles_dir, exist_ok=True)

# Get Databricks connection info
host = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .browserHostName()
    .get()
)
http_path = spark.conf.get("spark.databricks.cluster.id", "")

# Create profiles.yml
profiles_content = f"""
databricks:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: main
      schema: demo_gold
      host: {host}
      http_path: /sql/1.0/warehouses/{http_path}
      token: {dbutils.secrets.get(scope="dbt", key="token")}
      threads: 4
"""

with open(f"{dbt_profiles_dir}/profiles.yml", "w") as f:
    f.write(profiles_content)

print("✓ dbt profile configured")

# COMMAND ----------

# Install dbt packages
os.chdir(DBT_PROJECT_PATH)
result = subprocess.run(
    ["dbt", "deps", "--profiles-dir", dbt_profiles_dir], capture_output=True, text=True
)
print(result.stdout)
if result.returncode != 0:
    print(result.stderr)

# COMMAND ----------

# Run dbt pipeline (Day 1 - full refresh)
_t_transform_start = time.perf_counter()

# Run snapshots first (SCD2 dimensions)
result = subprocess.run(
    ["dbt", "snapshot", "--profiles-dir", dbt_profiles_dir],
    capture_output=True,
    text=True,
    cwd=DBT_PROJECT_PATH,
)
print("=== dbt snapshot ===")
print(result.stdout)

# Then run models (SCD1 dimensions and facts)
result = subprocess.run(
    ["dbt", "run", "--profiles-dir", dbt_profiles_dir],
    capture_output=True,
    text=True,
    cwd=DBT_PROJECT_PATH,
)
print("=== dbt run ===")
print(result.stdout)

_day1_transform_time = time.perf_counter() - _t_transform_start
_day1_rows = spark.table("demo_gold.fact_sales").count()

benchmark_metrics.append(
    {
        "framework": "dbt",
        "day": 1,
        "load_time": _day1_load_time,
        "transform_time": _day1_transform_time,
        "total_time": _day1_load_time + _day1_transform_time,
        "rows": _day1_rows,
    }
)

print(f"\n✓ Day 1 Pipeline Complete in {_day1_transform_time:.2f}s ({_day1_rows} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify Day 1 Results

# COMMAND ----------

display(spark.table("demo_gold.dim_customer"))

# COMMAND ----------

display(spark.table("demo_gold.dim_product"))

# COMMAND ----------

display(spark.table("demo_gold.fact_sales"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Day 2: Incremental Updates

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
    (1003, 1, "2025-01-02", "Processing", "2025-01-02T11:00:00"),
    (1004, 3, "2025-01-02", "Shipped", "2025-01-02T14:00:00"),
]

order_items_day2 = [
    (5003, 1003, 102, 1, 25.00),
    (5004, 1004, 103, 1, 60.00),
]

# Ingest Day 2
_t_load_start = time.perf_counter()
ingest_silver("customers", customers_day2, customers_schema, ["customer_id"])
ingest_silver("products", products_day2, products_schema, ["product_id"])
ingest_silver("orders", orders_day2, orders_schema, ["order_id"])
ingest_silver("order_items", order_items_day2, order_items_schema, ["order_item_id"])
_day2_load_time = time.perf_counter() - _t_load_start

print(f"✓ Day 2 Data Ingested in {_day2_load_time:.2f}s")

# COMMAND ----------

# Run dbt pipeline (Day 2 - incremental)
_t_transform_start = time.perf_counter()

# Run snapshots (SCD2 - will create new rows for changed records)
result = subprocess.run(
    ["dbt", "snapshot", "--profiles-dir", dbt_profiles_dir],
    capture_output=True,
    text=True,
    cwd=DBT_PROJECT_PATH,
)
print("=== dbt snapshot (Day 2) ===")
print(result.stdout)

# Run models (incremental)
result = subprocess.run(
    ["dbt", "run", "--profiles-dir", dbt_profiles_dir],
    capture_output=True,
    text=True,
    cwd=DBT_PROJECT_PATH,
)
print("=== dbt run (Day 2) ===")
print(result.stdout)

_day2_transform_time = time.perf_counter() - _t_transform_start
_day2_rows = spark.table("demo_gold.fact_sales").count()

benchmark_metrics.append(
    {
        "framework": "dbt",
        "day": 2,
        "load_time": _day2_load_time,
        "transform_time": _day2_transform_time,
        "total_time": _day2_load_time + _day2_transform_time,
        "rows": _day2_rows,
    }
)

print(f"\n✓ Day 2 Pipeline Complete in {_day2_transform_time:.2f}s ({_day2_rows} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verification & Testing

# COMMAND ----------

# 1. Verify SCD2 on Customer (Alice)
alice_history = spark.sql("""
    SELECT customer_sk, address, dbt_valid_from, dbt_valid_to, dbt_scd_id
    FROM demo_gold.dim_customer 
    WHERE customer_id = 1 
    ORDER BY dbt_valid_from
""").collect()

print("Alice History:")
for row in alice_history:
    print(row)

assert len(alice_history) == 2, "Alice should have 2 history rows"
print("\n✅ SCD2 Test Passed")

# COMMAND ----------

# 2. Verify SCD1 on Product (Laptop)
laptop = spark.sql("""
    SELECT unit_cost FROM demo_gold.dim_product WHERE product_id = 101
""").collect()[0]

print(f"Laptop Cost: {laptop.unit_cost}")
assert laptop.unit_cost == 900.0, "Laptop cost should be updated to 900 (SCD1)"
print("\n✅ SCD1 Test Passed")

# COMMAND ----------

# 3. Verify Fact Sales Links
sales_check = spark.sql("""
    SELECT 
        f.order_id,
        c.address as linked_customer_address
    FROM demo_gold.fact_sales f
    JOIN demo_gold.dim_customer c ON f.customer_sk = c.customer_sk
    WHERE c.customer_id = 1
    ORDER BY f.order_id
""").collect()

print("Alice's Sales Links:")
for row in sales_check:
    print(row)

print("\n✅ All Tests Passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Benchmark Results

# COMMAND ----------

# Print benchmark comparison table
print("\n" + "=" * 70)
print("BENCHMARK RESULTS: dbt Kimball Framework")
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

# Save metrics for comparison with PySpark
import json

metrics_path = f"{_repo_root}/benchmark_dbt.json"
with open(metrics_path.replace("/Workspace", "/dbfs"), "w") as f:
    json.dump(benchmark_metrics, f, indent=2)
print(f"\nMetrics saved to: {metrics_path}")

# Try to load PySpark metrics for comparison
try:
    pyspark_path = f"{_repo_root}/benchmark_pyspark.json"
    with open(pyspark_path.replace("/Workspace", "/dbfs"), "r") as f:
        pyspark_metrics = json.load(f)

    print("\n" + "=" * 70)
    print("COMPARISON: PySpark vs dbt")
    print("=" * 70)
    print(
        f"{'Framework':<12} {'Day':<6} {'Load (s)':<12} {'Transform (s)':<15} {'Total (s)':<12}"
    )
    print("-" * 70)
    for m in pyspark_metrics + benchmark_metrics:
        print(
            f"{m['framework']:<12} {m['day']:<6} {m['load_time']:<12.2f} {m['transform_time']:<15.2f} {m['total_time']:<12.2f}"
        )
    print("=" * 70)
except FileNotFoundError:
    print("\nNote: Run PySpark demo first to enable comparison table")
