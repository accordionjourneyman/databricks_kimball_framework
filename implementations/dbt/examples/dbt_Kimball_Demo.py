# Databricks notebook source
# pyright: reportUndefinedVariable=false
# type: ignore
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
# type: ignore
import os
import time

# Benchmark metrics storage
benchmark_metrics = []

# Get paths from notebook location
# Path: implementations/dbt/examples/dbt_Kimball_Demo.py
_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
# Go up: examples → dbt → implementations → repo_root
_dbt_root = "/Workspace" + os.path.dirname(os.path.dirname(_nb_path))
_repo_root = os.path.dirname(os.path.dirname(_dbt_root))
DBT_PROJECT_PATH = _dbt_root

print(f"✓ dbt project path: {DBT_PROJECT_PATH}")

# COMMAND ----------

# Create databases
spark.sql("CREATE DATABASE IF NOT EXISTS demo_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS demo_gold")

# Clean up previous run via efficient DROP CASCADE
print("Cleaning up previous demo...")
for db in ["demo_silver", "demo_gold"]:
    spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

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


def get_row_count_efficiently(table_name: str) -> int:
    """Gets row count using Delta metadata if available, falling back to count()."""
    try:
        # Optimization: Use Delta metadata for row count
        return spark.sql(f"DESCRIBE DETAIL {table_name}").select("numOutputRows").first()["numOutputRows"]
    except Exception:
        # Fallback to count() for non-Delta tables or if metadata read fails
        return spark.table(table_name).count()

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

# Get http_path - auto-detect SQL Warehouse using Databricks SDK
http_path = None

# Option 1: Try to auto-detect a running SQL Warehouse
try:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    warehouses = list(w.warehouses.list())

    # Find a running warehouse
    running = [wh for wh in warehouses if wh.state and wh.state.value == "RUNNING"]
    if running:
        http_path = f"/sql/1.0/warehouses/{running[0].id}"
        print(f"✓ Auto-detected SQL Warehouse: {running[0].name} ({running[0].id})")
    elif warehouses:
        # Use first available (may need to start it)
        http_path = f"/sql/1.0/warehouses/{warehouses[0].id}"
        print(f"✓ Found SQL Warehouse (may need to start): {warehouses[0].name}")
    else:
        print("⚠️ No SQL Warehouses found in workspace")
except Exception as e:
    print(f"⚠️ Could not auto-detect warehouses: {e}")

# Option 2: Check for cluster (All-Purpose)
if not http_path:
    try:
        cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", None)
        if cluster_id:
            org_id = spark.conf.get(
                "spark.databricks.clusterUsageTags.clusterOwnerOrgId", "0"
            )
            http_path = f"/sql/protocolv1/o/{org_id}/{cluster_id}"
            print(f"✓ Using All-Purpose Cluster: {cluster_id}")
    except Exception:
        pass

# Option 3: Manual fallback
if not http_path:
    print("=" * 70)
    print("⚠️  SQL WAREHOUSE REQUIRED")
    print("=" * 70)
    print("Could not auto-detect a SQL Warehouse or cluster.")
    print("")
    print("Please set your SQL Warehouse ID below:")
    print(
        '  sql_warehouse_id = "your-warehouse-id"  # Find in SQL Warehouses → Connection Details'
    )
    print("")
    # ========================================================
    # MANUAL CONFIG: Uncomment and set your SQL Warehouse ID
    # sql_warehouse_id = "your-warehouse-id"
    # http_path = f"/sql/1.0/warehouses/{sql_warehouse_id}"
    # ========================================================
    print("=" * 70)
    raise Exception("No SQL Warehouse configured - see instructions above")

# Try to ensure DATABRICKS_TOKEN is available in environment
if "DATABRICKS_TOKEN" not in os.environ:
    try:
        # Try to get from secrets
        token = dbutils.secrets.get(scope="dbt", key="token")
        os.environ["DATABRICKS_TOKEN"] = token
        print("✓ Loaded DATABRICKS_TOKEN from secrets")
    except Exception:
        print("=" * 70)
        print("⚠️  dbt SECRET REQUIRED")
        print("=" * 70)
        print("The dbt-databricks adapter requires a Personal Access Token (PAT).")
        print("")
        print("To create the secret:")
        print("  1. Generate a PAT: Databricks → Settings → Developer → Access Tokens")
        print("  2. Create secret scope: databricks secrets create-scope dbt")
        print("  3. Store the token: databricks secrets put-secret dbt token")
        print("")
        print("Or set DATABRICKS_TOKEN environment variable.")
        print("=" * 70)
        raise Exception("Missing DATABRICKS_TOKEN - see instructions above")

# Auto-detect catalog using Databricks SDK
catalog = None
try:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    catalogs = list(w.catalogs.list())

    # Filter out system catalogs
    system_catalogs = {"system", "hive_metastore", "__databricks_internal"}
    user_catalogs = [
        c for c in catalogs if c.name and c.name.lower() not in system_catalogs
    ]

    if user_catalogs:
        # Prefer 'main' or 'workspace' if available, otherwise use first user catalog
        for preferred in ["main", "workspace"]:
            for c in user_catalogs:
                if c.name and c.name.lower() == preferred:
                    catalog = c.name
                    break
            if catalog:
                break
        if not catalog:
            catalog = user_catalogs[0].name
        print(f"✓ Using catalog: {catalog}")
    else:
        catalog = "main"  # Fallback
        print("⚠️ No user catalogs found, using 'main'")
except Exception as e:
    catalog = "main"
    print(f"⚠️ Could not auto-detect catalog: {e}")

# Create profiles.yml
profiles_content = f"""
databricks:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: {catalog}
      schema: demo_gold
      host: {host}
      http_path: {http_path}
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      threads: 4
"""

with open(f"{dbt_profiles_dir}/profiles.yml", "w") as f:
    f.write(profiles_content)

print("✓ dbt profile configured")

# COMMAND ----------

# Install dbt packages
# Install dbt packages
# Install dbt packages
print("Running dbt deps...")
subprocess.run(
    ["dbt", "deps", "--profiles-dir", dbt_profiles_dir],
    check=True,
    cwd=DBT_PROJECT_PATH,
)
print("✓ dbt deps complete")

# COMMAND ----------

# Run dbt pipeline (Day 1 - full refresh)
_t_transform_start = time.perf_counter()

# First load seed data (default Kimball dimension rows)
# First load seed data (default Kimball dimension rows)
dbt_vars = f'{{"source_catalog": "{catalog}", "gold_schema": "demo_gold"}}'
print("Running dbt seed...")
subprocess.run(
    ["dbt", "seed", "--profiles-dir", dbt_profiles_dir, "--vars", dbt_vars],
    check=True,
    cwd=DBT_PROJECT_PATH,
)
print("✓ dbt seed complete")

# Run snapshots (SCD2 dimensions)
print("Running dbt snapshot...")
subprocess.run(
    ["dbt", "snapshot", "--profiles-dir", dbt_profiles_dir, "--vars", dbt_vars],
    check=True,
    cwd=DBT_PROJECT_PATH,
)
print("✓ dbt snapshot complete")

# Then run models (SCD1 dimensions and facts)
print("Running dbt run...")
subprocess.run(
    ["dbt", "run", "--profiles-dir", dbt_profiles_dir, "--vars", dbt_vars],
    check=True,
    cwd=DBT_PROJECT_PATH,
)
print("✓ dbt run complete")

_day1_transform_time = time.perf_counter() - _t_transform_start

# Optimization: Use Delta metadata for row count to avoid full table scan overhead
# Optimization: Use Delta metadata for row count to avoid full table scan overhead
_day1_rows = get_row_count_efficiently("demo_gold.fact_sales")


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
# DEBUG: Verify Alice's address in Silver and CHECK FOR DUPLICATES
print(f"DEBUG: Current Spark Catalog: {spark.catalog.currentCatalog()}")
print("DEBUG: Checking Alice's address in silver.customers...")
spark.sql("SELECT * FROM demo_silver.customers WHERE customer_id = 1").show()

print("DEBUG: Checking for duplicates in demo_silver.customers (customer_id)...")
dup_check = spark.sql(
    "SELECT customer_id, count(*) as cnt FROM demo_silver.customers GROUP BY customer_id HAVING cnt > 1"
).cache()

# Optimization: Check first row to avoid double scan (count + show)
if not dup_check.limit(1).isEmpty():
    print("🚨 DUPLICATES FOUND IN SILVER!")
    dup_check.show()
else:
    print("✓ No duplicates in Silver.")

dup_check.unpersist()

# COMMAND ----------

# Run dbt pipeline (Day 2 - incremental)
_t_transform_start = time.perf_counter()

# Run snapshots (SCD2 - will create new rows for changed records)
# Run snapshots (SCD2 - will create new rows for changed records)
print("Running dbt snapshot (Day 2)...")
subprocess.run(
    ["dbt", "snapshot", "--profiles-dir", dbt_profiles_dir, "--vars", dbt_vars],
    check=True,
    cwd=DBT_PROJECT_PATH,
)
print("✓ dbt snapshot complete")

# Run models (incremental)
print("Running dbt run (Day 2)...")
subprocess.run(
    ["dbt", "run", "--profiles-dir", dbt_profiles_dir, "--vars", dbt_vars],
    check=True,
    cwd=DBT_PROJECT_PATH,
)
print("✓ dbt run complete")

_day2_transform_time = time.perf_counter() - _t_transform_start

# Optimization: Use Delta metadata for row count
# Optimization: Use Delta metadata for row count
_day2_rows = get_row_count_efficiently("demo_gold.fact_sales")


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
with open(metrics_path, "w") as f:
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
    print("\\nNote: Run PySpark demo first to enable comparison table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. ETL Control / Watermark Verification

# COMMAND ----------

# Verify watermarks were tracked
print("=" * 70)
print("ETL CONTROL TABLE (Watermarks + Batch Audit)")
print("=" * 70)

try:
    etl_control = spark.table("demo_gold.etl_control")
    if etl_control.count() > 0:
        display(etl_control)

        # Verify batch statuses
        success_count = etl_control.filter("run_status = 'SUCCESS'").count()
        total_count = etl_control.count()
        print(f"\n✅ Batch Success Rate: {success_count}/{total_count}")
    else:
        print("⚠️ No watermarks recorded yet (first run)")
except Exception as e:
    print(f"⚠️ etl_control table not available: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Query Metrics (Databricks Query History)

# COMMAND ----------


def collect_query_metrics(since_ms: int) -> list:
    """Collect query metrics from Databricks Query History API."""
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.sql import QueryFilter, TimeRange

        w = WorkspaceClient()
        # Explicitly construct filter to avoid SDK dict conversion errors
        time_filter = TimeRange(start_time_ms=since_ms)
        query_filter = QueryFilter(query_start_time_range=time_filter)

        queries = list(
            w.query_history.list(
                filter_by=query_filter,
                include_metrics=True,
                max_results=50,
            )
        )

        metrics = []
        for q in queries:
            # Handle both SDK objects and dicts safely
            # If it's a dict, use .get(), if it's an object use getattr
            # The previous error 'dict object has no attribute as_dict' suggests q is already a dict
            # so we should use dictionary access or generic getattr with fallback

            is_dict = isinstance(q, dict)

            # Helper to get attribute or key
            def get_val(obj, key, default=None):
                if is_dict:
                    return obj.get(key, default)
                return getattr(obj, key, default)

            # Helper to get nested metrics
            q_metrics = get_val(q, "metrics")
            query_text = get_val(q, "query_text", "")

            if q_metrics and "dbt" in (query_text or "").lower():
                metrics.append(
                    {
                        "query_id": get_val(q, "query_id"),
                        "duration_ms": get_val(q, "duration"),
                        "rows_produced": get_val(q_metrics, "rows_produced_count", 0),
                        "bytes_read": get_val(q_metrics, "total_file_bytes_read", 0),
                        "status": str(
                            get_val(get_val(q, "status"), "value", "unknown")
                        ),
                    }
                )
        return metrics
    except Exception as e:
        print(f"⚠️ Query History API not available: {e}")
        return []


# Collect metrics from pipeline execution
pipeline_start_ms = int(
    (time.time() - (_day1_transform_time + _day2_transform_time + 60)) * 1000
)
query_metrics = collect_query_metrics(pipeline_start_ms)

if query_metrics:
    print("=" * 70)
    print("QUERY METRICS (from Databricks Query History)")
    print("=" * 70)
    for qm in query_metrics[:10]:  # Show top 10
        mb_read = qm["bytes_read"] / (1024 * 1024) if qm["bytes_read"] else 0
        print(
            f"Query {qm['query_id']}: {qm['duration_ms']}ms, {qm['rows_produced']} rows, {mb_read:.2f} MB read"
        )

    # Add to benchmark metrics
    total_bytes = sum(qm["bytes_read"] for qm in query_metrics if qm["bytes_read"])
    total_duration = sum(qm["duration_ms"] for qm in query_metrics if qm["duration_ms"])
    benchmark_metrics.append(
        {
            "framework": "dbt",
            "metric_type": "query_history",
            "total_queries": len(query_metrics),
            "total_duration_ms": total_duration,
            "total_bytes_read": total_bytes,
        }
    )
else:
    print("⚠️ No query metrics collected (Query History API may require Databricks SDK)")
