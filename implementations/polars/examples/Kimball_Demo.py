# Databricks notebook source
# MAGIC %md
# MAGIC # Polars Kimball Framework Demo (Databricks Notebook)
# MAGIC
# MAGIC This notebook demonstrates the Polars-based Kimball framework with:
# MAGIC - Delta MERGE for SCD1
# MAGIC - SCD2 with proper history tracking
# MAGIC - Fact table with dimension lookups
# MAGIC - Watermark tracking

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Required Packages

# COMMAND ----------

# DBTITLE 1,Install Dependencies and Setup Paths
import subprocess
import os

# Derive paths from notebook location (no hardcoding needed)
# Path: implementations/polars/examples/Kimball_Demo.py → go up 3 levels to polars root
_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
# Go up: examples → polars → implementations → repo_root
_polars_root = "/Workspace" + os.path.dirname(os.path.dirname(_nb_path))
_repo_root = os.path.dirname(os.path.dirname(_polars_root))

# Install polars and deltalake
subprocess.check_call(["pip", "install", "polars", "deltalake", "-q"])

# Install kimball_polars from source
subprocess.check_call(["pip", "install", _polars_root, "-q"])
print(f"✓ Installed kimball_polars from {_polars_root}")

# COMMAND ----------

# DBTITLE 1,Restart Python to pick up new packages
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Re-derive paths after Python restart
import os

# Re-derive paths after restart (variables are lost on restart)
_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
_polars_root = "/Workspace" + os.path.dirname(os.path.dirname(_nb_path))
_repo_root = os.path.dirname(os.path.dirname(_polars_root))
print(f"✓ Repo root: {_repo_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and Setup

# COMMAND ----------

# DBTITLE 1,Imports
import json
import shutil
import time
from pathlib import Path

import polars as pl
from deltalake import DeltaTable, write_deltalake

from kimball_polars import (
    apply_scd1,
    apply_scd2,
    get_table_version,
    update_watermark,
)

# COMMAND ----------

# DBTITLE 1,Configuration
# Benchmark metrics storage
benchmark_metrics = []

# Data directory for Delta tables (use DBFS or Unity Catalog volumes in production)
DATA_DIR = Path("/dbfs/tmp/kimball_polars_demo/data")
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
CONTROL_DIR = DATA_DIR / "control"

print("=" * 70)
print("Polars Kimball Framework Demo")
print("  - Delta MERGE for SCD1")
print("  - SCD2 with history tracking")
print("  - Fact table with FK lookups")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Initialize Data Directories
# Clean up previous run
if DATA_DIR.exists():
    shutil.rmtree(DATA_DIR)
DATA_DIR.mkdir(parents=True)
SILVER_DIR.mkdir()
GOLD_DIR.mkdir()
CONTROL_DIR.mkdir()
print(f"✓ Data directory: {DATA_DIR}")

# COMMAND ----------


# DBTITLE 1,Helper Functions
def write_silver(df: pl.DataFrame, name: str):
    """Write to silver layer."""
    path = SILVER_DIR / name
    write_deltalake(str(path), df.to_arrow(), mode="overwrite")
    return path


def read_delta(path: Path) -> pl.DataFrame:
    """Read Delta table."""
    return pl.from_arrow(DeltaTable(str(path)).to_pyarrow_table())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 1: Initial Load

# COMMAND ----------

# DBTITLE 1,Day 1 - Source Data
print("## Day 1: Initial Load")

# --- Day 1 Data ---
customers_day1 = pl.DataFrame(
    {
        "customer_id": [1, 2],
        "first_name": ["Alice", "Bob"],
        "last_name": ["Smith", "Jones"],
        "email": ["alice@example.com", "bob@example.com"],
        "address": ["123 Apple St, NY", "456 Banana Blvd, SF"],
        "updated_at": ["2025-01-01T10:00:00", "2025-01-01T10:00:00"],
    }
)

products_day1 = pl.DataFrame(
    {
        "product_id": [101, 102],
        "name": ["Laptop", "Mouse"],
        "category": ["Electronics", "Electronics"],
        "unit_cost": [1000.00, 20.00],
        "updated_at": ["2025-01-01T10:00:00", "2025-01-01T10:00:00"],
    }
)

orders_day1 = pl.DataFrame(
    {
        "order_id": [1001, 1002],
        "customer_id": [1, 2],
        "order_date": ["2025-01-01", "2025-01-01"],
        "status": ["Completed", "Processing"],
    }
)

order_items_day1 = pl.DataFrame(
    {
        "order_item_id": [5001, 5002],
        "order_id": [1001, 1002],
        "product_id": [101, 102],
        "quantity": [1, 2],
        "sales_amount": [1200.00, 50.00],
    }
)

display(customers_day1.to_pandas())
display(products_day1.to_pandas())

# COMMAND ----------

# DBTITLE 1,Day 1 - Write Silver Tables
_t_load_start = time.perf_counter()
write_silver(customers_day1, "customers")
write_silver(products_day1, "products")
write_silver(orders_day1, "orders")
write_silver(order_items_day1, "order_items")
_day1_load_time = time.perf_counter() - _t_load_start
print(f"✓ Silver tables written in {_day1_load_time:.4f}s")

# COMMAND ----------

# DBTITLE 1,Day 1 - Apply SCD Logic
_t_transform_start = time.perf_counter()

# SCD2 for customers
dim_customer = apply_scd2(
    target_path=GOLD_DIR / "dim_customer",
    source_df=customers_day1,
    natural_keys=["customer_id"],
    track_columns=["first_name", "last_name", "email", "address"],
    surrogate_key="customer_sk",
)
print(f"  dim_customer: {len(dim_customer)} rows")

# SCD1 for products (uses Delta MERGE)
dim_product = apply_scd1(
    target_path=GOLD_DIR / "dim_product",
    source_df=products_day1,
    natural_keys=["product_id"],
    surrogate_key="product_sk",
)
print(f"  dim_product: {len(dim_product)} rows")

# COMMAND ----------

# DBTITLE 1,Day 1 - Build Fact Table
# Build fact table with FK lookups (current rows only)
orders = read_delta(SILVER_DIR / "orders")
order_items = read_delta(SILVER_DIR / "order_items")

# Join order_items with orders
fact_source = order_items.join(orders, on="order_id")

# Get current dimension rows
current_customers = dim_customer.filter(pl.col("__is_current")).select(
    ["customer_id", "customer_sk"]
)
current_products = dim_product.select(["product_id", "product_sk", "unit_cost"])

# Build fact with FK lookups
fact_sales = (
    fact_source.join(current_customers, on="customer_id", how="left")
    .join(current_products, on="product_id", how="left")
    .with_columns(
        [
            pl.col("customer_sk").fill_null(-1),  # Unknown customer
            pl.col("product_sk").fill_null(-1),  # Unknown product
            (pl.col("sales_amount") - (pl.col("unit_cost") * pl.col("quantity"))).alias(
                "net_profit"
            ),
        ]
    )
    .select(
        [
            "order_item_id",
            "order_id",
            "customer_sk",
            "product_sk",
            "order_date",
            "quantity",
            "sales_amount",
            "net_profit",
        ]
    )
)

# Write fact table
write_deltalake(str(GOLD_DIR / "fact_sales"), fact_sales.to_arrow(), mode="overwrite")

# Update watermarks
for table in ["customers", "products", "orders", "order_items"]:
    version = get_table_version(SILVER_DIR / table)
    update_watermark(CONTROL_DIR / "watermarks", table, version)

_day1_transform_time = time.perf_counter() - _t_transform_start

benchmark_metrics.append(
    {
        "framework": "polars",
        "day": 1,
        "load_time": _day1_load_time,
        "transform_time": _day1_transform_time,
        "total_time": _day1_load_time + _day1_transform_time,
        "rows": len(fact_sales),
    }
)

print(f"  fact_sales: {len(fact_sales)} rows")
print(f"✓ Day 1 Complete in {_day1_load_time + _day1_transform_time:.4f}s")

display(fact_sales.to_pandas())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 2: Incremental Updates
# MAGIC
# MAGIC - Alice moves to LA (SCD2 → new history row)
# MAGIC - Laptop price drops to $900 (SCD1 → MERGE update)
# MAGIC - Charlie joins (new customer)

# COMMAND ----------

# DBTITLE 1,Day 2 - Source Data
print("## Day 2: Incremental Updates")
print("- Alice moves to LA (SCD2 → new history row)")
print("- Laptop price drops to $900 (SCD1 → MERGE update)")
print("- Charlie joins (new customer)")

# --- Day 2 Data ---
customers_day2 = pl.DataFrame(
    {
        "customer_id": [1, 2, 3],
        "first_name": ["Alice", "Bob", "Charlie"],
        "last_name": ["Smith", "Jones", "Brown"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "address": ["789 Cherry Ln, LA", "456 Banana Blvd, SF", "321 Date Dr, TX"],
        "updated_at": [
            "2025-01-02T09:00:00",
            "2025-01-01T10:00:00",
            "2025-01-02T10:00:00",
        ],
    }
)

products_day2 = pl.DataFrame(
    {
        "product_id": [101, 102, 103],
        "name": ["Laptop", "Mouse", "Keyboard"],
        "category": ["Electronics", "Electronics", "Electronics"],
        "unit_cost": [900.00, 20.00, 50.00],
        "updated_at": [
            "2025-01-02T09:00:00",
            "2025-01-01T10:00:00",
            "2025-01-02T10:00:00",
        ],
    }
)

orders_day2 = pl.DataFrame(
    {
        "order_id": [1003, 1004],
        "customer_id": [1, 3],
        "order_date": ["2025-01-02", "2025-01-02"],
        "status": ["Processing", "Shipped"],
    }
)

order_items_day2 = pl.DataFrame(
    {
        "order_item_id": [5003, 5004],
        "order_id": [1003, 1004],
        "product_id": [102, 103],
        "quantity": [1, 1],
        "sales_amount": [25.00, 60.00],
    }
)

display(customers_day2.to_pandas())
display(products_day2.to_pandas())

# COMMAND ----------

# DBTITLE 1,Day 2 - Write Silver Tables
_t_load_start = time.perf_counter()
write_silver(customers_day2, "customers")
write_silver(products_day2, "products")

# Append orders
existing_orders = read_delta(SILVER_DIR / "orders")
all_orders = pl.concat([existing_orders, orders_day2])
write_silver(all_orders, "orders")

existing_items = read_delta(SILVER_DIR / "order_items")
all_items = pl.concat([existing_items, order_items_day2])
write_silver(all_items, "order_items")

_day2_load_time = time.perf_counter() - _t_load_start
print(f"✓ Silver tables updated in {_day2_load_time:.4f}s")

# COMMAND ----------

# DBTITLE 1,Day 2 - Apply SCD Logic
_t_transform_start = time.perf_counter()

# SCD2 for customers - should create history for Alice
dim_customer = apply_scd2(
    target_path=GOLD_DIR / "dim_customer",
    source_df=customers_day2,
    natural_keys=["customer_id"],
    track_columns=["first_name", "last_name", "email", "address"],
    surrogate_key="customer_sk",
)
print(f"  dim_customer: {len(dim_customer)} rows (with history)")

# SCD1 for products - uses Delta MERGE
dim_product = apply_scd1(
    target_path=GOLD_DIR / "dim_product",
    source_df=products_day2,
    natural_keys=["product_id"],
    surrogate_key="product_sk",
)
print(f"  dim_product: {len(dim_product)} rows")

# COMMAND ----------

# DBTITLE 1,Day 2 - Build Fact Table
# Build fact for all orders
orders = read_delta(SILVER_DIR / "orders")
order_items = read_delta(SILVER_DIR / "order_items")
fact_source = order_items.join(orders, on="order_id")

current_customers = dim_customer.filter(pl.col("__is_current")).select(
    ["customer_id", "customer_sk"]
)
current_products = dim_product.select(["product_id", "product_sk", "unit_cost"])

fact_sales = (
    fact_source.join(current_customers, on="customer_id", how="left")
    .join(current_products, on="product_id", how="left")
    .with_columns(
        [
            pl.col("customer_sk").fill_null(-1),
            pl.col("product_sk").fill_null(-1),
            (pl.col("sales_amount") - (pl.col("unit_cost") * pl.col("quantity"))).alias(
                "net_profit"
            ),
        ]
    )
    .select(
        [
            "order_item_id",
            "order_id",
            "customer_sk",
            "product_sk",
            "order_date",
            "quantity",
            "sales_amount",
            "net_profit",
        ]
    )
)

write_deltalake(str(GOLD_DIR / "fact_sales"), fact_sales.to_arrow(), mode="overwrite")

_day2_transform_time = time.perf_counter() - _t_transform_start

benchmark_metrics.append(
    {
        "framework": "polars",
        "day": 2,
        "load_time": _day2_load_time,
        "transform_time": _day2_transform_time,
        "total_time": _day2_load_time + _day2_transform_time,
        "rows": len(fact_sales),
    }
)

print(f"  fact_sales: {len(fact_sales)} rows")
print(f"✓ Day 2 Complete in {_day2_load_time + _day2_transform_time:.4f}s")

display(fact_sales.to_pandas())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# DBTITLE 1,Verify SCD2 - Alice History
print("## Verification")

# 1. SCD2 Test - Alice should have 2 rows
dim_customer = read_delta(GOLD_DIR / "dim_customer")
alice_history = dim_customer.filter(pl.col("customer_id") == 1)
print("\nAlice History (SCD2):")
display(
    alice_history.select(
        ["customer_sk", "address", "__valid_from", "__valid_to", "__is_current"]
    ).to_pandas()
)

assert len(alice_history) == 2, f"Alice should have 2 rows, got {len(alice_history)}"
print("✅ SCD2 Test Passed")

# COMMAND ----------

# DBTITLE 1,Verify SCD1 - Laptop Price Update
# 2. SCD1 Test - Laptop should have new price
dim_product = read_delta(GOLD_DIR / "dim_product")
laptop = dim_product.filter(pl.col("product_id") == 101)
print("\nLaptop (SCD1 via MERGE):")
display(laptop.select(["product_sk", "name", "unit_cost"]).to_pandas())

assert laptop["unit_cost"][0] == 900.0, "Laptop cost should be 900"
print("✅ SCD1 Test Passed")

# COMMAND ----------

# DBTITLE 1,Verify Fact Table
# 3. Fact table
fact_sales = read_delta(GOLD_DIR / "fact_sales")
print(f"\nFact Sales: {len(fact_sales)} rows")
display(fact_sales.to_pandas())

assert len(fact_sales) == 4, f"Should have 4 fact rows, got {len(fact_sales)}"
print("✅ Fact Table Test Passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benchmark Results

# COMMAND ----------

# DBTITLE 1,Display Benchmark Results
print("\n" + "=" * 70)
print("BENCHMARK RESULTS: Polars Kimball Framework")
print("  (with Delta MERGE + SCD2 History)")
print("=" * 70)
print(
    f"{'Day':<6} {'Load (s)':<12} {'Transform (s)':<15} {'Total (s)':<12} {'Rows':<8}"
)
print("-" * 70)
for m in benchmark_metrics:
    print(
        f"{m['day']:<6} {m['load_time']:<12.4f} {m['transform_time']:<15.4f} {m['total_time']:<12.4f} {m['rows']:<8}"
    )
print("=" * 70)

# Display as DataFrame for better visualization
import pandas as pd

benchmark_df = pd.DataFrame(benchmark_metrics)
display(benchmark_df)

# COMMAND ----------

# DBTITLE 1,Save Metrics (Optional)
# Save metrics to DBFS (update path as needed)
metrics_path = "/dbfs/tmp/kimball_polars_demo/benchmark_polars.json"
Path(metrics_path).parent.mkdir(parents=True, exist_ok=True)
with open(metrics_path, "w") as f:
    json.dump(benchmark_metrics, f, indent=2)
print(f"\nMetrics saved to: {metrics_path}")
print(f"Delta tables saved to: {DATA_DIR}")

print("\n✅ All Tests Passed!")
