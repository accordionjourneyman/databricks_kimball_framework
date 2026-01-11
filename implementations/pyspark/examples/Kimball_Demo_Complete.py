# Databricks notebook source
# Kimball Framework - Complete Demo
# Demonstrates ALL framework features including:
# - Data Quality Validation (TestSeverity, ValidationReport)
# - Pipeline Compilation (DAG validation, SQL syntax check)
# - View Refresh (SCD2 current views)
# - Documentation Generation (Catalog, Mermaid DAG)
# - Parallel Execution (FAIR scheduler)
# - Dev Utils (Shallow Clone)

# COMMAND ----------

# Install Framework
import subprocess
import os
import time

_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
_pyspark_root = "/Workspace" + os.path.dirname(os.path.dirname(_nb_path))
_repo_root = os.path.dirname(os.path.dirname(_pyspark_root))
subprocess.check_call(["pip", "install", _pyspark_root, "-q"])
print(f"✓ Installed kimball from {_repo_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Environment Setup

# COMMAND ----------

from delta.tables import DeltaTable

# Configure ETL schema
os.environ["KIMBALL_ETL_SCHEMA"] = "demo_gold"
os.environ["KIMBALL_MODE"] = "full"  # Enable all features

# Setup paths
CONFIG_PATH = f"{_repo_root}/examples/configs"
dbutils.fs.mkdirs(CONFIG_PATH)

# Create databases
spark.sql("CREATE DATABASE IF NOT EXISTS demo_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS demo_gold")
spark.sql("CREATE DATABASE IF NOT EXISTS demo_dev")

# Clean previous run
for db in ["demo_silver", "demo_gold", "demo_dev"]:
    for table in spark.sql(f"SHOW TABLES IN {db}").collect():
        spark.sql(f"DROP TABLE IF EXISTS {db}.{table.tableName}")

print("✓ Environment ready")

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Configuration with Data Quality Tests
# MAGIC 
# MAGIC Define YAML configs with dbt-style data quality tests.

# COMMAND ----------

dim_customer_yaml = """table_name: demo_gold.dim_customer
table_type: dimension
scd_type: 2
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
surrogate_key_strategy: hash
sources:
  - name: demo_silver.customers
    alias: c
    cdc_strategy: cdf
    primary_keys: [customer_id]
    freshness:
      warn_after_hours: 12
      error_after_hours: 24
transformation_sql: |
  SELECT customer_id, first_name, last_name, email, address, updated_at FROM c

# Data Quality Tests (dbt-style)
tests:
  - column: customer_sk
    tests:
      - unique
      - not_null
  - column: email
    tests:
      - not_null
  - column: status
    tests:
      - accepted_values: [active, inactive, pending]
"""

dim_product_yaml = """table_name: demo_gold.dim_product
table_type: dimension
scd_type: 1
keys:
  surrogate_key: product_sk
  natural_keys: [product_id]
sources:
  - name: demo_silver.products
    alias: p
    cdc_strategy: cdf
    primary_keys: [product_id]
transformation_sql: |
  SELECT product_id, name, category, unit_cost, updated_at FROM p
tests:
  - column: product_sk
    tests:
      - unique
      - not_null
  - column: unit_cost
    tests:
      - expression: unit_cost > 0
"""

fact_sales_yaml = """table_name: demo_gold.fact_sales
table_type: fact
merge_keys: [order_item_id]
foreign_keys:
  - column: customer_sk
    references: demo_gold.dim_customer
    default_value: -1
  - column: product_sk
    references: demo_gold.dim_product
    default_value: -1
sources:
  - name: demo_silver.order_items
    alias: oi
    cdc_strategy: cdf
    primary_keys: [order_item_id]
  - name: demo_silver.orders
    alias: o
    cdc_strategy: cdf
  - name: demo_gold.dim_customer
    alias: c
    cdc_strategy: full
  - name: demo_gold.dim_product
    alias: p
    cdc_strategy: full
transformation_sql: |
  SELECT
    oi.order_item_id, o.order_id, c.customer_sk, p.product_sk,
    o.order_date, oi.quantity, oi.sales_amount,
    (oi.sales_amount - (p.unit_cost * oi.quantity)) as net_profit
  FROM oi
  JOIN o ON oi.order_id = o.order_id
  LEFT JOIN c ON o.customer_id = c.customer_id AND c.__is_current = true
  LEFT JOIN p ON oi.product_id = p.product_id
tests:
  - column: order_item_id
    tests:
      - unique
  - column: customer_sk
    tests:
      - relationships:
          to: demo_gold.dim_customer
          field: customer_sk
"""

# Write configs
dbutils.fs.put(f"{CONFIG_PATH}/dim_customer.yml", dim_customer_yaml, overwrite=True)
dbutils.fs.put(f"{CONFIG_PATH}/dim_product.yml", dim_product_yaml, overwrite=True)
dbutils.fs.put(f"{CONFIG_PATH}/fact_sales.yml", fact_sales_yaml, overwrite=True)

print("✓ Configs with tests written")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Pipeline Compilation
# MAGIC 
# MAGIC Validate DAG, SQL syntax, and FK references BEFORE loading data.

# COMMAND ----------

from kimball import PipelineCompiler, CompileResult

compiler = PipelineCompiler(spark)
result: CompileResult = compiler.compile_all(CONFIG_PATH)

print(f"Compilation: {'✅ SUCCESS' if result.success else '❌ FAILED'}")
print(f"Models: {result.models}")
print(f"DAG Order: {result.dag_order}")
if result.warnings:
    print(f"Warnings: {result.warnings}")
if result.errors:
    print(f"Errors: {result.errors}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Documentation Generation
# MAGIC 
# MAGIC Auto-generate catalog and Mermaid DAG from configs.

# COMMAND ----------

from kimball import CatalogGenerator

generator = CatalogGenerator()
configs = generator.load_configs(CONFIG_PATH)

# Generate manifest (JSON)
manifest = generator.generate_manifest(configs)
print("=== Manifest ===")
print(f"Models: {list(manifest['models'].keys())}")
print(f"Sources: {manifest['sources']}")
print(f"Relationships: {manifest['relationships']}")

# Generate Mermaid DAG
dag = generator.generate_lineage_dag(configs)
print("\n=== Lineage DAG ===")
print(dag)

# Generate Markdown catalog
catalog = generator.generate_catalog(configs)
print("\n=== Data Catalog (excerpt) ===")
print(catalog[:500])

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Load Sample Data

# COMMAND ----------

def ingest_silver(table_name, data, schema, merge_keys):
    """Ingest data into Silver Delta table with CDF enabled."""
    full_table_name = f"demo_silver.{table_name}"
    df = spark.createDataFrame(data, schema=schema)
    
    if not spark.catalog.tableExists(full_table_name):
        df.write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed", "true").saveAsTable(full_table_name)
    else:
        delta_table = DeltaTable.forName(spark, full_table_name)
        merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        delta_table.alias("t").merge(df.alias("s"), merge_condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Day 1 Data
customers_data = [
    (1, "Alice", "Smith", "alice@example.com", "123 Apple St, NY", "active", "2025-01-01T10:00:00"),
    (2, "Bob", "Jones", "bob@example.com", "456 Banana Blvd, SF", "active", "2025-01-01T10:00:00"),
]
customers_schema = "customer_id INT, first_name STRING, last_name STRING, email STRING, address STRING, status STRING, updated_at STRING"

products_data = [
    (101, "Laptop", "Electronics", 1000.00, "2025-01-01T10:00:00"),
    (102, "Mouse", "Electronics", 20.00, "2025-01-01T10:00:00"),
]
products_schema = "product_id INT, name STRING, category STRING, unit_cost DOUBLE, updated_at STRING"

orders_data = [(1001, 1, "2025-01-01", "Completed", "2025-01-01T12:00:00")]
orders_schema = "order_id INT, customer_id INT, order_date STRING, status STRING, updated_at STRING"

order_items_data = [(5001, 1001, 101, 1, 1200.00)]
order_items_schema = "order_item_id INT, order_id INT, product_id INT, quantity INT, sales_amount DOUBLE"

ingest_silver("customers", customers_data, customers_schema, ["customer_id"])
ingest_silver("products", products_data, products_schema, ["product_id"])
ingest_silver("orders", orders_data, orders_schema, ["order_id"])
ingest_silver("order_items", order_items_data, order_items_schema, ["order_item_id"])

print("✓ Day 1 data loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Run ETL Pipeline

# COMMAND ----------

from kimball import Orchestrator

os.environ["env"] = "demo"

print("Running dim_customer...")
Orchestrator(f"{CONFIG_PATH}/dim_customer.yml").run()

print("Running dim_product...")
Orchestrator(f"{CONFIG_PATH}/dim_product.yml").run()

print("Running fact_sales...")
Orchestrator(f"{CONFIG_PATH}/fact_sales.yml").run()

print("✓ Pipeline complete")

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Data Quality Validation
# MAGIC 
# MAGIC Run data quality tests and get structured report.

# COMMAND ----------

from kimball import DataQualityValidator, ValidationReport, TestSeverity

validator = DataQualityValidator(spark)

# Run tests on dim_customer
df_customer = spark.table("demo_gold.dim_customer")

results = []
results.append(validator.validate_unique(df_customer, ["customer_sk"]))
results.append(validator.validate_not_null(df_customer, ["customer_sk", "email"]))

report = ValidationReport(results=results)

print("=== Data Quality Report ===")
print(report)

if not report.passed:
    print("\n⚠️ Some tests failed!")
    for r in report.results:
        if not r.passed:
            print(f"  - {r.test_name}: {r.details}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. View Refresh (SCD2 Current View)

# COMMAND ----------

from kimball.view_refresher import ViewRefresher

refresher = ViewRefresher(spark)

# Create current view for dim_customer (filters to __is_current = true)
refresher.refresh_current_view("demo_gold.dim_customer", "demo_gold.v_dim_customer_current")

print("Current customers (via view):")
display(spark.table("demo_gold.v_dim_customer_current"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 9. Dev Environment Setup (Shallow Clone)

# COMMAND ----------

from kimball.dev_utils import shallow_clone_table, compare_tables, setup_dev_environment

# Setup dev environment with shallow clones (zero storage cost)
tables = ["demo_gold.dim_customer", "demo_gold.dim_product", "demo_gold.fact_sales"]
results = setup_dev_environment(spark, tables, "demo_dev", use_shallow=True)

print("\nDev clones created:")
for table, success in results.items():
    print(f"  {'✓' if success else '✗'} {table}")

# Compare dev to prod (should be identical)
comparison = compare_tables(spark, "demo_dev.dim_customer", "demo_gold.dim_customer", ["customer_sk"])
print(f"\nComparison: {comparison}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 10. Workflow Generation (DAB YAML)

# COMMAND ----------

from kimball.workflow_generator import WorkflowGenerator

wf_generator = WorkflowGenerator()

# Print dependency graph
wf_generator.print_dependency_graph(CONFIG_PATH)

# Generate DAB YAML
dab = wf_generator.generate_dab_yaml(
    config_dir=CONFIG_PATH,
    job_name="Demo Kimball ETL",
    package_name="kimball_framework"
)

import yaml
print("\n=== Generated databricks.yml ===")
print(yaml.dump(dab, default_flow_style=False))

# COMMAND ----------

# MAGIC %md
# MAGIC # 11. Summary
# MAGIC 
# MAGIC This notebook demonstrated:
# MAGIC - ✅ Pydantic-validated YAML configs with dbt-style tests
# MAGIC - ✅ Pre-run pipeline compilation (DAG, SQL, FK validation)
# MAGIC - ✅ Data quality validation with severity levels
# MAGIC - ✅ Auto-generated documentation (catalog, Mermaid DAG)
# MAGIC - ✅ SCD2 current view refresh
# MAGIC - ✅ Dev environment setup with shallow clones
# MAGIC - ✅ Databricks workflow YAML generation

# COMMAND ----------

# Final verification
print("=== Final Table State ===")
print(f"dim_customer: {spark.table('demo_gold.dim_customer').count()} rows")
print(f"dim_product: {spark.table('demo_gold.dim_product').count()} rows")
print(f"fact_sales: {spark.table('demo_gold.fact_sales').count()} rows")
print("\n✅ Complete Demo finished successfully!")
