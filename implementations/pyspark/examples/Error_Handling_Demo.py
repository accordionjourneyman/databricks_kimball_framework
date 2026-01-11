# Databricks notebook source
# Kimball Framework - Error Handling Demo
# Demonstrates how the framework handles and reports errors:
# - Configuration validation errors
# - Data quality validation failures
# - SQL transformation errors
# - Missing source tables
# - Retriable vs Non-retriable errors

# COMMAND ----------

# Install Framework
import subprocess
import os

_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
_pyspark_root = "/Workspace" + os.path.dirname(os.path.dirname(_nb_path))
subprocess.check_call(["pip", "install", _pyspark_root, "-q"])
print(f"✓ Installed kimball framework")

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Configuration Validation Errors
# MAGIC 
# MAGIC The framework validates configs at load time using JSON Schema and Pydantic.
# MAGIC Invalid configs fail fast with clear error messages.

# COMMAND ----------

from kimball.config import ConfigLoader

# Test 1: Dimension without required keys
invalid_dimension = """
table_name: gold.dim_test
table_type: dimension
sources:
  - name: silver.test
"""

import tempfile
with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
    f.write(invalid_dimension)
    temp_path = f.name

try:
    loader = ConfigLoader()
    config = loader.load_config(temp_path)
    print("❌ Should have raised an error!")
except ValueError as e:
    print("✅ Config validation error caught:")
    print(f"   {e}")

# COMMAND ----------

# Test 2: Invalid table_type
invalid_type = """
table_name: gold.test_table
table_type: invalid_type
sources:
  - name: silver.test
"""

with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
    f.write(invalid_type)
    temp_path = f.name

try:
    loader = ConfigLoader()
    config = loader.load_config(temp_path)
    print("❌ Should have raised an error!")
except ValueError as e:
    print("✅ Invalid table_type error caught:")
    print(f"   {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Pydantic Model Validation
# MAGIC 
# MAGIC Pydantic models provide even stricter validation with custom validators.

# COMMAND ----------

from kimball.models import DimensionConfig, FactConfig, SourceConfigModel
from pydantic import ValidationError

# Test 1: Empty natural_keys
try:
    config = DimensionConfig(
        table_name="gold.dim_test",
        surrogate_key="test_sk",
        natural_keys=[],  # Empty - should fail!
        sources=[SourceConfigModel(name="silver.test")]
    )
    print("❌ Should have raised an error!")
except ValidationError as e:
    print("✅ Pydantic validation error caught:")
    for error in e.errors():
        print(f"   - {error['loc']}: {error['msg']}")

# COMMAND ----------

# Test 2: Invalid test name
from kimball.models import ColumnTestModel

try:
    test = ColumnTestModel(
        column="test_col",
        tests=["unique", "invalid_test_name"]  # Invalid test name
    )
    print("❌ Should have raised an error!")
except ValidationError as e:
    print("✅ Invalid test name caught:")
    for error in e.errors():
        print(f"   - {error['msg']}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Pipeline Compilation Errors
# MAGIC 
# MAGIC The PipelineCompiler catches errors BEFORE data processing begins.

# COMMAND ----------

os.environ["KIMBALL_ETL_SCHEMA"] = "demo_gold"
spark.sql("CREATE DATABASE IF NOT EXISTS demo_gold")

from kimball import PipelineCompiler

compiler = PipelineCompiler(spark)

# Test 1: Missing source table
missing_source_config = """
table_name: demo_gold.test_table
table_type: fact
merge_keys: [id]
sources:
  - name: nonexistent_schema.nonexistent_table
transformation_sql: |
  SELECT * FROM nonexistent_table
"""

with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
    f.write(missing_source_config)
    config_dir = os.path.dirname(f.name)


# Clean up any existing configs in temp dir and write our test config
import glob
for old_file in glob.glob(os.path.join(config_dir, "*.yml")):
    os.remove(old_file)
    
with open(os.path.join(config_dir, "test_config.yml"), 'w') as f:
    f.write(missing_source_config)

result = compiler.compile_all(config_dir)

print(f"Compilation Success: {result.success}")
print(f"Errors: {result.errors}")
print(f"Warnings: {result.warnings}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Data Quality Validation Errors
# MAGIC 
# MAGIC DataQualityValidator returns structured results, not exceptions.
# MAGIC Use severity levels to control what blocks the pipeline.

# COMMAND ----------

from kimball import DataQualityValidator, ValidationReport, TestSeverity

# Create test data with quality issues
test_data = [
    (1, "Alice", "alice@example.com"),
    (2, "Bob", None),  # NULL email
    (1, "Alice Duplicate", "alice2@example.com"),  # Duplicate ID
    (3, "Charlie", "charlie@example.com"),
]

df = spark.createDataFrame(test_data, ["id", "name", "email"])
df.createOrReplaceTempView("test_quality")

validator = DataQualityValidator(spark)

# Run validation tests
results = []
results.append(validator.validate_unique(df, ["id"]))  # Will fail
results.append(validator.validate_not_null(df, ["email"]))  # Will fail
results.append(validator.validate_not_null(df, ["name"]))  # Will pass

report = ValidationReport(results=results)

print("=== Validation Report ===")
print(report)

# Check each result
for r in report.results:
    status = "✅ PASS" if r.passed else "❌ FAIL"
    print(f"{status} {r.test_name}: {r.details}")

# COMMAND ----------

# Test raise_on_failure behavior
from kimball.errors import DataQualityError

try:
    report.raise_on_failure()
    print("No errors raised")
except DataQualityError as e:
    print(f"✅ DataQualityError raised: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Warning vs Error Severity
# MAGIC 
# MAGIC Use TestSeverity.WARN for non-blocking issues.

# COMMAND ----------

# Create report with mixed severities
from kimball.validation import TestResult

mixed_results = [
    TestResult(test_name="critical_test", passed=False, severity=TestSeverity.ERROR, failed_rows=5),
    TestResult(test_name="non_critical_test", passed=False, severity=TestSeverity.WARN, failed_rows=2),
    TestResult(test_name="passing_test", passed=True),
]

# Report with errors blocks
error_report = ValidationReport(results=mixed_results)
print(f"Report passed: {error_report.passed}")  # False - has errors
print(f"Error count: {error_report.error_count}")
print(f"Warning count: {error_report.warning_count}")

# Report with only warnings does NOT block
warn_only_results = [
    TestResult(test_name="warn1", passed=False, severity=TestSeverity.WARN, failed_rows=1),
    TestResult(test_name="pass1", passed=True),
]
warn_report = ValidationReport(results=warn_only_results)
print(f"\nWarn-only report passed: {warn_report.passed}")  # True - warnings don't block!

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. SQL Transformation Errors
# MAGIC 
# MAGIC Invalid SQL is caught at compile time with clear error messages.

# COMMAND ----------

bad_sql_config = """
table_name: demo_gold.test_sql
table_type: fact
merge_keys: [id]
sources:
  - name: demo_silver.test_source
transformation_sql: |
  SELCT * FORM test_source WHERE invalid_syntax
"""

# Write config
with open(os.path.join(config_dir, "bad_sql.yml"), 'w') as f:
    f.write(bad_sql_config)

# Create the source table first
spark.sql("CREATE DATABASE IF NOT EXISTS demo_silver")
spark.createDataFrame([(1, "test")], ["id", "val"]).write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed", "true").saveAsTable("demo_silver.test_source")

# Compile - should catch SQL error
result = compiler.compile_all(config_dir)

print(f"Compilation Success: {result.success}")
if not result.success:
    print("SQL Errors detected:")
    for error in result.errors:
        print(f"  - {error}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Error Classification (Retriable vs Non-Retriable)
# MAGIC 
# MAGIC The framework distinguishes between transient errors (retry) and permanent errors (fail fast).

# COMMAND ----------

from kimball.errors import (
    RetriableError,
    NonRetriableError,
    SourceTableBusyError,
    DeltaConcurrentModificationError,
    ConfigurationError,
    TransformationSQLError,
)

# Retriable errors - should be retried
print("=== Retriable Errors ===")
retriable_errors = [
    SourceTableBusyError("silver.orders", "Another job is writing"),
    DeltaConcurrentModificationError("gold.dim_customer", "Concurrent modification"),
]
for e in retriable_errors:
    print(f"  - {type(e).__name__}: can retry = {isinstance(e, RetriableError)}")

# Non-retriable errors - fail fast
print("\n=== Non-Retriable Errors ===")
non_retriable_errors = [
    ConfigurationError("Invalid YAML syntax"),
    TransformationSQLError("SELECT * FORM invalid_table"),
    DataQualityError("3 tests failed"),
]
for e in non_retriable_errors:
    print(f"  - {type(e).__name__}: should fail = {isinstance(e, NonRetriableError)}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. Summary: Error Handling Best Practices
# MAGIC 
# MAGIC 1. **Config Errors** → Fail at load time (before cluster resources used)
# MAGIC 2. **Compilation Errors** → Fail before data processing starts
# MAGIC 3. **Data Quality** → Use severity levels (ERROR blocks, WARN logs)
# MAGIC 4. **Transient Errors** → Implement retry logic (or use Databricks Jobs retry)
# MAGIC 5. **Permanent Errors** → Fail fast with clear error messages

# COMMAND ----------

# Cleanup
spark.sql("DROP DATABASE IF EXISTS demo_silver CASCADE")
spark.sql("DROP DATABASE IF EXISTS demo_gold CASCADE")
print("✓ Cleanup complete")
