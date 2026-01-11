# Databricks notebook source
# Kimball Framework - Recovery Demo
# Demonstrates how the framework handles and recovers from failures:
# - Batch lifecycle tracking (start, complete, fail)
# - Watermark-based resume after failures
# - Delta table rollback
# - Checkpoint-based recovery
# - Orphaned staging table cleanup

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

os.environ["KIMBALL_ETL_SCHEMA"] = "demo_gold"
os.environ["KIMBALL_MODE"] = "full"

from delta.tables import DeltaTable
from kimball import ETLControlManager, Orchestrator

# Setup
spark.sql("CREATE DATABASE IF NOT EXISTS demo_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS demo_gold")

# Clean previous run
for db in ["demo_silver", "demo_gold"]:
    for table in spark.sql(f"SHOW TABLES IN {db}").collect():
        spark.sql(f"DROP TABLE IF EXISTS {db}.{table.tableName}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Batch Lifecycle Tracking
# MAGIC 
# MAGIC The ETLControlManager tracks batch start, complete, and fail states.
# MAGIC This enables monitoring and recovery.

# COMMAND ----------

etl = ETLControlManager(etl_schema="demo_gold")

# Simulate batch lifecycle
target = "demo_gold.dim_test"
source = "demo_silver.source_test"

# Start batch
batch_id = etl.batch_start(target, source)
print(f"Started batch: {batch_id}")

# Check status
status = etl.get_batch_status(target, source)
print(f"Status: {status['batch_status']}")
print(f"Started at: {status['batch_started_at']}")

# COMMAND ----------

# Simulate successful completion
etl.batch_complete(
    target, source,
    new_version=42,
    rows_read=1000,
    rows_written=50
)

status = etl.get_batch_status(target, source)
print(f"Status: {status['batch_status']}")
print(f"Watermark: {status['last_processed_version']}")
print(f"Rows read: {status['rows_read']}")
print(f"Rows written: {status['rows_written']}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Failure Handling
# MAGIC 
# MAGIC When a batch fails, the watermark is NOT updated.
# MAGIC This allows the next run to retry from the same point.

# COMMAND ----------

# Start another batch
batch_id = etl.batch_start(target, source)
print(f"Started new batch: {batch_id}")

# Simulate failure
etl.batch_fail(target, source, "Simulated error: Database connection timeout")

status = etl.get_batch_status(target, source)
print(f"\nAfter failure:")
print(f"Status: {status['batch_status']}")
print(f"Watermark: {status['last_processed_version']}")  # Still 42!
print(f"Error: {status['error_message']}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Resume After Failure
# MAGIC 
# MAGIC Since the watermark wasn't updated on failure, the next run
# MAGIC automatically resumes from where it left off.

# COMMAND ----------

# Get watermark - will return 42 (last successful point)
watermark = etl.get_watermark(target, source)
print(f"Resume point (watermark): {watermark}")

# On next run, CDF will read from version 43 onwards
print(f"Next run will read from version: {watermark + 1}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Delta Table Rollback
# MAGIC 
# MAGIC Use Delta time travel to rollback to a known good state.

# COMMAND ----------

from kimball.dev_utils import restore_table_version, get_table_history

# Create a test table with multiple versions
test_data = [(1, "v1"), (2, "v1")]
df = spark.createDataFrame(test_data, ["id", "version"])
df.write.format("delta").mode("overwrite").saveAsTable("demo_gold.rollback_test")

# Update to v2
df2 = spark.createDataFrame([(1, "v2"), (2, "v2"), (3, "v2")], ["id", "version"])
DeltaTable.forName(spark, "demo_gold.rollback_test").alias("t").merge(
    df2.alias("s"), "t.id = s.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Update to v3
df3 = spark.createDataFrame([(1, "v3"), (2, "v3"), (3, "v3"), (4, "v3")], ["id", "version"])
DeltaTable.forName(spark, "demo_gold.rollback_test").alias("t").merge(
    df3.alias("s"), "t.id = s.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Show current state
print("Current state:")
display(spark.table("demo_gold.rollback_test"))

# Show history
print("\nTable history:")
display(get_table_history(spark, "demo_gold.rollback_test"))

# COMMAND ----------

# Rollback to version 1 (after first update)
restore_table_version(spark, "demo_gold.rollback_test", version=1)

print("After rollback to version 1:")
display(spark.table("demo_gold.rollback_test"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Coordinated Rollback (Target + Source Watermark)
# MAGIC 
# MAGIC The V3.0 spec supports coordinated rollback using target_version mapping.
# MAGIC This ensures source and target stay in sync.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Future Enhancement: Target Version Tracking
# MAGIC 
# MAGIC The watermark table will track `target_version` to enable queries like:
# MAGIC ```sql
# MAGIC SELECT source_version 
# MAGIC FROM etl_control 
# MAGIC WHERE target_table = 'dim_customer' 
# MAGIC AND target_version = 100
# MAGIC ```
# MAGIC 
# MAGIC This allows coordinated rollback:
# MAGIC 1. Restore target table to version 100
# MAGIC 2. Query the source version that produced version 100
# MAGIC 3. Next ETL run resumes from that source version + 1

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Orphaned Staging Table Cleanup
# MAGIC 
# MAGIC If a job crashes mid-merge, staging tables may be left behind.
# MAGIC The framework automatically cleans these up.

# COMMAND ----------

from kimball.orchestrator import StagingCleanupManager

# Create "orphaned" staging tables (simulating crash)
spark.createDataFrame([(1,)], ["dummy"]).write.format("delta").mode("overwrite").saveAsTable("demo_gold._staging_crash_test_1")
spark.createDataFrame([(2,)], ["dummy"]).write.format("delta").mode("overwrite").saveAsTable("demo_gold._staging_crash_test_2")

print("Created orphaned staging tables:")
print(spark.sql("SHOW TABLES IN demo_gold LIKE '_staging*'").collect())

# The cleanup runs automatically at start of Orchestrator.run()
# But we can also trigger it manually:
# cleanup_manager = StagingCleanupManager()
# cleanup_manager.cleanup_staging_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Checkpoint-Based Recovery
# MAGIC 
# MAGIC For complex DAGs, the framework saves checkpoints at key stages.
# MAGIC This enables resuming from a specific stage after failure.

# COMMAND ----------

from kimball.orchestrator import PipelineCheckpoint

checkpoint = PipelineCheckpoint()

# Save checkpoint at transformation stage
pipeline_id = "demo_pipeline_123"
checkpoint.save_checkpoint(
    pipeline_id=pipeline_id,
    stage="transformation_complete",
    state={
        "rows_transformed": 5000,
        "sources_loaded": ["silver.customers", "silver.orders"],
        "timestamp": "2025-01-10T12:00:00Z"
    }
)

# Load checkpoint to resume
saved_state = checkpoint.load_checkpoint(pipeline_id, "transformation_complete")
print(f"Loaded checkpoint: {saved_state}")

# Clear checkpoint after successful completion
checkpoint.clear_checkpoint(pipeline_id, "transformation_complete")
print("Checkpoint cleared after success")

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. Recovery Pattern: Try-Except with Batch Tracking

# COMMAND ----------

def run_with_recovery(target: str, source: str, etl: ETLControlManager):
    """Pattern for running ETL with proper recovery handling."""
    
    # Start batch
    batch_id = etl.batch_start(target, source)
    print(f"Started batch {batch_id}")
    
    try:
        # Get watermark
        watermark = etl.get_watermark(target, source) or 0
        print(f"Resuming from version {watermark}")
        
        # Simulate ETL work
        rows_read = 1000
        rows_written = 50
        new_version = watermark + 10
        
        # Simulate potential failure (uncomment to test)
        # raise Exception("Simulated failure!")
        
        # Mark complete on success
        etl.batch_complete(
            target, source,
            new_version=new_version,
            rows_read=rows_read,
            rows_written=rows_written
        )
        print(f"✅ Batch complete. New watermark: {new_version}")
        
    except Exception as e:
        # Mark failed - watermark NOT updated
        etl.batch_fail(target, source, str(e))
        print(f"❌ Batch failed: {e}")
        print("Watermark preserved for retry")
        raise  # Re-raise for Databricks Jobs retry

# Run the pattern
run_with_recovery("demo_gold.dim_recovery_test", "demo_silver.source_test", etl)

# COMMAND ----------

# MAGIC %md
# MAGIC # 9. Summary: Recovery Best Practices
# MAGIC 
# MAGIC 1. **Batch Tracking** → Use ETLControlManager for start/complete/fail
# MAGIC 2. **Watermarks** → Automatically tracks resume points
# MAGIC 3. **Fail-Safe** → Watermark only updated on SUCCESS
# MAGIC 4. **Delta Rollback** → Use RESTORE TABLE for coordinated rollback
# MAGIC 5. **Checkpoints** → Save state for complex DAG recovery
# MAGIC 6. **Cleanup** → Auto-cleanup orphaned staging tables
# MAGIC 7. **Retry** → Use Databricks Jobs retry for transient errors

# COMMAND ----------

# Cleanup
spark.sql("DROP DATABASE IF EXISTS demo_silver CASCADE")
spark.sql("DROP DATABASE IF EXISTS demo_gold CASCADE")
print("✓ Cleanup complete")
