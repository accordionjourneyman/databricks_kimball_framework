"""
ETL Control Manager - Kimball-style batch auditing and watermark management.

This module provides:
- Watermark tracking for incremental ETL (last_processed_version)
- Batch lifecycle tracking (start, complete, fail)
- Operational metrics (rows_read, rows_written, duration)
- Partitioned for concurrent writes from parallel pipelines

Environment Variables:
    KIMBALL_ETL_SCHEMA: Default schema for the ETL control table.
                        Set this once at the start of your notebook to avoid
                        passing etl_schema to every Orchestrator/PipelineExecutor call.
                        Example: os.environ["KIMBALL_ETL_SCHEMA"] = "my_catalog.etl_config"
"""

from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.functions import current_timestamp, col, lit
from delta.tables import DeltaTable
from typing import Optional
from databricks.sdk.runtime import spark
import uuid
import os
import warnings


# Environment variable for default ETL schema
KIMBALL_ETL_SCHEMA_ENV = "KIMBALL_ETL_SCHEMA"


def get_etl_schema() -> Optional[str]:
    """Get the ETL schema from environment variable."""
    return os.environ.get(KIMBALL_ETL_SCHEMA_ENV)


class ETLControlManager:
    """
    Manages ETL control records including watermarks and batch auditing.
    
    This is a Kimball-style ETL control table that tracks:
    - Watermarks: Last processed version for each (target, source) pair
    - Batch lifecycle: When batches start, complete, or fail
    - Metrics: Rows read/written, duration
    
    The table is partitioned by (target_table, source_table) to enable
    zero-contention concurrent writes from parallel pipelines.
    
    Configuration:
        Set the KIMBALL_ETL_SCHEMA environment variable to configure the default schema:
        
            import os
            os.environ["KIMBALL_ETL_SCHEMA"] = "my_catalog.etl_config"
        
        This avoids passing etl_schema to every Orchestrator/PipelineExecutor call.
    
    Usage:
        # Option 1: Use environment variable (recommended)
        import os
        os.environ["KIMBALL_ETL_SCHEMA"] = "gold"
        etl = ETLControlManager()  # Uses KIMBALL_ETL_SCHEMA
        
        # Option 2: Explicit schema
        etl = ETLControlManager(etl_schema="gold")
        
        # Start a batch
        batch_id = etl.batch_start("gold.dim_customer", "silver.customers")
        
        try:
            # ... ETL logic ...
            etl.batch_complete("gold.dim_customer", "silver.customers", 
                              new_version=42, rows_read=1000, rows_written=50)
        except Exception as e:
            etl.batch_fail("gold.dim_customer", "silver.customers", str(e))
            raise
    """

    DEFAULT_TABLE_NAME = "etl_control"

    def __init__(
        self,
        etl_schema: str = None,
        table_name: str = DEFAULT_TABLE_NAME,
        # Deprecated parameter - for backward compatibility
        database: str = None,
    ):
        """
        Args:
            etl_schema: Schema where the control table will be created
                        (e.g. 'gold', 'my_catalog.etl_config'). 
                        If not provided, uses KIMBALL_ETL_SCHEMA environment variable.
            table_name: Name of the control table (default 'etl_control').
                        If a fully-qualified name (db.table) is provided, `etl_schema`
                        is ignored.
            database: **Deprecated** - Use etl_schema instead.
        """
        # Handle deprecated 'database' parameter
        if database is not None:
            warnings.warn(
                "The 'database' parameter is deprecated. Use 'etl_schema' instead.",
                DeprecationWarning,
                stacklevel=2
            )
            if etl_schema is None:
                etl_schema = database
        
        # Resolve schema: explicit param > env var > error
        if etl_schema is None:
            etl_schema = get_etl_schema()
        
        # Resolve fully-qualified table name
        if "." in table_name:
            self.fq_table = table_name
            self.schema = table_name.split(".")[0]
        else:
            if not etl_schema:
                raise ValueError(
                    "ETLControlManager requires either:\n"
                    "  1. Set KIMBALL_ETL_SCHEMA environment variable, or\n"
                    "  2. Pass etl_schema parameter explicitly\n"
                    "Example: os.environ['KIMBALL_ETL_SCHEMA'] = 'gold'"
                )
            self.schema = etl_schema
            self.fq_table = f"{self.schema}.{table_name}"
        
        # Backward compatibility alias
        self.database = self.schema

        self._ensure_table_exists()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_table_exists(self):
        """Create schema and ETL control table if they don't exist.
        
        The table is partitioned by (target_table, source_table) to enable
        zero-contention concurrent writes from parallel pipelines.
        """
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.schema}")

        if not spark.catalog.tableExists(self.fq_table):
            spark.sql(f"""
                CREATE TABLE {self.fq_table} (
                    -- Watermark columns
                    target_table STRING NOT NULL,
                    source_table STRING NOT NULL,
                    last_processed_version LONG,
                    
                    -- Batch audit columns (Kimball-style)
                    batch_id STRING,
                    batch_started_at TIMESTAMP,
                    batch_completed_at TIMESTAMP,
                    batch_status STRING,
                    rows_read LONG,
                    rows_written LONG,
                    error_message STRING,
                    
                    updated_at TIMESTAMP NOT NULL
                )
                USING DELTA
                PARTITIONED BY (target_table, source_table)
                COMMENT 'Kimball ETL Control Table: watermarks + batch auditing. Partitioned for concurrent writes.'
            """)

    # ------------------------------------------------------------------
    # Watermark API (backward compatible)
    # ------------------------------------------------------------------

    def get_watermark(self, target_table: str, source_table: str) -> Optional[int]:
        """Retrieve the last processed version for a (target, source) pair.
        Returns None if no watermark exists (first run).
        """
        df = spark.table(self.fq_table)
        result = df.filter(
            (col("target_table") == target_table) &
            (col("source_table") == source_table)
        ).select("last_processed_version").first()

        if result:
            return result["last_processed_version"]
        return None

    def update_watermark(self, target_table: str, source_table: str, version: int):
        """Insert or update the watermark row atomically via Delta MERGE.
        
        Note: Prefer using batch_complete() which updates both watermark and metrics.
        This method is kept for backward compatibility.
        """
        self._upsert_control_record(
            target_table=target_table,
            source_table=source_table,
            updates={
                "last_processed_version": version,
                "batch_status": "SUCCESS",
                "batch_completed_at": datetime.now(),
            }
        )

    # ------------------------------------------------------------------
    # Batch Lifecycle API
    # ------------------------------------------------------------------

    def batch_start(self, target_table: str, source_table: str) -> str:
        """Mark the start of a batch for a (target, source) pair.
        
        Returns:
            batch_id: UUID for this batch run (can be used for correlation)
        """
        batch_id = str(uuid.uuid4())
        
        self._upsert_control_record(
            target_table=target_table,
            source_table=source_table,
            updates={
                "batch_id": batch_id,
                "batch_started_at": datetime.now(),
                "batch_completed_at": None,
                "batch_status": "RUNNING",
                "rows_read": None,
                "rows_written": None,
                "error_message": None,
            }
        )
        
        return batch_id

    def batch_complete(
        self, 
        target_table: str, 
        source_table: str, 
        new_version: int,
        rows_read: int = None,
        rows_written: int = None
    ):
        """Mark a batch as successfully completed and update watermark.
        
        Args:
            target_table: Target table name
            source_table: Source table name  
            new_version: New watermark version to record
            rows_read: Number of rows read from source (optional)
            rows_written: Number of rows written to target (optional)
        """
        self._upsert_control_record(
            target_table=target_table,
            source_table=source_table,
            updates={
                "last_processed_version": new_version,
                "batch_completed_at": datetime.now(),
                "batch_status": "SUCCESS",
                "rows_read": rows_read,
                "rows_written": rows_written,
                "error_message": None,
            }
        )

    def batch_fail(
        self, 
        target_table: str, 
        source_table: str, 
        error_message: str
    ):
        """Mark a batch as failed.
        
        Note: Does NOT update the watermark, so the batch can be retried.
        
        Args:
            target_table: Target table name
            source_table: Source table name
            error_message: Error details for debugging
        """
        # Truncate error message if too long
        if error_message and len(error_message) > 4000:
            error_message = error_message[:4000] + "... (truncated)"
            
        self._upsert_control_record(
            target_table=target_table,
            source_table=source_table,
            updates={
                # Don't update last_processed_version on failure!
                "batch_completed_at": datetime.now(),
                "batch_status": "FAILED",
                "error_message": error_message,
            }
        )

    def get_batch_status(self, target_table: str, source_table: str) -> Optional[dict]:
        """Get the current batch status for a (target, source) pair.
        
        Returns:
            dict with batch_id, batch_status, batch_started_at, etc.
            None if no record exists.
        """
        df = spark.table(self.fq_table)
        result = df.filter(
            (col("target_table") == target_table) &
            (col("source_table") == source_table)
        ).first()

        if result:
            return {
                "batch_id": result["batch_id"],
                "batch_status": result["batch_status"],
                "batch_started_at": result["batch_started_at"],
                "batch_completed_at": result["batch_completed_at"],
                "last_processed_version": result["last_processed_version"],
                "rows_read": result["rows_read"],
                "rows_written": result["rows_written"],
                "error_message": result["error_message"],
            }
        return None

    # ------------------------------------------------------------------
    # Internal MERGE helper
    # ------------------------------------------------------------------

    # Schema for the update DataFrame - must match etl_control table schema
    _UPDATE_SCHEMA = StructType([
        StructField("target_table", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("last_processed_version", LongType(), True),
        StructField("batch_id", StringType(), True),
        StructField("batch_started_at", TimestampType(), True),
        StructField("batch_completed_at", TimestampType(), True),
        StructField("batch_status", StringType(), True),
        StructField("rows_read", LongType(), True),
        StructField("rows_written", LongType(), True),
        StructField("error_message", StringType(), True),
        StructField("updated_at", TimestampType(), False),
    ])

    def _upsert_control_record(
        self, 
        target_table: str, 
        source_table: str, 
        updates: dict
    ):
        """Upsert a control record with the given updates."""
        delta_table = DeltaTable.forName(spark, self.fq_table)
        
        # Build the full record for insert
        full_record = {
            "target_table": target_table,
            "source_table": source_table,
            "last_processed_version": updates.get("last_processed_version"),
            "batch_id": updates.get("batch_id"),
            "batch_started_at": updates.get("batch_started_at"),
            "batch_completed_at": updates.get("batch_completed_at"),
            "batch_status": updates.get("batch_status"),
            "rows_read": updates.get("rows_read"),
            "rows_written": updates.get("rows_written"),
            "error_message": updates.get("error_message"),
            "updated_at": datetime.now(),
        }
        
        # Create DataFrame with explicit schema to avoid type inference issues
        # (Spark Connect fails when all values are None)
        update_df = spark.createDataFrame([full_record], schema=self._UPDATE_SCHEMA)
        
        # Build update set - only update fields that are in the updates dict
        update_set = {"updated_at": "current_timestamp()"}
        for key in updates:
            update_set[key] = f"u.{key}"
        
        # Build insert values
        insert_values = {
            "target_table": "u.target_table",
            "source_table": "u.source_table",
            "updated_at": "current_timestamp()",
        }
        for key in full_record:
            if key not in ("target_table", "source_table", "updated_at"):
                insert_values[key] = f"u.{key}"

        (delta_table.alias("w")
         .merge(
             update_df.alias("u"),
             "w.target_table = u.target_table AND w.source_table = u.source_table"
         )
         .whenMatchedUpdate(set=update_set)
         .whenNotMatchedInsert(values=insert_values)
         .execute())
