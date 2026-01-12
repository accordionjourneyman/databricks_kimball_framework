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

from __future__ import annotations

import os
import uuid
import warnings
from datetime import datetime
from typing import TYPE_CHECKING, Any, TypedDict, cast

from delta.tables import DeltaTable
from pyspark.sql import Column, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if TYPE_CHECKING:
    pass

# Environment variable for default ETL schema
KIMBALL_ETL_SCHEMA_ENV = "KIMBALL_ETL_SCHEMA"


def get_etl_schema() -> str | None:
    """Get the ETL schema from environment variable."""
    return os.environ.get(KIMBALL_ETL_SCHEMA_ENV)


class ETLControlRecord(TypedDict, total=False):
    """Schema for ETL Control table records."""

    target_table: str
    source_table: str
    last_processed_version: int | None
    batch_id: str | None
    batch_started_at: datetime | None
    batch_completed_at: datetime | None
    batch_status: str | None
    rows_read: int | None
    rows_written: int | None
    error_message: str | None
    updated_at: datetime


class ETLControlManager:
    """
    Manages ETL control records including watermarks and batch auditing.

    This is a Kimball-style ETL control table that tracks:
    - Watermarks: Last processed version for each (target, source) pair
    - Batch lifecycle: When batches start, complete, or fail
    - Metrics: Rows read/written, duration

    The table is partitioned by (target_table) to enable
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

        # Option 3: With injected SparkSession (for testing)
        etl = ETLControlManager(etl_schema="gold", spark_session=mock_spark)

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
        etl_schema: str | None = None,
        table_name: str = DEFAULT_TABLE_NAME,
        # Deprecated parameter - for backward compatibility
        database: str | None = None,
        # DIP: allow SparkSession injection for testability
        spark_session: SparkSession | None = None,
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
            spark_session: Optional SparkSession for dependency injection.
                          If not provided, uses global spark from databricks.sdk.runtime.
        """
        self._spark = spark_session

        # Handle deprecated 'database' parameter
        if database is not None:
            warnings.warn(
                "The 'database' parameter is deprecated. Use 'etl_schema' instead.",
                DeprecationWarning,
                stacklevel=2,
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

    @property
    def spark(self) -> SparkSession:
        """Lazy-load SparkSession from Databricks runtime if not injected."""
        if self._spark is None:
            from databricks.sdk.runtime import spark

            self._spark = spark
        return self._spark

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_table_exists(self) -> None:
        """Create schema and ETL control table if they don't exist.

        The table is partitioned by (target_table, source_table) to enable
        zero-contention concurrent writes from parallel pipelines.
        """
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.schema}")

        if not self.spark.catalog.tableExists(self.fq_table):
            self.spark.sql(f"""
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
                COMMENT 'Kimball ETL Control Table. Partitioned by (target_table, source_table) for concurrent pipeline isolation.'
            """)

    # ------------------------------------------------------------------
    # Watermark API (backward compatible)
    # ------------------------------------------------------------------

    def get_watermark(self, target_table: str, source_table: str) -> int | None:
        """Retrieve the last processed version for a (target, source) pair.
        Returns None if no watermark exists (first run).
        """
        df = self.spark.table(self.fq_table)
        result = (
            df.filter(
                (col("target_table") == target_table)
                & (col("source_table") == source_table)
            )
            .select("last_processed_version")
            .first()
        )

        if result:
            version = result["last_processed_version"]
            return int(version) if version is not None else None
        return None

    def update_watermark(
        self, target_table: str, source_table: str, version: int
    ) -> None:
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
            },
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
            },
        )

        return batch_id

    def batch_start_all(
        self, target_table: str, source_tables: list[str]
    ) -> dict[str, str]:
        """
        Mark the start of a batch for multiple sources in a single transaction.

        This is a performance optimization over calling batch_start() in a loop,
        reducing Spark overhead from O(N) to O(1).

        Args:
            target_table: The target table name.
            source_tables: List of source table names.

        Returns:
            Dictionary mapping source_table_name -> batch_id.
        """
        batch_ids = {}
        updates_list = []

        timestamp = datetime.now()

        for source in source_tables:
            bid = str(uuid.uuid4())
            batch_ids[source] = bid
            updates_list.append(
                {
                    "target_table": target_table,
                    "source_table": source,
                    "batch_id": bid,
                    "batch_started_at": timestamp,
                    "batch_completed_at": None,
                    "batch_status": "RUNNING",
                    "rows_read": None,
                    "rows_written": None,
                    "error_message": None,
                }
            )

        if updates_list:
            self._upsert_control_records(cast(list[ETLControlRecord], updates_list))

        return batch_ids

    def batch_complete(
        self,
        target_table: str,
        source_table: str,
        new_version: int,
        rows_read: int | None = None,
        rows_written: int | None = None,
    ) -> None:
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
            },
        )

    def batch_fail(
        self, target_table: str, source_table: str, error_message: str
    ) -> None:
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
            },
        )

    def get_batch_status(
        self, target_table: str, source_table: str
    ) -> dict[str, Any] | None:
        """Get the current batch status for a (target, source) pair.

        Returns:
            dict with batch_id, batch_status, batch_started_at, etc.
            None if no record exists.
        """
        df = self.spark.table(self.fq_table)
        result = df.filter(
            (col("target_table") == target_table)
            & (col("source_table") == source_table)
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

    def get_running_batches(self, target_table: str) -> list[str]:
        """Get list of batch_ids that are currently marked as RUNNING for this target.

        Used by Orchestrator for zombie recovery (identifying crashed batches).

        Args:
            target_table: The target table name to check.

        Returns:
            List of batch_id strings that are still in 'RUNNING' state.
        """
        try:
            df = self.spark.table(self.fq_table)
            rows = (
                df.filter(
                    (col("target_table") == target_table)
                    & (col("batch_status") == "RUNNING")
                )
                .select("batch_id")
                .collect()
            )
            return [row["batch_id"] for row in rows if row["batch_id"]]
        except Exception:
            # If table doesn't exist or other error, return empty list to safely skip recovery
            return []

    # ------------------------------------------------------------------
    # Internal MERGE helper
    # ------------------------------------------------------------------

    # Schema for the update DataFrame - must match etl_control table schema
    _UPDATE_SCHEMA = StructType(
        [
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
        ]
    )

    def _upsert_control_record(
        self, target_table: str, source_table: str, updates: ETLControlRecord
    ) -> None:
        """Wrapper for single record upsert."""
        record = updates.copy()
        record["target_table"] = target_table
        record["source_table"] = source_table
        # Ensure updated_at is set if not present
        if "updated_at" not in record:
            record["updated_at"] = datetime.now()

        self._upsert_control_records([record])

    def _upsert_control_records(self, records: list[ETLControlRecord]) -> None:
        """
        Upsert multiple control records in a single transaction.
        Handles schema validation and dynamic update set generation.
        """
        if not records:
            return

        delta_table = DeltaTable.forName(self.spark, self.fq_table)

        # 1. Normalize records to match schema
        # We need to ensure all fields in _UPDATE_SCHEMA are present (or None)
        # to create the DataFrame, while preserving the intent of which columns to update.
        normalized_records = []
        for r in records:
            norm = {
                field.name: r.get(field.name) for field in self._UPDATE_SCHEMA.fields
            }
            # Ensure mandatory keys are present
            norm["target_table"] = r["target_table"]
            norm["source_table"] = r["source_table"]  # Ensure updated_at
            if not norm.get("updated_at"):
                norm["updated_at"] = datetime.now()
            normalized_records.append(norm)

        # 2. Create DataFrame
        update_df = self.spark.createDataFrame(
            normalized_records, schema=self._UPDATE_SCHEMA
        )

        # 3. Determine Update Set
        # We update columns that are present in the INPUT records keys (from the first record).
        # This assumes all records in the batch update the same set of columns.
        # This is true for batch_start_all (all set start time/status).
        sample_keys = records[0].keys()

        update_set = {"updated_at": "u.updated_at"}
        for key in sample_keys:
            if key not in ("target_table", "source_table", "updated_at"):
                # Only update if the key corresponds to a valid column
                if key in update_df.columns:
                    update_set[key] = f"u.{key}"

        # 4. Determine Insert Values
        insert_values = {
            "target_table": "u.target_table",
            "source_table": "u.source_table",
            "updated_at": "u.updated_at",
        }
        # For Insert, we use all available columns
        for field in self._UPDATE_SCHEMA.fields:
            name = field.name
            if name not in insert_values:
                insert_values[name] = f"u.{name}"

        # 5. Execute Merge
        (
            delta_table.alias("w")
            .merge(
                update_df.alias("u"),
                "w.target_table = u.target_table AND w.source_table = u.source_table",
            )
            .whenMatchedUpdate(set=cast(dict[str, str | Column], update_set))
            .whenNotMatchedInsert(values=cast(dict[str, str | Column], insert_values))
            .execute()
        )
