"""
Resilience features for Kimball ETL pipelines.

This module provides optional features for production-ready pipelines:
- QueryMetricsCollector: Basic query execution metrics
- StagingCleanupManager: Cleanup of orphaned staging tables
- StagingTableManager: Context manager for staging tables
- PipelineCheckpoint: Checkpointing for complex DAG pipelines

Features are OFF by default (lite mode). Enable via:
- KIMBALL_MODE=full (enables all features)
- KIMBALL_ENABLE_<FEATURE>=1 (enables specific feature)

Available features: checkpoints, staging_cleanup, metrics, auto_cluster
"""

from __future__ import annotations

import json
import os
import time
import traceback
from types import TracebackType
from typing import Any, cast

from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, desc
from pyspark.sql.types import StringType, StructField, StructType, TimestampType


def _feature_enabled(feature: str) -> bool:
    """Check if a feature is enabled via environment variable.

    Args:
        feature: Feature name (checkpoints, staging_cleanup, metrics, auto_cluster).

    Returns:
        True if feature is enabled, False otherwise.

    Environment Variables:
        KIMBALL_MODE: Set to "full" to enable all features.
        KIMBALL_ENABLE_<FEATURE>: Set to "1" to enable specific feature.
    """
    # Full mode enables all features
    if os.environ.get("KIMBALL_MODE", "").lower() == "full":
        return True
    # Otherwise check specific feature flag
    return os.environ.get(f"KIMBALL_ENABLE_{feature.upper()}") == "1"


class QueryMetricsCollector:
    """
    Collects basic query execution metrics for observability.
    Uses DataFrame operations and timing instead of Spark listeners.
    """

    def __init__(self) -> None:
        self.metrics: list[dict[str, Any]] = []
        self.start_time: float | None = None

    def start_collection(self) -> None:
        """Start collecting query execution metrics."""
        self.start_time = time.time()
        self.metrics = []

    def add_operation_metric(
        self, operation_name: str, df: DataFrame | None = None, **kwargs: Any
    ) -> None:
        """Add a metric for a specific operation.

        Args:
            operation_name: Name of the operation being measured.
            df: Optional DataFrame to extract metadata from.
            **kwargs: Additional metrics to record. Common keys:
                - duration_ms (float): Operation duration in milliseconds.
        """
        try:
            metric: dict[str, Any] = {
                "operation": operation_name,
                "timestamp": time.time(),
                "duration_ms": kwargs.get("duration_ms", 0),
            }

            # Add DataFrame metrics if available
            if df is not None and isinstance(df, DataFrame):
                try:
                    # Try to get some basic metrics from the logical plan
                    # Note: accessing _jdf or plan might still be internal,
                    # but checking type existence is safer.
                    # Ideally we would check df.explain(mode="cost") but that prints to stdout.
                    # For now just flag that we have a valid DF.
                    metric["has_logical_plan"] = True
                except Exception:
                    metric["has_logical_plan"] = False

            # Add any additional metrics passed
            metric.update(kwargs)
            self.metrics.append(metric)

        except Exception as e:
            print(
                f"Failed to collect metric for {operation_name}: {e}\n{traceback.format_exc()}"
            )

    def stop_collection(self) -> list[dict[str, Any]]:
        """Stop collecting metrics and return collected data."""
        if self.start_time:
            total_duration = time.time() - self.start_time
            self.add_operation_metric(
                "total_pipeline", duration_ms=total_duration * 1000
            )
        return self.metrics

    def get_summary(self) -> dict[str, Any]:
        """Get summary of collected metrics."""
        if not self.metrics:
            return {}

        # Single-pass iteration for efficiency
        total_time = 0
        operation_count = 0
        for m in self.metrics:
            total_time += m.get("duration_ms", 0)
            if m.get("operation") != "total_pipeline":
                operation_count += 1

        return {
            "total_operations": operation_count,
            "total_execution_time_ms": total_time,
            "operations": self.metrics,
            "avg_operation_time_ms": total_time / operation_count
            if operation_count > 0
            else 0,
        }


class StagingCleanupManager:
    """
    Manages cleanup of staging tables with crash resilience using Delta table registry.
    Provides ACID-compliant registry to prevent race conditions in multi-pipeline environments.
    """

    def __init__(self, registry_table: str | None = None) -> None:
        # Use Delta table for registry instead of JSON file to prevent race conditions
        if registry_table is None:
            registry_table = os.getenv(
                "KIMBALL_CLEANUP_REGISTRY_TABLE", "default.kimball_staging_registry"
            )

        self.registry_table = registry_table
        self._ensure_registry_table()

    def _ensure_registry_table(self) -> None:
        """Ensure the registry Delta table exists."""
        if not spark.catalog.tableExists(self.registry_table):
            schema = StructType(
                [
                    StructField("pipeline_id", StringType(), True),
                    StructField("staging_table", StringType(), True),
                    StructField("created_at", TimestampType(), True),
                    StructField("batch_id", StringType(), True),
                ]
            )
            empty_df = spark.createDataFrame([], schema)
            empty_df.write.format("delta").saveAsTable(self.registry_table)
            print(f"Created staging cleanup registry table: {self.registry_table}")

    def register_staging_table(
        self,
        staging_table: str,
        pipeline_id: str | None = None,
        batch_id: str | None = None,
    ) -> None:
        """Register a staging table for cleanup using Delta table."""
        from delta.tables import DeltaTable

        # Create DataFrame for the new entry
        new_entry = spark.createDataFrame(
            [(pipeline_id or "unknown", staging_table, batch_id or "unknown")],
            ["pipeline_id", "staging_table", "batch_id"],
        ).withColumn("created_at", current_timestamp())

        # Use DeltaTable API for atomic registration
        registry_table = DeltaTable.forName(spark, self.registry_table)
        registry_table.alias("target").merge(
            new_entry.alias("source"), "target.staging_table = source.staging_table"
        ).whenNotMatchedInsertAll().execute()

        print(f"Registered staging table for cleanup: {staging_table}")

    def unregister_staging_table(self, staging_table: str) -> None:
        """Unregister a staging table after successful cleanup."""
        from delta.tables import DeltaTable

        registry_table = DeltaTable.forName(spark, self.registry_table)
        registry_table.delete(col("staging_table") == staging_table)
        print(f"Unregistered staging table from cleanup: {staging_table}")

    def cleanup_staging_tables(
        self,
        spark_session: SparkSession | None = None,
        pipeline_id: str | None = None,
        max_age_hours: int = 24,
    ) -> tuple[int, int]:
        """Clean up orphaned staging tables using DataFrame API.

        Uses DataFrame API for filtering (avoiding SQL injection) and
        driver-side cleanup (avoiding DeltaTable serialization issues).

        Args:
            spark_session: Spark session to use. Defaults to global spark.
            pipeline_id: Optional filter for specific pipeline's tables.
            max_age_hours: Remove tables older than this. Default 24h. Use 0 to disable.

        Returns:
            Tuple of (cleaned_count, failed_count).
        """
        if spark_session is None:
            spark_session = spark

        from pyspark.sql.functions import expr

        # Use DataFrame API instead of SQL string building (fixes SQL injection)
        registry_df = spark_session.table(self.registry_table)  # type: ignore

        # Apply age filter using DataFrame API
        if max_age_hours > 0:
            threshold = current_timestamp() - expr(f"INTERVAL {max_age_hours} HOURS")
            registry_df = registry_df.filter(col("created_at") < threshold)

        # Apply pipeline filter using DataFrame API (safe from injection)
        if pipeline_id:
            registry_df = registry_df.filter(col("pipeline_id") == pipeline_id)

        # Single Spark action: collect with limit+1 to detect overflow
        MAX_CLEANUP_BATCH = 1000
        rows = registry_df.limit(MAX_CLEANUP_BATCH + 1).collect()
        if len(rows) > MAX_CLEANUP_BATCH:
            print(f"Warning: Registry exceeds {MAX_CLEANUP_BATCH} entries, truncating")
            rows = rows[:MAX_CLEANUP_BATCH]
        tables_to_cleanup = [row.staging_table for row in rows]

        # Cleanup on driver side (single pass, no double evaluation)
        cleaned, failed = 0, 0
        for staging_table_name in tables_to_cleanup:
            try:
                spark_session.sql(f"DROP TABLE IF EXISTS {staging_table_name}")  # type: ignore
                self.unregister_staging_table(staging_table_name)
                cleaned += 1
                print(f"Cleaned up orphaned staging table: {staging_table_name}")
            except Exception as e:
                print(f"Failed to cleanup {staging_table_name}: {e}")
                failed += 1

        print(f"Staging cleanup completed: {cleaned} cleaned, {failed} failed")
        return cleaned, failed


class StagingTableManager:
    """
    Context manager for staging tables that ensures cleanup regardless of execution outcome.
    Prevents workspace pollution from failed ETL operations.
    """

    def __init__(self, cleanup_manager: StagingCleanupManager) -> None:
        self.cleanup_manager = cleanup_manager
        self.staging_tables: list[str] = []

    def register_staging_table(
        self,
        staging_table: str,
        pipeline_id: str | None = None,
        batch_id: str | None = None,
    ) -> None:
        """Register a staging table for management and cleanup."""
        self.staging_tables.append(staging_table)
        self.cleanup_manager.register_staging_table(
            staging_table, pipeline_id, batch_id
        )

    def __enter__(self) -> StagingTableManager:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        """Ensure staging tables are cleaned up even if an exception occurs."""
        for staging_table in self.staging_tables:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {staging_table}")
                self.cleanup_manager.unregister_staging_table(staging_table)
                print(
                    f"Cleaned up staging table during exception recovery: {staging_table}"
                )
            except Exception as e:
                print(f"Warning: Failed to clean up staging table {staging_table}: {e}")


class PipelineCheckpoint:
    """
    Handles checkpointing for complex DAG pipelines using Delta table for ACID compliance.
    Saves and restores pipeline state to enable resumability with atomic guarantees.
    """

    def __init__(self, checkpoint_table: str | None = None) -> None:
        # Use Delta table for atomic checkpoint storage
        if checkpoint_table is None:
            checkpoint_table = os.getenv(
                "KIMBALL_CHECKPOINT_TABLE", "default.kimball_pipeline_checkpoints"
            )

        self.checkpoint_table = checkpoint_table
        self._ensure_checkpoint_table()

    def _ensure_checkpoint_table(self) -> None:
        """Ensure the checkpoint Delta table exists."""
        if not spark.catalog.tableExists(self.checkpoint_table):
            schema = StructType(
                [
                    StructField("pipeline_id", StringType(), True),
                    StructField("stage", StringType(), True),
                    StructField("timestamp", TimestampType(), True),
                    StructField("state", StringType(), True),
                ]
            )
            empty_df = spark.createDataFrame([], schema)
            empty_df.write.format("delta").partitionBy("pipeline_id").saveAsTable(
                self.checkpoint_table
            )
            print(f"Created pipeline checkpoint table: {self.checkpoint_table}")

    def save_checkpoint(
        self, pipeline_id: str, stage: str, state: dict[str, Any]
    ) -> None:
        """Save pipeline state at a specific stage using atomic Delta operations.

        If checkpoint exists for this pipeline_id/stage, it is atomically updated.
        Otherwise, a new checkpoint is created.

        Args:
            pipeline_id: Unique identifier for the pipeline.
            stage: Stage name within the pipeline.
            state: JSON-serializable dictionary of state data.
        """
        from delta.tables import DeltaTable

        state_json = json.dumps(state)

        # Create DataFrame for the checkpoint entry
        checkpoint_entry = spark.createDataFrame(
            [(pipeline_id, stage, state_json)], ["pipeline_id", "stage", "state"]
        ).withColumn("timestamp", current_timestamp())

        # Use DeltaTable API for atomic checkpoint updates
        checkpoint_table = DeltaTable.forName(spark, self.checkpoint_table)
        checkpoint_table.alias("target").merge(
            checkpoint_entry.alias("source"),
            "target.pipeline_id = source.pipeline_id AND target.stage = source.stage",
        ).whenMatchedUpdate(
            set={"timestamp": "source.timestamp", "state": "source.state"}
        ).whenNotMatchedInsertAll().execute()

        print(f"Checkpoint saved: {pipeline_id} -> {stage}")

    def load_checkpoint(self, pipeline_id: str, stage: str) -> dict[str, Any] | None:
        """Load pipeline state from Delta table checkpoint.

        Args:
            pipeline_id: Pipeline identifier.
            stage: Stage name.

        Returns:
            State dictionary if found, None if checkpoint doesn't exist or load fails.
        """
        checkpoint_df = spark.table(self.checkpoint_table)
        result_df = (
            checkpoint_df.filter(
                (col("pipeline_id") == pipeline_id) & (col("stage") == stage)
            )
            .orderBy(col("timestamp").desc())
            .limit(1)
        )

        try:
            row = result_df.first()  # Single Spark action (avoids isEmpty + first)
            if row is None:
                return None
            state_json = row["state"]
            state: dict[str, Any] = json.loads(state_json)
            print(f"Checkpoint loaded: {pipeline_id} -> {stage}")
            return state
        except Exception as e:
            print(f"Failed to load checkpoint {pipeline_id}:{stage}: {e}")
            return None

    def clear_checkpoint(self, pipeline_id: str, stage: str) -> None:
        """Clear a checkpoint from Delta table."""
        from delta.tables import DeltaTable

        checkpoint_table = DeltaTable.forName(spark, self.checkpoint_table)
        checkpoint_table.delete(
            (col("pipeline_id") == pipeline_id) & (col("stage") == stage)
        )

        print(f"Checkpoint cleared: {pipeline_id} -> {stage}")

    def list_checkpoints(self, pipeline_id: str | None = None) -> DataFrame:
        """List all checkpoints, optionally filtered by pipeline_id."""
        checkpoint_df = spark.table(self.checkpoint_table)
        if pipeline_id:
            checkpoint_df = checkpoint_df.filter(col("pipeline_id") == pipeline_id)
        return cast(
            DataFrame, checkpoint_df.orderBy("pipeline_id", "stage", desc("timestamp"))
        )
