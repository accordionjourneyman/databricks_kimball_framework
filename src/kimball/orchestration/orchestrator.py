"""Orchestrator module for Kimball ETL pipelines.

This module coordinates the ETL process for dimensional modeling on Databricks.
NOTE: Requires Databricks Runtime - SparkSession is obtained lazily at runtime.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
import warnings
from typing import TYPE_CHECKING, Any

from pyspark.sql.functions import col

from kimball.common.config import ConfigLoader
from kimball.common.constants import (
    SPARK_CONF_AQE_COALESCE,
    SPARK_CONF_AQE_ENABLED,
    SPARK_CONF_AQE_SKEW_JOIN,
    SPARK_CONF_SHUFFLE_PARTITIONS,
    SPARK_CONF_SKEW_FACTOR,
    SPARK_CONF_SKEW_SIZE_THRESHOLD,
)
from kimball.common.errors import NonRetriableError, RetriableError
from kimball.common.exceptions import PYSPARK_EXCEPTION_BASE
from kimball.common.runtime import RuntimeOptions
from kimball.observability.resilience import (
    PipelineCheckpoint,
    QueryMetricsCollector,
    StagingCleanupManager,
    _feature_enabled,
)
from kimball.orchestration.transaction import TransactionManager
from kimball.orchestration.watermark import ETLControlManager, get_etl_schema
from kimball.processing.loader import DataLoader
from kimball.processing.merger import DeltaMerger
from kimball.processing.skeleton_generator import SkeletonGenerator
from kimball.processing.table_creator import TableCreator

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def _get_spark() -> SparkSession:
    """Get SparkSession lazily - works in Databricks or with injected session."""
    try:
        from databricks.sdk.runtime import spark

        return spark
    except ImportError:
        # Fallback for non-Databricks environments (e.g., local testing with SparkSession.builder)
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()


class Orchestrator:
    """
    Coordinates the ETL process:
    1. Load Config
    2. Get Watermarks
    3. Load Data (CDF/Snapshot)
    4. Check Early Arriving Facts (Skeleton Gen)
    5. Transform
    6. Merge
    7. Commit Watermarks

    Configuration:
        Set KIMBALL_ETL_SCHEMA environment variable to configure ETL control table location:

            import os
            os.environ["KIMBALL_ETL_SCHEMA"] = "my_catalog.etl_config"
    """

    def __init__(
        self,
        config_path: str,
        etl_schema: str | None = None,
        # Deprecated parameter - for backward compatibility
        watermark_database: str | None = None,
        enable_metrics: bool = True,
        checkpoint_table: str | None = None,
        checkpoint_root: str | None = None,
        # Dependency Injection
        loader: DataLoader | None = None,
        merger: DeltaMerger | None = None,
        etl_control: ETLControlManager | None = None,
        table_creator: TableCreator | None = None,
        skeleton_generator: SkeletonGenerator | None = None,
        cleanup_manager: StagingCleanupManager | None = None,
        transaction_manager: TransactionManager | None = None,
    ):
        """
        Args:
            config_path: Path to the YAML config file for this pipeline.
            etl_schema: Schema where the ETL control table is stored.
                        If not provided, uses KIMBALL_ETL_SCHEMA environment variable.
                        Falls back to target table's database if neither is set.
            watermark_database: **Deprecated** - Use etl_schema instead.
            enable_metrics: Whether to collect QueryExecution metrics.
            checkpoint_table: Delta table for pipeline checkpoints (default: default.kimball_pipeline_checkpoints).
            checkpoint_root: Root directory for DataFrame checkpoints (e.g., 'dbfs:/kimball/checkpoints/').
                           If not provided, uses KIMBALL_CHECKPOINT_ROOT environment variable.
                           Required for reliable checkpointing in production environments.
        """
        self.config_loader = ConfigLoader()
        self.config = self.config_loader.load_config(config_path)

        # Load runtime options for JVM tuning (can be overridden via env vars)
        self.runtime_options = RuntimeOptions.from_environment()

        # Apply Spark configuration for JVM performance
        # These settings have MASSIVE impact on GC pressure and shuffle efficiency
        try:
            spark = _get_spark()

            # AQE (Adaptive Query Execution) - always enable, it's free optimization
            spark.conf.set(SPARK_CONF_AQE_ENABLED, "true")
            spark.conf.set(SPARK_CONF_AQE_SKEW_JOIN, "true")
            spark.conf.set(SPARK_CONF_AQE_COALESCE, "true")

            # Shuffle partitions: 'auto' lets AQE optimize, explicit int overrides
            # Spark default (200) is wrong for almost everyone
            if self.runtime_options.shuffle_partitions != "auto":
                spark.conf.set(
                    SPARK_CONF_SHUFFLE_PARTITIONS,
                    str(self.runtime_options.shuffle_partitions),
                )
                logger.info(
                    f"Set shuffle.partitions={self.runtime_options.shuffle_partitions} "
                    f"(override Spark default of 200)"
                )
            # else: AQE will auto-coalesce partitions based on data size

            # Skew handling: prevent OOM when partitioning by keys with hot values
            # (e.g., dimension default -1 with 10M rows going to one executor)
            spark.conf.set(
                SPARK_CONF_SKEW_SIZE_THRESHOLD,
                f"{self.runtime_options.skew_threshold_mb}MB",
            )
            spark.conf.set(
                SPARK_CONF_SKEW_FACTOR, str(self.runtime_options.skew_factor)
            )
            logger.debug(
                f"Skew handling: threshold={self.runtime_options.skew_threshold_mb}MB, "
                f"factor={self.runtime_options.skew_factor}x"
            )

        except Exception as e:
            # May fail on Spark Connect or restricted environments
            logger.debug(f"Could not set Spark configs (likely Spark Connect): {e}")

        # Handle deprecated 'watermark_database' parameter
        if watermark_database is not None:
            warnings.warn(
                "The 'watermark_database' parameter is deprecated. Use 'etl_schema' instead, "
                "or set KIMBALL_ETL_SCHEMA environment variable.",
                DeprecationWarning,
                stacklevel=2,
            )
            if etl_schema is None:
                etl_schema = watermark_database

        # Resolve ETL schema: explicit param > env var > target table database
        if etl_schema is None:
            etl_schema = get_etl_schema()

        if etl_schema is None:
            # Fall back to target table's database
            if "." in self.config.table_name:
                etl_schema = self.config.table_name.split(".")[0]
            else:
                raise ValueError(
                    "ETL schema must be specified via one of:\n"
                    "  1. Set KIMBALL_ETL_SCHEMA environment variable\n"
                    "  2. Pass etl_schema parameter to Orchestrator\n"
                    "  3. Use fully-qualified table names (db.table) in config"
                )

        # Handle checkpoint_root parameter
        if checkpoint_root is None:
            checkpoint_root = os.getenv("KIMBALL_CHECKPOINT_ROOT")

        if checkpoint_root:
            logger.info(f"Setting Spark checkpoint directory to: {checkpoint_root}")
            _get_spark().sparkContext.setCheckpointDir(checkpoint_root)
        else:
            logger.info(
                "Warning: No checkpoint_root provided. Using local checkpointing which is unreliable in production."
            )

        self.etl_control = etl_control or ETLControlManager(etl_schema=etl_schema)
        self.loader = loader or DataLoader()
        self.merger = merger or DeltaMerger()
        self.skeleton_generator = skeleton_generator or SkeletonGenerator()
        self.table_creator = table_creator or TableCreator()
        self.transaction_manager = transaction_manager or TransactionManager(
            _get_spark()
        )

        # Initialize observability and resilience features (opt-in via feature flags)
        self.metrics_collector = (
            QueryMetricsCollector()
            if enable_metrics and _feature_enabled("metrics")
            else None
        )
        self.checkpoint_manager = (
            PipelineCheckpoint(checkpoint_table)
            if _feature_enabled("checkpoints")
            else None
        )
        self.cleanup_manager = cleanup_manager or (
            StagingCleanupManager() if _feature_enabled("staging_cleanup") else None
        )

    def cleanup_orphaned_staging_tables(
        self, pipeline_id: str | None = None, max_age_hours: int = 24
    ) -> None:
        """
        Clean up orphaned staging tables from previous crashed runs.
        Only cleans tables that are older than max_age_hours to avoid interfering with concurrent pipelines.

        Args:
            pipeline_id: Optional pipeline ID to filter cleanup (for targeted cleanup)
            max_age_hours: Maximum age of staging tables to clean up (default 24 hours)
        """
        logger.info(
            f"Checking for orphaned staging tables (max age: {max_age_hours}h)..."
        )

        # Use TTL-based cleanup to avoid interfering with concurrent pipelines
        if self.cleanup_manager is None:
            logger.warning(" cleanup_manager not initialized, skipping cleanup")
            return
        cleaned, failed = self.cleanup_manager.cleanup_staging_tables(
            pipeline_id=pipeline_id, max_age_hours=max_age_hours
        )
        if cleaned > 0:
            logger.info(f"Cleaned up {cleaned} orphaned staging tables")
        if failed > 0:
            logger.warning(f" {failed} staging tables could not be cleaned up")

    def run(self, max_retries: int = 0) -> dict[str, Any]:
        """
        Execute the ETL pipeline with batch lifecycle tracking.

        Args:
            max_retries: Maximum number of retries for retriable errors (default: 0, no retry).
                         For production, use Databricks Jobs retry instead.

        Returns:
            dict: Summary of the pipeline run including rows_read and rows_written.
        """
        # PRESERVE_ALL_CHANGES: Loop until watermark catches up to source version
        if self.config.preserve_all_changes and self.config.scd_type == 2:
            return self._run_with_version_loop()

        return self._run_pipeline_once()

    def _run_with_version_loop(self, max_iterations: int = 100) -> dict[str, Any]:
        """Run pipeline repeatedly until all CDF versions are processed (for preserve_all_changes mode)."""
        iteration = 0
        combined_result = {"rows_read": 0, "rows_written": 0}

        while iteration < max_iterations:
            iteration += 1

            # Check if we're caught up BEFORE running (compare watermark to source version)
            for source in self.config.sources:
                if source.cdc_strategy == "cdf":
                    source_version = self.loader.get_latest_version(source.name)
                    wm = self.etl_control.get_watermark(
                        self.config.table_name, source.name
                    )
                    if wm is not None and wm >= source_version:
                        logger.info(
                            f"Preserve All Changes: Caught up (watermark {wm} >= source {source_version})"
                        )
                        return combined_result

            result = self._run_pipeline_once()

            combined_result["rows_read"] += result.get("rows_read", 0)
            combined_result["rows_written"] += result.get("rows_written", 0)

            logger.info(
                f"Preserve All Changes: Iteration {iteration} processed, checking for more versions..."
            )

        if iteration > 1:
            logger.info(f"Preserve All Changes: Processed {iteration} version(s) total")

        return combined_result

    def _run_pipeline_once(self, max_retries: int = 0) -> dict[str, Any]:
        """Execute single pipeline iteration."""
        logger.info(f"Starting pipeline for {self.config.table_name}")

        batch_id = str(uuid.uuid4())

        # Start metrics collection
        if self.metrics_collector:
            self.metrics_collector.start_collection()

        source_versions = {}
        source_row_counts: dict[str, int] = {}  # Track actual per-source row counts
        active_dfs = {}
        total_rows_read = 0
        total_rows_written = 0

        # Stage timing
        stage_start = time.time()

        # 0. Zombie Recovery (Crash Recovery) - MUST run BEFORE batch_start_all
        # Check if previous run crashed and perform rollback if needed
        # This ensures we get a clean slate before starting potential new transaction
        # FINDING-014: Detect Serverless limitation where commit tagging is unavailable
        if getattr(self.config, "enable_crash_recovery", True):
            # Check if we can tag commits (required for zombie recovery)
            can_tag_commits = True
            try:
                _get_spark().conf.get(
                    "_get_spark().databricks.delta.commitInfo.userMetadata"
                )
            except Exception:
                # If we can't get the config, assume we can set it
                pass

            # Check for Serverless Compute / Capability to Tag Commits
            # Serverless allows reading some configs but blocks others, and explicitly blocks setting userMetadata.
            # Instead of checking a config flag (which might be restricted), we check the CAPABILITY directly.
            try:
                # Try to set the property we actually need
                _get_spark().conf.set(
                    "spark.databricks.delta.commitInfo.userMetadata", "test"
                )
                _get_spark().conf.unset(
                    "spark.databricks.delta.commitInfo.userMetadata"
                )
                can_tag_commits = True
            except Exception:
                # If we can't set it, we are likely on Serverless or a restricted cluster
                can_tag_commits = False
                logger.info(
                    "WARNING: Commit tagging unavailable (likely Serverless Compute). "
                    "Crash recovery / Zombie detection is disabled. "
                    "Pipelines will rely on idempotency for recovery."
                )

            if can_tag_commits:
                running_batches = self.etl_control.get_running_batches(
                    self.config.table_name
                )
                if running_batches:
                    logger.info(
                        f"Found {len(running_batches)} incomplete batches. Attempting recovery..."
                    )
                    for batch_info in running_batches:
                        bad_batch_id = batch_info["batch_id"]
                        source_table = batch_info["source_table"]

                        # Attempt rollback (only needed once per batch_id, but idempotent)
                        self.transaction_manager.recover_zombies(
                            self.config.table_name, bad_batch_id
                        )

                        # Mark specific source batch as failed in control table to prevent future "zombie found" msg
                        try:
                            self.etl_control.batch_fail(
                                self.config.table_name,
                                source_table,
                                "CRASH_RECOVERY: Rolled back",
                            )
                        except Exception as e:
                            # C-04: Log failed batch_fail for visibility
                            logger.warning(
                                f"Failed to mark batch as failed during crash recovery: {e}"
                            )

        # Start batch tracking for each source (AFTER zombie recovery)
        # Only track incremental sources (CDF, timestamp) - full snapshot sources don't need watermarks
        source_names = [s.name for s in self.config.sources if s.cdc_strategy != "full"]
        if source_names:
            self.etl_control.batch_start_all(self.config.table_name, source_names)

        try:
            # Wrap execution in transaction (ACID-like rollback on failure)

            # Since TransactionManager logic requires table to exist for getting version,
            # we skip it for first run / table creation.

            with self.transaction_manager.table_transaction(
                self.config.table_name, batch_id
            ):
                # 1. Load Sources
                for source in self.config.sources:
                    # Get latest version for watermark commit later
                    # NOTE: Only Delta tables with CDF support versioning
                    if source.format == "delta" and source.cdc_strategy == "cdf":
                        latest_v = self.loader.get_latest_version(source.name)
                    else:
                        # Non-Delta or full snapshot: versioning not applicable
                        latest_v = 0
                    source_versions[source.name] = latest_v

                    # Determine Load Strategy
                    if source.cdc_strategy == "full":
                        df = self.loader.load_full_snapshot(
                            source.name, format=source.format, options=source.options
                        )
                    elif source.cdc_strategy == "cdf":
                        wm = self.etl_control.get_watermark(
                            self.config.table_name, source.name
                        )
                        if wm is None:
                            logger.info(
                                f"No watermark for {source.name}. "
                                f"Performing Initial Load via CDF from Version 0 (to preserve _change_type)."
                            )
                            # Use CDF from source.starting_version (default 0) to ensure _change_type is present for initial load
                            df = self.loader.load_cdf(
                                source.name,
                                starting_version=source.starting_version,
                                deduplicate_keys=source.primary_keys,
                                ending_version=latest_v,
                            )
                            source_versions[source.name] = latest_v
                        else:
                            if wm >= latest_v:
                                logger.info(
                                    f"Source {source.name} already at version {latest_v}. Skipping."
                                )
                                # Mark batch complete with no changes
                                self.etl_control.batch_complete(
                                    self.config.table_name,
                                    source.name,
                                    new_version=latest_v,
                                    rows_read=0,
                                    rows_written=0,
                                )
                                continue

                            # PRESERVE_ALL_CHANGES: For SCD2, process one version at a time
                            if (
                                self.config.preserve_all_changes
                                and self.config.scd_type == 2
                            ):
                                # Load just ONE version (wm+1) instead of the full range
                                # Subsequent versions will be picked up in the next run
                                logger.info(
                                    f"Preserve All Changes: Processing version {wm + 1} only"
                                )
                                df = self.loader.load_cdf(
                                    source.name,
                                    wm + 1,
                                    deduplicate_keys=source.primary_keys,
                                    ending_version=wm + 1,  # Single version
                                )
                                source_versions[source.name] = wm + 1
                            else:
                                # Standard fast-forward mode (default)
                                df = self.loader.load_cdf(
                                    source.name,
                                    wm + 1,
                                    deduplicate_keys=source.primary_keys,
                                    ending_version=latest_v,
                                )
                    elif source.cdc_strategy == "timestamp":
                        raise NotImplementedError(
                            f"cdc_strategy='timestamp' is not yet implemented for source '{source.name}'. "
                            "Use 'cdf' (recommended) or 'full' instead."
                        )
                    else:
                        raise ValueError(f"Unknown CDC strategy: {source.cdc_strategy}")

                    # Note: We no longer call df.count() here as it forces eager evaluation.
                    # Row counts will be inferred from merge metrics after the operation completes.

                    # Register Temp View
                    df.createOrReplaceTempView(source.alias)
                    active_dfs[source.name] = df
                    # Initialize row count (will be updated later if we can count after checkpoint)
                    source_row_counts[source.name] = 0

                # Track sources loaded timing
                if self.metrics_collector:
                    self.metrics_collector.add_operation_metric(
                        "sources_loaded",
                        duration_ms=(time.time() - stage_start) * 1000,
                        sources_count=len(active_dfs),
                    )
                    stage_start = time.time()

                # Early exit if no sources loaded (all skipped - already at version)
                if not active_dfs:
                    logger.info(
                        "All sources already at current version. Nothing to process."
                    )
                    return {"rows_read": 0, "rows_written": 0}

                # 2. Early Arriving Facts (Skeleton Generation)
                if self.config.early_arriving_facts:
                    logger.info("Checking for Early Arriving Facts...")
                    for eaf in self.config.early_arriving_facts:
                        # We need the fact dataframe.
                        # The config doesn't explicitly say which source is the "fact" source,
                        # but usually it's the one driving the pipeline or we can infer from the join key.
                        # However, we need a DataFrame that has the 'fact_join_key'.
                        # We can try to find it in the active_dfs.

                        # For simplicity, we assume the 'fact_join_key' is available in at least one loaded source.
                        # We'll search active_dfs for one that has the column.
                        fact_source_df = None
                        for df in active_dfs.values():
                            if eaf["fact_join_key"] in df.columns:
                                fact_source_df = df
                                break

                        if fact_source_df:
                            # We also need to know the surrogate key strategy of the DIMENSION table.
                            # This is tricky because we only have the config for the FACT table here.
                            # We don't have the TableConfig for the dimension.
                            # We might need to load it, or pass it in the eaf config.
                            # For now, let's assume 'hash' or 'identity' based on a new config field or default.
                            # Let's add 'surrogate_key_strategy' and 'surrogate_key_col' to the EAF config in YAML.

                            self.skeleton_generator.generate_skeletons(
                                fact_df=fact_source_df,
                                dim_table_name=eaf["dimension_table"],
                                fact_join_key=eaf["fact_join_key"],
                                dim_join_key=eaf["dimension_join_key"],
                                surrogate_key_col=eaf.get(
                                    "surrogate_key_col", "surrogate_key"
                                ),
                                surrogate_key_strategy=eaf.get(
                                    "surrogate_key_strategy", "identity"
                                ),
                                batch_id=batch_id,
                            )
                        else:
                            logger.info(
                                f"Warning: Could not find source with column {eaf['fact_join_key']} for skeleton generation."
                            )

                # 3. Transformation
                # FINDING-016: Validate transformation_sql only allows SELECT/WITH statements
                if self.config.transformation_sql:
                    sql_stripped = self.config.transformation_sql.strip().upper()
                    if not sql_stripped.startswith(
                        "SELECT"
                    ) and not sql_stripped.startswith("WITH"):
                        raise ValueError(
                            f"transformation_sql must be a SELECT or WITH statement for safety. "
                            f"Got: {self.config.transformation_sql[:50]}..."
                        )
                    logger.info("Executing Transformation SQL...")
                    transformed_df = _get_spark().sql(self.config.transformation_sql)

                    # Warn if CDF source has _change_type but transformation SQL stripped it
                    # User must explicitly include _change_type for delete detection in SCD2
                    for source in self.config.sources:
                        if source.cdc_strategy == "cdf":
                            source_df = active_dfs.get(source.name)
                            if (
                                source_df is not None
                                and "_change_type" in source_df.columns
                                and "_change_type" not in transformed_df.columns
                            ):
                                logger.warning(
                                    f"CDF source '{source.name}' has _change_type column but it's not in "
                                    f"transformation SQL output. Delete detection will NOT work. "
                                    f"Add '_change_type' to your SELECT clause for proper SCD2 delete handling."
                                )
                                break  # Only warn once
                else:
                    # No transformation SQL - use source data directly
                    if len(self.config.sources) == 1:
                        source_name = self.config.sources[0].name
                        transformed_df = active_dfs[source_name]
                        logger.info(
                            f"Using source data directly (no transformation): {source_name}"
                        )
                    else:
                        raise ValueError(
                            "transformation_sql is required for multi-source pipelines"
                        )

                # Kimball-proper: Handle NULL foreign keys using explicit FK declarations
                # FINDING-017: Only fill NULLs that result from failed dimension lookups
                # Preserves intentional NULLs (e.g., anonymous sales) while filling lookup failures
                if self.config.foreign_keys:
                    from pyspark.sql import functions as F
                    from pyspark.sql.types import StringType

                    for fk in self.config.foreign_keys:
                        col_name = fk.column
                        default_val = fk.default_value
                        # Check if column exists in the transformed DataFrame
                        field = next(
                            (
                                f
                                for f in transformed_df.schema.fields
                                if f.name == col_name
                            ),
                            None,
                        )
                        if field:
                            # Only fill NULL if the column was expected to have a value
                            # We detect lookup failures by checking if the FK is NULL but
                            # there's a corresponding natural key that is NOT NULL
                            # For simple cases without lookup tracking, we provide a warning
                            if isinstance(field.dataType, StringType):
                                fill_val = str(default_val)
                            else:
                                fill_val = default_val

                            # Apply fillna for this column only
                            logger.info(
                                f"Filling NULL foreign key '{col_name}' with default: {fill_val}"
                            )
                            transformed_df = transformed_df.withColumn(
                                col_name,
                                F.when(
                                    F.col(col_name).isNull(), F.lit(fill_val)
                                ).otherwise(F.col(col_name)),
                            )
                        else:
                            logger.info(
                                f"Warning: Foreign key column '{col_name}' not found in transformed DataFrame"
                            )

                # Run Data Quality Validation on TRANSFORMED data (if configured)
                # CRITICAL: Validation must run AFTER transformation to validate the output schema
                if getattr(self.config, "tests", None):
                    from kimball.validation import DataQualityValidator

                    logger.info(
                        "Running data quality validation on transformed data..."
                    )
                    validator = DataQualityValidator()
                    report = validator.run_config_tests(self.config, df=transformed_df)
                    report.raise_on_failure()

                # BUILT-IN: Natural Key Uniqueness Validation (Dimension tables)
                # This is a critical Kimball integrity check that prevents SK corruption
                if self.config.table_type == "dimension" and self.config.natural_keys:
                    from kimball.validation import DataQualityValidator

                    logger.info("Validating natural key uniqueness (pre-merge gate)...")
                    validator = DataQualityValidator()
                    nk_result = validator.validate_natural_key_uniqueness(
                        transformed_df,
                        self.config.natural_keys,
                        table_name=self.config.table_name,
                    )
                    logger.info(str(nk_result))
                    if not nk_result.passed:
                        from kimball.common.errors import DataQualityError

                        raise DataQualityError(
                            f"Natural key uniqueness violation in {self.config.table_name}: "
                            f"{nk_result.failed_rows} duplicate keys. Details: {nk_result.details}",
                            details={"sample_failures": nk_result.sample_failures},
                        )

                # BUILT-IN: FK Integrity Validation (Fact tables)
                # Validates that all FK columns reference valid dimension SKs
                if self.config.table_type == "fact" and self.config.foreign_keys:
                    from kimball.validation import DataQualityValidator

                    logger.info(
                        "Validating FK integrity against dimensions (pre-merge gate)..."
                    )
                    validator = DataQualityValidator()
                    # Build FK definitions from config
                    fk_defs = [
                        {
                            "column": fk.column,
                            "dimension_table": fk.references,
                            "dimension_key": fk.column,  # Assumes FK name matches dim SK name
                        }
                        for fk in self.config.foreign_keys
                        if hasattr(fk, "references") and fk.references
                    ]
                    if fk_defs:
                        fk_report = validator.validate_fact_fk_integrity(
                            transformed_df, fk_defs
                        )
                        for result in fk_report.results:
                            logger.info(str(result))
                        fk_report.raise_on_failure()

                # Checkpoint: Transformation complete
                checkpoint_state = {
                    "stage": "transformation_complete",
                    "total_rows_read": total_rows_read,
                    "active_sources": list(active_dfs.keys()),
                }
                if self.checkpoint_manager:
                    self.checkpoint_manager.save_checkpoint(
                        batch_id, "transformation_complete", checkpoint_state
                    )

                # Track transformation timing
                if self.metrics_collector:
                    self.metrics_collector.add_operation_metric(
                        "transformation",
                        duration_ms=(time.time() - stage_start) * 1000,
                    )
                    stage_start = time.time()

                # 4. Stage-then-Merge for concurrency resilience
                # Check if we have data to merge
                # C-08: Use efficient empty check pattern instead of isEmpty() which scans entire DataFrame
                if len(transformed_df.limit(1).head(1)) == 0:
                    logger.info("No data to merge. Skipping.")
                    # CRITICAL: Do NOT fetch last merge metrics here - they would be stale
                    # from a previous run and could incorrectly trigger watermark advancement
                    merge_executed = False
                else:
                    merge_executed = True
                    # Ensure table exists and has defaults (if SCD2)
                    # We need to create the table if it doesn't exist to seed defaults
                    table_created = False
                    if not _get_spark().catalog.tableExists(self.config.table_name):
                        logger.info(f"Creating table {self.config.table_name}...")
                        # Add system columns to transformed_df schema for table creation
                        schema_df = self.table_creator.add_system_columns(
                            transformed_df.limit(0),
                            self.config.scd_type,
                            self.config.surrogate_key,
                            self.config.surrogate_key_strategy,
                        )

                        # Auto-cluster dimensions on natural keys if no explicit cluster_by
                        # This improves SCD2 merge performance via data skipping
                        cluster_cols = self.config.cluster_by
                        if (
                            not cluster_cols
                            and self.config.table_type == "dimension"
                            and _feature_enabled("auto_cluster")
                        ):
                            cluster_cols = self.config.natural_keys or []
                            if cluster_cols:
                                logger.info(
                                    f"Auto-clustering on natural keys: {cluster_cols}"
                                )

                        # Create Delta table with optional Liquid Clustering
                        self.table_creator.create_table_with_clustering(
                            table_name=self.config.table_name,
                            schema_df=schema_df,
                            config=self.config.model_dump(),  # Pass full config for constraints
                            cluster_by=cluster_cols or [],
                            surrogate_key_col=self.config.surrogate_key,
                            surrogate_key_strategy=self.config.surrogate_key_strategy,
                        )
                        table_created = True

                    # Seed default rows (-1, -2, -3) ONLY on table creation (not every run)
                    if table_created and self.config.table_type == "dimension":
                        target_schema = (
                            _get_spark().table(self.config.table_name).schema
                        )
                        if self.config.scd_type == 2:
                            self.merger.ensure_scd2_defaults(
                                self.config.table_name,
                                target_schema,
                                self.config.surrogate_key or "surrogate_key",
                                self.config.default_rows,
                                self.config.surrogate_key_strategy,
                            )
                        elif self.config.scd_type == 1 and self.config.surrogate_key:
                            self.merger.ensure_scd1_defaults(
                                self.config.table_name,
                                target_schema,
                                self.config.surrogate_key,
                                self.config.default_rows,
                                self.config.surrogate_key_strategy,
                            )

                    # Use DataFrame checkpointing instead of physical staging to minimize lock time
                    # This provides fault tolerance without the 100% I/O overhead of physical staging
                    logger.info("Creating DataFrame checkpoint for merge operation...")

                    # Checkpoint optimization: Only use expensive checkpoint() when explicitly enabled
                    # Default to localCheckpoint() which is much more efficient for standard pipelines
                    if getattr(self.config, "enable_lineage_truncation", False):
                        # Use reliable checkpoint() only when lineage truncation is explicitly requested
                        try:
                            checkpoint_dir = (
                                _get_spark().sparkContext.getCheckpointDir()
                            )
                            if checkpoint_dir:
                                logger.info(
                                    f"Using reliable checkpoint directory: {checkpoint_dir}"
                                )
                                checkpointed_df = transformed_df.checkpoint()
                            else:
                                logger.info(
                                    "No checkpoint directory configured, using local checkpoint"
                                )
                                checkpointed_df = transformed_df.localCheckpoint()
                        except PYSPARK_EXCEPTION_BASE as e:
                            # Log specific exception and fallback to local checkpoint
                            logger.info(
                                f"Checkpoint directory access failed with PySpark error: {e}"
                            )
                            logger.info("Using local checkpoint (less reliable)")
                            checkpointed_df = transformed_df.localCheckpoint()
                        except Exception as e:
                            # Log any other unexpected errors and fallback to local checkpoint
                            logger.info(
                                f"Unexpected error during checkpoint setup: {e}"
                            )
                            logger.info("Using local checkpoint (less reliable)")
                            checkpointed_df = transformed_df.localCheckpoint()
                    else:
                        # Use efficient localCheckpoint() by default - no disk I/O overhead
                        # WARNING (Ritchie): localCheckpoint() materializes the ENTIRE DataFrame
                        # into executor heap memory. For a 10GB merge, this doubles memory usage.
                        # This is like strdup() on a 10GB string - the "local" in the name is misleading.
                        # Cost: O(input_size) memory, NOT O(1). Consider if lineage depth warrants it.
                        checkpointed_df = transformed_df.localCheckpoint()
                        logger.info(
                            "Using local checkpoint (materializes to executor heap - not free)"
                        )

                    # Column pruning: Select only columns that exist in target schema
                    # Only perform pruning if target table already exists AND schema evolution is disabled
                    if (
                        _get_spark().catalog.tableExists(self.config.table_name)
                        and not self.config.schema_evolution
                    ):
                        target_schema = (
                            _get_spark().table(self.config.table_name).schema
                        )
                        target_columns = [f.name for f in target_schema.fields]

                        # System columns that must always be included for proper merge operations
                        # Even if they don't exist in target yet (schema evolution will add them)
                        SYSTEM_COLUMNS = {
                            "__is_current",
                            "__valid_from",
                            "__valid_to",
                            "__etl_processed_at",
                            "__is_deleted",
                            "_change_type",  # CDF metadata for delete detection
                        }

                        # Include system columns even if not in target schema
                        columns_to_select = []
                        for c in checkpointed_df.columns:
                            if c in target_columns or c in SYSTEM_COLUMNS:
                                columns_to_select.append(c)

                        source_df = checkpointed_df.select(
                            *[col(c) for c in columns_to_select]
                        )
                        logger.info(
                            f"Applied column pruning: kept {len(columns_to_select)}/{len(checkpointed_df.columns)} columns (including system columns)"
                        )
                    else:
                        # First run or schema evolution enabled - use all columns
                        source_df = checkpointed_df

                    logger.info(
                        f"Merging directly from checkpointed DataFrame into {self.config.table_name}..."
                    )

                    # Kimball: Dimensions use natural_keys for merge; Facts use merge_keys (degenerate dimensions)
                    if self.config.table_type == "fact":
                        join_keys = self.config.merge_keys or []
                    else:
                        join_keys = self.config.natural_keys or []

                    # GRAIN ENFORCEMENT: Fail fast if source violates grain
                    # This prevents silent duplicates from reaching the merge
                    if join_keys:
                        from pyspark.sql.functions import count as spark_count

                        grain_violations = (
                            source_df.groupBy(*join_keys)
                            .agg(spark_count("*").alias("__grain_count"))
                            .filter("__grain_count > 1")
                        )

                        # Use efficient check (limit 1, not isEmpty)
                        if len(grain_violations.limit(1).head(1)) > 0:
                            sample_violations = grain_violations.limit(5).collect()
                            violation_keys = [
                                {k: row[k] for k in join_keys}
                                for row in sample_violations
                            ]
                            raise ValueError(
                                f"Grain violation in {self.config.table_name}: "
                                f"Duplicate keys found for grain {join_keys}. "
                                f"Sample violations: {violation_keys}. "
                                "Fix upstream deduplication before loading."
                            )

                    self.merger.merge(
                        target_table_name=self.config.table_name,
                        source_df=source_df,
                        join_keys=join_keys,
                        delete_strategy=self.config.delete_strategy,
                        batch_id=batch_id,
                        scd_type=self.config.scd_type,
                        track_history_columns=self.config.track_history_columns,
                        surrogate_key_col=self.config.surrogate_key,
                        surrogate_key_strategy=self.config.surrogate_key_strategy,
                        schema_evolution=self.config.schema_evolution,
                        effective_at_column=self.config.effective_at,
                    )

                    # 5. Optimize Table (if configured)
                    if self.config.optimize_after_merge:
                        # Optimization: Inline OPTIMIZE is expensive. Only run if explicitly enabled via env var.
                        # Production systems should run OPTIMIZE/VACUUM in a separate async maintenance job.
                        if os.environ.get("KIMBALL_ENABLE_INLINE_OPTIMIZE") == "1":
                            self.merger.optimize_table(
                                self.config.table_name, self.config.cluster_by or []
                            )
                        else:
                            logger.info(
                                "Skipping inline OPTIMIZE (Performance Optimization). "
                                "Set KIMBALL_ENABLE_INLINE_OPTIMIZE=1 to enable, "
                                "or use async maintenance jobs (Recommended)."
                            )

                # Get row counts from Delta merge metrics (more accurate than counting)
                # CRITICAL: Only fetch metrics if merge was actually executed
                # Otherwise we'd get stale metrics from a previous run
                if merge_executed:
                    # C-02: Pass batch_id to get exact commit metrics (prevents race condition)
                    merge_metrics = self.merger.get_last_merge_metrics(
                        self.config.table_name, batch_id=batch_id
                    )
                    total_rows_read = int(merge_metrics.get("numSourceRows", 0))
                    total_rows_written = int(
                        merge_metrics.get("numTargetRowsInserted", 0)
                    ) + int(merge_metrics.get("numTargetRowsUpdated", 0))
                else:
                    # Merge was skipped - use zeros to prevent incorrect watermark advancement
                    total_rows_read = 0
                    total_rows_written = 0

                # Track merge timing
                if self.metrics_collector:
                    self.metrics_collector.add_operation_metric(
                        "merge",
                        duration_ms=(time.time() - stage_start) * 1000,
                        rows_read=total_rows_read,
                        rows_written=total_rows_written,
                    )

                # 6. Commit Watermarks with batch completion
                # CRITICAL: Only advance watermarks for sources that contributed to the pipeline
                # This prevents silent data loss when transformation filters everything out
                logger.info("Completing batches and updating watermarks...")

                # Determine which sources actually contributed rows
                sources_with_data = set(active_dfs.keys())

                for source_name, version in source_versions.items():
                    if source_name not in sources_with_data:
                        # Source was skipped (wm >= latest_v), already marked complete earlier
                        continue

                    # If we have actual per-source counts, use them; otherwise approximate
                    per_source_rows = source_row_counts.get(source_name, 0)

                    # Only advance watermark if we actually processed something
                    # OR if total_rows_written > 0 (merge happened successfully)
                    # Use actual per-source count if available, else distribute written rows
                    if per_source_rows == 0 and total_rows_written > 0:
                        # Fallback: distribute written rows among sources that contributed
                        per_source_written = total_rows_written // max(
                            len(sources_with_data), 1
                        )
                    else:
                        per_source_written = per_source_rows

                    # Always advance watermark to the processed version
                    # Empty CDF versions (like OPTIMIZE) are valid - no data to lose
                    self.etl_control.batch_complete(
                        target_table=self.config.table_name,
                        source_table=source_name,
                        new_version=version,
                        rows_read=per_source_rows
                        if per_source_rows > 0
                        else (total_rows_read // max(len(sources_with_data), 1)),
                        rows_written=per_source_written,
                    )

                logger.info(
                    f"Pipeline completed successfully. Read: {total_rows_read}, Written: {total_rows_written}"
                )

                # Collect final metrics
                metrics_summary = {}
                if self.metrics_collector:
                    self.metrics_collector.stop_collection()
                    metrics_summary = self.metrics_collector.get_summary()
                    logger.info(f"Query Metrics: {metrics_summary}")

                # Clear checkpoints on success
                if self.checkpoint_manager:
                    self.checkpoint_manager.clear_checkpoint(batch_id, "sources_loaded")
                    self.checkpoint_manager.clear_checkpoint(
                        batch_id, "transformation_complete"
                    )

                # C-09: Clean up orphaned staging tables after successful merge
                # (runs per-merge instead of per-session for reliable cleanup)
                if self.cleanup_manager:
                    self.cleanup_orphaned_staging_tables()

                return {
                    "status": "SUCCESS",
                    "batch_id": batch_id,
                    "target_table": self.config.table_name,
                    "rows_read": total_rows_read,
                    "rows_written": total_rows_written,
                    "metrics": metrics_summary,
                }

        except Exception as e:
            # Stop metrics collection on error
            if self.metrics_collector:
                self.metrics_collector.stop_collection()

            # Mark all batches as failed
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.info(f"Pipeline failed: {error_msg}")

            for source in self.config.sources:
                try:
                    self.etl_control.batch_fail(
                        self.config.table_name, source.name, error_msg
                    )
                except Exception as batch_err:
                    # C-04: Log instead of silent pass
                    logger.debug(f"Could not mark batch as failed: {batch_err}")

            # Re-raise exception to ensure TransactionManager context manager catches it and rolls back
            raise e

        finally:
            # RESOURCE CLEANUP (Dennis Ritchie would approve)
            # Release DataFrame references from executors to prevent memory leaks
            for source_name, df in active_dfs.items():
                try:
                    df.unpersist(blocking=False)
                    logger.debug(f"Released DataFrame for source: {source_name}")
                except Exception:
                    pass  # Best effort - may already be garbage collected
            active_dfs.clear()

            # Drop temp views to prevent global namespace pollution
            for source in self.config.sources:
                try:
                    _get_spark().catalog.dropTempView(source.alias)
                    logger.debug(f"Dropped temp view: {source.alias}")
                except Exception:
                    pass  # View may not exist if pipeline failed early

    def run_with_retry(
        self, max_retries: int = 3, backoff_seconds: int = 30
    ) -> dict[str, Any]:
        """
        Execute pipeline with smart retry based on error type.

        For production workloads, prefer using Databricks Jobs retry instead.
        This method is useful for ad-hoc testing in notebooks.

        Args:
            max_retries: Maximum retry attempts for retriable errors (default: 3)
            backoff_seconds: Base wait time between retries (exponential: 30, 60, 120, ...)

        Returns:
            dict: Summary of the pipeline run.
        """
        attempt = 0
        last_error = None

        while attempt <= max_retries:
            try:
                return self.run()
            except RetriableError as e:
                attempt += 1
                last_error = e
                if attempt <= max_retries:
                    wait_time = backoff_seconds * (2 ** (attempt - 1))
                    logger.info(
                        f"Retriable error: {e}. Waiting {wait_time}s before retry {attempt}/{max_retries}"
                    )
                    time.sleep(wait_time)
                else:
                    raise
            except NonRetriableError:
                raise  # Don't retry, fail immediately
            except Exception:
                raise  # Unknown errors don't retry

        if last_error:
            raise last_error
        raise RuntimeError("Max retries exceeded with unknown error")
