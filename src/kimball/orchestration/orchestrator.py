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

from pyspark.sql import functions as F
from pyspark.sql.functions import col, count as spark_count
from pyspark.sql.types import StringType

from kimball.common.config import ConfigLoader
from kimball.common.constants import (
    SPARK_CONF_AQE_COALESCE,
    SPARK_CONF_AQE_ENABLED,
    SPARK_CONF_AQE_SKEW_JOIN,
    SPARK_CONF_SHUFFLE_PARTITIONS,
    SPARK_CONF_SKEW_FACTOR,
    SPARK_CONF_SKEW_SIZE_THRESHOLD,
)
from kimball.common.errors import DataQualityError, NonRetriableError, RetriableError
try:
    from pyspark.errors import PySparkException as PYSPARK_EXCEPTION_BASE
except ImportError:
    try:
        from pyspark.sql.utils import AnalysisException as PYSPARK_EXCEPTION_BASE
    except ImportError:
        PYSPARK_EXCEPTION_BASE = Exception
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
from kimball.processing import merger as _merger
from kimball.processing.skeleton_generator import SkeletonGenerator
from kimball.processing.table_creator import TableCreator
from kimball.validation import DataQualityValidator

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

        if max_retries > 0:
            return self.run_with_retry(max_retries=max_retries)

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

    def _ensure_target_table(self, transformed_df) -> bool:
        """Create the target table and seed defaults when this is the first run."""
        if _get_spark().catalog.tableExists(self.config.table_name):
            return False

        logger.info(f"Creating table {self.config.table_name}...")
        schema_df = self.table_creator.add_system_columns(
            transformed_df.limit(0),
            self.config.scd_type,
            self.config.surrogate_key,
            self.config.surrogate_key_strategy,
            current_value_columns=self.config.current_value_columns,
        )

        cluster_cols = self.config.cluster_by
        if (
            not cluster_cols
            and self.config.table_type == "dimension"
            and _feature_enabled("auto_cluster")
        ):
            cluster_cols = self.config.natural_keys or []
            if cluster_cols:
                logger.info(f"Auto-clustering on natural keys: {cluster_cols}")

        self.table_creator.create_table_with_clustering(
            table_name=self.config.table_name,
            schema_df=schema_df,
            config=self.config.model_dump(),
            cluster_by=cluster_cols or [],
            surrogate_key_col=self.config.surrogate_key,
            surrogate_key_strategy=self.config.surrogate_key_strategy,
        )

        if self.config.scd_type == 4 and self.config.history_table:
            self.table_creator.create_history_table(self.config.history_table)

        return True

    def _prepare_source_df_for_merge(self, transformed_df) -> DataFrame:
        """Checkpoint and prune the transformed data before merge execution."""
        logger.info("Creating DataFrame checkpoint for merge operation...")

        if getattr(self.config, "enable_lineage_truncation", False):
            try:
                checkpoint_dir = _get_spark().sparkContext.getCheckpointDir()
                if checkpoint_dir:
                    logger.info(f"Using reliable checkpoint directory: {checkpoint_dir}")
                    checkpointed_df = transformed_df.checkpoint()
                else:
                    logger.info("No checkpoint directory configured, using local checkpoint")
                    checkpointed_df = transformed_df.localCheckpoint()
            except PYSPARK_EXCEPTION_BASE as e:
                logger.info(f"Checkpoint directory access failed with PySpark error: {e}")
                logger.info("Using local checkpoint (less reliable)")
                checkpointed_df = transformed_df.localCheckpoint()
            except Exception as e:
                logger.info(f"Unexpected error during checkpoint setup: {e}")
                logger.info("Using local checkpoint (less reliable)")
                checkpointed_df = transformed_df.localCheckpoint()
        else:
            checkpointed_df = transformed_df.localCheckpoint()
            logger.info(
                "Using local checkpoint (materializes to executor heap - not free)"
            )

        if (
            _get_spark().catalog.tableExists(self.config.table_name)
            and not self.config.schema_evolution
        ):
            target_schema = _get_spark().table(self.config.table_name).schema
            target_columns = [f.name for f in target_schema.fields]
            SYSTEM_COLUMNS = {
                "__is_current",
                "__valid_from",
                "__valid_to",
                "__etl_processed_at",
                "__is_deleted",
                "_change_type",
            }
            columns_to_select = []
            for c in checkpointed_df.columns:
                if c in target_columns or c in SYSTEM_COLUMNS:
                    columns_to_select.append(c)

            source_df = checkpointed_df.select(*[col(c) for c in columns_to_select])
            logger.info(
                f"Applied column pruning: kept {len(columns_to_select)}/{len(checkpointed_df.columns)} columns (including system columns)"
            )
        else:
            source_df = checkpointed_df

        return source_df

    def _recover_zombies(self) -> bool:
        """Recover from crashed pipeline runs. Returns True if commit tagging is available."""
        if not getattr(self.config, "enable_crash_recovery", True):
            return True

        try:
            _get_spark().conf.set(
                "spark.databricks.delta.commitInfo.userMetadata", "test"
            )
            _get_spark().conf.unset(
                "spark.databricks.delta.commitInfo.userMetadata"
            )
        except Exception:
            logger.info(
                "WARNING: Commit tagging unavailable (likely Serverless Compute). "
                "Crash recovery / Zombie detection is disabled. "
                "Pipelines will rely on idempotency for recovery."
            )
            return False

        running_batches = self.etl_control.get_running_batches(self.config.table_name)
        if running_batches:
            logger.info(
                f"Found {len(running_batches)} incomplete batches. Attempting recovery..."
            )
            for batch_info in running_batches:
                bad_batch_id = batch_info["batch_id"]
                source_table = batch_info["source_table"]
                self.transaction_manager.recover_zombies(
                    self.config.table_name, bad_batch_id
                )
                try:
                    self.etl_control.batch_fail(
                        self.config.table_name,
                        source_table,
                        "CRASH_RECOVERY: Rolled back",
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to mark batch as failed during crash recovery: {e}"
                    )
        return True

    def _load_active_sources(self, batch_id: str) -> tuple[dict[str, Any], dict[str, DataFrame], dict[str, int]]:
        """Load sources for the current pipeline run."""
        source_versions: dict[str, Any] = {}
        active_dfs: dict[str, DataFrame] = {}
        source_row_counts: dict[str, int] = {}

        stage_start = time.time()
        for source in self.config.sources:
            if source.format == "delta" and source.cdc_strategy == "cdf":
                latest_v = self.loader.get_latest_version(source.name)
            else:
                latest_v = 0
            source_versions[source.name] = latest_v

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
                        self.etl_control.batch_complete(
                            self.config.table_name,
                            source.name,
                            new_version=latest_v,
                            rows_read=0,
                            rows_written=0,
                        )
                        continue

                    if (
                        self.config.preserve_all_changes
                        and self.config.scd_type == 2
                    ):
                        logger.info(
                            f"Preserve All Changes: Processing version {wm + 1} only"
                        )
                        df = self.loader.load_cdf(
                            source.name,
                            wm + 1,
                            deduplicate_keys=source.primary_keys,
                            ending_version=wm + 1,
                        )
                        source_versions[source.name] = wm + 1
                    else:
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

            df.createOrReplaceTempView(source.alias)
            active_dfs[source.name] = df
            source_row_counts[source.name] = 0

        if self.metrics_collector:
            self.metrics_collector.add_operation_metric(
                "sources_loaded",
                duration_ms=(time.time() - stage_start) * 1000,
                sources_count=len(active_dfs),
            )
        return source_versions, active_dfs, source_row_counts

    def _generate_skeletons(self, active_dfs, batch_id):
        if self.config.early_arriving_facts:
            logger.info("Checking for Early Arriving Facts...")
            for eaf in self.config.early_arriving_facts:
                fact_source_df = None
                for df in active_dfs.values():
                    if eaf["fact_join_key"] in df.columns:
                        fact_source_df = df
                        break

                if fact_source_df:
                    self.skeleton_generator.generate_skeletons(
                        fact_df=fact_source_df,
                        dim_table_name=eaf["dimension_table"],
                        fact_join_key=eaf["fact_join_key"],
                        dim_join_key=eaf["dimension_join_key"],
                        surrogate_key_col=eaf.get("surrogate_key_col", "surrogate_key"),
                        surrogate_key_strategy=eaf.get("surrogate_key_strategy", "identity"),
                        batch_id=batch_id,
                    )
                else:
                    logger.info(
                        f"Warning: Could not find source with column {eaf['fact_join_key']} for skeleton generation."
                    )

    def _transform_and_validate(self, active_dfs):
        spark = _get_spark()

        if self.config.transformation_sql:
            sql_stripped = self.config.transformation_sql.strip().upper()
            if not sql_stripped.startswith("SELECT") and not sql_stripped.startswith("WITH"):
                raise ValueError(
                    f"transformation_sql must be a SELECT or WITH statement for safety. "
                    f"Got: {self.config.transformation_sql[:50]}..."
                )
            logger.info("Executing Transformation SQL...")
            transformed_df = spark.sql(self.config.transformation_sql)

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
                        break
        else:
            if len(self.config.sources) == 1:
                source_name = self.config.sources[0].name
                transformed_df = active_dfs[source_name]
            else:
                raise ValueError(
                    "transformation_sql is required for multi-source pipelines"
                )

        if self.config.foreign_keys:
            for fk in self.config.foreign_keys:
                col_name = fk.column
                default_val = fk.default_value
                field = next(
                    (f for f in transformed_df.schema.fields if f.name == col_name),
                    None,
                )
                if field:
                    if isinstance(field.dataType, StringType):
                        fill_val = str(default_val)
                    else:
                        fill_val = default_val

                    logger.info(
                        f"Filling NULL foreign key '{col_name}' with default: {fill_val}"
                    )
                    transformed_df = transformed_df.withColumn(
                        col_name,
                        F.when(F.col(col_name).isNull(), F.lit(fill_val)).otherwise(F.col(col_name)),
                    )
                else:
                    logger.info(
                        f"Warning: Foreign key column '{col_name}' not found in transformed DataFrame"
                    )

        if getattr(self.config, "tests", None):
            logger.info("Running data quality validation on transformed data...")
            validator = DataQualityValidator()
            report = validator.run_config_tests(self.config, df=transformed_df)
            report.raise_on_failure()

        if self.config.table_type == "dimension" and self.config.natural_keys:
            logger.info("Validating natural key uniqueness (pre-merge gate)...")
            validator = DataQualityValidator()
            nk_result = validator.validate_natural_key_uniqueness(
                transformed_df,
                self.config.natural_keys,
                table_name=self.config.table_name,
            )
            logger.info(str(nk_result))
            if not nk_result.passed:
                raise DataQualityError(
                    f"Natural key uniqueness violation in {self.config.table_name}: "
                    f"{nk_result.failed_rows} duplicate keys. Details: {nk_result.details}",
                    details={"sample_failures": nk_result.sample_failures},
                )

        if self.config.table_type == "fact" and self.config.foreign_keys:
            logger.info("Validating FK integrity against dimensions (pre-merge gate)...")
            validator = DataQualityValidator()
            fk_defs = [
                {
                    "column": fk.column,
                    "dimension_table": fk.references,
                    "dimension_key": fk.dimension_key or fk.column,
                }
                for fk in self.config.foreign_keys
                if hasattr(fk, "references") and fk.references
            ]
            if fk_defs:
                fk_report = validator.validate_fact_fk_integrity(transformed_df, fk_defs)
                for result in fk_report.results:
                    logger.info(str(result))
                fk_report.raise_on_failure()

        return transformed_df

    def _run_pipeline_once(self, max_retries: int = 0) -> dict[str, Any]:
        """Execute single pipeline iteration."""
        logger.info(f"Starting pipeline for {self.config.table_name}")

        batch_id = str(uuid.uuid4())
        if self.metrics_collector:
            self.metrics_collector.start_collection()

        self._recover_zombies()
        source_names = [s.name for s in self.config.sources if s.cdc_strategy != "full"]
        if source_names:
            self.etl_control.batch_start_all(self.config.table_name, source_names)

        try:
            with self.transaction_manager.table_transaction(self.config.table_name, batch_id):
                source_versions, active_dfs, source_row_counts = self._load_active_sources(batch_id)
                if not active_dfs:
                    return {"rows_read": 0, "rows_written": 0}

                self._generate_skeletons(active_dfs, batch_id)
                transformed_df = self._transform_and_validate(active_dfs)

                if len(transformed_df.limit(1).head(1)) == 0:
                    merge_executed = False
                else:
                    merge_executed = True
                    table_created = self._ensure_target_table(transformed_df)

                    if table_created and self.config.table_type == "dimension":
                        target_schema = _get_spark().table(self.config.table_name).schema
                        if self.config.scd_type == 2:
                            _merger.ensure_scd2_defaults(
                                self.config.table_name,
                                target_schema,
                                self.config.surrogate_key or "surrogate_key",
                                self.config.default_rows,
                                self.config.surrogate_key_strategy,
                            )
                        elif self.config.scd_type == 1 and self.config.surrogate_key:
                            _merger.ensure_scd1_defaults(
                                self.config.table_name,
                                target_schema,
                                self.config.surrogate_key,
                                self.config.default_rows,
                                self.config.surrogate_key_strategy,
                            )

                    source_df = self._prepare_source_df_for_merge(transformed_df)

                    if self.config.table_type == "fact":
                        join_keys = self.config.merge_keys or []
                    else:
                        join_keys = self.config.natural_keys or []

                    if join_keys:
                        grain_violations = (
                            source_df.groupBy(*join_keys)
                            .agg(spark_count("*").alias("__grain_count"))
                            .filter("__grain_count > 1")
                        )
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

                    _merger.merge(
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
                        history_table=self.config.history_table,
                        current_value_columns=self.config.current_value_columns,
                    )

                    if self.config.optimize_after_merge:
                        if os.environ.get("KIMBALL_ENABLE_INLINE_OPTIMIZE") == "1":
                            _merger.optimize_table(
                                self.config.table_name, self.config.cluster_by or []
                            )
                        else:
                            logger.info(
                                "Skipping inline OPTIMIZE (Performance Optimization). "
                                "Set KIMBALL_ENABLE_INLINE_OPTIMIZE=1 to enable, "
                                "or use async maintenance jobs (Recommended)."
                            )

                if merge_executed:
                    merge_metrics = _merger.get_last_merge_metrics(
                        self.config.table_name, batch_id=batch_id
                    )
                    total_rows_read = int(merge_metrics.get("numSourceRows", 0))
                    total_rows_written = int(
                        merge_metrics.get("numTargetRowsInserted", 0)
                    ) + int(merge_metrics.get("numTargetRowsUpdated", 0))
                else:
                    total_rows_read = 0
                    total_rows_written = 0

                if self.metrics_collector:
                    self.metrics_collector.add_operation_metric(
                        "merge",
                        duration_ms=(time.time() - stage_start) * 1000,
                        rows_read=total_rows_read,
                        rows_written=total_rows_written,
                    )

            if not merge_executed:
                logger.info(
                    "Merge was skipped because no rows remained after transformation. "
                    "Watermarks will NOT be advanced to prevent data loss."
                )
            else:
                sources_with_data = set(active_dfs.keys())
                for source_name, version in source_versions.items():
                    if source_name not in sources_with_data:
                        continue
                    per_source_rows = source_row_counts.get(source_name, 0)
                    if per_source_rows == 0 and total_rows_written > 0:
                        per_source_written = total_rows_written // max(
                            len(sources_with_data), 1
                        )
                    else:
                        per_source_written = per_source_rows
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

            metrics_summary = {}
            if self.metrics_collector:
                self.metrics_collector.stop_collection()
                metrics_summary = self.metrics_collector.get_summary()
                logger.info(f"Query Metrics: {metrics_summary}")

            if self.checkpoint_manager:
                self.checkpoint_manager.clear_checkpoint(batch_id, "sources_loaded")
                self.checkpoint_manager.clear_checkpoint(
                    batch_id, "transformation_complete"
                )

            if self.cleanup_manager:
                self._cleanup_orphaned_staging_tables()

            return {
                "status": "SUCCESS",
                "batch_id": batch_id,
                "target_table": self.config.table_name,
                "rows_read": total_rows_read,
                "rows_written": total_rows_written,
                "metrics": metrics_summary,
            }

        except Exception as e:
            if self.metrics_collector:
                self.metrics_collector.stop_collection()
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.info(f"Pipeline failed: {error_msg}")

            for source in self.config.sources:
                try:
                    self.etl_control.batch_fail(
                        self.config.table_name, source.name, error_msg
                    )
                except Exception as batch_err:
                    logger.debug(f"Could not mark batch as failed: {batch_err}")

            raise e

        finally:
            for source_name, df in active_dfs.items():
                try:
                    df.unpersist(blocking=False)
                except Exception:
                    pass
            active_dfs.clear()

            for source in self.config.sources:
                try:
                    _get_spark().catalog.dropTempView(source.alias)
                except Exception:
                    pass

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
