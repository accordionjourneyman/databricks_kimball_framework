import os
import time
import uuid
import warnings
from typing import Any

from databricks.sdk.runtime import spark
from pyspark.sql.functions import col

from kimball.common.config import ConfigLoader
from kimball.common.constants import (
    SPARK_CONF_AQE_COALESCE,
    SPARK_CONF_AQE_ENABLED,
    SPARK_CONF_AQE_SKEW_JOIN,
)
from kimball.common.errors import NonRetriableError, RetriableError
from kimball.observability.resilience import (
    PipelineCheckpoint,
    QueryMetricsCollector,
    StagingCleanupManager,
    _feature_enabled,
)
from kimball.orchestration.watermark import ETLControlManager, get_etl_schema
from kimball.processing.loader import DataLoader
from kimball.processing.merger import DeltaMerger
from kimball.processing.skeleton_generator import SkeletonGenerator
from kimball.processing.table_creator import TableCreator

# Handle PySpark exception location changes between Runtime versions
PYSPARK_EXCEPTION_BASE: type[Exception]

try:
    # Databricks Runtime 13+ - errors moved to pyspark.errors
    from pyspark.errors import PySparkException

    PYSPARK_EXCEPTION_BASE = PySparkException
except ImportError:
    # Fallback for older Databricks Runtime versions
    import pyspark.sql.utils

    PYSPARK_EXCEPTION_BASE = pyspark.sql.utils.AnalysisException  # type: ignore


# Session-level flag to avoid repeated cleanup scans per table
_staging_cleanup_done_this_session = False


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

        # Enable Photon and AQE for performance (best-effort, may not be available on Spark Connect)
        try:
            spark.conf.set(SPARK_CONF_AQE_ENABLED, "true")
            spark.conf.set(SPARK_CONF_AQE_SKEW_JOIN, "true")
            spark.conf.set(SPARK_CONF_AQE_COALESCE, "true")
        except Exception:
            # Spark Connect (Databricks Free Edition) does not allow setting these configs
            pass

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
            print(f"Setting Spark checkpoint directory to: {checkpoint_root}")
            spark.sparkContext.setCheckpointDir(checkpoint_root)
        else:
            print(
                "Warning: No checkpoint_root provided. Using local checkpointing which is unreliable in production."
            )

        self.etl_control = etl_control or ETLControlManager(etl_schema=etl_schema)
        self.loader = loader or DataLoader()
        self.merger = merger or DeltaMerger()
        self.skeleton_generator = skeleton_generator or SkeletonGenerator()
        self.table_creator = table_creator or TableCreator()

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
        print(f"Checking for orphaned staging tables (max age: {max_age_hours}h)...")

        # Use TTL-based cleanup to avoid interfering with concurrent pipelines
        if self.cleanup_manager is None:
            print("Warning: cleanup_manager not initialized, skipping cleanup")
            return
        cleaned, failed = self.cleanup_manager.cleanup_staging_tables(
            pipeline_id=pipeline_id, max_age_hours=max_age_hours
        )
        if cleaned > 0:
            print(f"Cleaned up {cleaned} orphaned staging tables")
        if failed > 0:
            print(f"Warning: {failed} staging tables could not be cleaned up")

    def run(self, max_retries: int = 0) -> dict[str, Any]:
        """
        Execute the ETL pipeline with batch lifecycle tracking.

        Args:
            max_retries: Maximum number of retries for retriable errors (default: 0, no retry).
                         For production, use Databricks Jobs retry instead.

        Returns:
            dict: Summary of the pipeline run including rows_read and rows_written.
        """
        print(f"Starting pipeline for {self.config.table_name}")

        # Clean up orphaned staging tables (only once per session to avoid repeated overhead)
        global _staging_cleanup_done_this_session
        if self.cleanup_manager and not _staging_cleanup_done_this_session:
            self.cleanup_orphaned_staging_tables()
            _staging_cleanup_done_this_session = True

        batch_id = str(uuid.uuid4())

        # Start metrics collection
        if self.metrics_collector:
            self.metrics_collector.start_collection()

        source_versions = {}
        active_dfs = {}
        total_rows_read = 0
        total_rows_written = 0

        # Start batch tracking for each source
        # Start batch tracking for all sources (bulk)
        source_names = [s.name for s in self.config.sources]
        self.etl_control.batch_start_all(self.config.table_name, source_names)

        # Stage timing
        stage_start = time.time()

        try:
            # 1. Load Sources
            for source in self.config.sources:
                # Get latest version for watermark commit later
                latest_v = self.loader.get_latest_version(source.name)
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
                        print(
                            f"No watermark for {source.name}. Performing Full Snapshot."
                        )
                        df = self.loader.load_full_snapshot(
                            source.name, format=source.format, options=source.options
                        )
                    else:
                        if wm >= latest_v:
                            print(
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
                        print(f"Loading {source.name} from version {wm + 1}")
                        # Pass primary_keys for deduplication to handle multiple updates to same row
                        df = self.loader.load_cdf(
                            source.name, wm + 1, deduplicate_keys=source.primary_keys
                        )
                else:
                    raise ValueError(f"Unknown CDC strategy: {source.cdc_strategy}")

                # Note: We no longer call df.count() here as it forces eager evaluation.
                # Row counts will be inferred from merge metrics after the operation completes.

                # Register Temp View
                df.createOrReplaceTempView(source.alias)
                active_dfs[source.name] = df

            # Track sources loaded timing
            if self.metrics_collector:
                self.metrics_collector.add_operation_metric(
                    "sources_loaded",
                    duration_ms=(time.time() - stage_start) * 1000,
                    sources_count=len(active_dfs),
                )
                stage_start = time.time()

            # 2. Early Arriving Facts (Skeleton Generation)
            if self.config.early_arriving_facts:
                print("Checking for Early Arriving Facts...")
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
                        )
                    else:
                        print(
                            f"Warning: Could not find source with column {eaf['fact_join_key']} for skeleton generation."
                        )

            # 3. Transformation
            if self.config.transformation_sql:
                print("Executing Transformation SQL...")
                transformed_df = spark.sql(self.config.transformation_sql)
            else:
                # No transformation SQL - use source data directly
                if len(self.config.sources) == 1:
                    source_name = self.config.sources[0].name
                    transformed_df = active_dfs[source_name]
                    print(
                        f"Using source data directly (no transformation): {source_name}"
                    )
                else:
                    raise ValueError(
                        "transformation_sql is required for multi-source pipelines"
                    )

                # Kimball-proper: Handle NULL foreign keys using explicit FK declarations
                # This replaces the old naming convention hack (endswith "_sk")
                if self.config.foreign_keys:
                    from pyspark.sql.types import StringType

                    sk_fill_map: dict[str, Any] = {}
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
                            # Use string default for string-typed SKs, numeric otherwise
                            if isinstance(field.dataType, StringType):
                                sk_fill_map[col_name] = str(default_val)
                            else:
                                sk_fill_map[col_name] = default_val
                        else:
                            print(
                                f"Warning: Foreign key column '{col_name}' not found in transformed DataFrame"
                            )

                    if sk_fill_map:
                        print(f"Filling NULL foreign keys with defaults: {sk_fill_map}")
                        transformed_df = transformed_df.fillna(sk_fill_map)
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
            if transformed_df.isEmpty():
                print("No data to merge. Skipping.")
            else:
                # Ensure table exists and has defaults (if SCD2)
                # We need to create the table if it doesn't exist to seed defaults
                table_created = False
                if not spark.catalog.tableExists(self.config.table_name):
                    print(f"Creating table {self.config.table_name}...")
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
                            print(f"Auto-clustering on natural keys: {cluster_cols}")

                    # Create Delta table with optional Liquid Clustering
                    self.table_creator.create_table_with_clustering(
                        table_name=self.config.table_name,
                        schema_df=schema_df,
                        cluster_by=cluster_cols or [],
                        surrogate_key_col=self.config.surrogate_key,
                        surrogate_key_strategy=self.config.surrogate_key_strategy,
                    )
                    table_created = True

                # Seed default rows (-1, -2, -3) ONLY on table creation (not every run)
                if table_created and self.config.table_type == "dimension":
                    target_schema = spark.table(self.config.table_name).schema
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
                print("Creating DataFrame checkpoint for merge operation...")

                # Checkpoint optimization: Only use expensive checkpoint() when explicitly enabled
                # Default to localCheckpoint() which is much more efficient for standard pipelines
                if getattr(self.config, "enable_lineage_truncation", False):
                    # Use reliable checkpoint() only when lineage truncation is explicitly requested
                    try:
                        checkpoint_dir = spark.sparkContext.getCheckpointDir()
                        if checkpoint_dir:
                            print(
                                f"Using reliable checkpoint directory: {checkpoint_dir}"
                            )
                            checkpointed_df = transformed_df.checkpoint()
                        else:
                            print(
                                "No checkpoint directory configured, using local checkpoint"
                            )
                            checkpointed_df = transformed_df.localCheckpoint()
                    except PYSPARK_EXCEPTION_BASE as e:
                        # Log specific exception and fallback to local checkpoint
                        print(
                            f"Checkpoint directory access failed with PySpark error: {e}"
                        )
                        print("Using local checkpoint (less reliable)")
                        checkpointed_df = transformed_df.localCheckpoint()
                    except Exception as e:
                        # Log any other unexpected errors and fallback to local checkpoint
                        print(f"Unexpected error during checkpoint setup: {e}")
                        print("Using local checkpoint (less reliable)")
                        checkpointed_df = transformed_df.localCheckpoint()
                else:
                    # Use efficient localCheckpoint() by default - no disk I/O overhead
                    checkpointed_df = transformed_df.localCheckpoint()
                    print("Using local checkpoint (efficient, no lineage truncation)")

                # Column pruning: Select only columns that exist in target schema
                # Only perform pruning if target table already exists AND schema evolution is disabled
                if (
                    spark.catalog.tableExists(self.config.table_name)
                    and not self.config.schema_evolution
                ):
                    target_schema = spark.table(self.config.table_name).schema
                    target_columns = [f.name for f in target_schema.fields]

                    # System columns that must always be included for proper merge operations
                    # Even if they don't exist in target yet (schema evolution will add them)
                    SYSTEM_COLUMNS = {
                        "__is_current",
                        "__valid_from",
                        "__valid_to",
                        "__etl_processed_at",
                        "__is_deleted",
                    }

                    # Include system columns even if not in target schema
                    columns_to_select = []
                    for c in checkpointed_df.columns:
                        if c in target_columns or c in SYSTEM_COLUMNS:
                            columns_to_select.append(c)

                    source_df = checkpointed_df.select(
                        *[col(c) for c in columns_to_select]
                    )
                    print(
                        f"Applied column pruning: kept {len(columns_to_select)}/{len(checkpointed_df.columns)} columns (including system columns)"
                    )
                else:
                    # First run or schema evolution enabled - use all columns
                    source_df = checkpointed_df

                print(
                    f"Merging directly from checkpointed DataFrame into {self.config.table_name}..."
                )

                # Kimball: Dimensions use natural_keys for merge; Facts use merge_keys (degenerate dimensions)
                if self.config.table_type == "fact":
                    join_keys = self.config.merge_keys or []
                else:
                    join_keys = self.config.natural_keys or []

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
                )

                # 5. Optimize Table (if configured)
                if self.config.optimize_after_merge:
                    self.merger.optimize_table(
                        self.config.table_name, self.config.cluster_by or []
                    )

            # Get row counts from Delta merge metrics (more accurate than counting)
            merge_metrics = self.merger.get_last_merge_metrics(self.config.table_name)
            total_rows_read = int(merge_metrics.get("numSourceRows", 0))
            total_rows_written = int(
                merge_metrics.get("numTargetRowsInserted", 0)
            ) + int(merge_metrics.get("numTargetRowsUpdated", 0))

            # Track merge timing
            if self.metrics_collector:
                self.metrics_collector.add_operation_metric(
                    "merge",
                    duration_ms=(time.time() - stage_start) * 1000,
                    rows_read=total_rows_read,
                    rows_written=total_rows_written,
                )

            # 6. Commit Watermarks with batch completion
            print("Completing batches and updating watermarks...")
            for source_name, version in source_versions.items():
                # Calculate per-source rows (approximate: divide by number of sources)
                per_source_rows = total_rows_read // max(len(source_versions), 1)
                per_source_written = total_rows_written // max(len(source_versions), 1)

                self.etl_control.batch_complete(
                    target_table=self.config.table_name,
                    source_table=source_name,
                    new_version=version,
                    rows_read=per_source_rows,
                    rows_written=per_source_written,
                )

            print(
                f"Pipeline completed successfully. Read: {total_rows_read}, Written: {total_rows_written}"
            )

            # Collect final metrics
            metrics_summary = {}
            if self.metrics_collector:
                self.metrics_collector.stop_collection()
                metrics_summary = self.metrics_collector.get_summary()
                print(f"Query Metrics: {metrics_summary}")

            # Clear checkpoints on success
            if self.checkpoint_manager:
                self.checkpoint_manager.clear_checkpoint(batch_id, "sources_loaded")
                self.checkpoint_manager.clear_checkpoint(
                    batch_id, "transformation_complete"
                )

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
            print(f"Pipeline failed: {error_msg}")

            for source in self.config.sources:
                try:
                    self.etl_control.batch_fail(
                        self.config.table_name, source.name, error_msg
                    )
                except Exception:
                    pass  # Don't mask original error

            # Re-raise based on error type
            if isinstance(e, RetriableError) and max_retries > 0:
                print(f"Retriable error encountered. Retries remaining: {max_retries}")
                time.sleep(30)  # Backoff before retry
                return self.run(max_retries=max_retries - 1)

            raise

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
                    print(
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
