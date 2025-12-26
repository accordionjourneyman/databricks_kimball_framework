import uuid
import time
import os
import warnings
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, LongType
from pyspark.sql.functions import lit, current_timestamp
from kimball.config import ConfigLoader, TableConfig
from kimball.watermark import ETLControlManager, get_etl_schema
from kimball.loader import DataLoader
from kimball.merger import DeltaMerger
from kimball.skeleton_generator import SkeletonGenerator
from kimball.table_creator import TableCreator
from kimball.errors import RetriableError, NonRetriableError, KimballError
from databricks.sdk.runtime import spark
from pyspark.sql import SparkSession
from typing import Dict, Any, Optional
import json

class QueryMetricsCollector:
    """
    Collects basic query execution metrics for observability.
    Uses DataFrame operations and timing instead of Spark listeners.
    """

    def __init__(self):
        self.metrics = []
        self.start_time = None

    def start_collection(self):
        """Start collecting query execution metrics."""
        self.start_time = time.time()
        self.metrics = []

    def add_operation_metric(self, operation_name: str, df=None, **kwargs):
        """Add a metric for a specific operation."""
        try:
            metric = {
                'operation': operation_name,
                'timestamp': time.time(),
                'duration_ms': kwargs.get('duration_ms', 0),
            }

            # Add DataFrame metrics if available
            if df is not None:
                try:
                    # Get basic stats without collecting data
                    if hasattr(df, '_jdf'):
                        # Try to get some basic metrics from the logical plan
                        metric['has_logical_plan'] = True
                    else:
                        metric['has_logical_plan'] = False
                except:
                    metric['has_logical_plan'] = False

            # Add any additional metrics passed
            metric.update(kwargs)
            self.metrics.append(metric)

        except Exception as e:
            print(f"Failed to collect metric for {operation_name}: {e}")

    def stop_collection(self):
        """Stop collecting metrics and return collected data."""
        if self.start_time:
            total_duration = time.time() - self.start_time
            self.add_operation_metric('total_pipeline', duration_ms=total_duration * 1000)
        return self.metrics

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of collected metrics."""
        if not self.metrics:
            return {}

        total_time = sum(m.get('duration_ms', 0) for m in self.metrics)
        operation_count = len([m for m in self.metrics if m.get('operation') != 'total_pipeline'])

        return {
            'total_operations': operation_count,
            'total_execution_time_ms': total_time,
            'operations': self.metrics,
            'avg_operation_time_ms': total_time / operation_count if operation_count > 0 else 0
        }

class StagingCleanupManager:
    """
    Manages cleanup of staging tables with crash resilience.
    Registers staging tables in persistent storage and provides cleanup methods.
    """
    
    def __init__(self, cleanup_registry_path: str = None):
        # Use configurable path for cleanup registry
        if cleanup_registry_path is None:
            cleanup_registry_path = os.getenv("KIMBALL_CLEANUP_REGISTRY", "/dbfs/kimball/staging_cleanup.json")
        
        self.cleanup_registry_path = cleanup_registry_path
        self.active_staging_tables = set()
        self._load_registry()
    
    def _load_registry(self):
        """Load existing cleanup registry."""
        try:
            if os.path.exists(self.cleanup_registry_path):
                with open(self.cleanup_registry_path, 'r') as f:
                    registry = json.load(f)
                    self.active_staging_tables = set(registry.get('active_tables', []))
        except Exception as e:
            print(f"Warning: Failed to load cleanup registry: {e}")
            self.active_staging_tables = set()
    
    def _save_registry(self):
        """Save cleanup registry to persistent storage."""
        try:
            registry = {
                'active_tables': list(self.active_staging_tables),
                'last_updated': time.time()
            }
            with open(self.cleanup_registry_path, 'w') as f:
                json.dump(registry, f, indent=2)
        except Exception as e:
            print(f"Warning: Failed to save cleanup registry: {e}")
    
    def register_staging_table(self, table_name: str):
        """Register a staging table for cleanup."""
        self.active_staging_tables.add(table_name)
        self._save_registry()
        print(f"Registered staging table for cleanup: {table_name}")
    
    def unregister_staging_table(self, table_name: str):
        """Unregister a staging table after successful cleanup."""
        self.active_staging_tables.discard(table_name)
        self._save_registry()
    
    def cleanup_staging_tables(self, spark_session=None):
        """Clean up all registered staging tables."""
        if spark_session is None:
            from databricks.sdk.runtime import spark
            spark_session = spark
        
        cleaned_count = 0
        failed_count = 0
        
        for table_name in list(self.active_staging_tables):
            try:
                spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
                self.unregister_staging_table(table_name)
                cleaned_count += 1
                print(f"Cleaned up staging table: {table_name}")
            except Exception as e:
                failed_count += 1
                print(f"Failed to cleanup staging table {table_name}: {e}")
        
        print(f"Staging cleanup complete: {cleaned_count} cleaned, {failed_count} failed")
        return cleaned_count, failed_count

class PipelineCheckpoint:
    """
    Handles checkpointing for complex DAG pipelines.
    Saves and restores pipeline state to enable resumability.
    
    Uses persistent storage (DBFS) instead of ephemeral /tmp for reliability.
    """
    
    def __init__(self, checkpoint_dir: str = None):
        # Use configurable persistent storage instead of hardcoded /dbfs
        # Allow override via environment variable for different environments
        if checkpoint_dir is None:
            checkpoint_dir = os.getenv("KIMBALL_CHECKPOINT_DIR", "/dbfs/kimball/checkpoints")

        self.checkpoint_dir = checkpoint_dir
        os.makedirs(checkpoint_dir, exist_ok=True)
        
    def save_checkpoint(self, pipeline_id: str, stage: str, state: Dict[str, Any]):
        """Save pipeline state at a specific stage."""
        checkpoint_path = os.path.join(self.checkpoint_dir, f"{pipeline_id}_{stage}.json")
        checkpoint_data = {
            'pipeline_id': pipeline_id,
            'stage': stage,
            'timestamp': time.time(),
            'state': state
        }
        
        with open(checkpoint_path, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
            
        print(f"Checkpoint saved: {checkpoint_path}")
        
    def load_checkpoint(self, pipeline_id: str, stage: str) -> Optional[Dict[str, Any]]:
        """Load pipeline state from a checkpoint."""
        checkpoint_path = os.path.join(self.checkpoint_dir, f"{pipeline_id}_{stage}.json")
        
        if not os.path.exists(checkpoint_path):
            return None
            
        try:
            with open(checkpoint_path, 'r') as f:
                data = json.load(f)
            print(f"Checkpoint loaded: {checkpoint_path}")
            return data.get('state')
        except Exception as e:
            print(f"Failed to load checkpoint {checkpoint_path}: {e}")
            return None
            
    def clear_checkpoint(self, pipeline_id: str, stage: str):
        """Clear a checkpoint file."""
        checkpoint_path = os.path.join(self.checkpoint_dir, f"{pipeline_id}_{stage}.json")
        
        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)
            print(f"Checkpoint cleared: {checkpoint_path}")

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
        etl_schema: str = None,
        # Deprecated parameter - for backward compatibility
        watermark_database: str = None,
        enable_metrics: bool = True,
        checkpoint_dir: str = "/tmp/kimball_checkpoints"
    ):
        """
        Args:
            config_path: Path to the YAML config file for this pipeline.
            etl_schema: Schema where the ETL control table is stored.
                        If not provided, uses KIMBALL_ETL_SCHEMA environment variable.
                        Falls back to target table's database if neither is set.
            watermark_database: **Deprecated** - Use etl_schema instead.
            enable_metrics: Whether to collect QueryExecution metrics.
            checkpoint_dir: Directory for pipeline checkpoints.
        """
        self.config_loader = ConfigLoader()
        self.config = self.config_loader.load_config(config_path)
        
        # Enable Photon and AQE for performance
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # Handle deprecated 'watermark_database' parameter
        if watermark_database is not None:
            warnings.warn(
                "The 'watermark_database' parameter is deprecated. Use 'etl_schema' instead, "
                "or set KIMBALL_ETL_SCHEMA environment variable.",
                DeprecationWarning,
                stacklevel=2
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

        self.etl_control = ETLControlManager(etl_schema=etl_schema)
        self.loader = DataLoader()
        self.merger = DeltaMerger()
        self.skeleton_generator = SkeletonGenerator()
        self.table_creator = TableCreator()
        
        # Initialize observability and resilience features
        self.metrics_collector = QueryMetricsCollector() if enable_metrics else None
        self.checkpoint_manager = PipelineCheckpoint(checkpoint_dir)
        self.cleanup_manager = StagingCleanupManager()

    def _add_system_columns(self, df, scd_type: int, surrogate_key: str, surrogate_key_strategy: str):
        """
        Add system/audit columns to a DataFrame for table creation.
        SCD1: __etl_processed_at, __etl_batch_id, __is_deleted (for soft deletes)
        SCD2: above + __is_current, __valid_from, __valid_to, hashdiff
        """
        # Common audit columns
        result_df = df.withColumn("__etl_processed_at", current_timestamp())
        result_df = result_df.withColumn("__etl_batch_id", lit(None).cast(StringType()))
        result_df = result_df.withColumn("__is_deleted", lit(False))
        
        if scd_type == 2:
            # SCD2 specific columns
            result_df = result_df.withColumn("__is_current", lit(True))
            result_df = result_df.withColumn("__valid_from", current_timestamp())
            result_df = result_df.withColumn("__valid_to", lit(None).cast(TimestampType()))
            result_df = result_df.withColumn("hashdiff", lit(None).cast(StringType()))
        
        # Add surrogate key column according to strategy
        if surrogate_key:
            if surrogate_key_strategy in ("identity", "sequence"):
                # numeric surrogate key
                result_df = result_df.withColumn(surrogate_key, lit(None).cast(LongType()))
            else:
                # default to string for hash or unknown strategies
                result_df = result_df.withColumn(surrogate_key, lit(None).cast(StringType()))

        return result_df

    def cleanup_orphaned_staging_tables(self):
        """
        Clean up any orphaned staging tables from previous crashed runs.
        Should be called at the beginning of pipeline execution.
        """
        print("Checking for orphaned staging tables from previous runs...")
        cleaned, failed = self.cleanup_manager.cleanup_staging_tables()
        if cleaned > 0:
            print(f"Cleaned up {cleaned} orphaned staging tables")
        if failed > 0:
            print(f"Warning: {failed} staging tables could not be cleaned up")

    def run(self, max_retries: int = 0):
        """
        Execute the ETL pipeline with batch lifecycle tracking.
        
        Args:
            max_retries: Maximum number of retries for retriable errors (default: 0, no retry).
                         For production, use Databricks Jobs retry instead.
        
        Returns:
            dict: Summary of the pipeline run including rows_read and rows_written.
        """
        print(f"Starting pipeline for {self.config.table_name}")
        
        # Clean up any orphaned staging tables from crashed runs
        self.cleanup_orphaned_staging_tables()
        
        batch_id = str(uuid.uuid4())
        
        # Start metrics collection
        if self.metrics_collector:
            self.metrics_collector.start_collection()
        
        source_versions = {}
        active_dfs = {}
        total_rows_read = 0
        total_rows_written = 0
        
        # Start batch tracking for each source
        for source in self.config.sources:
            self.etl_control.batch_start(self.config.table_name, source.name)

        try:
            # 1. Load Sources
            for source in self.config.sources:
                # Get latest version for watermark commit later
                latest_v = self.loader.get_latest_version(source.name)
                source_versions[source.name] = latest_v
                
                # Determine Load Strategy
                if source.cdc_strategy == "full":
                    df = self.loader.load_full_snapshot(source.name)
                elif source.cdc_strategy == "cdf":
                    wm = self.etl_control.get_watermark(self.config.table_name, source.name)
                    if wm is None:
                        print(f"No watermark for {source.name}. Performing Full Snapshot.")
                        df = self.loader.load_full_snapshot(source.name)
                    else:
                        if wm >= latest_v:
                            print(f"Source {source.name} already at version {latest_v}. Skipping.")
                            # Mark batch complete with no changes
                            self.etl_control.batch_complete(
                                self.config.table_name, source.name,
                                new_version=latest_v, rows_read=0, rows_written=0
                            )
                            continue
                        print(f"Loading {source.name} from version {wm + 1}")
                        # Pass primary_keys for deduplication to handle multiple updates to same row
                        df = self.loader.load_cdf(
                            source.name, 
                            wm + 1,
                            deduplicate_keys=source.primary_keys
                        )
                else:
                    raise ValueError(f"Unknown CDC strategy: {source.cdc_strategy}")

                # Track rows read
                rows_read = df.count()
                total_rows_read += rows_read
                print(f"  Loaded {rows_read} rows from {source.name}")
                
                # Register Temp View
                df.createOrReplaceTempView(source.alias)
                active_dfs[source.name] = df

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
                        if eaf['fact_join_key'] in df.columns:
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
                            dim_table_name=eaf['dimension_table'],
                            fact_join_key=eaf['fact_join_key'],
                            dim_join_key=eaf['dimension_join_key'],
                            surrogate_key_col=eaf.get('surrogate_key_col', 'surrogate_key'),
                            surrogate_key_strategy=eaf.get('surrogate_key_strategy', 'identity')
                        )
                    else:
                        print(f"Warning: Could not find source with column {eaf['fact_join_key']} for skeleton generation.")

            # 3. Transformation
            if self.config.transformation_sql:
                print("Executing Transformation SQL...")
                transformed_df = spark.sql(self.config.transformation_sql)
                
                # Kimball-proper: Handle NULL foreign keys using explicit FK declarations
                # This replaces the old naming convention hack (endswith "_sk")
                if self.config.foreign_keys:
                    from pyspark.sql.types import StringType
                    sk_fill_map = {}
                    for fk in self.config.foreign_keys:
                        col_name = fk.column
                        default_val = fk.default_value
                        # Check if column exists in the transformed DataFrame
                        field = next((f for f in transformed_df.schema.fields if f.name == col_name), None)
                        if field:
                            # Use string default for string-typed SKs, numeric otherwise
                            if isinstance(field.dataType, StringType):
                                sk_fill_map[col_name] = str(default_val)
                            else:
                                sk_fill_map[col_name] = default_val
                        else:
                            print(f"Warning: Foreign key column '{col_name}' not found in transformed DataFrame")
                    
                    if sk_fill_map:
                        print(f"Filling NULL foreign keys with defaults: {sk_fill_map}")
                        transformed_df = transformed_df.fillna(sk_fill_map)
            # Checkpoint: Transformation complete
            checkpoint_state = {
                'stage': 'transformation_complete', 
                'total_rows_read': total_rows_read,
                'active_sources': list(active_dfs.keys())
            }
            self.checkpoint_manager.save_checkpoint(batch_id, 'transformation_complete', checkpoint_state)

            # 4. Stage-then-Merge for concurrency resilience
            # Check if we have data to merge
            if transformed_df.isEmpty():
                print("No data to merge. Skipping.")
            else:
                # Ensure table exists and has defaults (if SCD2)
                # We need to create the table if it doesn't exist to seed defaults
                if not spark.catalog.tableExists(self.config.table_name):
                    print(f"Creating table {self.config.table_name}...")
                    # Add system columns to transformed_df schema for table creation
                    schema_df = self._add_system_columns(
                        transformed_df.limit(0),
                        self.config.scd_type,
                        self.config.surrogate_key,
                        self.config.surrogate_key_strategy,
                    )
                    
                    # Create Delta table with optional Liquid Clustering
                    self.table_creator.create_table_with_clustering(
                        table_name=self.config.table_name,
                        schema_df=schema_df,
                        cluster_by=self.config.cluster_by,
                        surrogate_key_col=self.config.surrogate_key,
                        surrogate_key_strategy=self.config.surrogate_key_strategy
                    )

                # Seed default rows (-1, -2, -3) for DIMENSION tables only (not facts)
                if self.config.table_type == 'dimension' and spark.catalog.tableExists(self.config.table_name):
                    target_schema = spark.table(self.config.table_name).schema
                    if self.config.scd_type == 2:
                        self.merger.ensure_scd2_defaults(
                            self.config.table_name, 
                            target_schema, 
                            self.config.surrogate_key,
                            self.config.default_rows
                        )
                    elif self.config.scd_type == 1 and self.config.surrogate_key:
                        self.merger.ensure_scd1_defaults(
                            self.config.table_name, 
                            target_schema, 
                            self.config.surrogate_key,
                            self.config.default_rows,
                            self.config.surrogate_key_strategy
                        )

                # Stage the transformed data to minimize lock time
                # Handle multi-part table names (catalog.schema.table) properly
                parts = self.config.table_name.split(".")
                parts[-1] = f"{parts[-1]}_staging_{batch_id.replace('-', '_')}"
                staging_table = ".".join(parts)
                try:
                    print(f"Staging data to {staging_table}...")
                    transformed_df.write.format("delta").mode("overwrite").saveAsTable(staging_table)
                    
                    # Register staging table for cleanup (crash resilience)
                    self.cleanup_manager.register_staging_table(staging_table)

                    # Column pruning: Select only columns that exist in target schema
                    # Only perform pruning if target table already exists
                    if spark.catalog.tableExists(self.config.table_name):
                        target_schema = spark.table(self.config.table_name).schema
                        target_columns = [f.name for f in target_schema.fields]
                        staging_df = spark.table(staging_table).select(*[col(c) for c in spark.table(staging_table).columns if c in target_columns])
                    else:
                        # First run - no target table exists yet, use all staging columns
                        staging_df = spark.table(staging_table)

                    print(f"Merging from {staging_table} into {self.config.table_name}...")

                    # Kimball: Dimensions use natural_keys for merge; Facts use merge_keys (degenerate dimensions)
                    if self.config.table_type == 'fact':
                        join_keys = self.config.merge_keys or []
                    else:
                        join_keys = self.config.natural_keys or []

                    self.merger.merge(
                        target_table_name=self.config.table_name,
                        source_df=staging_df,
                        join_keys=join_keys, 
                        delete_strategy=self.config.delete_strategy,
                        batch_id=batch_id,
                        scd_type=self.config.scd_type,
                        track_history_columns=self.config.track_history_columns,
                        surrogate_key_col=self.config.surrogate_key,
                        surrogate_key_strategy=self.config.surrogate_key_strategy,
                        schema_evolution=self.config.schema_evolution
                    )
                finally:
                    # Clean up staging table - ensure this runs even if merge fails
                    spark.sql(f"DROP TABLE IF EXISTS {staging_table}")
                    # Unregister from cleanup manager after successful cleanup
                    self.cleanup_manager.unregister_staging_table(staging_table)

                # 5. Optimize Table (if configured)
                if self.config.optimize_after_merge:
                    self.merger.optimize_table(
                        self.config.table_name,
                        self.config.cluster_by
                    )

            # Track rows written (approximate from transformed_df count)
            total_rows_written = transformed_df.count()

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
                    rows_written=per_source_written
                )
            
            print(f"Pipeline completed successfully. Read: {total_rows_read}, Written: {total_rows_written}")
            
            # Collect final metrics
            metrics_summary = {}
            if self.metrics_collector:
                self.metrics_collector.stop_collection()
                metrics_summary = self.metrics_collector.get_summary()
                print(f"Query Metrics: {metrics_summary}")
            
            # Clear checkpoints on success
            self.checkpoint_manager.clear_checkpoint(batch_id, 'sources_loaded')
            self.checkpoint_manager.clear_checkpoint(batch_id, 'transformation_complete')
            
            return {
                "status": "SUCCESS",
                "batch_id": batch_id,
                "target_table": self.config.table_name,
                "rows_read": total_rows_read,
                "rows_written": total_rows_written,
                "metrics": metrics_summary
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
                        self.config.table_name,
                        source.name,
                        error_msg
                    )
                except Exception:
                    pass  # Don't mask original error
            
            # Re-raise based on error type
            if isinstance(e, RetriableError) and max_retries > 0:
                print(f"Retriable error encountered. Retries remaining: {max_retries}")
                time.sleep(30)  # Backoff before retry
                return self.run(max_retries=max_retries - 1)
            
            raise

    def run_with_retry(self, max_retries: int = 3, backoff_seconds: int = 30):
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
                    print(f"Retriable error: {e}. Waiting {wait_time}s before retry {attempt}/{max_retries}")
                    time.sleep(wait_time)
                else:
                    raise
            except NonRetriableError:
                raise  # Don't retry, fail immediately
            except Exception:
                raise  # Unknown errors don't retry
        
        raise last_error
