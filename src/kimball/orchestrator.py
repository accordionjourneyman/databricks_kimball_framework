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
    Manages cleanup of staging tables with crash resilience using Delta table registry.
    Provides ACID-compliant registry to prevent race conditions in multi-pipeline environments.
    """

    def __init__(self, registry_table: str = None):
        # Use Delta table for registry instead of JSON file to prevent race conditions
        if registry_table is None:
            registry_table = os.getenv("KIMBALL_CLEANUP_REGISTRY_TABLE", "default.kimball_staging_registry")

        self.registry_table = registry_table
        self._ensure_registry_table()

    def _ensure_registry_table(self):
        """Ensure the registry Delta table exists."""
        if not spark.catalog.tableExists(self.registry_table):
            # Create registry table with proper schema
            spark.sql(f"""
                CREATE TABLE {self.registry_table} (
                    pipeline_id STRING,
                    staging_table STRING,
                    created_at TIMESTAMP,
                    batch_id STRING
                )
                USING DELTA
                TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
            """)
            print(f"Created staging cleanup registry table: {self.registry_table}")

    def register_staging_table(self, staging_table: str, pipeline_id: str = None, batch_id: str = None):
        """Register a staging table for cleanup using Delta table."""
        from pyspark.sql.functions import current_timestamp

        # Use MERGE for atomic registration (handles concurrent writes)
        spark.sql(f"""
            MERGE INTO {self.registry_table} target
            USING (SELECT '{pipeline_id or 'unknown'}' as pipeline_id,
                          '{staging_table}' as staging_table,
                          current_timestamp() as created_at,
                          '{batch_id or 'unknown'}' as batch_id) source
            ON target.staging_table = source.staging_table
            WHEN NOT MATCHED THEN INSERT *
        """)

        print(f"Registered staging table for cleanup: {staging_table}")

    def unregister_staging_table(self, staging_table: str):
        """Unregister a staging table after successful cleanup."""
        spark.sql(f"DELETE FROM {self.registry_table} WHERE staging_table = '{staging_table}'")
        print(f"Unregistered staging table from cleanup: {staging_table}")

    def cleanup_staging_tables(self, spark_session=None, pipeline_id: str = None, max_age_hours: int = 24):
        """Clean up orphaned staging tables using TTL-based filtering."""
        if spark_session is None:
            spark_session = spark

        # Build WHERE clause for TTL-based cleanup
        where_conditions = []
        
        # Only clean tables older than max_age_hours to avoid interfering with concurrent pipelines
        if max_age_hours > 0:
            from pyspark.sql.functions import current_timestamp, expr
            age_filter = f"current_timestamp() - created_at > INTERVAL {max_age_hours} HOURS"
            where_conditions.append(age_filter)
        
        # Optional pipeline-specific filtering
        if pipeline_id:
            where_conditions.append(f"pipeline_id = '{pipeline_id}'")
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

        # Get tables to clean up
        cleanup_df = spark.sql(f"""
            SELECT staging_table FROM {self.registry_table}
            WHERE {where_clause}
        """)

        cleaned_count = 0
        failed_count = 0

        for row in cleanup_df.collect():
            staging_table = row.staging_table
            try:
                spark_session.sql(f"DROP TABLE IF EXISTS {staging_table}")
                self.unregister_staging_table(staging_table)
                cleaned_count += 1
                print(f"Cleaned up orphaned staging table: {staging_table}")
            except Exception as e:
                failed_count += 1
                print(f"Failed to clean up {staging_table}: {e}")

        print(f"TTL-based staging cleanup complete: {cleaned_count} cleaned, {failed_count} failed")
        return cleaned_count, failed_count

class PipelineCheckpoint:
    """
    Handles checkpointing for complex DAG pipelines using Delta table for ACID compliance.
    Saves and restores pipeline state to enable resumability with atomic guarantees.
    """

    def __init__(self, checkpoint_table: str = None):
        # Use Delta table for atomic checkpoint storage
        if checkpoint_table is None:
            checkpoint_table = os.getenv("KIMBALL_CHECKPOINT_TABLE", "default.kimball_pipeline_checkpoints")

        self.checkpoint_table = checkpoint_table
        self._ensure_checkpoint_table()

    def _ensure_checkpoint_table(self):
        """Ensure the checkpoint Delta table exists."""
        if not spark.catalog.tableExists(self.checkpoint_table):
            # Create checkpoint table with proper schema
            spark.sql(f"""
                CREATE TABLE {self.checkpoint_table} (
                    pipeline_id STRING,
                    stage STRING,
                    timestamp TIMESTAMP,
                    state STRING
                )
                USING DELTA
                TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
                PARTITIONED BY (pipeline_id)
            """)
            print(f"Created pipeline checkpoint table: {self.checkpoint_table}")

    def save_checkpoint(self, pipeline_id: str, stage: str, state: Dict[str, Any]):
        """Save pipeline state at a specific stage using atomic Delta operations."""
        import json
        state_json = json.dumps(state)

        # Use MERGE for atomic checkpoint updates
        spark.sql(f"""
            MERGE INTO {self.checkpoint_table} target
            USING (SELECT '{pipeline_id}' as pipeline_id,
                          '{stage}' as stage,
                          current_timestamp() as timestamp,
                          '{state_json}' as state) source
            ON target.pipeline_id = source.pipeline_id AND target.stage = source.stage
            WHEN MATCHED THEN UPDATE SET
                timestamp = source.timestamp,
                state = source.state
            WHEN NOT MATCHED THEN INSERT *
        """)

        print(f"Checkpoint saved: {pipeline_id} -> {stage}")

    def load_checkpoint(self, pipeline_id: str, stage: str) -> Optional[Dict[str, Any]]:
        """Load pipeline state from Delta table checkpoint."""
        import json

        result_df = spark.sql(f"""
            SELECT state FROM {self.checkpoint_table}
            WHERE pipeline_id = '{pipeline_id}' AND stage = '{stage}'
            ORDER BY timestamp DESC
            LIMIT 1
        """)

        if result_df.count() == 0:
            return None

        try:
            state_json = result_df.first()['state']
            state = json.loads(state_json)
            print(f"Checkpoint loaded: {pipeline_id} -> {stage}")
            return state
        except Exception as e:
            print(f"Failed to load checkpoint {pipeline_id}:{stage}: {e}")
            return None

    def clear_checkpoint(self, pipeline_id: str, stage: str):
        """Clear a checkpoint from Delta table."""
        deleted_count = spark.sql(f"""
            DELETE FROM {self.checkpoint_table}
            WHERE pipeline_id = '{pipeline_id}' AND stage = '{stage}'
        """).count()

        if deleted_count > 0:
            print(f"Checkpoint cleared: {pipeline_id} -> {stage}")

    def list_checkpoints(self, pipeline_id: str = None) -> DataFrame:
        """List all checkpoints, optionally filtered by pipeline_id."""
        where_clause = f"WHERE pipeline_id = '{pipeline_id}'" if pipeline_id else ""
        return spark.sql(f"SELECT * FROM {self.checkpoint_table} {where_clause} ORDER BY pipeline_id, stage, timestamp DESC")

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
        checkpoint_table: str = None
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
        self.checkpoint_manager = PipelineCheckpoint(checkpoint_table)
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

    def cleanup_orphaned_staging_tables(self, pipeline_id: str = None, max_age_hours: int = 24):
        """
        Clean up orphaned staging tables from previous crashed runs.
        Only cleans tables that are older than max_age_hours to avoid interfering with concurrent pipelines.
        
        Args:
            pipeline_id: Optional pipeline ID to filter cleanup (for targeted cleanup)
            max_age_hours: Maximum age of staging tables to clean up (default 24 hours)
        """
        print(f"Checking for orphaned staging tables (max age: {max_age_hours}h)...")
        
        # Use TTL-based cleanup to avoid interfering with concurrent pipelines
        cleaned, failed = self.cleanup_manager.cleanup_staging_tables(
            pipeline_id=pipeline_id, 
            max_age_hours=max_age_hours
        )
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

                # Use DataFrame checkpointing instead of physical staging to minimize lock time
                # This provides fault tolerance without the 100% I/O overhead of physical staging
                print("Creating DataFrame checkpoint for merge operation...")
                checkpointed_df = transformed_df.localCheckpoint()

                # Column pruning: Select only columns that exist in target schema
                # Only perform pruning if target table already exists AND schema evolution is disabled
                if spark.catalog.tableExists(self.config.table_name) and not self.config.schema_evolution:
                    target_schema = spark.table(self.config.table_name).schema
                    target_columns = [f.name for f in target_schema.fields]
                    source_df = checkpointed_df.select(*[col(c) for c in checkpointed_df.columns if c in target_columns])
                else:
                    # First run or schema evolution enabled - use all columns
                    source_df = checkpointed_df

                print(f"Merging directly from checkpointed DataFrame into {self.config.table_name}...")

                # Kimball: Dimensions use natural_keys for merge; Facts use merge_keys (degenerate dimensions)
                if self.config.table_type == 'fact':
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
                    schema_evolution=self.config.schema_evolution
                )

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
