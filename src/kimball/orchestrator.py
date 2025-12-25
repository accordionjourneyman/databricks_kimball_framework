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
    ):
        """
        Args:
            config_path: Path to the YAML config file for this pipeline.
            etl_schema: Schema where the ETL control table is stored.
                        If not provided, uses KIMBALL_ETL_SCHEMA environment variable.
                        Falls back to target table's database if neither is set.
            watermark_database: **Deprecated** - Use etl_schema instead.
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
        batch_id = str(uuid.uuid4())
        
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
            else:
                # Fallback: If only 1 source, just use it. Else error.
                if len(self.config.sources) == 1:
                    transformed_df = active_dfs[self.config.sources[0].name]
                else:
                    raise ValueError("transformation_sql is required for multi-source pipelines")

            # 4. Merge
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

                print(f"Merging into {self.config.table_name}...")

                # Column pruning: Select only columns that exist in target schema
                target_schema = spark.table(self.config.table_name).schema
                target_columns = [f.name for f in target_schema.fields]
                transformed_df = transformed_df.select(*[col(c) for c in transformed_df.columns if c in target_columns])

                # Kimball: Dimensions use natural_keys for merge; Facts use merge_keys (degenerate dimensions)
                if self.config.table_type == 'fact':
                    join_keys = self.config.merge_keys or []
                else:
                    join_keys = self.config.natural_keys or []

                self.merger.merge(
                    target_table_name=self.config.table_name,
                    source_df=transformed_df,
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
            
            return {
                "status": "SUCCESS",
                "batch_id": batch_id,
                "target_table": self.config.table_name,
                "rows_read": total_rows_read,
                "rows_written": total_rows_written,
            }

        except Exception as e:
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
