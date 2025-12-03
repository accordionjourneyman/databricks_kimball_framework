import uuid
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, LongType
from pyspark.sql.functions import lit, current_timestamp
from kimball.config import ConfigLoader, TableConfig
from kimball.watermark import WatermarkManager
from kimball.loader import DataLoader
from kimball.merger import DeltaMerger
from kimball.skeleton_generator import SkeletonGenerator
from kimball.table_creator import TableCreator
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
    """

    def __init__(self, config_path: str, watermark_database: str = None):
        """
        Args:
            config_path: Path to the YAML config file for this pipeline.
            watermark_database: Database/schema where the watermark table will be stored.
                                If not provided, defaults to the target table's database
                                (e.g. 'demo_gold' for 'demo_gold.dim_customer').
        """
        self.config_loader = ConfigLoader()
        self.config = self.config_loader.load_config(config_path)

        # Derive watermark database from target table if not explicitly provided
        if watermark_database is None:
            # Extract database from table_name (e.g. 'demo_gold.dim_customer' -> 'demo_gold')
            if "." in self.config.table_name:
                watermark_database = self.config.table_name.split(".")[0]
            else:
                raise ValueError(
                    "watermark_database must be specified when target table_name "
                    "is not fully-qualified (e.g. 'demo_gold.dim_customer')."
                )

        self.watermark_manager = WatermarkManager(database=watermark_database)
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

    def run(self):
        print(f"Starting pipeline for {self.config.table_name}")
        batch_id = str(uuid.uuid4())
        
        source_versions = {}
        active_dfs = {}

        # 1. Load Sources
        for source in self.config.sources:
            # Get latest version for watermark commit later
            latest_v = self.loader.get_latest_version(source.name)
            source_versions[source.name] = latest_v
            
            # Determine Load Strategy
            if source.cdc_strategy == "full":
                df = self.loader.load_full_snapshot(source.name)
            elif source.cdc_strategy == "cdf":
                wm = self.watermark_manager.get_watermark(self.config.table_name, source.name)
                if wm is None:
                    print(f"No watermark for {source.name}. Performing Full Snapshot.")
                    df = self.loader.load_full_snapshot(source.name)
                else:
                    print(f"Loading {source.name} from version {wm + 1}")
                    df = self.loader.load_cdf(source.name, wm + 1)
            else:
                raise ValueError(f"Unknown CDC strategy: {source.cdc_strategy}")

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
            # Ensure foreign surrogate-key columns produced by joins are not left NULL.
            # Coalesce any column that looks like a surrogate key (ends with '_sk')
            # to the standard default (-1). Use string '-1' for string-typed SKs.
            sk_fill_map = {}
            for f in transformed_df.schema.fields:
                if f.name.endswith("_sk"):
                    # Choose fill value based on field type
                    from pyspark.sql.types import StringType
                    if isinstance(f.dataType, StringType):
                        sk_fill_map[f.name] = "-1"
                    else:
                        # numeric surrogate keys (Long/Integer) or others -> use -1
                        sk_fill_map[f.name] = -1
            if sk_fill_map:
                transformed_df = transformed_df.fillna(sk_fill_map)
        else:
            # Fallback: If only 1 source, just use it. Else error.
            if len(self.config.sources) == 1:
                transformed_df = active_dfs[self.config.sources[0].name]
            else:
                raise ValueError("transformation_sql is required for multi-source pipelines")

        # 3. Merge
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
                        self.config.default_rows
                    )

            print(f"Merging into {self.config.table_name}...")

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

            # 4. Optimize Table (if configured)
            if self.config.optimize_after_merge:
                self.merger.optimize_table(
                    self.config.table_name,
                    self.config.cluster_by
                )

            # 5. Commit Watermarks
            print("Updating Watermarks...")
            for source_name, version in source_versions.items():
                self.watermark_manager.update_watermark(
                    self.config.table_name, 
                    source_name, 
                    version
                )
        
        print("Pipeline completed successfully.")
