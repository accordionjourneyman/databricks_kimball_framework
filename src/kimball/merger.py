from datetime import datetime, date
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit, col, when
from pyspark.sql.types import (
    StringType, IntegerType, LongType, ShortType, DoubleType, FloatType,
    BooleanType, TimestampType, DateType, DecimalType
)
from delta.tables import DeltaTable
from typing import List
import time
from functools import wraps
try:
    # Databricks Runtime 13+ - errors moved to pyspark.errors
    from pyspark.errors import PySparkException
    PYSPARK_EXCEPTION_BASE = PySparkException
except ImportError:
    # Fallback for older Databricks Runtime versions
    import pyspark.sql.utils
    PYSPARK_EXCEPTION_BASE = pyspark.sql.utils.AnalysisException
from kimball.key_generator import KeyGenerator, IdentityKeyGenerator, HashKeyGenerator, SequenceKeyGenerator
from kimball.hashing import compute_hashdiff
from functools import reduce
from databricks.sdk.runtime import spark

def retry_on_concurrent_exception(max_retries=3, backoff_base=2):
    """
    Decorator to retry merge operations on ConcurrentAppendException with exponential backoff.
    Updated for Databricks Runtime 13+ compatibility.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except PYSPARK_EXCEPTION_BASE as e:
                    # Check for concurrent write exceptions in modern Databricks Runtime
                    error_str = str(e)
                    is_concurrent = any(x in error_str for x in ["ConcurrentAppendException", "WriteConflictException"])
                    
                    # Only retry on concurrent exceptions, not all PySpark exceptions
                    if is_concurrent and attempt < max_retries:
                        wait_time = backoff_base ** attempt
                        print(f"Concurrent write detected, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries + 1})")
                        time.sleep(wait_time)
                        continue
                    raise
            return func(*args, **kwargs)
        return wrapper
    return decorator

class DeltaMerger:
    """
    Handles the MERGE operation into the target Delta table.
    Includes Audit Column injection and Delete handling.
    """

    @retry_on_concurrent_exception()
    def merge(self, 
              target_table_name: str, 
              source_df: DataFrame, 
              join_keys: List[str], 
              delete_strategy: str = "hard",
              batch_id: str = None,
              scd_type: int = 1,
              track_history_columns: List[str] = None,
              surrogate_key_col: str = "surrogate_key",
              surrogate_key_strategy: str = "identity",
              schema_evolution: bool = False):
        """
        Executes the MERGE operation.
        """
        # 1. Inject Audit Columns into Source DataFrame
        enriched_df = source_df.withColumn("__etl_processed_at", current_timestamp())
        if batch_id:
            enriched_df = enriched_df.withColumn("__etl_batch_id", lit(batch_id))

        if scd_type == 2:
            self._merge_scd2(target_table_name, enriched_df, join_keys, track_history_columns, surrogate_key_col, surrogate_key_strategy, schema_evolution)
        else:
            self._merge_scd1(target_table_name, enriched_df, join_keys, delete_strategy, schema_evolution, surrogate_key_col, surrogate_key_strategy)

    def _merge_scd1(self, target_table_name, source_df, join_keys, delete_strategy, schema_evolution, surrogate_key_col=None, surrogate_key_strategy="identity"):
        # 2. Construct Merge Condition
        # Use null-safe equality (<=>) to handle NULLs in composite keys
        merge_condition = " AND ".join([f"target.{k} <=> source.{k}" for k in join_keys])

        # 3. Execute Merge
        delta_table = DeltaTable.forName(spark, target_table_name)
        
        # For Databricks Runtime we enable table-level auto-merge so MERGE can evolve schema.
        # Use table properties so this works on managed tables: set delta.schema.autoMerge.enabled = true
        if schema_evolution:
            try:
                self._set_table_auto_merge(target_table_name, True)
            except Exception:
                # If setting the property fails, fall back to leaving Spark conf untouched
                pass

        # Generate surrogate keys for new rows if needed
        if surrogate_key_col and surrogate_key_strategy:
            # Initialize Key Generator
            max_key = 0
            if surrogate_key_strategy == "identity":
                key_gen = IdentityKeyGenerator()
            elif surrogate_key_strategy == "hash":
                key_gen = HashKeyGenerator(join_keys)
            elif surrogate_key_strategy == "sequence":
                # WARNING: SequenceKeyGenerator is deprecated and may cause OOM on large datasets
                # Consider using 'identity' or 'hash' strategy instead
                try:
                    max_key_row = delta_table.toDF().agg({surrogate_key_col: "max"}).first()
                    max_key = max_key_row[0] if max_key_row else 0
                except PYSPARK_EXCEPTION_BASE as e:
                    # Log warning but continue with max_key = 0
                    print(f"Warning: Failed to retrieve max key for {surrogate_key_col}: {e}")
                    max_key = 0
                key_gen = SequenceKeyGenerator()
            else:
                key_gen = None
            
            if key_gen and surrogate_key_strategy != "identity":
                # Generate keys for source rows (for hash/sequence strategies)
                source_df = key_gen.generate_keys(source_df, surrogate_key_col, existing_max_key=max_key)

        merge_builder = delta_table.alias("target").merge(
            source_df.alias("source"),
            merge_condition
        )

        # Handle Deletes (if source has _change_type = 'delete')
        if "_change_type" in source_df.columns:
            if delete_strategy == "hard":
                merge_builder = merge_builder.whenMatchedDelete(
                    condition="source._change_type = 'delete'"
                )
            elif delete_strategy == "soft":
                merge_builder = merge_builder.whenMatchedUpdate(
                    condition="source._change_type = 'delete'",
                    set={"__is_deleted": "true", "__etl_processed_at": "current_timestamp()"}
                )

        update_condition = "source._change_type != 'delete'" if "_change_type" in source_df.columns else None

        # Build explicit update and insert maps so system columns like __is_deleted are set
        source_cols_map = {c: f"source.{c}" for c in source_df.columns}

        # Ensure system columns are explicitly set on update/insert
        # For updates, do NOT overwrite the existing surrogate key
        update_map = {c: f"source.{c}" for c in source_df.columns if c != surrogate_key_col}
        update_map["__is_deleted"] = "false"
        update_map["__etl_processed_at"] = "current_timestamp()"

        # For inserts, include the surrogate key (unless using identity column)
        insert_map = {**source_cols_map,
                      "__is_deleted": "false",
                      "__etl_processed_at": "current_timestamp()"}
        
        # If using Identity, remove SK from insert map so DB generates it
        if surrogate_key_strategy == "identity" and surrogate_key_col in insert_map:
            del insert_map[surrogate_key_col]

        merge_builder = merge_builder.whenMatchedUpdate(
            condition=update_condition,
            set=update_map
        )

        merge_builder = merge_builder.whenNotMatchedInsert(
            condition=update_condition,
            values=insert_map
        )

        merge_builder.execute()

    def _merge_scd2(self, target_table_name, source_df, join_keys, track_history_columns, surrogate_key_col, surrogate_key_strategy, schema_evolution: bool = False):
        """
        Implements SCD Type 2 Merge using Hashdiff and Surrogate Keys with Deletion Vectors.
        """
        if not track_history_columns:
            raise ValueError("track_history_columns must be provided for SCD Type 2")
        
        # For Databricks Runtime we enable table-level auto-merge so MERGE can evolve schema.
        if schema_evolution:
            try:
                self._set_table_auto_merge(target_table_name, True)
            except Exception:
                pass

        delta_table = DeltaTable.forName(spark, target_table_name)
        
        # 1. Compute Hashdiff on Source
        source_df = source_df.withColumn("hashdiff", compute_hashdiff(track_history_columns))
        
        # 2. Prepare Staged Source for Merge
        # We need to identify:
        # - Rows to Close (Update): Match on Keys + is_current + Hashdiff Changed
        # - Rows to Insert (New Version): Match on Keys + is_current + Hashdiff Changed
        # - Rows to Insert (New Key): No Match on Keys
        
        # We use the "Union" approach to handle both update and insert in one MERGE.
        # Source rows are duplicated for the "Update" and "Insert" actions if they are changes.
        
        # To do this efficiently, we join source with target to find changes.
        # Note: For very large tables, consider using a window function on source or other optimizations.
        
        target_df = delta_table.toDF().filter("__is_current = true")
        
        # Join condition: build a single Column expression combining null-safe equality on all join keys
        join_conditions = [source_df[k].eqNullSafe(target_df[k]) for k in join_keys]
        if join_conditions:
            combined_join_cond = reduce(lambda a, b: a & b, join_conditions)
        else:
            combined_join_cond = None

        # Identify Changes
        # We join source and target.
        # If match AND hashdiff differs -> Change
        # If no match -> New

        # We need to bring 'hashdiff' from target to compare.
        # Assuming target has 'hashdiff' column. If not, we might need to compute it or assume change.
        # For this implementation, we assume target table is created with hashdiff.

        joined_df = source_df.alias("s").join(target_df.alias("t"), combined_join_cond, "left") \
            .select("s.*", col("t.hashdiff").alias("target_hashdiff"), col("t." + surrogate_key_col).alias("target_sk"))
            
        # Rows that are NEW (no match in target)
        rows_new = joined_df.filter(col("target_sk").isNull()) \
            .drop("target_hashdiff", "target_sk") \
            .withColumn("__merge_action", lit("INSERT_NEW"))
            
        # Rows that CHANGED (match in target, but hashdiff differs)
        rows_changed = joined_df.filter(col("target_sk").isNotNull() & (col("hashdiff") != col("target_hashdiff"))) \
            .drop("target_hashdiff", "target_sk")
            
        # For changed rows, we need TWO actions:
        # 1. UPDATE old row (expire it)
        # 2. INSERT new row
        
        rows_to_expire = rows_changed.withColumn("__merge_action", lit("UPDATE_EXPIRE"))
        rows_to_insert_version = rows_changed.withColumn("__merge_action", lit("INSERT_VERSION"))
        
        # Combine all rows for MERGE
        staged_source = rows_new.union(rows_to_expire).union(rows_to_insert_version)
        
        # 3. Generate Surrogate Keys for INSERT rows
        # We only generate keys for INSERT_NEW and INSERT_VERSION
        # UPDATE_EXPIRE rows don't need a new key (they match on natural key to expire)
        
        # Filter for rows needing keys
        rows_needing_keys = staged_source.filter(col("__merge_action").isin("INSERT_NEW", "INSERT_VERSION"))
        rows_no_keys = staged_source.filter(col("__merge_action") == "UPDATE_EXPIRE")
        
        # Initialize Key Generator
        max_key = 0  # Initialize max_key before conditional blocks
        if surrogate_key_strategy == "identity":
            key_gen = IdentityKeyGenerator()
        elif surrogate_key_strategy == "hash":
            key_gen = HashKeyGenerator(join_keys) # Use natural keys for hash key
        elif surrogate_key_strategy == "sequence":
            # WARNING: SequenceKeyGenerator is deprecated and may cause OOM on large datasets
            # Consider using 'identity' or 'hash' strategy instead
            # For sequence, we'd need the max key.
            # This is expensive. For now, use 0 or fetch if feasible.
            # In production, use a separate generator service or Identity columns.
            try:
                max_key_row = delta_table.toDF().agg({surrogate_key_col: "max"}).first()
                max_key = max_key_row[0] if max_key_row else 0
            except PYSPARK_EXCEPTION_BASE as e:
                # Log warning but continue with max_key = 0
                print(f"Warning: Failed to retrieve max key for {surrogate_key_col}: {e}")
                max_key = 0
            key_gen = SequenceKeyGenerator()
        else:
            raise ValueError(f"Unknown key strategy: {surrogate_key_strategy}")
            
        # Generate keys
        # Note: For Identity, this might drop the column or do nothing.
        rows_with_keys = key_gen.generate_keys(rows_needing_keys, surrogate_key_col, existing_max_key=max_key if surrogate_key_strategy == "sequence" else 0)
        
        # Union back
        # Ensure schema matches (if key gen added a column, we need to add it to rows_no_keys as null)
        if surrogate_key_col in rows_with_keys.columns and surrogate_key_col not in rows_no_keys.columns:
            rows_no_keys = rows_no_keys.withColumn(surrogate_key_col, lit(None))
            
        final_source = rows_with_keys.unionByName(rows_no_keys, allowMissingColumns=True)
        
        # 4. Execute MERGE
        
        # Merge Condition:
        # We match on Natural Keys AND __is_current = true
        # BUT for INSERT rows, we want to force a NOT MATCH.
        # So we nullify the join keys in source for INSERT actions.
        # IMPORTANT: Preserve original values before nullifying so we can insert them!
        
        from pyspark.sql.functions import when as _when
        for k in join_keys:
            # Preserve original value in a temp column for INSERT
            final_source = final_source.withColumn(f"__orig_{k}", col(k))
            # Nullify the join key for INSERT rows so they don't match
            final_source = final_source.withColumn(k,
                _when(col("__merge_action") == "UPDATE_EXPIRE", col(k)).otherwise(lit(None))
            )
            
        merge_condition = " AND ".join([f"target.{k} <=> source.{k}" for k in join_keys]) + " AND target.__is_current = true"
        
        # Define Values for Insert
        # We insert all columns from source, plus SCD2 cols
        # Use the preserved __orig_ columns for natural keys since join keys were nullified
        insert_values = {}
        for c in source_df.columns:
            if c == "__merge_action":
                continue
            if c in join_keys:
                # Use preserved original value
                insert_values[c] = f"source.__orig_{c}"
            else:
                insert_values[c] = f"source.{c}"
        
        # Add SCD2 system columns
        # For __valid_from:
        #   - INSERT_NEW (first version): use 1900-01-01 so historical facts can join
        #   - INSERT_VERSION (new version after change): use current timestamp
        insert_values.update({
            "__is_current": "true",
            "__valid_from": "CASE WHEN source.__merge_action = 'INSERT_NEW' THEN cast('1900-01-01 00:00:00' as timestamp) ELSE source.__etl_processed_at END",
            "__valid_to": "cast('9999-12-31 23:59:59' as timestamp)",
            "__etl_processed_at": "current_timestamp()",
            "__is_deleted": "false"
        })
        
        # If key gen provided a key, include it.
        if surrogate_key_col in final_source.columns:
            insert_values[surrogate_key_col] = f"source.{surrogate_key_col}"
            
        # If using Identity, we DO NOT include the key in insert (unless we want to override, which we usually don't).
        if surrogate_key_strategy == "identity" and surrogate_key_col in insert_values:
            del insert_values[surrogate_key_col]

        delta_table.alias("target").merge(
            final_source.alias("source"),
            merge_condition
        ).whenMatchedUpdate(
            condition = "source.__merge_action = 'UPDATE_EXPIRE'",
            set = {
                "__is_current": "false",
                "__valid_to": "source.__etl_processed_at",
                "__etl_processed_at": "current_timestamp()"
            }
        ).whenNotMatchedInsert(
            values = insert_values
        ).execute()

    def _is_delta_table(self, table_name: str) -> bool:
        """
        Check if a managed/external table is a Delta table by inspecting its provider.
        DeltaTable.isDeltaTable() requires a path, not a table name.
        """
        try:
            DeltaTable.forName(spark, table_name)
            return True
        except Exception:
            return False

    def _set_table_auto_merge(self, table_name: str, enabled: bool):
        """
        Set the table property 'delta.schema.autoMerge.enabled' on a Delta table.
        This enables Databricks Runtime's auto-merge behavior for MERGE operations.
        Uses ALTER TABLE ... SET TBLPROPERTIES for managed tables.
        """
        val = 'true' if enabled else 'false'
        # Use ALTER TABLE to set the table property
        sql = f"ALTER TABLE `{table_name}` SET TBLPROPERTIES ('delta.schema.autoMerge.enabled' = '{val}')"
        spark.sql(sql)

    def ensure_scd2_defaults(self, target_table_name, schema, surrogate_key, default_values=None, surrogate_key_strategy="identity"):
        """
        Ensures that the standard SCD2 default rows (-1, -2, -3) exist in the table.
        """
        # Table must exist (Orchestrator creates it as Delta before calling this)
        if not spark.catalog.tableExists(target_table_name):
            print(f"ensure_scd2_defaults: table {target_table_name} does not exist. Skipping.")
            return

        # Verify it's a Delta table (use our helper for managed tables)
        if not self._is_delta_table(target_table_name):
            raise ValueError(f"ensure_scd2_defaults: {target_table_name} exists but is not a Delta table. "
                           "The Orchestrator should create it as Delta before calling this method.")

        delta_table = DeltaTable.forName(spark, target_table_name)
        
        # Define the standard defaults
        # -1: Unknown
        # -2: Not Applicable
        # -3: Error
        
        standard_defaults = {
            -1: "Unknown",
            -2: "Not Applicable",
            -3: "Error"
        }
        
        rows_to_insert = []
        
        for key, label in standard_defaults.items():
            # Construct row
            row = {surrogate_key: key}
            
            # Fill other columns
            for field in schema.fields:
                    col_name = field.name
                    if col_name == surrogate_key:
                        continue
                        
                    # System columns
                    if col_name == "__is_current":
                        row[col_name] = True
                    elif col_name == "__valid_from":
                        row[col_name] = datetime(1900, 1, 1, 0, 0, 0)  # Standard start
                    elif col_name == "__valid_to":
                        row[col_name] = datetime(9999, 12, 31, 23, 59, 59)  # Standard end for default rows
                    elif col_name.startswith("__"):
                        # Other system cols: provide a fallback if the target field is non-nullable
                        if not field.nullable:
                            dtype = field.dataType
                            # Timestamp/Date -> use safe historical date/time
                            if isinstance(dtype, TimestampType):
                                row[col_name] = datetime(1900, 1, 1, 0, 0, 0)
                            elif isinstance(dtype, DateType):
                                row[col_name] = date(1900, 1, 1)
                            elif isinstance(dtype, (IntegerType, LongType, ShortType, DoubleType, FloatType, DecimalType)):
                                row[col_name] = -1
                            elif isinstance(dtype, BooleanType):
                                row[col_name] = False
                            else:
                                # Default to empty string for string-like types
                                row[col_name] = ""
                        else:
                            row[col_name] = None # Other system cols (nullable)
                    else:
                        # User columns
                        # Check if user provided a specific default in config
                        if default_values and col_name in default_values:
                            row[col_name] = default_values[col_name]
                        else:
                            # Infer based on type
                            dtype = str(field.dataType)
                            if "String" in dtype:
                                row[col_name] = label
                            elif "Int" in dtype or "Long" in dtype or "Double" in dtype:
                                row[col_name] = -1
                            elif "Timestamp" in dtype:
                                row[col_name] = datetime(1900, 1, 1, 0, 0, 0)
                            elif "Date" in dtype:
                                row[col_name] = date(1900, 1, 1)
                            else:
                                row[col_name] = None
                
            rows_to_insert.append(row)
        
        if rows_to_insert:
            print(f"Seeding {len(rows_to_insert)} default rows into {target_table_name}...")
            df = spark.createDataFrame(rows_to_insert, schema)
            
            # Use atomic MERGE operation to prevent duplicates in concurrent environments
            delta_table.alias("target").merge(
                df.alias("source"),
                f"target.{surrogate_key} = source.{surrogate_key}"
            ).whenNotMatchedInsertAll().execute()

    def ensure_scd1_defaults(self, target_table_name, schema, surrogate_key, default_values=None, surrogate_key_strategy="identity"):
        """
        Ensures that the standard SCD1 default rows (-1, -2, -3) exist in the table.
        Similar to SCD2 but without the SCD2-specific system columns.
        """
        # Table must exist (Orchestrator creates it as Delta before calling this)
        if not spark.catalog.tableExists(target_table_name):
            print(f"ensure_scd1_defaults: table {target_table_name} does not exist. Skipping.")
            return

        # Verify it's a Delta table (use our helper for managed tables)
        if not self._is_delta_table(target_table_name):
            raise ValueError(f"ensure_scd1_defaults: {target_table_name} exists but is not a Delta table.")

        delta_table = DeltaTable.forName(spark, target_table_name)
        
        rows_to_insert = []
        
        standard_defaults = {
            -1: "Unknown",
            -2: "Not Applicable",
            -3: "Error"
        }
        
        for key, label in standard_defaults.items():
            row = {surrogate_key: key}
            
            for field in schema.fields:
                    col_name = field.name
                    if col_name == surrogate_key:
                        continue
                    
                    # System columns (SCD1 has fewer than SCD2)
                    if col_name.startswith("__"):
                        if not field.nullable:
                            dtype = field.dataType
                            if isinstance(dtype, TimestampType):
                                row[col_name] = datetime(1900, 1, 1, 0, 0, 0)
                            elif isinstance(dtype, DateType):
                                row[col_name] = date(1900, 1, 1)
                            elif isinstance(dtype, (IntegerType, LongType, ShortType, DoubleType, FloatType, DecimalType)):
                                row[col_name] = -1
                            elif isinstance(dtype, BooleanType):
                                row[col_name] = False
                            else:
                                row[col_name] = ""
                        else:
                            row[col_name] = None
                    else:
                        # User columns
                        if default_values and col_name in default_values:
                            row[col_name] = default_values[col_name]
                        else:
                            dtype = str(field.dataType)
                            if "String" in dtype:
                                row[col_name] = label
                            elif "Int" in dtype or "Long" in dtype or "Double" in dtype:
                                row[col_name] = -1
                            elif "Timestamp" in dtype:
                                row[col_name] = datetime(1900, 1, 1, 0, 0, 0)
                            elif "Date" in dtype:
                                row[col_name] = date(1900, 1, 1)
                            else:
                                row[col_name] = None
                
            rows_to_insert.append(row)
        
        if rows_to_insert:
            print(f"Seeding {len(rows_to_insert)} default rows into {target_table_name}...")
            df = spark.createDataFrame(rows_to_insert, schema)
            
            # Use atomic MERGE operation to prevent duplicates in concurrent environments
            delta_table.alias("target").merge(
                df.alias("source"),
                f"target.{surrogate_key} = source.{surrogate_key}"
            ).whenNotMatchedInsertAll().execute()

    def optimize_table(self, table_name: str, cluster_by: List[str] = None):
        """
        Runs OPTIMIZE on the target table.
        
        Args:
            table_name: The table to optimize
            cluster_by: Optional list of columns for clustering (Liquid Clustering)
        """
        print(f"Optimizing table {table_name}...")
        
        if cluster_by:
            # If cluster_by is specified, ensure the table is configured for Liquid Clustering
            # Note: This assumes the table was created with CLUSTER BY clause
            # We just run OPTIMIZE, which will use the existing clustering spec
            spark.sql(f"OPTIMIZE `{table_name}`")
            print(f"Optimized {table_name} using Liquid Clustering on {cluster_by}")
        else:
            # Standard OPTIMIZE without clustering
            spark.sql(f"OPTIMIZE `{table_name}`")
            print(f"Optimized {table_name}")
