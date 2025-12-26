from typing import List
from databricks.sdk.runtime import spark

class TableCreator:
    """
    Handles creation of Delta tables with Liquid Clustering support.
    """
    
    def create_table_with_clustering(self, 
                                     table_name: str, 
                                     schema_df, 
                                     config: dict = None,
                                     cluster_by: List[str] = None,
                                     partition_by: List[str] = None,
                                     surrogate_key_col: str = None,
                                     surrogate_key_strategy: str = None):
        """
        Creates a Delta table with optional Liquid Clustering.
        
        Args:
            table_name: Full table name (catalog.schema.table)
            schema_df: DataFrame with the desired schema
            config: Table configuration from YAML
            cluster_by: Columns for Liquid Clustering (deprecated, use config)
            partition_by: Columns for partitioning (optional, usually not needed with clustering)
            surrogate_key_col: Name of the surrogate key column (if any)
            surrogate_key_strategy: Strategy for SK generation ('identity', 'hash', 'sequence')
        """
        if spark.catalog.tableExists(table_name):
            print(f"Table {table_name} already exists. Skipping creation.")
            return
        
        # Check config for liquid clustering
        if config and 'liquid_clustering' in config:
            cluster_by = config['liquid_clustering']
            partition_by = None  # Don't use partitioning with liquid clustering
            print(f"Using Liquid Clustering from config: {cluster_by}")
        elif cluster_by:
            print(f"Using provided cluster_by: {cluster_by}")
        
        # Build CREATE TABLE statement
        # We'll use the DataFrame schema to infer column definitions
        columns = []
        for field in schema_df.schema.fields:
            col_name = field.name
            
            # Check if this is the surrogate key column with identity strategy
            if col_name == surrogate_key_col and surrogate_key_strategy == "identity":
                # Use GENERATED ALWAYS AS IDENTITY - strict auto-generation
                # Allows OVERRIDING SYSTEM VALUE for restores/backfills
                col_def = f"{col_name} BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1)"
            else:
                col_def = f"{col_name} {field.dataType.simpleString()}"
                if not field.nullable:
                    col_def += " NOT NULL"
            columns.append(col_def)
        
        columns_sql = ",\n  ".join(columns)
        
        create_sql = f"""
        CREATE TABLE {table_name} (
          {columns_sql}
        )
        USING DELTA
        """
        
        if cluster_by:
            cluster_cols = ", ".join(cluster_by)
            create_sql += f"\nCLUSTER BY ({cluster_cols})"
        elif partition_by:
            partition_cols = ", ".join(partition_by)
            create_sql += f"\nPARTITIONED BY ({partition_cols})"
        
        # Enable Change Data Feed by default
        create_sql += "\nTBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"

        # Add basic Delta Constraints
        constraints = []
        if surrogate_key_col:
            constraints.append(f"CONSTRAINT sk_not_null CHECK ({surrogate_key_col} IS NOT NULL)")
        if "__is_current" in [f.name for f in schema_df.schema.fields]:
            constraints.append("CONSTRAINT is_current_check CHECK (__is_current IN (true, false))")
        
        if constraints:
            constraint_sql = ",\n  ".join(constraints)
            create_sql += f"\nCONSTRAINTS (\n  {constraint_sql}\n)"

        if surrogate_key_col and surrogate_key_strategy == "identity":
            print(f"  - Surrogate key '{surrogate_key_col}' using IDENTITY column")
        if cluster_by:
            print(f"  - Liquid Clustering on {cluster_by}")
        spark.sql(create_sql)
        print(f"Table {table_name} created successfully.")
        
        # Enable Delta optimizations after table creation
        try:
            self.enable_predictive_optimization(table_name)
        except Exception as e:
            print(f"Warning: Could not enable Predictive Optimization: {e}")
        
        # Enable Deletion Vectors for tables that will use MERGE operations
        try:
            self.enable_deletion_vectors(table_name)
        except Exception as e:
            print(f"Warning: Could not enable Deletion Vectors: {e}")
        
        # Apply additional constraints from config
        if config:
            self.apply_delta_constraints(table_name, config)

    def apply_delta_constraints(self, table_name: str, config: dict):
        """
        Apply Delta constraints based on YAML configuration.
        
        Args:
            table_name: Full table name
            config: Table configuration from YAML
        """
        # Apply NOT NULL constraints for natural keys
        natural_keys = config.get('natural_keys', [])
        for key in natural_keys:
            alter_sql = f"ALTER TABLE {table_name} ALTER COLUMN {key} SET NOT NULL"
            try:
                spark.sql(alter_sql)
                print(f"Applied NOT NULL constraint to {key}")
            except Exception as e:
                print(f"Failed to apply NOT NULL to {key}: {e}")
        
        # Apply business domain constraints
        constraints = config.get('constraints', [])
        for constraint in constraints:
            constraint_name = constraint.get('name')
            constraint_expr = constraint.get('expression')
            if constraint_name and constraint_expr:
                alter_sql = f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} CHECK ({constraint_expr})"
                try:
                    spark.sql(alter_sql)
                    print(f"Applied constraint {constraint_name}")
                except Exception as e:
                    print(f"Failed to apply constraint {constraint_name}: {e}")

    def enable_schema_auto_merge(self):
        """
        Enable schema auto-merge for the current session.
        """
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        print("Enabled schema auto-merge for current session")

    def enable_predictive_optimization(self, table_name: str):
        """
        Enables Predictive Optimization for a Delta table.
        This allows Databricks to automatically optimize table layout and performance.
        """
        alter_sql = f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.enablePredictiveOptimization' = 'true')"
        spark.sql(alter_sql)
        print(f"Predictive Optimization enabled for {table_name}")

    def enable_deletion_vectors(self, table_name: str):
        """
        Enables Deletion Vectors for a Delta table.
        This improves performance for MERGE operations with many updates/deletes.
        """
        alter_sql = f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')"
        spark.sql(alter_sql)
        print(f"Deletion Vectors enabled for {table_name}")
