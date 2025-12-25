from typing import List
from databricks.sdk.runtime import spark

class TableCreator:
    """
    Handles creation of Delta tables with Liquid Clustering support.
    """
    
    def create_table_with_clustering(self, 
                                     table_name: str, 
                                     schema_df, 
                                     cluster_by: List[str] = None,
                                     partition_by: List[str] = None,
                                     surrogate_key_col: str = None,
                                     surrogate_key_strategy: str = None):
        """
        Creates a Delta table with optional Liquid Clustering.
        
        Args:
            table_name: Full table name (catalog.schema.table)
            schema_df: DataFrame with the desired schema
            cluster_by: Columns for Liquid Clustering
            partition_by: Columns for partitioning (optional, usually not needed with clustering)
            surrogate_key_col: Name of the surrogate key column (if any)
            surrogate_key_strategy: Strategy for SK generation ('identity', 'hash', 'sequence')
        """
        if spark.catalog.tableExists(table_name):
            print(f"Table {table_name} already exists. Skipping creation.")
            return
        
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
        
        if partition_by:
            partition_cols = ", ".join(partition_by)
            create_sql += f"\nPARTITIONED BY ({partition_cols})"
        
        # Enable Change Data Feed by default
        create_sql += "\nTBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"

        # Add Delta Constraints for data integrity
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
