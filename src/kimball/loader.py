from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
from delta.tables import DeltaTable
from typing import List, Optional
from databricks.sdk.runtime import spark

class DataLoader:
    """
    Handles reading data from source tables using various strategies (CDF, Snapshot).
    """

    def load_full_snapshot(self, table_name: str) -> DataFrame:
        """
        Reads the full snapshot of a Delta table.
        """
        return spark.read.format("delta").table(table_name)

    def load_cdf(
        self, 
        table_name: str, 
        starting_version: int,
        deduplicate_keys: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Reads changes from a Delta table using Change Data Feed (CDF).
        
        Args:
            table_name: The source table to read from.
            starting_version: The commit version to start reading from (inclusive).
            deduplicate_keys: Columns to use for deduplication. If provided, keeps only
                              the latest version per key (based on _commit_version).
                              This is CRITICAL when the same row is updated multiple times
                              between the watermark and current version, otherwise MERGE
                              will fail with "cannot merge multiple source rows to same target".
        
        Note: We filter out 'update_preimage' rows as they represent the state
        before the update and would cause duplicate key matches during merge.
        """
        df = (spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", starting_version)
                .table(table_name))
        
        # Filter out update_preimage - we only need insert, update_postimage, and delete
        if "_change_type" in df.columns:
            df = df.filter("_change_type != 'update_preimage'")
        
        # Deduplicate: keep only the latest version per key
        # This handles multiple scenarios:
        # 1. Same row updated multiple times (v100, v101, v103) → keep latest update
        # 2. Row deleted then reinserted (delete v102, insert v103) → keep insert (correct final state)
        # 3. Row inserted then deleted (insert v100, delete v103) → keep delete (correct final state)
        # The _change_type is preserved, so MERGE knows whether to insert/update/delete.
        if deduplicate_keys and "_commit_version" in df.columns:
            window = Window.partitionBy(deduplicate_keys).orderBy(col("_commit_version").desc())
            df = (df
                  .withColumn("_rn", row_number().over(window))
                  .filter(col("_rn") == 1)
                  .drop("_rn"))
        
        return df

    def get_latest_version(self, table_name: str) -> int:
        """
        Gets the latest commit version of a Delta table.
        Useful for updating the watermark after a successful load.
        """
        dt = DeltaTable.forName(spark, table_name)
        row = dt.history(1).select("version").first()
        return row["version"] if row else 0

