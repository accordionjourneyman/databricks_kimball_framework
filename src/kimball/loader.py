from pyspark.sql import DataFrame
from delta.tables import DeltaTable
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

    def load_cdf(self, table_name: str, starting_version: int) -> DataFrame:
        """
        Reads changes from a Delta table using Change Data Feed (CDF).
        
        Args:
            table_name: The source table to read from.
            starting_version: The commit version to start reading from (inclusive).
                              Note: The framework usually passes last_processed + 1.
        
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
        
        return df

    def get_latest_version(self, table_name: str) -> int:
        """
        Gets the latest commit version of a Delta table.
        Useful for updating the watermark after a successful load.
        """
        dt = DeltaTable.forName(spark, table_name)
        return dt.history(1).select("version").collect()[0]["version"]
