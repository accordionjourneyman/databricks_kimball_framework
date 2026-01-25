"""
Data loading utilities for Kimball ETL pipelines.

Provides strategies for loading data from source tables:
- Full snapshot loading
- Change Data Feed (CDF) incremental loading
"""

from typing import TYPE_CHECKING, cast

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

if TYPE_CHECKING:
    pass


class DataLoader:
    """
    Handles reading data from source tables using various strategies (CDF, Snapshot).

    Supports dependency injection of SparkSession for testability.

    Args:
        spark_session: Optional SparkSession. If not provided, uses global spark
                       from databricks.sdk.runtime (for Databricks compatibility).
    """

    def __init__(self, spark_session: SparkSession | None = None):
        self._spark = spark_session

    @property
    def spark(self) -> SparkSession:
        """Lazy-load SparkSession from Databricks runtime if not injected."""
        if self._spark is None:
            from databricks.sdk.runtime import spark

            self._spark = spark
        return self._spark

    def load_full_snapshot(
        self,
        table_name: str,
        format: str = "delta",
        options: dict[str, str] | None = None,
    ) -> DataFrame:
        """
        Reads the full snapshot of a source table/file.
        Supports Delta (default), Parquet, CSV, JDBC, etc.
        """
        reader = self.spark.read.format(format)
        if options:
            reader = reader.options(**options)

        # For Delta tables in catalog, use .table()
        # For files (path inputs) or other formats, use .load()
        if format == "delta" and "/" not in table_name:
            return cast(DataFrame, reader.table(table_name))

        # For paths or non-catalog sources
        return cast(DataFrame, reader.load(table_name))

    def load_cdf(
        self,
        table_name: str,
        starting_version: int,
        deduplicate_keys: list[str] | None = None,
        ending_version: int | None = None,
    ) -> DataFrame:
        """
        Reads changes from a Delta table using Change Data Feed (CDF).

        Args:
            table_name: The source table to read from.
            starting_version: The commit version to start reading from (inclusive).
            deduplicate_keys: Columns to use for deduplication. If provided, keeps only
                              the latest version per key (based on _commit_version).
            ending_version: Optional commit version to stop reading at (inclusive).
                          CRITICAL for preventing race conditions if new data arrives
                          during processing.
        """
        # C-07: Fix race condition by auto-fetching latest version if not provided
        if ending_version is None:
            ending_version = self.get_latest_version(table_name)

        reader = (
            self.spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", starting_version)
        )

        if ending_version is not None:
            reader = reader.option("endingVersion", ending_version)

        df = cast(DataFrame, reader.table(table_name))

        # Filter out update_preimage - we only need insert, update_postimage, and delete
        if "_change_type" in df.columns:
            df = df.filter("_change_type != 'update_preimage'")

        # Deduplicate: keep only the latest version per key
        # FINDING-002: Add secondary ordering by _change_type to ensure deletes win
        #
        # Performance Note: Window function with row_number() over partitionBy(keys)
        # Complexity: O(n log n) per partition (sort within partition) + shuffle by key
        # This is the "correct algorithm" for latest-per-key semantics with change_type priority
        # Cost factors:
        # - Partitioning: shuffle if keys aren't already co-located
        # - Sorting: per-partition sort by version + change_type
        # - Projection should happen BEFORE this window to minimize shuffle data size
        # Alternative approach (not implemented): groupBy + max(struct(...)) can avoid row_number
        # but is harder to get correct with change_type priority logic
        if deduplicate_keys and "_commit_version" in df.columns:
            from pyspark.sql import functions as F

            # Priority: delete (0) > update_postimage (1) > insert (2)
            window = Window.partitionBy(deduplicate_keys).orderBy(
                col("_commit_version").desc(),
                F.when(col("_change_type") == "delete", 0)
                .when(col("_change_type") == "update_postimage", 1)
                .otherwise(2),
            )
            df = (
                df.withColumn("_rn", row_number().over(window))
                .filter(col("_rn") == 1)
                .drop("_rn")
            )

        return df

    def get_latest_version(self, table_name: str) -> int:
        """
        Gets the latest commit version of a Delta table.
        Useful for updating the watermark after a successful load.
        """
        if not self.spark.catalog.tableExists(table_name):
            raise ValueError(f"Source table does not exist: {table_name}")
        dt = DeltaTable.forName(self.spark, table_name)
        row = dt.history(1).select("version").first()
        return row["version"] if row else 0
