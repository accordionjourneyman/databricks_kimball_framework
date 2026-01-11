from typing import cast

from databricks.sdk.runtime import spark
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


class DataLoader:
    """
    Handles reading data from source tables using various strategies (CDF, Snapshot).
    """

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
        reader = spark.read.format(format)
        if options:
            reader = reader.options(**options)

        # For Delta tables in catalog, use .table()
        # For files (path inputs) or other formats, use .load()
        if format == "delta" and "/" not in table_name:
            return cast(DataFrame, reader.table(table_name))

        # For paths or non-catalog sources
        # Note: JDBC requires 'url'/'dbtable' in options, table_name passed to load() is often ignored or path
        # But for spark.read.jdbc, usage is different. format("jdbc").options(...).load()
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
        reader = (
            spark.read.format("delta")
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
        if deduplicate_keys and "_commit_version" in df.columns:
            window = Window.partitionBy(deduplicate_keys).orderBy(
                col("_commit_version").desc()
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
        dt = DeltaTable.forName(spark, table_name)
        row = dt.history(1).select("version").first()
        return row["version"] if row else 0
