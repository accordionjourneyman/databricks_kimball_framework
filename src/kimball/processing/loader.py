from typing import cast

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


class DataLoader:
    def __init__(self, spark_session: SparkSession | None = None):
        self._spark = spark_session

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            from kimball.common.spark_session import get_spark

            self._spark = get_spark()
        return self._spark

    def load_full_snapshot(
        self,
        table_name: str,
        format: str = "delta",
        options: dict[str, str] | None = None,
    ) -> DataFrame:
        reader = self.spark.read.format(format)
        if options:
            reader = reader.options(**options)
        return cast(
            DataFrame,
            reader.table(table_name)
            if format == "delta" and "/" not in table_name
            else reader.load(table_name),
        )

    def load_cdf(
        self,
        table_name: str,
        starting_version: int,
        deduplicate_keys: list[str] | None = None,
        ending_version: int | None = None,
    ) -> DataFrame:
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
        if "_change_type" in df.columns:
            df = df.filter("_change_type != 'update_preimage'")
        return self.deduplicate_cdf(df, deduplicate_keys)

    @staticmethod
    def deduplicate_cdf(df: DataFrame, deduplicate_keys: list[str] | None) -> DataFrame:
        """Keep the latest CDF row per key after callers inspect raw ordering."""
        if not deduplicate_keys or "_commit_version" not in df.columns:
            return df
        from pyspark.sql import functions as F

        window = Window.partitionBy(deduplicate_keys).orderBy(
            col("_commit_version").desc(),
            F.when(col("_change_type") == "delete", 0)
            .when(col("_change_type") == "update_postimage", 1)
            .otherwise(2),
        )
        return df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")

    def get_latest_version(self, table_name: str) -> int:
        if not self.spark.catalog.tableExists(table_name):
            raise ValueError(f"Source table does not exist: {table_name}")
        row = (
            DeltaTable.forName(self.spark, table_name)
            .history(1)
            .select("version")
            .first()
        )
        return row["version"] if row else 0
