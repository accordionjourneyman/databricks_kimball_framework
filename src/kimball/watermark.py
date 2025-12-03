from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.functions import current_timestamp, col
from delta.tables import DeltaTable
from typing import Optional
from databricks.sdk.runtime import spark

class WatermarkManager:
    """
    Manages the high-water marks for incremental ETL loads using Delta CDF.
    Tracks the last processed commit/version for each (target, source) pair.

    The watermark table is created in a user-specified database/schema.  Best
    practice is to store it in either:
      • the **target schema** (e.g. `gold`), or
      • a dedicated **config schema** (e.g. `etl_config`)

    No default schema is assumed so that this class is fully reusable across
    projects.
    """

    DEFAULT_TABLE_NAME = "watermarks"

    # Default table schema (can be overridden via `table_schema` param)
    DEFAULT_SCHEMA = StructType([
        StructField("target_table", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("last_processed_version", LongType(), True),
        StructField("updated_at", TimestampType(), False)
    ])

    def __init__(
        self,
        database: str,
        table_name: str = DEFAULT_TABLE_NAME,
        table_schema: Optional[StructType] = None,
    ):
        """
        Args:
            database: Database/schema where the watermark table will be created
                      (e.g. 'gold', 'etl_config'). **Required**.
            table_name: Name of the watermark table (default 'watermarks').
                        If a fully-qualified name (db.table) is provided, `database`
                        is ignored.
            table_schema: Optional StructType to enforce; if not provided the
                          default 4-column schema is used.
        """

        # Resolve fully-qualified table name
        if "." in table_name:
            self.fq_table = table_name
            self.database = table_name.split(".")[0]
        else:
            if not database:
                raise ValueError(
                    "WatermarkManager requires an explicit `database` parameter "
                    "(e.g. 'gold' or 'etl_config'). No default schema is assumed."
                )
            self.database = database
            self.fq_table = f"{self.database}.{table_name}"

        self.schema = table_schema if table_schema else self.DEFAULT_SCHEMA
        self._ensure_table_exists()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_table_exists(self):
        """Create database and watermark table if they don't exist.
        If the table exists, validate its schema matches the expected schema.
        """
        # Create database if it doesn't exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database}")

        if not spark.catalog.tableExists(self.fq_table):
            # Create empty Delta table with expected schema
            (spark.createDataFrame([], self.schema)
             .write
             .format("delta")
             .mode("ignore")
             .saveAsTable(self.fq_table))
        else:
            # Validate existing schema matches expected (compare simpleString to ignore nullable differences)
            existing_schema = spark.table(self.fq_table).schema
            if existing_schema.simpleString() != self.schema.simpleString():
                raise ValueError(
                    f"Existing watermark table {self.fq_table} schema mismatch.\n"
                    f"Expected: {self.schema.simpleString()}\n"
                    f"Found:    {existing_schema.simpleString()}\n"
                    "Drop/alter the table or provide a compatible `table_schema`."
                )

    # ------------------------------------------------------------------
    # Public API

    def get_watermark(self, target_table: str, source_table: str) -> Optional[int]:
        """Retrieve the last processed version for a (target, source) pair.
        Returns None if no watermark exists (first run).
        """
        df = spark.table(self.fq_table)
        result = df.filter(
            (col("target_table") == target_table) &
            (col("source_table") == source_table)
        ).select("last_processed_version").collect()

        if result:
            return result[0]["last_processed_version"]
        return None

    def update_watermark(self, target_table: str, source_table: str, version: int):
        """Insert or update the watermark row atomically via Delta MERGE."""
        delta_table = DeltaTable.forName(spark, self.fq_table)

        update_df = spark.createDataFrame([
            {
                "target_table": target_table,
                "source_table": source_table,
                "last_processed_version": version,
                "updated_at": datetime.now()  # placeholder, overwritten by merge
            }
        ], schema=self.schema)

        (delta_table.alias("w")
         .merge(
             update_df.alias("u"),
             "w.target_table = u.target_table AND w.source_table = u.source_table"
         )
         .whenMatchedUpdate(set={
             "last_processed_version": "u.last_processed_version",
             "updated_at": "current_timestamp()"
         })
         .whenNotMatchedInsert(values={
             "target_table": "u.target_table",
             "source_table": "u.source_table",
             "last_processed_version": "u.last_processed_version",
             "updated_at": "current_timestamp()"
         })
         .execute())
