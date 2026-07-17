import logging
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col, current_timestamp, lit

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SkeletonGenerationResult:
    """Evidence from one early-arriving-fact check."""

    dimension_table: str
    missing_rows: int
    samples: list[dict]
    status: str


class SkeletonGenerator:
    def __init__(self, spark_session: SparkSession | None = None) -> None:
        if spark_session is not None:
            self.spark = spark_session
            return
        from kimball.common.spark_session import get_spark

        try:
            self.spark = get_spark()
        except (ImportError, AttributeError, RuntimeError):
            self.spark = __import__("databricks.sdk.runtime", fromlist=["spark"]).spark

    def generate_skeletons(
        self,
        fact_df: DataFrame,
        dim_table_name: str,
        fact_join_key: str,
        dim_join_key: str,
        surrogate_key_col: str,
        batch_id: str | None = None,
        effective_at_column: str | None = None,
        create: bool = True,
    ) -> SkeletonGenerationResult:
        if not self.spark.catalog.tableExists(dim_table_name):
            logger.info(
                f"Dimension table {dim_table_name} does not exist. Skipping skeleton generation."
            )
            return SkeletonGenerationResult(dim_table_name, 0, [], "missing_dimension")
        dim_table = DeltaTable.forName(self.spark, dim_table_name)
        dim_df = dim_table.toDF()
        has_skeleton_col = "__is_skeleton" in [f.name for f in dim_df.schema.fields]
        if not has_skeleton_col:
            logger.info(
                f"Dimension table {dim_table_name} does not have __is_skeleton column. Skipping skeleton generation."
            )
            return SkeletonGenerationResult(
                dim_table_name, 0, [], "unsupported_dimension"
            )
        fact_keys = fact_df.select(col(fact_join_key).alias("key")).distinct()
        dim_keys = dim_df.select(col(dim_join_key).alias("key"))
        missing = fact_keys.join(broadcast(dim_keys), "key", "left_anti")
        if missing.isEmpty():
            logger.info(f"No missing keys found for {dim_table_name}.")
            return SkeletonGenerationResult(dim_table_name, 0, [], "no_missing_keys")
        missing_count = missing.count()
        samples = [row.asDict() for row in missing.limit(5).collect()]
        if not create:
            return SkeletonGenerationResult(
                dim_table_name, missing_count, samples, "detected"
            )
        skeletons = missing.withColumnRenamed("key", dim_join_key)
        from datetime import datetime

        skeletons = (
            skeletons.withColumn("__is_current", lit(True))
            .withColumn("__valid_from", lit(datetime(1800, 1, 1, 0, 0, 0)))
            .withColumn("__valid_to", lit(None).cast("timestamp"))
            .withColumn("__etl_processed_at", current_timestamp())
            .withColumn("__etl_batch_id", lit(batch_id or "SKELETON_GEN"))
            .withColumn("__is_skeleton", lit(True))
            .withColumn("__skeleton_created_at", current_timestamp())
            .withColumn("__is_deleted", lit(False))
        )
        exprs = [col(c) for c in skeletons.columns]
        existing = set(skeletons.columns)
        for field in dim_df.schema.fields:
            if field.name not in existing and field.name != surrogate_key_col:
                exprs.append(lit(None).cast(field.dataType).alias(field.name))
        skeletons = skeletons.select(*exprs)
        from kimball.processing.key_generator import HashKeyGenerator

        skeletons = HashKeyGenerator(
            [dim_join_key], version_column=effective_at_column
        ).generate_keys(skeletons, surrogate_key_col)
        cols = [f.name for f in dim_df.schema.fields]
        dim_table.alias("target").merge(
            skeletons.select(*cols).alias("source"),
            f"target.{dim_join_key} <=> source.{dim_join_key}",
        ).whenNotMatchedInsertAll().execute()
        logger.info(f"Inserted skeleton rows into {dim_table_name} (via atomic MERGE).")
        return SkeletonGenerationResult(
            dim_table_name, missing_count, samples, "created"
        )
