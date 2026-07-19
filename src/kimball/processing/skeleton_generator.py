"""Generate skeleton placeholder rows for early-arriving facts.

When a fact table references a dimension key that does not yet exist in the
dimension, a *skeleton* row is created as a placeholder. Skeletons are
flagged with ``__is_skeleton = True`` and receive substitute values for
all non-key columns. When real data arrives later, the skeleton is
hydrated (replaced or updated in place) via
:class:`LateArrivingDimensionProcessor`.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
)

from kimball.common.constants import DEFAULT_VALID_FROM, DEFAULT_VALID_TO

logger = logging.getLogger(__name__)


def _placeholder_column(field):  # noqa: ANN001
    """Return a ``F.lit`` substitution that matches *field*'s data type."""
    dt = field.dataType
    if isinstance(dt, StringType):
        return F.lit("Not Yet Available").cast(dt)
    if isinstance(dt, (IntegerType, LongType, ShortType)):
        return F.lit(-3).cast(dt)
    if isinstance(dt, (DecimalType,)):
        return F.lit(-3).cast(dt)
    if isinstance(dt, (DoubleType, FloatType)):
        return F.lit(-3.0).cast(dt)
    if isinstance(dt, BooleanType):
        return F.lit(False)
    if isinstance(dt, TimestampType):
        return F.lit(DEFAULT_VALID_FROM).cast(dt)
    if isinstance(dt, DateType):
        return F.lit(DEFAULT_VALID_FROM.date()).cast(dt)
    raise ValueError(
        f"Skeleton requires an explicit substitute for {field.name} ({dt})"
    )


class SkeletonGenerator:
    """Create skeleton placeholder rows for missing dimension keys.

    Usage::

        gen = SkeletonGenerator(spark)
        gen.generate_skeletons(
            fact_df=fact_df,
            dim_table_name="warehouse.dim_customer",
            fact_join_key="customer_id",
            dim_join_key="customer_id",
            surrogate_key_col="customer_sk",
            batch_id="batch-001",
        )
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def generate_skeletons(
        self,
        fact_df: DataFrame,
        dim_table_name: str,
        fact_join_key: str,
        dim_join_key: str,
        surrogate_key_col: str,
        batch_id: str | None = None,
    ) -> None:
        """Insert skeleton rows for *fact_df* keys missing from *dim_table_name*.

        No-op if the dimension table does not exist or lacks ``__is_skeleton``.
        """
        if not self.spark.catalog.tableExists(dim_table_name):
            logger.debug(
                "Skeleton generation skipped: %s does not exist", dim_table_name
            )
            return
        dim = self.spark.table(dim_table_name)
        if "__is_skeleton" not in dim.columns:
            logger.debug(
                "Skeleton generation skipped: %s has no __is_skeleton column",
                dim_table_name,
            )
            return
        if not self.spark.catalog.tableExists(dim_table_name):
            return

        source_keys = fact_df.select(fact_join_key).distinct()
        dim_keys = dim.select(dim_join_key).distinct()
        missing = source_keys.join(dim_keys, source_keys[fact_join_key] == dim_keys[dim_join_key], "left_anti")
        missing_keys = missing.select(fact_join_key).collect()
        if not missing_keys:
            return

        skeletons = missing.select(
            F.col(fact_join_key).alias(dim_join_key),
        )
        existing_columns = set(dim.columns)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        for field in dim.schema.fields:
            name = field.name
            if name == dim_join_key:
                continue
            if name == "__is_skeleton":
                skeletons = skeletons.withColumn(name, F.lit(True))
            elif name == "__is_current":
                skeletons = skeletons.withColumn(name, F.lit(True))
            elif name == "__is_deleted":
                skeletons = skeletons.withColumn(name, F.lit(False))
            elif name == "__valid_from":
                skeletons = skeletons.withColumn(name, F.lit(DEFAULT_VALID_FROM))
            elif name == "__valid_to":
                skeletons = skeletons.withColumn(name, F.lit(None).cast(TimestampType()))
            elif name == "__etl_processed_at":
                skeletons = skeletons.withColumn(name, F.current_timestamp())
            elif name == "__etl_batch_id":
                skeletons = skeletons.withColumn(
                    name, F.lit(batch_id or "skeleton")
                )
            elif name == "__skeleton_created_at":
                skeletons = skeletons.withColumn(name, F.current_timestamp())
            elif name in ("__merge_action", "__scd2_intermediate", "__scd2_seq", "__scd2_total"):
                continue
            else:
                skeletons = skeletons.withColumn(name, F.lit(None).cast(field.dataType))

        if surrogate_key_col not in skeletons.columns:
            skeletons = skeletons.withColumn(
                surrogate_key_col,
                F.monotonically_increasing_id(),
            )

        target_alias = "target"
        source_alias = "source"
        condition = f"{target_alias}.`{dim_join_key}` = {source_alias}.`{dim_join_key}`"
        DeltaTable.forName(self.spark, dim_table_name).alias(target_alias).merge(
            skeletons.alias(source_alias), condition
        ).whenNotMatchedInsertAll().execute()
        logger.info(
            "Inserted %d skeleton row(s) into %s", len(missing_keys), dim_table_name
        )
