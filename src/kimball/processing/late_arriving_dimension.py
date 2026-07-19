"""Hydrate skeleton placeholder rows with real data.

When a skeleton row (``__is_skeleton = True``) exists in a dimension and
real source data arrives for the same natural key, this processor
updates the skeleton in place: fills in the real attribute values and
flips ``__is_skeleton`` to ``False``.
"""

from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class LateArrivingDimensionProcessor:
    """Update skeleton rows when real dimension data arrives.

    Usage::

        processor = LateArrivingDimensionProcessor(spark)
        updated = processor.update_skeletons_with_real_data(
            dimension_table="warehouse.dim_customer",
            source_df=real_data_df,
            natural_keys=["customer_id"],
        )
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def update_skeletons_with_real_data(
        self,
        dimension_table: str,
        source_df: DataFrame,
        natural_keys: list[str],
    ) -> int:
        """Update skeleton rows matching *source_df* keys.

        Returns the number of skeleton rows hydrated.
        """
        if not self.spark.catalog.tableExists(dimension_table):
            return 0
        dim = self.spark.table(dimension_table)
        if "__is_skeleton" not in dim.columns:
            return 0

        skeleton_keys = (
            dim.filter("__is_skeleton = true")
            .select(*natural_keys)
            .distinct()
        )
        if skeleton_keys.head(1) is None or skeleton_keys.head(1) == []:
            return 0

        incoming = source_df.select(*natural_keys).distinct()
        to_update = incoming.join(
            skeleton_keys,
            on=natural_keys,
            how="inner",
        )
        if to_update.head(1) is None or to_update.head(1) == []:
            return 0

        update_count = to_update.count()
        source_alias = "source"
        target_alias = "target"
        match_condition = " AND ".join(
            f"{target_alias}.`{k}` = {source_alias}.`{k}`" for k in natural_keys
        )
        skeleton_condition = f"{target_alias}.__is_skeleton = true"

        source_columns = [c for c in source_df.columns if c not in ("__merge_action",)]
        set_map = {
            col_name: f"source.`{col_name}`"
            for col_name in source_columns
            if col_name in dim.columns and col_name not in natural_keys
        }
        set_map["__is_skeleton"] = "false"
        set_map["__etl_processed_at"] = "current_timestamp()"

        full_condition = f"({match_condition}) AND {skeleton_condition}"

        DeltaTable.forName(self.spark, dimension_table).alias(target_alias).merge(
            source_df.select(*source_columns).alias(source_alias),
            full_condition,
        ).whenMatchedUpdate(set=set_map).execute()

        logger.info(
            "Hydrated %d skeleton row(s) in %s", update_count, dimension_table
        )
        return update_count
