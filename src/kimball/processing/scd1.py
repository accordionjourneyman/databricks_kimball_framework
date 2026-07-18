from __future__ import annotations

import logging
from typing import Any, cast

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

from kimball.common.spark_session import get_spark
from kimball.processing.merge_helpers import (
    _CDF_METADATA,
    apply_schema_evolution,
    build_merge_condition,
    dedup_cdf,
    generate_keys,
)

logger = logging.getLogger(__name__)


def merge_scd1(
    source_df: DataFrame,
    *,
    target_table_name: str,
    join_keys: list[str],
    delete_strategy: str = "hard",
    schema_evolution: bool = False,
    surrogate_key_col: str | None = None,
    append_only: bool = False,
) -> None:
    if not join_keys:
        raise ValueError("join_keys must be provided for SCD1 MERGE.")
    source_df = dedup_cdf(source_df, join_keys)
    if append_only:
        if "__is_deleted" not in source_df.columns:
            from pyspark.sql.functions import lit

            source_df = source_df.withColumn("__is_deleted", lit(False))
        if "__etl_processed_at" not in source_df.columns:
            source_df = source_df.withColumn("__etl_processed_at", current_timestamp())
        source_df.write.format("delta").mode("append").saveAsTable(target_table_name)
        return

    apply_schema_evolution(target_table_name, schema_evolution, source_df)
    if surrogate_key_col:
        source_df = generate_keys(source_df, join_keys, surrogate_key_col, scd_type=1)

    delta_table = DeltaTable.forName(get_spark(), target_table_name)
    merge_builder = delta_table.alias("target").merge(
        source_df.alias("source"), build_merge_condition(join_keys)
    )
    if "_change_type" in source_df.columns:
        if delete_strategy == "hard":
            merge_builder = merge_builder.whenMatchedDelete(
                condition="source._change_type = 'delete'"
            )
        elif delete_strategy == "soft":
            merge_builder = merge_builder.whenMatchedUpdate(
                condition="source._change_type = 'delete'",
                set={
                    "__is_deleted": "true",
                    "__etl_processed_at": "current_timestamp()",
                    "__etl_batch_id": "source.__etl_batch_id",
                },
            )

    update_map = {
        column: f"source.{column}"
        for column in source_df.columns
        if column not in (surrogate_key_col, "_change_type")
        and column not in _CDF_METADATA
    }
    update_map.update(
        {"__is_deleted": "false", "__etl_processed_at": "current_timestamp()"}
    )
    changed_columns = [
        column
        for column in update_map
        if column not in {"__is_deleted", "__etl_processed_at", "__etl_batch_id"}
    ]
    changed_condition = (
        " OR ".join(
            f"NOT (target.{column} <=> source.{column})" for column in changed_columns
        )
        or "false"
    )
    non_delete = (
        "source._change_type != 'delete'"
        if "_change_type" in source_df.columns
        else None
    )
    update_condition = (
        f"({non_delete}) AND ({changed_condition})" if non_delete else changed_condition
    )
    insert_map = {
        column: f"source.{column}"
        for column in source_df.columns
        if column not in _CDF_METADATA
    }
    insert_map.update(
        {"__is_deleted": "false", "__etl_processed_at": "current_timestamp()"}
    )
    merge_builder.whenMatchedUpdate(
        condition=cast(Any, update_condition), set=cast(Any, update_map)
    ).whenNotMatchedInsert(
        condition=cast(Any, non_delete), values=cast(Any, insert_map)
    ).execute()
