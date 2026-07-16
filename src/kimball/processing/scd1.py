from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col, current_timestamp

from kimball.common.spark_session import get_spark
from kimball.processing.hashing import compute_hashdiff
from kimball.processing.merge_helpers import (
    PYSPARK_EXCEPTION_BASE,
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
    if source_df.isEmpty():
        logger.info("SCD1 no-op: dedup produced zero rows — skipping merge")
        return
    if append_only:
        logger.info("SCD1 append-only: inserting rows without MERGE")
        from pyspark.sql.functions import lit
        if "__is_deleted" not in source_df.columns:
            source_df = source_df.withColumn("__is_deleted", lit(False))
        if "__etl_processed_at" not in source_df.columns:
            source_df = source_df.withColumn("__etl_processed_at", current_timestamp())
        source_df.write.format("delta").mode("append").saveAsTable(target_table_name)
        return
    merge_condition = build_merge_condition(join_keys)
    delta_table = DeltaTable.forName(get_spark(), target_table_name)

    if "_change_type" not in source_df.columns and not source_df.isEmpty():
        try:
            target_df = delta_table.toDF()
            data_cols = [
                c for c in source_df.columns
                if c not in _CDF_METADATA
                and not c.startswith("__")
                and c != surrogate_key_col
                and c in target_df.columns
            ]
            if data_cols:
                source_hashed = source_df.withColumn("_hash", compute_hashdiff(data_cols))
                target_hashed = target_df.select(*join_keys, *[c for c in data_cols if c in target_df.columns]).withColumn("_hash", compute_hashdiff(data_cols))
                mismatched = source_hashed.join(
                    target_hashed.select(*join_keys, "_hash").withColumnRenamed("_hash", "_tgt_hash"),
                    join_keys, "left",
                ).filter(col("_tgt_hash").isNull() | (col("_hash") != col("_tgt_hash")))
                if mismatched.limit(1).count() == 0:
                    logger.info("SCD1 no-op: all source hashes match target — skipping merge")
                    return
        except PYSPARK_EXCEPTION_BASE:
            pass

    apply_schema_evolution(target_table_name, schema_evolution, source_df)
    if surrogate_key_col:
        source_df = generate_keys(source_df, join_keys, surrogate_key_col, scd_type=1)
    merge_builder = delta_table.alias("target").merge(source_df.alias("source"), merge_condition)
    if "_change_type" in source_df.columns:
        if delete_strategy == "hard":
            merge_builder = merge_builder.whenMatchedDelete(condition="source._change_type = 'delete'")
        elif delete_strategy == "soft":
            merge_builder = merge_builder.whenMatchedUpdate(
                condition="source._change_type = 'delete'",
                set={
                    "__is_deleted": "true",
                    "__etl_processed_at": "current_timestamp()",
                    "__etl_batch_id": "source.__etl_batch_id",
                },
            )
    update_cond = "source._change_type != 'delete'" if "_change_type" in source_df.columns else None
    update_map = {
        c: f"source.{c}"
        for c in source_df.columns
        if c not in (surrogate_key_col, "_change_type") and c not in _CDF_METADATA
    }
    update_map.update({"__is_deleted": "false", "__etl_processed_at": "current_timestamp()"})
    insert_map = {c: f"source.{c}" for c in source_df.columns if c not in _CDF_METADATA}
    insert_map.update({"__is_deleted": "false", "__etl_processed_at": "current_timestamp()"})
    merge_builder.whenMatchedUpdate(condition=update_cond, set=update_map).whenNotMatchedInsert(
        condition=update_cond, values=insert_map
    ).execute()