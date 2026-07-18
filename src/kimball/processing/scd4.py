from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, expr, lit, row_number

from kimball.processing.scd1 import merge_scd1

logger = logging.getLogger(__name__)


def merge_scd4(
    source_df: DataFrame,
    *,
    target_table_name: str,
    history_table_name: str,
    join_keys: list[str],
    track_history_columns: list[str],
    surrogate_key_col: str = "surrogate_key",
    schema_evolution: bool = False,
    effective_at_column: str | None = None,
) -> None:
    reusable_source = source_df.persist()
    try:
        merge_scd1(
            reusable_source,
            target_table_name=target_table_name,
            join_keys=join_keys,
            delete_strategy="hard",
            schema_evolution=schema_evolution,
            surrogate_key_col=surrogate_key_col,
        )
        _merge_history(
            reusable_source,
            target_table_name,
            history_table_name,
            join_keys,
            track_history_columns,
            surrogate_key_col,
            effective_at_column or "__etl_processed_at",
        )
    finally:
        reusable_source.unpersist(blocking=False)


def _merge_history(
    source_df: DataFrame,
    target_table_name: str,
    history_table_name: str,
    join_keys: list[str],
    track_history_columns: list[str],
    surrogate_key_col: str,
    effective_at_column: str,
) -> None:
    spark = source_df.sparkSession
    if track_history_columns == ["*"]:
        exclude = {
            surrogate_key_col,
            effective_at_column,
            "_change_type",
            "__etl_processed_at",
            "__etl_batch_id",
        }
        track_cols = [
            c for c in source_df.columns if c not in exclude and not c.startswith("__")
        ]
    else:
        track_cols = track_history_columns

    stack_expr = ", ".join([f"'{c}', cast({c} as string)" for c in track_cols])
    select_cols = [
        surrogate_key_col,
        f"stack({len(track_cols)}, {stack_expr}) as (field, value)",
        f"{effective_at_column} as effective_at",
    ]
    if "_change_type" in source_df.columns:
        select_cols.append("_change_type")
    else:
        select_cols.append("'insert' as _change_type")
    unpivoted = source_df.selectExpr(*select_cols)

    history = spark.table(history_table_name).filter("__is_current = true")
    compared = (
        unpivoted.alias("src")
        .join(
            history.alias("tgt"),
            (col("src." + surrogate_key_col) == col("tgt.surrogate_key"))
            & (col("src.field") == col("tgt.field")),
            "left",
        )
        .filter(
            col("tgt.value").isNull()
            | (col("tgt.value") != col("src.value"))
            | (col("src._change_type") == "delete")
        )
        .select(col("src.*"), col("tgt.value").alias("old_value"))
    )
    inserts = compared.filter(col("_change_type") != "delete").select(
        col(surrogate_key_col).alias("surrogate_key"),
        col("field"),
        col("value"),
        col("effective_at").alias("valid_from"),
        lit("9999-12-31 23:59:59").cast("timestamp").alias("valid_to"),
        lit(True).alias("__is_current"),
        lit("INSERT").alias("__action"),
    )
    expires = compared.filter(col("old_value").isNotNull()).select(
        col(surrogate_key_col).alias("surrogate_key"),
        col("field"),
        col("old_value").alias("value"),
        lit(None).cast("timestamp").alias("valid_from"),
        (col("effective_at") - expr("INTERVAL 1 MICROSECOND")).alias("valid_to"),
        lit(False).alias("__is_current"),
        lit("EXPIRE").alias("__action"),
    )
    dedup_window = Window.partitionBy("surrogate_key", "field", "value").orderBy(
        col("valid_from").desc()
    )
    inserts_deduped = (
        inserts.withColumn("_rn", row_number().over(dedup_window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    staged = inserts_deduped.unionByName(expires)
    DeltaTable.forName(spark, history_table_name).alias("target").merge(
        staged.alias("source"),
        "target.surrogate_key = source.surrogate_key AND target.field = source.field AND target.value <=> source.value AND target.__is_current = true AND source.__action = 'EXPIRE'",
    ).whenMatchedUpdate(
        set={"valid_to": "source.valid_to", "__is_current": "source.__is_current"}
    ).whenNotMatchedInsert(
        condition="source.__action = 'INSERT'",
        values={
            "surrogate_key": "source.surrogate_key",
            "field": "source.field",
            "value": "source.value",
            "valid_from": "source.valid_from",
            "valid_to": "source.valid_to",
            "__is_current": "source.__is_current",
        },
    ).execute()
