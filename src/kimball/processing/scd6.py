from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col, lit, when

from kimball.processing.hashing import compute_hashdiff
from kimball.processing.key_generator import HashKeyGenerator
from kimball.processing.merge_helpers import filter_cdf_deletes

logger = logging.getLogger(__name__)


def merge_scd6(
    source_df: DataFrame,
    *,
    target_table_name: str,
    join_keys: list[str],
    track_history_columns: list[str],
    current_value_columns: list[str],
    surrogate_key_col: str = "surrogate_key",
    schema_evolution: bool = False,
    effective_at_column: str | None = None,
) -> None:
    spark = source_df.sparkSession
    current_cols = current_value_columns
    upserts, deletes = filter_cdf_deletes(source_df)
    upserts = upserts.withColumn("hashdiff", compute_hashdiff(track_history_columns))
    changed_keys = upserts.select(*join_keys).distinct()
    target_df = spark.table(target_table_name)
    all_existing = target_df.join(changed_keys, join_keys, "inner")
    current_values = upserts.select(
        *join_keys, "hashdiff",
        *[col(c).alias(f"new_current_{c}") for c in current_cols],
    )
    staged_updates = (
        all_existing.alias("old")
        .join(current_values, join_keys)
        .select(
            col(f"old.{surrogate_key_col}"),
            *[col(f"old.{c}") for c in all_existing.columns if c != surrogate_key_col and not c.startswith("current_")],
            *[col(f"new_current_{c}").alias(f"current_{c}") for c in current_cols],
            when(col("old.__is_current") & (col("old.hashdiff") != col("current_values.hashdiff")), lit("EXPIRE"))
            .otherwise(lit("UPDATE"))
            .alias("__action"),
        )
    )
    target_current = (
        target_df.filter("__is_current = true").select(*join_keys, "hashdiff").alias("tgt")
    )
    source_with_flag = (
        upserts.alias("src")
        .join(target_current, join_keys, "left")
        .select(
            col("src.*"),
            when(col("tgt.hashdiff").isNull(), lit("INSERT_NEW"))
            .when(col("tgt.hashdiff") != col("src.hashdiff"), lit("INSERT_VERSION"))
            .otherwise(lit("NO_OP"))
            .alias("__insert_action"),
        )
    )
    rows_to_insert = (
        source_with_flag.filter(col("__insert_action") != "NO_OP")
        .drop("__insert_action")
        .select(
            *[col(c) for c in upserts.columns],
            *[col(c).alias(f"current_{c}") for c in current_cols],
            lit("INSERT").alias("__action"),
        )
    )
    staged_upserts = rows_to_insert.unionByName(
        staged_updates.select(*rows_to_insert.columns), allowMissingColumns=True
    )
    effective_col = effective_at_column or "__etl_processed_at"
    if deletes is not None and not deletes.isEmpty():
        delete_targets = (
            target_df.filter("__is_current = true")
            .join(deletes, join_keys)
            .select(
                col(surrogate_key_col), col(effective_col).alias("_effective_at"),
                lit("EXPIRE_DELETE").alias("__action"),
            )
        )
        staged_final = staged_upserts.unionByName(delete_targets, allowMissingColumns=True)
    else:
        staged_final = staged_upserts
    # Only generate new SKs for INSERT rows — UPDATE/EXPIRE/EXPIRE_DELETE
    # rows already carry the correct target SK and must not be overwritten.
    insert_rows = staged_final.filter(col("__action") == "INSERT")
    insert_rows_with_sk = HashKeyGenerator(join_keys, version_column=effective_at_column).generate_keys(
        insert_rows, surrogate_key_col
    )
    non_insert_rows = staged_final.filter(col("__action") != "INSERT")
    staged_final = insert_rows_with_sk.unionByName(non_insert_rows, allowMissingColumns=True)
    DeltaTable.forName(spark, target_table_name).alias("t").merge(
        staged_final.alias("s"), f"t.{surrogate_key_col} = s.{surrogate_key_col}"
    ).whenMatchedUpdate(
        condition="s.__action = 'EXPIRE'",
        set={"__is_current": "false", "__valid_to": f"s.{effective_col}", **{f"current_{c}": f"s.current_{c}" for c in current_cols}},
    ).whenMatchedUpdate(
        condition="s.__action = 'EXPIRE_DELETE'",
        set={"__is_current": "false", "__valid_to": f"s.{effective_col}", "__is_deleted": "true"},
    ).whenMatchedUpdate(
        condition="s.__action = 'UPDATE'",
        set={f"current_{c}": f"s.current_{c}" for c in current_cols},
    ).whenNotMatchedInsert(
        condition="s.__action = 'INSERT'",
        values={c: f"s.{c}" for c in staged_final.columns if c != "__action"},
    ).execute()