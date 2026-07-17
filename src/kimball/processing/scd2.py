from __future__ import annotations

import logging
from functools import reduce
from typing import Any, cast

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    lead,
    lit,
    row_number,
    when,
)
from pyspark.sql.window import Window

from kimball.common.spark_session import get_spark
from kimball.processing.hashing import compute_hashdiff
from kimball.processing.merge_helpers import (
    _CDF_METADATA,
    apply_schema_evolution,
    build_merge_condition,
    filter_cdf_deletes,
    generate_keys,
    get_current_df,
    get_validity_col,
)

logger = logging.getLogger(__name__)


def _select_payload_columns(
    source_df: DataFrame,
    join_keys: list[str],
    track_history_columns: list[str],
    include_meta: bool = False,
    effective_at_column: str | None = None,
) -> DataFrame:
    keep = set(join_keys) | set(track_history_columns)
    if "updated_at" in source_df.columns:
        keep.add("updated_at")
    if "effective_at" in source_df.columns:
        keep.add("effective_at")
    if effective_at_column and effective_at_column in source_df.columns:
        keep.add(effective_at_column)
    if include_meta:
        keep.update(
            c
            for c in ["_change_type", "_commit_version", "_commit_timestamp"]
            if c in source_df.columns
        )
    keep.update(c for c in source_df.columns if c.startswith("__scd2_"))
    drop = {
        "__merge_action",
        "target_hashdiff",
        "target_sk",
        "target_is_skeleton",
        "hashdiff",
        "__etl_processed_at",
        "__etl_batch_id",
        "__is_current",
        "__valid_from",
        "__valid_to",
        "__is_deleted",
        "__is_skeleton",
    }
    return source_df.select(
        *(c for c in source_df.columns if c in keep and c not in drop)
    )


def _merge_single_pass(
    source_df: DataFrame,
    *,
    target_table_name: str,
    join_keys: list[str],
    track_history_columns: list[str],
    surrogate_key_col: str,
    schema_evolution: bool,
    effective_at_column: str | None,
) -> None:
    """Single-pass SCD2 MERGE using SK-based matching.

    Stages all source versions (not just the latest), computes the full
    history chain in the staging DataFrame, and executes a single MERGE
    where ``whenMatchedUpdate`` expires the old row (matched by SK) and
    ``whenNotMatchedInsert`` inserts all new versions.

    This eliminates the two-phase ``_merge_current`` + ``_rebuild_history``
    approach, reducing Spark actions from 3-4 to 1.
    """
    if not track_history_columns:
        raise ValueError("track_history_columns must be provided for SCD Type 2")
    upserts, deletes = filter_cdf_deletes(source_df)
    if upserts.isEmpty() and (deletes is None or deletes.isEmpty()):
        logger.info("SCD2 no-op: no upserts or deletes â€” skipping merge")
        return
    delta_table = DeltaTable.forName(get_spark(), target_table_name)
    target_has_skeleton_col = "__is_skeleton" in [
        f.name for f in delta_table.toDF().schema.fields
    ]

    # --- Handle explicit CDF deletes ---
    if deletes is not None and not deletes.isEmpty():
        vcol = (
            f"source.{effective_at_column}"
            if effective_at_column and effective_at_column in source_df.columns
            else "current_timestamp()"
        )
        deletes = deletes.dropDuplicates(join_keys)
        delta_table.alias("target").merge(
            deletes.alias("source"),
            build_merge_condition(join_keys, current_only=True),
        ).whenMatchedUpdate(
            set={
                "__is_current": "false",
                "__valid_to": vcol,
                "__etl_processed_at": "current_timestamp()",
                "__is_deleted": "true",
            }
        ).execute()
        logger.info("SCD2: CDF deletes expired")
        upserts = source_df.filter(col("_change_type") != "delete")
        if upserts.isEmpty():
            logger.info("SCD2 delete-only CDF batch completed")
            return

    # --- Full-snapshot delete detection ---
    # ``None`` means a full snapshot (no CDF marker exists).  An empty CDF
    # delete DataFrame means this incremental commit simply had no deletes.
    if deletes is None:
        current_target = get_current_df(delta_table)
        if target_has_skeleton_col:
            current_target = current_target.filter(~col("__is_skeleton"))
        missing_in_source = current_target.join(
            upserts.select(*join_keys).distinct(), join_keys, "left_anti"
        ).dropDuplicates(join_keys)
        if not missing_in_source.isEmpty():
            keys_expr = (
                " AND ".join([f"target.{k} <=> source.{k}" for k in join_keys])
                + " AND target.__is_current = true"
            )
            delta_table.alias("target").merge(
                missing_in_source.alias("source"), keys_expr
            ).whenMatchedUpdate(
                set={
                    "__is_current": "false",
                    "__valid_to": "current_timestamp()",
                    "__etl_processed_at": "current_timestamp()",
                    "__is_deleted": "true",
                }
            ).execute()
            logger.info("SCD2: full-snapshot delete detection merge executed")

    apply_schema_evolution(target_table_name, schema_evolution, upserts)
    upserts = upserts.withColumn("hashdiff", compute_hashdiff(track_history_columns))

    # --- Stage all versions with chain metadata ---
    order_col = next(
        (
            c
            for c in ("_commit_version", "_commit_timestamp", "__etl_processed_at")
            if c in upserts.columns
        ),
        None,
    )
    if order_col is None:
        order_col = "__etl_processed_at"
        if order_col not in upserts.columns:
            upserts = upserts.withColumn("__etl_processed_at", current_timestamp())

    # ``validity_col`` is SQL-qualified (for example ``source.updated_at``).
    # Spark window expressions need the bare source column name.
    _validity_col_name = effective_at_column or order_col or "__etl_processed_at"

    # Build validity boundaries across the complete incoming chain before
    # splitting latest and historical rows.  Computing ``lead`` over only
    # historical rows leaves the newest historical row open-ended whenever a
    # newer current row exists in the same batch.
    w_desc = Window.partitionBy(*join_keys).orderBy(col(order_col).desc())
    w_asc = Window.partitionBy(*join_keys).orderBy(col(order_col).asc())
    ranked = upserts.withColumn("_rn", row_number().over(w_desc)).withColumn(
        "__scd2_next_valid_from", lead(_validity_col_name, 1).over(w_asc)
    )
    latest = ranked.filter(col("_rn") == 1).drop("_rn")
    older = ranked.filter(col("_rn") > 1).drop("_rn")

    # Join latest to target to get target_sk and target_hashdiff
    source_keys = latest.select(*join_keys).distinct()
    target_df = get_current_df(delta_table).join(source_keys, join_keys, "semi")
    join_conditions = [latest[k].eqNullSafe(target_df[k]) for k in join_keys]
    combined = reduce(lambda a, b: a & b, join_conditions) if join_conditions else None
    joined = (
        latest.alias("s")
        .join(target_df.alias("t"), combined, "left")
        .select(
            "s.*",
            col("t.hashdiff").alias("target_hashdiff"),
            col("t." + surrogate_key_col).alias("target_sk"),
            col("t.__is_skeleton").alias("target_is_skeleton")
            if target_has_skeleton_col
            else lit(False).alias("target_is_skeleton"),
        )
    )

    # Classify rows â€” keep target_sk for expire rows (needed for SK-based MERGE match)
    rows_new = joined.filter(col("target_sk").isNull()).drop(
        "target_hashdiff", "target_sk", "target_is_skeleton"
    )
    rows_changed = joined.filter(
        col("target_sk").isNotNull()
        & ~col("target_is_skeleton")
        & (
            (col("hashdiff") != col("target_hashdiff"))
            | (col("hashdiff").isNull() != col("target_hashdiff").isNull())
        )
    ).drop("target_hashdiff", "target_is_skeleton")

    rows_to_hydrate = None
    if target_has_skeleton_col:
        # Keep target_sk so the HYDRATE row matches the skeleton via the SK-based
        # MERGE condition (target.sk = source.sk) and gets hydrated in place,
        # preserving the skeleton's original SK that fact FKs already point at.
        # Previously target_sk was dropped and no __merge_action was set, so these
        # rows failed to match and were inserted as null-SK duplicates while the
        # skeleton stayed unfilled (silent data loss / orphaned FKs).
        rows_to_hydrate = (
            joined.filter(
                col("target_sk").isNotNull()
                & col("target_is_skeleton")
                & (
                    (col("hashdiff") != col("target_hashdiff"))
                    | (col("hashdiff").isNull() != col("target_hashdiff").isNull())
                )
            )
            .drop("target_hashdiff", "target_is_skeleton")
            .withColumn("__merge_action", lit("HYDRATE"))
        )

    # Build the chain: expire row + all versions
    validity_col, validity_note = get_validity_col(
        effective_at_column, upserts, target_table_name
    )
    logger.info(f"SCD2 time semantics: using {validity_note}")
    # Oldest new version's valid_from per key. The old current target row must be
    # expired at (oldest_new_valid_from - 1us), NOT at the latest version's
    # valid_from: expiring at the latest value makes the old row's
    # [t0, latest_from] interval overlap the back-filled intermediate versions,
    # so a point-in-time read between the oldest and latest new version returns
    # the stale old row instead of the correct intermediate version.
    oldest_valid_from = upserts.groupBy(*join_keys).agg(
        expr(
            f"min(CAST(`{_validity_col_name}` AS TIMESTAMP)) as __scd2_oldest_valid_from"
        )
    )
    rows_changed = rows_changed.join(oldest_valid_from, join_keys, "left")

    # The expire row: carry oldest valid_from so the MERGE update can set
    # __valid_to = oldest_valid_from - 1 MICROSECOND (see EXPIRE set below).
    expire_rows = rows_changed.withColumn("__merge_action", lit("EXPIRE"))
    expire_rows = expire_rows.withColumn(
        "__valid_to",
        col("__scd2_oldest_valid_from") - expr("INTERVAL 1 MICROSECOND"),
    )

    # The latest version row: __is_current = true, __valid_to = null
    latest_version = rows_changed.withColumn("__merge_action", lit("INSERT_LATEST"))
    latest_version = latest_version.withColumn("__is_current", lit(True))
    latest_version = latest_version.withColumn(
        "__valid_to", lit(None).cast("timestamp")
    )

    # New rows (no prior version): __is_current = true, __valid_to = null
    new_rows = rows_new.withColumn("__merge_action", lit("INSERT_NEW"))
    new_rows = new_rows.withColumn("__is_current", lit(True))
    new_rows = new_rows.withColumn("__valid_to", lit(None).cast("timestamp"))

    # Older versions: __is_current = false, chain valid_to to next version
    older_versions = None
    if not older.isEmpty():
        older_versions = older.withColumn("__merge_action", lit("INSERT_OLDER"))
        older_versions = older_versions.withColumn("__is_current", lit(False))
        older_versions = older_versions.withColumn(
            "__valid_to",
            when(
                col("__scd2_next_valid_from").isNull(), lit(None).cast("timestamp")
            ).otherwise(
                col("__scd2_next_valid_from").cast("timestamp")
                - expr("INTERVAL 1 MICROSECOND")
            ),
        ).drop("__scd2_next_valid_from")

    # Union all staged rows
    # has_changed gates both the expire/latest staging AND the EXPIRE
    # whenMatchedUpdate branch below: the EXPIRE update set references
    # source.__scd2_oldest_valid_from, which only exists on expire_rows
    # (derived from rows_changed). When the only matched target is a skeleton
    # (routed to HYDRATE, not rows_changed) there are no expire rows and the
    # column is absent from final_source â€” adding the EXPIRE branch unconditionally
    # then fails Delta plan resolution with DELTA_MERGE_UNRESOLVED_EXPRESSION.
    has_changed = not rows_changed.isEmpty()
    staged = new_rows
    if has_changed:
        staged = staged.unionByName(expire_rows, allowMissingColumns=True)
        staged = staged.unionByName(latest_version, allowMissingColumns=True)
    if older_versions is not None:
        staged = staged.unionByName(older_versions, allowMissingColumns=True)
    if rows_to_hydrate is not None and not rows_to_hydrate.isEmpty():
        staged = staged.unionByName(rows_to_hydrate, allowMissingColumns=True)

    # Generate surrogate keys for all insert rows. EXPIRE and HYDRATE rows keep
    # the matched target row's existing SK (set below), so they are routed to
    # rows_no_keys rather than generate_keys.
    rows_needing_keys = staged.filter(
        col("__merge_action").isin("INSERT_NEW", "INSERT_LATEST", "INSERT_OLDER")
    )
    rows_no_keys = staged.filter(col("__merge_action").isin("EXPIRE", "HYDRATE"))
    rows_with_keys = generate_keys(
        rows_needing_keys,
        join_keys,
        surrogate_key_col,
        scd_type=2,
        effective_at_column=effective_at_column,
    )
    # EXPIRE / HYDRATE rows: set surrogate_key_col to the matched target row's
    # SK so the SK-based MERGE condition matches the existing target row
    # (target.customer_sk = source.customer_sk). For HYDRATE this is the
    # skeleton's SK, so the skeleton is hydrated in place and keeps its original
    # SK â€” fact FKs remain valid, no null-SK duplicate is inserted.
    if "target_sk" in rows_no_keys.columns:
        rows_no_keys = rows_no_keys.withColumn(surrogate_key_col, col("target_sk"))
    elif surrogate_key_col not in rows_no_keys.columns:
        rows_no_keys = rows_no_keys.withColumn(surrogate_key_col, lit(None))
    final_source = rows_with_keys.unionByName(rows_no_keys, allowMissingColumns=True)

    # Nullify join keys on insert rows so they don't match on NK
    for k in join_keys:
        final_source = final_source.withColumn(f"__orig_{k}", col(k))
        final_source = final_source.withColumn(
            k,
            when(col("__merge_action") == "EXPIRE", col(k)).otherwise(lit(None)),
        )

    # Build insert values dict â€” only include columns that exist in the target table
    target_col_names = {f.name for f in delta_table.toDF().schema.fields}
    insert_values: dict[str, str] = {}
    for c in upserts.columns:
        if c in _CDF_METADATA or c not in target_col_names:
            continue
        insert_values[c] = f"source.__orig_{c}" if c in join_keys else f"source.{c}"
    # System columns â€” read from the staged source so older versions get
    # __is_current=false and __valid_to set correctly.
    if "__is_current" in target_col_names:
        insert_values["__is_current"] = "source.__is_current"
    if "__valid_from" in target_col_names:
        insert_values["__valid_from"] = f"COALESCE({validity_col}, current_timestamp())"
    if "__valid_to" in target_col_names:
        insert_values["__valid_to"] = "source.__valid_to"
    if "__etl_processed_at" in target_col_names:
        insert_values["__etl_processed_at"] = "current_timestamp()"
    if "__is_deleted" in target_col_names:
        insert_values["__is_deleted"] = "false"
    if "__is_skeleton" in target_col_names:
        insert_values["__is_skeleton"] = "false"
    if (
        surrogate_key_col in final_source.columns
        and surrogate_key_col in target_col_names
    ):
        insert_values[surrogate_key_col] = f"source.{surrogate_key_col}"

    # Single MERGE: match on SK for expire, insert everything else
    merge_condition = f"target.{surrogate_key_col} = source.{surrogate_key_col}"
    merge_builder = delta_table.alias("target").merge(
        final_source.alias("source"), merge_condition
    )

    if target_has_skeleton_col and rows_to_hydrate is not None:
        hydration_set = {
            c: (f"source.__orig_{c}" if c in join_keys else f"source.{c}")
            for c in upserts.columns
            if c not in _CDF_METADATA
            and not c.startswith("__scd2_")
            and c in target_col_names
        }
        hydration_set.update(
            {
                "__is_skeleton": "false",
                "__valid_from": f"COALESCE({validity_col}, current_timestamp())",
                "__is_current": "true",
                "__etl_processed_at": "current_timestamp()",
                "__is_deleted": "false",
            }
        )
        hydration_set.pop(surrogate_key_col, None)
        merge_builder = merge_builder.whenMatchedUpdate(
            condition="target.__is_skeleton = true AND source.__merge_action = 'HYDRATE'",
            set=cast(Any, hydration_set),
        )

    if has_changed:
        merge_builder = merge_builder.whenMatchedUpdate(
            condition="source.__merge_action = 'EXPIRE'",
            set={
                "__is_current": "false",
                "__valid_to": "COALESCE(source.__scd2_oldest_valid_from - INTERVAL 1 MICROSECOND, current_timestamp())",
                "__etl_processed_at": "current_timestamp()",
                "__is_deleted": "false",
            },
        )
    merge_builder.whenNotMatchedInsert(values=cast(Any, insert_values)).execute()
    logger.info("SCD2 single-pass MERGE executed")


def merge_scd2(
    source_df: DataFrame,
    *,
    target_table_name: str,
    join_keys: list[str],
    track_history_columns: list[str],
    surrogate_key_col: str,
    schema_evolution: bool = False,
    effective_at_column: str | None = None,
) -> None:
    if not track_history_columns:
        raise ValueError("track_history_columns must be provided for SCD Type 2")
    _merge_single_pass(
        source_df,
        target_table_name=target_table_name,
        join_keys=join_keys,
        track_history_columns=track_history_columns,
        surrogate_key_col=surrogate_key_col,
        schema_evolution=schema_evolution,
        effective_at_column=effective_at_column,
    )
