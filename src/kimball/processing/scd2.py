from __future__ import annotations

import logging
import os
from functools import reduce
from typing import Any, cast

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,  # noqa: F401  (test suite patches this module attribute)
    expr,  # noqa: F401  (test suite patches this module attribute)
    lit,
    when,
)

from kimball.common.spark_session import get_spark
from kimball.processing.hashing import compute_hashdiff
from kimball.processing.key_integrity import validate_type7_keys
from kimball.processing.merge_helpers import (
    _CDF_METADATA,
    apply_schema_evolution,
    build_merge_condition,
    filter_cdf_deletes,
    generate_keys,
    get_current_df,
    get_validity_col,
)
from kimball.processing.staging import (
    choose_order_column,
    rank_source_versions,
    stage_scd2_changes,
)

logger = logging.getLogger(__name__)


def _merge_single_pass(
    source_df: DataFrame,
    *,
    target_table_name: str,
    join_keys: list[str],
    track_history_columns: list[str],
    surrogate_key_col: str,
    schema_evolution: bool,
    effective_at_column: str | None,
    full_snapshot_reconciliation: bool,
    durable_key_col: str | None,
    scd_type: int,
) -> None:
    """Single-pass SCD2 MERGE using SK-based matching.

    Stages all source versions (not just the latest), computes the full
    history chain in the staging DataFrame, and executes a single MERGE
    where ``whenMatchedUpdate`` expires the old row (matched by SK) and
    ``whenNotMatchedInsert`` inserts all new versions.

    This eliminates the retired two-phase ``_merge_current`` and
    ``_rebuild_history`` implementation.  The target mutation is one Delta
    MERGE; exact empty/change/collision safety checks remain eager Spark
    actions and are deliberately accounted for in the benchmark suite.
    """
    if not track_history_columns:
        raise ValueError("track_history_columns must be provided for SCD Type 2")

    lazy_eval = os.environ.get("KIMBALL_OPTIMIZE_SCD2_LAZY_EVAL") == "1"

    upserts, deletes = filter_cdf_deletes(source_df)

    source_is_empty = False if lazy_eval else upserts.isEmpty()

    if not lazy_eval and (
        source_is_empty
        and (deletes is None or deletes.isEmpty())
        and not full_snapshot_reconciliation
    ):
        logger.info("SCD2 no-op: no upserts or deletes; skipping merge")
        return
    delta_table = DeltaTable.forName(get_spark(), target_table_name)
    target_has_skeleton_col = "__is_skeleton" in [
        f.name for f in delta_table.toDF().schema.fields
    ]

    # --- Handle explicit CDF deletes ---
    if deletes is not None:
        has_deletes = True if lazy_eval else not deletes.isEmpty()
        if has_deletes:
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
            if source_is_empty:
                logger.info("SCD2 delete-only CDF batch completed")
                return

    # --- Full-snapshot delete detection ---
    # ``None`` means a full snapshot (no CDF marker exists).  An empty CDF
    # delete DataFrame means this incremental commit simply had no deletes.
    skip_delete_detection = os.environ.get("KIMBALL_SKIP_DELETE_DETECTION") == "1"
    if deletes is None and full_snapshot_reconciliation and not skip_delete_detection:
        current_target = get_current_df(delta_table)
        if target_has_skeleton_col:
            current_target = current_target.filter(~col("__is_skeleton"))
        missing_in_source = current_target.join(
            upserts.select(*join_keys).distinct(), join_keys, "left_anti"
        ).dropDuplicates(join_keys)
        has_missing = True if lazy_eval else not missing_in_source.isEmpty()
        if has_missing:
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

    if source_is_empty:
        return
    apply_schema_evolution(target_table_name, schema_evolution, upserts)
    upserts = upserts.withColumn("hashdiff", compute_hashdiff(track_history_columns))

    # --- Stage all versions with chain metadata ---
    # Staging algebra (order-column choice, ranking, target join, bucket
    # classification, validity boundaries, union) lives in
    # processing/staging.py as pure DataFrame functions (ADR-004). The
    # write layer below only orchestrates key generation and the MERGE.
    order_col, upserts = choose_order_column(upserts)

    # ``validity_col`` is SQL-qualified (for example ``source.updated_at``).
    # Spark window expressions need the bare source column name.
    _validity_col_name = effective_at_column or order_col or "__etl_processed_at"

    # Build validity boundaries across the complete incoming chain before
    # splitting latest and historical rows.  Computing ``lead`` over only
    # historical rows leaves the newest historical row open-ended whenever a
    # newer current row exists in the same batch.
    latest, older = rank_source_versions(
        upserts, join_keys, _validity_col_name, order_col
    )

    # Join latest to target to get target_sk and target_hashdiff
    source_keys = latest.select(*join_keys).distinct()
    target_df = get_current_df(delta_table).join(source_keys, join_keys, "semi")
    # Recompute the target hash with the current tracking contract
    # (always recompute; trust_stored_hashdiff flag removed in 0.3.0).
    target_df = target_df.withColumn(
        "__comparison_hashdiff", compute_hashdiff(track_history_columns)
    )
    join_conditions = [latest[k].eqNullSafe(target_df[k]) for k in join_keys]
    combined = reduce(lambda a, b: a & b, join_conditions) if join_conditions else None
    joined = (
        latest.alias("s")
        .join(target_df.alias("t"), combined, "left")
        .select(
            "s.*",
            col("t.__comparison_hashdiff").alias("target_hashdiff"),
            col(f"t.{surrogate_key_col}").alias("target_sk"),
            (
                col("t.__is_skeleton").alias("target_is_skeleton")
                if target_has_skeleton_col
                else lit(False).alias("target_is_skeleton")
            ),
        )
    )

    # Build the chain: expire row + all versions
    validity_col, validity_note = get_validity_col(
        effective_at_column, upserts, target_table_name
    )
    logger.info(f"SCD2 time semantics: using {validity_note}")

    staged_changes = stage_scd2_changes(
        joined=joined,
        upserts=upserts,
        older=older,
        join_keys=join_keys,
        validity_col=validity_col,
        validity_col_name=_validity_col_name,
        validity_note=validity_note,
        lazy_eval=lazy_eval,
    )
    staged = staged_changes.buckets["*"]
    has_changed = staged_changes.has_changed

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
        scd_type=scd_type,
        effective_at_column=effective_at_column,
        durable_key_col=durable_key_col,
    )
    if scd_type == 7 and durable_key_col:
        validate_type7_keys(
            rows_with_keys,
            delta_table.toDF(),
            surrogate_key=surrogate_key_col,
            durable_key=durable_key_col,
        )
    # EXPIRE / HYDRATE rows: set surrogate_key_col to the matched target row's
    # SK so the SK-based MERGE condition matches the existing target row
    # (target.customer_sk = source.customer_sk). For HYDRATE this is the
    # skeleton's SK, so the skeleton is hydrated in place and keeps its original
    # SK. Fact FKs remain valid and no null-SK duplicate is inserted.
    if "target_sk" in rows_no_keys.columns:
        rows_no_keys = rows_no_keys.withColumn(surrogate_key_col, col("target_sk"))
    elif surrogate_key_col not in rows_no_keys.columns:
        rows_no_keys = rows_no_keys.withColumn(surrogate_key_col, lit(None))
    if scd_type == 7 and durable_key_col:
        # HYDRATE rows bypass generate_keys (they keep the skeleton's SK), so
        # they would carry a NULL durable key into the hydration update set
        # (hydration_set[durable_key_col] = source.<dk>) and violate the
        # target's NOT NULL constraint. Derive the true Type 7 durable key for
        # the hydration bucket exactly as generate_keys does for insert rows.
        has_hydrate = (
            True
            if lazy_eval
            else rows_no_keys.filter(col("__merge_action") == "HYDRATE")
            .limit(1)
            .count()
            > 0
        )
        if has_hydrate:
            from kimball.processing.key_generator import stamp_type7_columns

            dk_key_type = delta_table.toDF().schema[durable_key_col].dataType
            fp_key_type = (
                delta_table.toDF().schema["__durable_key_fingerprint"].dataType
            )
            for column in (
                durable_key_col,
                "__durable_key_fingerprint",
                "__row_key_fingerprint",
            ):
                if column not in rows_no_keys.columns:
                    rows_no_keys = rows_no_keys.withColumn(
                        column,
                        lit(None).cast(
                            fp_key_type if "fingerprint" in column else dk_key_type
                        ),
                    )
            # effective_at_column is nullable in the signature but the
            # hydration bucket only exists for SCD7 merges where a validity
            # column is present; guard keeps mypy narrowing honest.
            # HYDRATE rows carry the skeleton's placeholder values; replace
            # them wholesale with the same derivation generate_keys uses for
            # insert rows. The skeleton's surrogate key is intentionally kept
            # (matched via the SK merge condition below).
            if effective_at_column and effective_at_column in rows_no_keys.columns:
                rows_no_keys = stamp_type7_columns(
                    rows_no_keys, join_keys, effective_at_column, durable_key_col
                )
    final_source = rows_with_keys.unionByName(rows_no_keys, allowMissingColumns=True)

    # Nullify join keys on insert rows so they don't match on NK
    for k in join_keys:
        final_source = final_source.withColumn(f"__orig_{k}", col(k))
        final_source = final_source.withColumn(
            k,
            when(col("__merge_action") == "EXPIRE", col(k)).otherwise(lit(None)),
        )

    # Build insert values only for columns that exist in the target table.
    target_col_names = {f.name for f in delta_table.toDF().schema.fields}
    insert_values: dict[str, str] = {}
    for c in upserts.columns:
        if c in _CDF_METADATA or c not in target_col_names:
            continue
        insert_values[c] = f"source.__orig_{c}" if c in join_keys else f"source.{c}"
    # Read system columns from the staged source so older versions get
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
    if durable_key_col and durable_key_col in target_col_names:
        insert_values[durable_key_col] = f"source.{durable_key_col}"
    for fingerprint in ("__durable_key_fingerprint", "__row_key_fingerprint"):
        if fingerprint in final_source.columns and fingerprint in target_col_names:
            insert_values[fingerprint] = f"source.{fingerprint}"
    for system_name, value in (
        ("__member_status", "'REAL'"),
        ("__key_origin", "'generated'"),
    ):
        if system_name in target_col_names:
            insert_values[system_name] = value

    # Single MERGE: match on SK for expire, insert everything else
    merge_condition = f"target.{surrogate_key_col} = source.{surrogate_key_col}"
    merge_builder = delta_table.alias("target").merge(
        final_source.alias("source"), merge_condition
    )

    if target_has_skeleton_col and staged_changes.has_hydrate:
        hydration_set = {
            c: (f"source.__orig_{c}" if c in join_keys else f"source.{c}")
            for c in upserts.columns
            if c not in _CDF_METADATA
            and not c.startswith("__scd2_")
            and c in target_col_names
        } | {
            "__is_skeleton": "false",
            "__is_current": "true",
            "__etl_processed_at": "current_timestamp()",
            "__is_deleted": "false",
        }
        # The skeleton row carried placeholder member metadata; a hydrated
        # member is a REAL generated row. Without these the target keeps
        # __key_origin='skeleton'/__member_status='NOT_YET_AVAILABLE', and on
        # SCD7 tables the skeleton's zero durable key (customer_dk = 0)
        # leaks into facts that resolve against the hydrated row.
        if "__member_status" in target_col_names:
            hydration_set["__member_status"] = "'REAL'"
        if "__key_origin" in target_col_names:
            hydration_set["__key_origin"] = "'generated'"
        if durable_key_col and durable_key_col in target_col_names:
            hydration_set[durable_key_col] = f"source.{durable_key_col}"
        if "__durable_key_fingerprint" in target_col_names:
            hydration_set["__durable_key_fingerprint"] = (
                "source.__durable_key_fingerprint"
            )
        if "__row_key_fingerprint" in target_col_names:
            hydration_set["__row_key_fingerprint"] = "source.__row_key_fingerprint"
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
                "__valid_to": "COALESCE(source.__scd2_oldest_valid_from, current_timestamp())",
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
    full_snapshot_reconciliation: bool = False,
    durable_key_col: str | None = None,
    scd_type: int = 2,
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
        full_snapshot_reconciliation=full_snapshot_reconciliation,
        durable_key_col=durable_key_col,
        scd_type=scd_type,
    )
