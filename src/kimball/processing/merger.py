from __future__ import annotations

import logging
import time
from collections.abc import Callable
from functools import reduce
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    broadcast,
    col,
    current_timestamp,
    expr,
    lead,
    lit,
    row_number,
    when,
)
from pyspark.sql.functions import (
    max as _spark_max,
)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

from kimball.common.constants import (
    DEFAULT_START_DATE,
    DEFAULT_VALID_FROM,
    DEFAULT_VALID_TO,
    SQL_DEFAULT_VALID_TO,
)

try:
    from pyspark.errors import PySparkException as PYSPARK_EXCEPTION_BASE
except ImportError:
    try:
        from pyspark.sql.utils import AnalysisException as PYSPARK_EXCEPTION_BASE
    except ImportError:
        PYSPARK_EXCEPTION_BASE = Exception

from kimball.common.runtime_policy import get_runtime_policy
from kimball.common.spark_session import get_spark
from kimball.common.utils import quote_table_name
from kimball.processing.hashing import compute_hashdiff
from kimball.processing.key_generator import HashKeyGenerator

logger = logging.getLogger(__name__)

_CDF_METADATA = {
    "_change_type",
    "_commit_version",
    "_commit_timestamp",
    "__merge_action",
    "__scd2_intermediate",
    "__scd2_seq",
    "__scd2_total",
}


def _dedup_cdf(source_df: DataFrame, join_keys: list[str]) -> DataFrame:
    if "_change_type" not in source_df.columns:
        return source_df
    if "_commit_version" in source_df.columns:
        order_col = "_commit_version"
    elif "_commit_timestamp" in source_df.columns:
        order_col = "_commit_timestamp"
    elif "__etl_processed_at" in source_df.columns:
        order_col = "__etl_processed_at"
    else:
        raise ValueError(
            "CDF deduplication requires an ordering column. "
            "Source has _change_type but none of _commit_version, "
            "_commit_timestamp, or __etl_processed_at. "
            "Add one of these columns to the source DataFrame."
        )
    window_spec = Window.partitionBy(*join_keys).orderBy(col(order_col).desc())
    return (
        source_df.withColumn("_rn", row_number().over(window_spec))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )


def _apply_schema_evolution(
    table_name: str, enabled: bool, source_df: DataFrame | None = None
) -> None:
    if not enabled:
        return
    try:
        get_spark().sql(
            f"ALTER TABLE {quote_table_name(table_name)} SET TBLPROPERTIES ('delta.schema.autoMerge.enabled' = 'true')"
        )
    except Exception as e:
        logger.warning(f"Could not enable schema auto-merge for {table_name}: {e}")
    if source_df is not None:
        try:
            target_df = DeltaTable.forName(get_spark(), table_name).toDF()
            target_cols = {f.name: f.dataType for f in target_df.schema.fields}
            for field in source_df.schema.fields:
                col_name = field.name
                if (
                    col_name.startswith("__")
                    or col_name == "_change_type"
                    or col_name in target_cols
                ):
                    continue
                if col_name in {"hashdiff", "__merge_action"}:
                    continue
                try:
                    get_spark().sql(
                        f"ALTER TABLE {quote_table_name(table_name)} ADD COLUMNS ({col_name} {field.dataType.simpleString()})"
                    )
                    logger.info(
                        f"Schema evolution: added column {col_name} to {table_name}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not add column {col_name} to {table_name}: {e}"
                    )
        except Exception as e:
            logger.warning(f"Schema evolution check failed for {table_name}: {e}")


def _build_merge_condition(join_keys: list[str], current_only: bool = False) -> str:
    cond = " AND ".join([f"target.{k} <=> source.{k}" for k in join_keys])
    if current_only:
        cond += " AND target.__is_current = true"
    return cond


def _generate_keys(
    source_df: DataFrame, strategy: str, join_keys: list[str], col_name: str
) -> DataFrame:
    if strategy == "identity":
        return source_df
    if strategy == "hash":
        return HashKeyGenerator(join_keys).generate_keys(
            source_df, col_name, existing_max_key=0
        )
    raise ValueError(
        f"Unknown key strategy: {strategy}. Use 'identity' (recommended) or 'hash'."
    )


def _filter_cdf_deletes(source_df: DataFrame):
    if "_change_type" not in source_df.columns:
        return source_df, None
    deletes = source_df.filter(col("_change_type") == "delete")
    upserts = source_df.filter(col("_change_type") != "delete")
    return upserts, deletes


def _get_validity_col(
    effective_at_column: str | None, source_df: DataFrame, target_table_name: str
) -> tuple[str, str]:
    if effective_at_column and effective_at_column in source_df.columns:
        return f"source.{effective_at_column}", f"business time ({effective_at_column})"
    import warnings

    warnings.warn(
        f"SCD2 table {target_table_name} using processing time for history. "
        "Configure 'effective_at' in YAML for correct business time semantics.",
        UserWarning,
        stacklevel=3,
    )
    return "source.__etl_processed_at", "processing time (__etl_processed_at)"


def _build_insert_values(
    source_df: DataFrame,
    join_keys: list[str],
    surrogate_key_col: str,
    validity_col: str,
    include_history: bool = True,
) -> dict[str, str]:
    values: dict[str, str] = {}
    for c in source_df.columns:
        if c in _CDF_METADATA:
            continue
        values[c] = f"source.__orig_{c}" if c in join_keys else f"source.{c}"
    values.update(
        {
            "__is_current": "true",
            "__valid_from": f"COALESCE({validity_col}, current_timestamp())",
            "__valid_to": SQL_DEFAULT_VALID_TO,
            "__etl_processed_at": "current_timestamp()",
            "__is_deleted": "false",
        }
    )
    if include_history:
        values["__is_skeleton"] = "false"
    return values


def merge_scd1(
    source_df: DataFrame,
    *,
    target_table_name: str,
    join_keys: list[str],
    delete_strategy: str = "hard",
    schema_evolution: bool = False,
    surrogate_key_col: str | None = None,
    surrogate_key_strategy: str = "identity",
) -> None:
    if not join_keys:
        raise ValueError("join_keys must be provided for SCD1 MERGE.")
    source_df = _dedup_cdf(source_df, join_keys)
    merge_condition = _build_merge_condition(join_keys)
    delta_table = DeltaTable.forName(get_spark(), target_table_name)
    _apply_schema_evolution(target_table_name, schema_evolution, source_df)
    source_df = _generate_keys(
        source_df,
        surrogate_key_strategy,
        join_keys,
        surrogate_key_col or "surrogate_key",
    )
    if (
        surrogate_key_strategy == "identity"
        and get_runtime_policy().should_include_sk_in_insert()
        and surrogate_key_col
        and surrogate_key_col not in source_df.columns
    ):
        source_df = _scd2_generate_identity_keys_for_insert(
            source_df, target_table_name, surrogate_key_col
        )
    merge_builder = delta_table.alias("target").merge(
        broadcast(source_df).alias("source"), merge_condition
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
    update_cond = (
        "source._change_type != 'delete'"
        if "_change_type" in source_df.columns
        else None
    )
    update_map = {
        c: f"source.{c}"
        for c in source_df.columns
        if c not in (surrogate_key_col, "_change_type")
        and c not in _CDF_METADATA
    }
    update_map.update(
        {"__is_deleted": "false", "__etl_processed_at": "current_timestamp()"}
    )
    insert_map = {
        c: f"source.{c}"
        for c in source_df.columns
        if c not in _CDF_METADATA
    }
    insert_map.update(
        {"__is_deleted": "false", "__etl_processed_at": "current_timestamp()"}
    )
    policy = get_runtime_policy()
    if (
        surrogate_key_strategy == "identity"
        and (surrogate_key_col or "surrogate_key") in insert_map
        and not policy.should_include_sk_in_insert()
    ):
        insert_map.pop(surrogate_key_col or "surrogate_key", None)
    merge_builder = merge_builder.whenMatchedUpdate(
        condition=update_cond, set=update_map
    )
    merge_builder = merge_builder.whenNotMatchedInsert(
        condition=update_cond, values=insert_map
    )
    merge_builder.execute()


def _scd2_has_multiple_versions_per_key(
    source_df: DataFrame,
    join_keys: list[str],
) -> bool:
    """Return True when the source DataFrame contains multiple distinct
    versions for the same natural key within one micro-batch.

    This happens in streaming CDF micro-batches (or batch
    preserve_all_changes) when a single key changes more than once between
    trigger intervals.  The single-merge SCD2 algorithm cannot process those
    rows because it would need to update and insert against the same target
    current row twice in one transaction, which Delta does not allow.
    """
    if len(join_keys) == 0:
        return False
    version_cols = ["_commit_version", "_commit_timestamp", "__etl_processed_at"]
    order_col = next((c for c in version_cols if c in source_df.columns), None)
    if order_col is None:
        # No ordering metadata means this is a plain full snapshot batch.  No
        # intra-batch versioning is expected; use the classic single merge.
        return False
    try:
        version_count = (
            source_df.groupBy(*join_keys)
            .agg(expr(f"count(distinct `{order_col}`) as _version_count"))
            .filter("_version_count > 1")
            .limit(1)
            .count()
        )
    except Exception:
        # If the DataFrame is mocked or count()/Window is unavailable (e.g.
        # unit tests without a Spark session), fall back to a deterministic
        # Python-side scan of the mocked rows.
        try:
            rows = source_df.collect()
            key_groups: dict[tuple[Any, ...], set[Any]] = {}
            for row in rows:
                key = tuple(getattr(row, k, None) for k in join_keys)
                key_groups.setdefault(key, set()).add(getattr(row, order_col, None))
            version_count = int(
                any(len(versions) > 1 for versions in key_groups.values())
            )
        except Exception:
            return False
    return bool(version_count > 0)


def _scd2_compute_ranked_versions(
    source_df: DataFrame,
    join_keys: list[str],
    effective_at_column: str | None,
) -> DataFrame:
    """Add __scd2_seq, __scd2_total and __scd2_valid_from/to columns.

    The input is expected to be the *latest* version of each key for the
    current merge plus a column ``__scd2_intermediate`` flag that marks the
    historical rows that must be back-filled.

    Ranking is done by the best available ordering column: effective_at,
    _commit_version, _commit_timestamp or __etl_processed_at.  We then build a
    chain of validity windows so that every historical row has a bounded
    __valid_to and the latest row remains current.
    """
    order_candidates = [
        effective_at_column,
        "_commit_version",
        "_commit_timestamp",
        "__etl_processed_at",
    ]
    order_col = next((c for c in order_candidates if c and c in source_df.columns), None)
    if order_col is None:
        raise ValueError(
            "SCD2 versioned rebuild requires an ordering column. "
            "Provide effective_at or ensure CDF/ETL metadata columns are present."
        )

    window_spec = Window.partitionBy(*join_keys).orderBy(col(order_col).asc())
    ranked = (
        source_df.withColumn("__scd2_seq", row_number().over(window_spec))
        .withColumn(
            "__scd2_total",
            expr("max(__scd2_seq) over (partition by {})".format(
                ", ".join(f"`{c}`" for c in join_keys)
            )),
        )
        .withColumn(
            "__scd2_next_valid_from",
            lead(order_col, 1).over(window_spec),
        )
    )
    # Bound historical rows: valid_to is one microsecond before the next row's
    # valid_from.  The latest row keeps the default open-ended valid_to.
    ranked = ranked.withColumn(
        "__scd2_is_current",
        col("__scd2_seq") == col("__scd2_total"),
    )
    ranked = ranked.withColumn(
        "__scd2_valid_to",
        when(
            col("__scd2_is_current"),
            lit(None).cast("timestamp"),
        ).otherwise(expr(f"`{order_col}` - INTERVAL 1 MICROSECOND")),
    )
    return ranked


def _scd2_select_payload_columns(
    source_df: DataFrame,
    join_keys: list[str],
    track_history_columns: list[str],
    include_meta: bool = False,
) -> DataFrame:
    """Select the columns that constitute an SCD2 row payload.

    Keeps natural keys, tracked columns, effective_at if present, and (when
    include_meta=True) the CDF ordering columns.  Drops internal merge/action
    flags and system columns so that the resulting DataFrame can be inserted
    cleanly into the target table.
    """
    keep = set(join_keys) | set(track_history_columns)
    if "updated_at" in source_df.columns:
        keep.add("updated_at")
    if "effective_at" in source_df.columns:
        keep.add("effective_at")
    if include_meta:
        keep.update(
            c for c in ["_change_type", "_commit_version", "_commit_timestamp"]
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
    cols = [c for c in source_df.columns if c in keep and c not in drop]
    return source_df.select(*cols)


def _scd2_generate_identity_keys_for_insert(
    df: DataFrame,
    target_table_name: str,
    surrogate_key_col: str,
) -> DataFrame:
    """Generate monotonic identity surrogate keys for rows about to be inserted.

    Reads the current max surrogate key from the target table and adds an
    offset row_number.  This matches the existing SCD2 identity-key behavior.
    """
    current_max = 0
    try:
        existing_df = get_spark().table(target_table_name)
        if surrogate_key_col in existing_df.columns:
            max_val = existing_df.agg(_spark_max(col(surrogate_key_col))).collect()[0][0]
            if max_val is not None:
                current_max = max_val
    except Exception:
        pass
    window = Window.orderBy(lit(1))
    return (
        df.withColumn("_rn", row_number().over(window))
        .withColumn(
            surrogate_key_col,
            (lit(int(current_max)) + col("_rn")).cast("bigint"),
        )
        .drop("_rn")
    )


def _merge_scd2_current(
    source_df: DataFrame,
    *,
    target_table_name: str,
    join_keys: list[str],
    track_history_columns: list[str],
    surrogate_key_col: str,
    surrogate_key_strategy: str,
    schema_evolution: bool,
    effective_at_column: str | None,
) -> DataFrame:
    """Phase 1: merge only the *latest* version of each key into the target.

    The source DataFrame may contain multiple versions per key.  We first
    deduplicate it to the latest version (using _commit_version/timestamp or
    __etl_processed_at) and then run the standard SCD2 merge logic on that
    deduplicated slice.  The resulting target table has the correct current
    rows; history rows for older intra-batch versions are produced in phase 2.

    Returns the *full* source DataFrame annotated with an ``__scd2_intermediate``
    flag so that phase 2 can identify which rows are intermediate history.
    """
    order_cols = ["_commit_version", "_commit_timestamp", "__etl_processed_at"]
    order_col = next((c for c in order_cols if c in source_df.columns), None)

    if order_col is None:
        # Plain full snapshot batch: no intra-batch versioning.
        source_df = source_df.withColumn("__scd2_intermediate", lit(False))
        _merge_scd2_classic(
            source_df,
            target_table_name=target_table_name,
            join_keys=join_keys,
            track_history_columns=track_history_columns,
            surrogate_key_col=surrogate_key_col,
            surrogate_key_strategy=surrogate_key_strategy,
            schema_evolution=schema_evolution,
            effective_at_column=effective_at_column,
        )
        return source_df

    # Mark the latest version per key as the "current" merge input.
    latest_per_key = _dedup_cdf(source_df, join_keys)
    latest_per_key = latest_per_key.withColumn("__scd2_intermediate", lit(False))

    _merge_scd2_classic(
        latest_per_key,
        target_table_name=target_table_name,
        join_keys=join_keys,
        track_history_columns=track_history_columns,
        surrogate_key_col=surrogate_key_col,
        surrogate_key_strategy=surrogate_key_strategy,
        schema_evolution=schema_evolution,
        effective_at_column=effective_at_column,
    )

    # Annotate the original source: rows that are NOT the latest for their key
    # are intermediate history rows that phase 2 must insert.
    window_spec = Window.partitionBy(*join_keys).orderBy(col(order_col).desc())
    return (
        source_df.withColumn("_rn", row_number().over(window_spec))
        .withColumn("__scd2_intermediate", col("_rn") > 1)
        .drop("_rn")
    )


def _merge_scd2_classic(
    source_df: DataFrame,
    *,
    target_table_name: str,
    join_keys: list[str],
    track_history_columns: list[str],
    surrogate_key_col: str,
    surrogate_key_strategy: str,
    schema_evolution: bool,
    effective_at_column: str | None,
) -> None:
    """Original single-merge SCD2 implementation.

    This is kept intact as the workhorse for phase 1.  It expects a source
    DataFrame with at most one row per natural key.
    """
    if not track_history_columns:
        raise ValueError("track_history_columns must be provided for SCD Type 2")
    upserts, deletes = _filter_cdf_deletes(source_df)
    if deletes is not None and not deletes.isEmpty():
        delete_count = deletes.count()
        logger.info(
            f"SCD2: Processing {delete_count} delete(s) - expiring current versions"
        )
        delta_table = DeltaTable.forName(get_spark(), target_table_name)
        vcol = (
            f"source.{effective_at_column}"
            if effective_at_column and effective_at_column in source_df.columns
            else "current_timestamp()"
        )
        delta_table.alias("target").merge(
            deletes.alias("source"),
            _build_merge_condition(join_keys, current_only=True),
        ).whenMatchedUpdate(
            set={
                "__is_current": "false",
                "__valid_to": vcol,
                "__etl_processed_at": "current_timestamp()",
                "__is_deleted": "true",
            }
        ).execute()
        logger.info("SCD2: Deleted keys expired successfully")
        upserts = source_df.filter(col("_change_type") != "delete")
    else:
        delta_table = DeltaTable.forName(get_spark(), target_table_name)
        current_target = delta_table.toDF().filter("__is_current = true")
        missing_in_source = current_target.join(
            upserts.select(*join_keys).distinct(), join_keys, "left_anti"
        ).dropDuplicates(join_keys)
        if not missing_in_source.isEmpty():
            missing_count = missing_in_source.count()
            logger.info(
                f"SCD2 full CDC: Detected {missing_count} delete(s) via anti-join - expiring current versions"
            )
            vcol = "current_timestamp()"
            keys_expr = (
                " AND ".join([f"target.{k} <=> source.{k}" for k in join_keys])
                + " AND target.__is_current = true"
            )
            delta_table.alias("target").merge(
                missing_in_source.alias("source"), keys_expr
            ).whenMatchedUpdate(
                set={
                    "__is_current": "false",
                    "__valid_to": vcol,
                    "__etl_processed_at": "current_timestamp()",
                    "__is_deleted": "true",
                }
            ).execute()
            logger.info("SCD2: Full CDC deletes expired successfully")
    _apply_schema_evolution(target_table_name, schema_evolution, upserts)
    delta_table = DeltaTable.forName(get_spark(), target_table_name)
    target_has_skeleton_col = "__is_skeleton" in [
        f.name for f in delta_table.toDF().schema.fields
    ]
    upserts = upserts.withColumn("hashdiff", compute_hashdiff(track_history_columns))
    SYSTEM_COLS = set(_CDF_METADATA) | {
        "__etl_processed_at",
        "__etl_batch_id",
        "__is_current",
        "__valid_from",
        "__valid_to",
        "__is_deleted",
        "hashdiff",
        "__scd2_intermediate",
    }
    source_cols = set(upserts.columns) - SYSTEM_COLS - set(join_keys or [])
    tracked_cols = set(track_history_columns or [])
    untracked = source_cols - tracked_cols
    if untracked and schema_evolution:
        logger.info(
            f"WARNING: Schema drift detected. Columns {sorted(untracked)} are NOT tracked for SCD2 history changes."
        )
    source_keys = broadcast(upserts.select(*join_keys).distinct())
    target_df = (
        delta_table.toDF()
        .filter("__is_current = true")
        .join(source_keys, join_keys, "semi")
    )
    join_conditions = [upserts[k].eqNullSafe(target_df[k]) for k in join_keys]
    combined_join_cond = (
        reduce(lambda a, b: a & b, join_conditions) if join_conditions else None
    )
    joined_df = (
        upserts.alias("s")
        .join(target_df.alias("t"), combined_join_cond, "left")
        .select(
            "s.*",
            col("t.hashdiff").alias("target_hashdiff"),
            col("t." + surrogate_key_col).alias("target_sk"),
            col("t.__is_skeleton").alias("target_is_skeleton")
            if target_has_skeleton_col
            else lit(False).alias("target_is_skeleton"),
        )
    )
    rows_new = (
        joined_df.filter(col("target_sk").isNull())
        .drop("target_hashdiff", "target_sk", "target_is_skeleton")
        .withColumn("__merge_action", lit("INSERT_NEW"))
    )
    rows_changed = joined_df.filter(
        col("target_sk").isNotNull()
        & ~col("target_is_skeleton")
        & (
            (col("hashdiff") != col("target_hashdiff"))
            | col("hashdiff").isNull()
            | col("target_hashdiff").isNull()
        )
    ).drop("target_hashdiff", "target_sk", "target_is_skeleton")
    rows_to_hydrate = None
    if target_has_skeleton_col:
        rows_to_hydrate = (
            joined_df.filter(
                col("target_sk").isNotNull()
                & col("target_is_skeleton")
                & (col("hashdiff") != col("target_hashdiff"))
            )
            .drop("target_hashdiff", "target_sk", "target_is_skeleton")
            .withColumn("__merge_action", lit("HYDRATE"))
        )
    rows_to_expire = rows_changed.withColumn("__merge_action", lit("UPDATE_EXPIRE"))
    rows_to_insert_version = rows_changed.withColumn(
        "__merge_action", lit("INSERT_VERSION")
    )
    staged_source = rows_new.union(rows_to_expire).union(rows_to_insert_version)
    rows_needing_keys = staged_source.filter(
        col("__merge_action").isin("INSERT_NEW", "INSERT_VERSION")
    )
    rows_no_keys = staged_source.filter(col("__merge_action") == "UPDATE_EXPIRE")
    rows_with_keys = _generate_keys(
        rows_needing_keys, surrogate_key_strategy, join_keys, surrogate_key_col
    )
    if (
        surrogate_key_strategy == "identity"
        and get_runtime_policy().should_include_sk_in_insert()
        and surrogate_key_col not in rows_with_keys.columns
    ):
        rows_with_keys = _scd2_generate_identity_keys_for_insert(
            rows_with_keys, target_table_name, surrogate_key_col
        )
    if rows_to_hydrate is not None:
        rows_with_keys = rows_with_keys.unionByName(
            rows_to_hydrate, allowMissingColumns=True
        )
    if (
        surrogate_key_col in rows_with_keys.columns
        and surrogate_key_col not in rows_no_keys.columns
    ):
        rows_no_keys = rows_no_keys.withColumn(surrogate_key_col, lit(None))
    final_source = rows_with_keys.unionByName(rows_no_keys, allowMissingColumns=True)

    for k in join_keys:
        final_source = final_source.withColumn(f"__orig_{k}", col(k))
        final_source = final_source.withColumn(
            k,
            when(col("__merge_action") == "INSERT_NEW", lit(None))
            .when(col("__merge_action") == "INSERT_VERSION", lit(None))
            .otherwise(col(k)),
        )
    merge_condition = _build_merge_condition(join_keys, current_only=True)
    validity_col, validity_note = _get_validity_col(
        effective_at_column, upserts, target_table_name
    )
    logger.info(f"SCD2 time semantics: using {validity_note} for history boundaries")
    insert_values = _build_insert_values(
        upserts, join_keys, surrogate_key_col, validity_col, include_history=True
    )
    if surrogate_key_col in final_source.columns:
        insert_values[surrogate_key_col] = f"source.{surrogate_key_col}"
    if (
        surrogate_key_strategy == "identity"
        and surrogate_key_col in insert_values
        and not get_runtime_policy().should_include_sk_in_insert()
    ):
        del insert_values[surrogate_key_col]
    if target_has_skeleton_col:
        skeleton_hydration_set = {
            c: (f"source.__orig_{c}" if c in join_keys else f"source.{c}")
            for c in upserts.columns
            if c not in _CDF_METADATA and not c.startswith("__scd2_")
        }
        skeleton_hydration_set.update(
            {
                "__is_skeleton": "false",
                "__valid_from": f"COALESCE({validity_col}, current_timestamp())",
                "__is_current": "true",
                "__etl_processed_at": "current_timestamp()",
                "__is_deleted": "false",
            }
        )
        skeleton_hydration_set.pop(surrogate_key_col, None)
        delta_table.alias("target").merge(
            final_source.alias("source"), merge_condition
        ).whenMatchedUpdate(
            condition="target.__is_skeleton = true AND source.__merge_action = 'HYDRATE'",
            set=skeleton_hydration_set,
        ).whenMatchedUpdate(
            condition="source.__merge_action = 'UPDATE_EXPIRE'",
            set={
                "__is_current": "false",
                "__valid_to": validity_col,
                "__etl_processed_at": "current_timestamp()",
            },
        ).whenNotMatchedInsert(values=insert_values).execute()
    else:
        delta_table.alias("target").merge(
            final_source.alias("source"), merge_condition
        ).whenMatchedUpdate(
            condition="source.__merge_action = 'UPDATE_EXPIRE'",
            set={
                "__is_current": "false",
                "__valid_to": validity_col,
                "__etl_processed_at": "current_timestamp()",
            },
        ).whenNotMatchedInsert(values=insert_values).execute()


def _rebuild_scd2_history(
    source_df: DataFrame,
    *,
    target_table_name: str,
    join_keys: list[str],
    track_history_columns: list[str],
    surrogate_key_col: str,
    surrogate_key_strategy: str,
    effective_at_column: str | None,
) -> None:
    """Phase 2: insert historical versions that were skipped in phase 1.

    Phase 1 only merged the *latest* version per key.  All earlier versions in
    the same micro-batch are intermediate history rows flagged with
    ``__scd2_intermediate = true``.  This function turns those rows into
    proper expired SCD2 history rows and appends them to the target table.

    For each intermediate row we need to know the surrogate key of the *next*
    version (which is either another intermediate row or the current row that
    phase 1 inserted/updated).  We use the target table after phase 1 plus the
    intermediate rows to reconstruct the chain and then insert only the
    intermediate rows with corrected validity boundaries.
    """
    intermediate_rows = source_df.filter("__scd2_intermediate = true")
    if intermediate_rows.isEmpty():
        logger.info("SCD2 phase 2: no intermediate versions to back-fill")
        return

    logger.info(
        f"SCD2 phase 2: back-filling {intermediate_rows.count()} intermediate version(s)"
    )

    spark = intermediate_rows.sparkSession
    target_df = spark.table(target_table_name)

    # Build a unified chain of all versions for the affected keys: intermediate
    # source rows + final target rows (current + history for those keys).
    affected_keys = intermediate_rows.select(*join_keys).distinct()
    target_slice = target_df.join(broadcast(affected_keys), join_keys, "inner")

    # Prepare intermediate rows for ranking: keep payload + ordering.
    intermediate_payload = _scd2_select_payload_columns(
        intermediate_rows, join_keys, track_history_columns, include_meta=True
    )
    # Add a validity column compatible with the target schema for ranking.
    if effective_at_column and effective_at_column in intermediate_payload.columns:
        rank_col = effective_at_column
    elif "_commit_version" in intermediate_payload.columns:
        rank_col = "_commit_version"
    elif "_commit_timestamp" in intermediate_payload.columns:
        rank_col = "_commit_timestamp"
    else:
        rank_col = "__etl_processed_at"

    # Union the intermediate rows (as new, open-ended rows) with the target
    # rows so we can compute the chain.  We only need join keys, ordering col,
    # surrogate key and current flag from the target side.
    target_chain_cols = [surrogate_key_col, "__is_current", "__valid_from"] + join_keys
    target_chain = target_slice.select(*target_chain_cols)

    # Intermediate rows need a temp surrogate key placeholder so the union has
    # the same shape.  They will receive real SKs when inserted.
    intermediate_chain = (
        intermediate_payload.withColumn(surrogate_key_col, lit(None).cast("bigint"))
        .withColumn("__is_current", lit(False))
        .withColumn("__valid_from", col(rank_col).cast("timestamp"))
        .select(*target_chain_cols)
    )

    chain = target_chain.unionByName(intermediate_chain)
    window_spec = Window.partitionBy(*join_keys).orderBy(col("__valid_from").asc())
    ranked = (
        chain.withColumn("__scd2_seq", row_number().over(window_spec))
        .withColumn(
            "__scd2_next_sk",
            lead(surrogate_key_col, 1).over(window_spec),
        )
        .withColumn(
            "__scd2_next_valid_from",
            lead("__valid_from", 1).over(window_spec),
        )
    )

    # We only need to insert intermediate rows.  They are the ones with a NULL
    # surrogate key in the ranked chain.
    rows_to_insert = ranked.filter(col(surrogate_key_col).isNull()).drop(
        surrogate_key_col
    )
    if rows_to_insert.isEmpty():
        return

    # Join back to the original intermediate payload to recover all columns.
    # We cannot rely on seq after the union; join on natural keys + ordering
    # column to match each intermediate row to its ranked chain entry.
    join_back_cond = reduce(
        lambda a, b: a & b,
        [rows_to_insert[k].eqNullSafe(intermediate_payload[k]) for k in join_keys],
    ) & (rows_to_insert["__valid_from"] == intermediate_payload[rank_col].cast("timestamp"))

    staged = (
        rows_to_insert.alias("r")
        .join(intermediate_payload.alias("i"), join_back_cond, "inner")
        .select("i.*", "r.__scd2_next_sk", "r.__scd2_next_valid_from")
    )

    # Build final insert DataFrame with SCD2 system columns.
    validity_from_col = (
        col(effective_at_column).cast("timestamp")
        if effective_at_column and effective_at_column in staged.columns
        else col(rank_col).cast("timestamp")
    )
    staged = (
        staged.withColumn("__valid_from", validity_from_col)
        .withColumn(
            "__valid_to",
            when(
                col("__scd2_next_valid_from").isNull(),
                lit(None).cast("timestamp"),
            ).otherwise(col("__scd2_next_valid_from") - expr("INTERVAL 1 MICROSECOND")),
        )
        .withColumn("__is_current", lit(False))
        .withColumn("__is_deleted", lit(False))
        .withColumn("__is_skeleton", lit(False))
        .withColumn("__etl_processed_at", current_timestamp())
        .withColumn("hashdiff", compute_hashdiff(track_history_columns))
    )

    if surrogate_key_strategy == "identity":
        staged = _scd2_generate_identity_keys_for_insert(
            staged, target_table_name, surrogate_key_col
        )
    else:
        staged = _generate_keys(staged, surrogate_key_strategy, join_keys, surrogate_key_col)

    # Keep only target columns so the append does not fail on temp helper cols.
    target_cols = {f.name for f in spark.table(target_table_name).schema.fields}
    payload_cols = [c for c in staged.columns if c in target_cols or c == surrogate_key_col]
    staged = staged.select(*payload_cols)

    staged.write.format("delta").mode("append").saveAsTable(target_table_name)
    logger.info("SCD2 phase 2: intermediate history rows inserted")


def merge_scd2(
    source_df: DataFrame,
    *,
    target_table_name: str,
    join_keys: list[str],
    track_history_columns: list[str],
    surrogate_key_col: str,
    surrogate_key_strategy: str = "identity",
    schema_evolution: bool = False,
    effective_at_column: str | None = None,
) -> None:
    """Merge a source DataFrame into an SCD2 target using a two-phase algorithm.

    Phase 1 merges only the latest version per natural key, which avoids
    Delta merge conflicts when a streaming micro-batch contains multiple
    updates to the same row.  Phase 2 back-fills the skipped historical
    versions as new expired rows.  The result matches batch
    ``preserve_all_changes`` semantics: every committed upstream version is
    preserved in history.
    """
    if not track_history_columns:
        raise ValueError("track_history_columns must be provided for SCD Type 2")

    if not _scd2_has_multiple_versions_per_key(source_df, join_keys):
        # Fast path: classic single-merge (batch full snapshots, SCD1-like
        # single-version CDC, etc.).
        _merge_scd2_classic(
            source_df,
            target_table_name=target_table_name,
            join_keys=join_keys,
            track_history_columns=track_history_columns,
            surrogate_key_col=surrogate_key_col,
            surrogate_key_strategy=surrogate_key_strategy,
            schema_evolution=schema_evolution,
            effective_at_column=effective_at_column,
        )
        return

    annotated_source = _merge_scd2_current(
        source_df,
        target_table_name=target_table_name,
        join_keys=join_keys,
        track_history_columns=track_history_columns,
        surrogate_key_col=surrogate_key_col,
        surrogate_key_strategy=surrogate_key_strategy,
        schema_evolution=schema_evolution,
        effective_at_column=effective_at_column,
    )
    _rebuild_scd2_history(
        annotated_source,
        target_table_name=target_table_name,
        join_keys=join_keys,
        track_history_columns=track_history_columns,
        surrogate_key_col=surrogate_key_col,
        surrogate_key_strategy=surrogate_key_strategy,
        effective_at_column=effective_at_column,
    )


def merge_scd4(
    source_df: DataFrame,
    *,
    target_table_name: str,
    history_table_name: str,
    join_keys: list[str],
    track_history_columns: list[str],
    surrogate_key_col: str = "surrogate_key",
    surrogate_key_strategy: str = "identity",
    schema_evolution: bool = False,
    effective_at_column: str | None = None,
) -> None:
    merge_scd1(
        source_df,
        target_table_name=target_table_name,
        join_keys=join_keys,
        delete_strategy="hard",
        schema_evolution=schema_evolution,
        surrogate_key_col=surrogate_key_col,
        surrogate_key_strategy=surrogate_key_strategy,
    )
    _merge_history(
        source_df,
        target_table_name,
        history_table_name,
        join_keys,
        track_history_columns,
        surrogate_key_col,
        surrogate_key_strategy,
        effective_at_column or "__etl_processed_at",
    )


def _merge_history(
    source_df: DataFrame,
    target_table_name: str,
    history_table_name: str,
    join_keys: list[str],
    track_history_columns: list[str],
    surrogate_key_col: str,
    surrogate_key_strategy: str,
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
    if surrogate_key_strategy == "identity":
        cols_to_select = (
            list(set(track_cols) | set(join_keys))
            + [effective_at_column]
            + (["_change_type"] if "_change_type" in source_df.columns else [])
        )
        source_subset = source_df.select(*cols_to_select)
        current_dim = spark.table(target_table_name).select(
            *join_keys, surrogate_key_col
        )
        source_with_sk = source_subset.join(current_dim, on=join_keys, how="inner")
    else:
        source_with_sk = source_df
    stack_expr = ", ".join([f"'{c}', cast({c} as string)" for c in track_cols])
    select_cols = [
        surrogate_key_col,
        f"stack({len(track_cols)}, {stack_expr}) as (field, value)",
        f"{effective_at_column} as effective_at",
    ]
    if "_change_type" in source_with_sk.columns:
        select_cols.append("_change_type")
    else:
        select_cols.append(lit("insert").alias("_change_type"))
    unpivoted = source_with_sk.selectExpr(*select_cols)
    history = spark.table(history_table_name).filter("__is_current = true")
    compared = (
        broadcast(unpivoted)
        .alias("src")
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
    from pyspark.sql.functions import expr

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
    staged = inserts.unionByName(expires)
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


def merge_scd6(
    source_df: DataFrame,
    *,
    target_table_name: str,
    join_keys: list[str],
    track_history_columns: list[str],
    current_value_columns: list[str],
    surrogate_key_col: str = "surrogate_key",
    surrogate_key_strategy: str = "identity",
    schema_evolution: bool = False,
    effective_at_column: str | None = None,
) -> None:
    spark = source_df.sparkSession
    current_cols = current_value_columns
    upserts, deletes = _filter_cdf_deletes(source_df)
    upserts = upserts.withColumn("hashdiff", compute_hashdiff(track_history_columns))
    changed_keys = broadcast(upserts.select(*join_keys).distinct())
    all_existing = spark.table(target_table_name).join(changed_keys, join_keys, "inner")
    current_values = broadcast(
        upserts.select(
            *join_keys,
            "hashdiff",
            *[col(c).alias(f"new_current_{c}") for c in current_cols],
        )
    )
    from pyspark.sql.functions import when

    staged_updates = (
        all_existing.alias("old")
        .join(current_values, join_keys)
        .select(
            col(f"old.{surrogate_key_col}"),
            *[
                col(f"old.{c}")
                for c in all_existing.columns
                if c != surrogate_key_col and not c.startswith("current_")
            ],
            *[col(f"new_current_{c}").alias(f"current_{c}") for c in current_cols],
            when(
                col("old.__is_current")
                & (col("old.hashdiff") != col("current_values.hashdiff")),
                lit("EXPIRE"),
            )
            .otherwise(lit("UPDATE"))
            .alias("__action"),
        )
    )
    target_current = (
        spark.table(target_table_name)
        .filter("__is_current = true")
        .select(*join_keys, "hashdiff")
        .alias("tgt")
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
            spark.table(target_table_name)
            .filter("__is_current = true")
            .join(deletes, join_keys)
            .select(
                col(surrogate_key_col),
                col(effective_col).alias("_effective_at"),
                lit("EXPIRE_DELETE").alias("__action"),
            )
        )
        staged_final = staged_upserts.unionByName(
            delete_targets, allowMissingColumns=True
        )
    else:
        staged_final = staged_upserts
    DeltaTable.forName(spark, target_table_name).alias("t").merge(
        staged_final.alias("s"), f"t.{surrogate_key_col} = s.{surrogate_key_col}"
    ).whenMatchedUpdate(
        condition="s.__action = 'EXPIRE'",
        set={
            "__is_current": "false",
            "__valid_to": f"s.{effective_col}",
            **{f"current_{c}": f"s.current_{c}" for c in current_cols},
        },
    ).whenMatchedUpdate(
        condition="s.__action = 'EXPIRE_DELETE'",
        set={
            "__is_current": "false",
            "__valid_to": f"s.{effective_col}",
            "__is_deleted": "true",
        },
    ).whenMatchedUpdate(
        condition="s.__action = 'UPDATE'",
        set={f"current_{c}": f"s.current_{c}" for c in current_cols},
    ).whenNotMatchedInsert(
        condition="s.__action = 'INSERT'",
        values={c: f"s.{c}" for c in staged_final.columns if c != "__action"},
    ).execute()


def _is_concurrent_exception(e: Exception) -> bool:
    try:
        from pyspark.errors.exceptions.base import ConcurrentModificationException

        if isinstance(e, ConcurrentModificationException):
            return True
    except ImportError:
        pass
    error_str = str(e)
    return any(
        x in error_str
        for x in [
            "ConcurrentAppendException",
            "WriteConflictException",
            "ConcurrentDeleteReadException",
            "ConcurrentModificationException",
        ]
    )


def merge(
    source_df: DataFrame,
    *,
    target_table_name: str,
    join_keys: list[str],
    delete_strategy: str = "hard",
    batch_id: str | None = None,
    scd_type: int = 1,
    track_history_columns: list[str] | None = None,
    surrogate_key_col: str = "surrogate_key",
    surrogate_key_strategy: str = "identity",
    schema_evolution: bool = False,
    effective_at_column: str | None = None,
    history_table: str | None = None,
    current_value_columns: list[str] | None = None,
    max_retries: int = 3,
) -> None:
    enriched_df = source_df.withColumn("__etl_processed_at", current_timestamp())
    if batch_id:
        enriched_df = enriched_df.withColumn("__etl_batch_id", lit(batch_id))
    merge_fn: Callable[[DataFrame], None]
    if scd_type == 1:

        def _merge_fn(df: DataFrame) -> None:
            merge_scd1(
                df,
                target_table_name=target_table_name,
                join_keys=join_keys,
                delete_strategy=delete_strategy,
                schema_evolution=schema_evolution,
                surrogate_key_col=surrogate_key_col,
                surrogate_key_strategy=surrogate_key_strategy,
            )

        merge_fn = _merge_fn
    elif scd_type == 2:

        def _merge_fn(df: DataFrame) -> None:
            merge_scd2(
                df,
                target_table_name=target_table_name,
                join_keys=join_keys,
                track_history_columns=track_history_columns or [],
                surrogate_key_col=surrogate_key_col,
                surrogate_key_strategy=surrogate_key_strategy,
                schema_evolution=schema_evolution,
                effective_at_column=effective_at_column,
            )

        merge_fn = _merge_fn
    elif scd_type == 4:
        if not history_table:
            raise ValueError("scd_type=4 requires history_table parameter")

        def _merge_fn(df: DataFrame) -> None:
            merge_scd4(
                df,
                target_table_name=target_table_name,
                history_table_name=history_table,
                join_keys=join_keys,
                track_history_columns=track_history_columns or ["*"],
                surrogate_key_col=surrogate_key_col,
                surrogate_key_strategy=surrogate_key_strategy,
                schema_evolution=schema_evolution,
                effective_at_column=effective_at_column,
            )

        merge_fn = _merge_fn
    elif scd_type == 6:
        if not current_value_columns:
            raise ValueError("scd_type=6 requires current_value_columns parameter")

        def _merge_fn(df: DataFrame) -> None:
            merge_scd6(
                df,
                target_table_name=target_table_name,
                join_keys=join_keys,
                track_history_columns=track_history_columns or [],
                current_value_columns=current_value_columns,
                surrogate_key_col=surrogate_key_col,
                surrogate_key_strategy=surrogate_key_strategy,
                schema_evolution=schema_evolution,
                effective_at_column=effective_at_column,
            )

        merge_fn = _merge_fn
    else:
        raise ValueError(f"Unsupported SCD type: {scd_type}. Supported: 1, 2, 4, 6")
    for attempt in range(max_retries + 1):
        try:
            merge_fn(enriched_df)
            return
        except PYSPARK_EXCEPTION_BASE as e:
            if _is_concurrent_exception(e) and attempt < max_retries:
                wait_time = 2**attempt
                logger.info(
                    f"Concurrent write detected, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries + 1})"
                )
                time.sleep(wait_time)
                continue
            raise


def get_last_merge_metrics(
    table_name: str, batch_id: str | None = None
) -> dict[str, Any]:
    try:
        delta_table = DeltaTable.forName(get_spark(), table_name)
        if batch_id:
            history = delta_table.history(10)
            matching = history.filter(col("userMetadata") == batch_id).first()
            if matching and matching.operationMetrics:
                return dict(matching.operationMetrics)
            logger.info(
                f"Warning: Could not find commit with batch_id={batch_id}. Using latest commit metrics (may be inaccurate if concurrent pipelines)."
            )
        history = delta_table.history(1).select("operationMetrics").first()
        if history and history.operationMetrics:
            return dict(history.operationMetrics)
    except Exception as e:
        logger.debug(f"Could not fetch merge metrics for {table_name}: {e}")
    return {}


def _seed_default_rows(
    target_table_name: str,
    schema: StructType,
    surrogate_key: str,
    default_values: dict[str, Any] | None = None,
    include_history_fields: bool = False,
) -> None:
    spark = get_spark()
    if not spark.catalog.tableExists(target_table_name):
        logger.info(
            f"ensure_defaults: table {target_table_name} does not exist. Skipping."
        )
        return
    try:
        DeltaTable.forName(spark, target_table_name)
    except Exception:
        raise ValueError(
            f"ensure_defaults: {target_table_name} exists but is not a Delta table."
        ) from None
    delta_table = DeltaTable.forName(spark, target_table_name)
    standard_defaults = {-1: "Unknown", -2: "Not Applicable", -3: "Error"}
    rows_to_insert = []
    for key, label in standard_defaults.items():
        row: dict[str, Any] = {surrogate_key: key}
        for field in schema.fields:
            col_name = field.name
            if col_name == surrogate_key:
                continue
            if include_history_fields and col_name == "__is_current":
                row[col_name] = True
            elif include_history_fields and col_name == "__valid_from":
                row[col_name] = DEFAULT_VALID_FROM
            elif include_history_fields and col_name == "__valid_to":
                row[col_name] = DEFAULT_VALID_TO
            elif col_name.startswith("__"):
                if not field.nullable:
                    dtype = field.dataType
                    if isinstance(dtype, TimestampType):
                        row[col_name] = DEFAULT_VALID_FROM
                    elif isinstance(dtype, DateType):
                        row[col_name] = DEFAULT_START_DATE
                    elif isinstance(dtype, (IntegerType, LongType, ShortType)):
                        row[col_name] = -1
                    elif isinstance(dtype, DecimalType):
                        from decimal import Decimal

                        row[col_name] = Decimal("-1.0")
                    elif isinstance(dtype, (DoubleType, FloatType)):
                        row[col_name] = -1.0
                    elif isinstance(dtype, BooleanType):
                        row[col_name] = False
                    else:
                        row[col_name] = ""
                else:
                    row[col_name] = None
            else:
                if default_values and col_name in default_values:
                    row[col_name] = default_values[col_name]
                else:
                    dtype_str = field.dataType.simpleString()
                    if "string" in dtype_str:
                        row[col_name] = label
                    elif (
                        "int" in dtype_str
                        or "long" in dtype_str
                        or "short" in dtype_str
                    ):
                        row[col_name] = -1
                    elif "decimal" in dtype_str:
                        from decimal import Decimal

                        row[col_name] = Decimal("-1.0")
                    elif "double" in dtype_str or "float" in dtype_str:
                        row[col_name] = -1.0
                    elif "timestamp" in dtype_str:
                        row[col_name] = DEFAULT_VALID_FROM
                    elif "date" in dtype_str:
                        row[col_name] = DEFAULT_START_DATE
                    else:
                        row[col_name] = None
        rows_to_insert.append(row)
    if rows_to_insert:
        logger.info(
            f"Seeding {len(rows_to_insert)} default rows into {target_table_name}..."
        )
        df = spark.createDataFrame(rows_to_insert, schema)
        delta_table.alias("target").merge(
            df.alias("source"), f"target.{surrogate_key} = source.{surrogate_key}"
        ).whenNotMatchedInsertAll().execute()


def ensure_scd2_defaults(
    target_table_name: str,
    schema: StructType,
    surrogate_key: str,
    default_values: dict[str, Any] | None = None,
    surrogate_key_strategy: str = "identity",
) -> None:
    _seed_default_rows(
        target_table_name,
        schema,
        surrogate_key,
        default_values,
        include_history_fields=True,
    )


def ensure_scd1_defaults(
    target_table_name: str,
    schema: StructType,
    surrogate_key: str,
    default_values: dict[str, Any] | None = None,
    surrogate_key_strategy: str = "identity",
) -> None:
    _seed_default_rows(
        target_table_name,
        schema,
        surrogate_key,
        default_values,
        include_history_fields=False,
    )


def optimize_table(table_name: str, cluster_by: list[str] | None = None) -> None:
    logger.info(f"Optimizing table {table_name}...")
    quoted_table_name = quote_table_name(table_name)
    get_spark().sql(f"OPTIMIZE {quoted_table_name}")
    if cluster_by:
        logger.info(f"Optimized {table_name} using Liquid Clustering on {cluster_by}")
    else:
        logger.info(f"Optimized {table_name}")
