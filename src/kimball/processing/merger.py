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
    lit,
    row_number,
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
    SQL_DEFAULT_VALID_FROM,
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

_CDF_METADATA = {"_change_type", "_commit_version", "_commit_timestamp", "__merge_action"}


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
    return source_df.withColumn("_rn", row_number().over(window_spec)).filter(col("_rn") == 1).drop("_rn")


def _apply_schema_evolution(table_name: str, enabled: bool, source_df: DataFrame | None = None) -> None:
    if not enabled:
        return
    try:
        get_spark().sql(f"ALTER TABLE {quote_table_name(table_name)} SET TBLPROPERTIES ('delta.schema.autoMerge.enabled' = 'true')")
    except Exception as e:
        logger.warning(f"Could not enable schema auto-merge for {table_name}: {e}")
    if source_df is not None:
        try:
            target_df = DeltaTable.forName(get_spark(), table_name).toDF()
            target_cols = {f.name: f.dataType for f in target_df.schema.fields}
            for field in source_df.schema.fields:
                col_name = field.name
                if col_name.startswith("__") or col_name == "_change_type" or col_name in target_cols:
                    continue
                if col_name in {"hashdiff", "__merge_action"}:
                    continue
                try:
                    get_spark().sql(f"ALTER TABLE {quote_table_name(table_name)} ADD COLUMNS ({col_name} {field.dataType.simpleString()})")
                    logger.info(f"Schema evolution: added column {col_name} to {table_name}")
                except Exception as e:
                    logger.warning(f"Could not add column {col_name} to {table_name}: {e}")
        except Exception as e:
            logger.warning(f"Schema evolution check failed for {table_name}: {e}")


def _build_merge_condition(join_keys: list[str], current_only: bool = False) -> str:
    cond = " AND ".join([f"target.{k} <=> source.{k}" for k in join_keys])
    if current_only:
        cond += " AND target.__is_current = true"
    return cond


def _generate_keys(source_df: DataFrame, strategy: str, join_keys: list[str], col_name: str) -> DataFrame:
    if strategy == "identity":
        return source_df
    if strategy == "hash":
        return HashKeyGenerator(join_keys).generate_keys(source_df, col_name, existing_max_key=0)
    raise ValueError(f"Unknown key strategy: {strategy}. Use 'identity' (recommended) or 'hash'.")


def _filter_cdf_deletes(source_df: DataFrame):
    if "_change_type" not in source_df.columns:
        return source_df, None
    deletes = source_df.filter(col("_change_type") == "delete")
    upserts = source_df.filter(col("_change_type") != "delete")
    return upserts, deletes


def _get_validity_col(effective_at_column: str | None, source_df: DataFrame, target_table_name: str) -> tuple[str, str]:
    if effective_at_column and effective_at_column in source_df.columns:
        return f"source.{effective_at_column}", f"business time ({effective_at_column})"
    import warnings
    warnings.warn(
        f"SCD2 table {target_table_name} using processing time for history. "
        "Configure 'effective_at' in YAML for correct business time semantics.",
        UserWarning, stacklevel=3,
    )
    return "source.__etl_processed_at", "processing time (__etl_processed_at)"


def _build_insert_values(source_df: DataFrame, join_keys: list[str], surrogate_key_col: str, validity_col: str, include_history: bool = True) -> dict[str, str]:
    values: dict[str, str] = {}
    for c in source_df.columns:
        if c in _CDF_METADATA:
            continue
        values[c] = f"source.__orig_{c}" if c in join_keys else f"source.{c}"
    values.update({"__is_current": "true", "__valid_from": f"COALESCE({validity_col}, current_timestamp())", "__valid_to": SQL_DEFAULT_VALID_TO, "__etl_processed_at": "current_timestamp()", "__is_deleted": "false"})
    if include_history:
        values["__is_skeleton"] = "false"
    return values


def merge_scd1(source_df: DataFrame, *, target_table_name: str, join_keys: list[str], delete_strategy: str = "hard", schema_evolution: bool = False, surrogate_key_col: str | None = None, surrogate_key_strategy: str = "identity") -> None:
    if not join_keys:
        raise ValueError("join_keys must be provided for SCD1 MERGE.")
    source_df = _dedup_cdf(source_df, join_keys)
    merge_condition = _build_merge_condition(join_keys)
    delta_table = DeltaTable.forName(get_spark(), target_table_name)
    _apply_schema_evolution(target_table_name, schema_evolution, source_df)
    source_df = _generate_keys(source_df, surrogate_key_strategy, join_keys, surrogate_key_col or "surrogate_key")
    merge_builder = delta_table.alias("target").merge(broadcast(source_df).alias("source"), merge_condition)
    if "_change_type" in source_df.columns:
        if delete_strategy == "hard":
            merge_builder = merge_builder.whenMatchedDelete(condition="source._change_type = 'delete'")
        elif delete_strategy == "soft":
            merge_builder = merge_builder.whenMatchedUpdate(condition="source._change_type = 'delete'", set={"__is_deleted": "true", "__etl_processed_at": "current_timestamp()", "__etl_batch_id": "source.__etl_batch_id"})
    update_cond = "source._change_type != 'delete'" if "_change_type" in source_df.columns else None
    update_map = {c: f"source.{c}" for c in source_df.columns if c not in (surrogate_key_col, "_change_type")}
    update_map.update({"__is_deleted": "false", "__etl_processed_at": "current_timestamp()"})
    insert_map = {c: f"source.{c}" for c in source_df.columns if c != "_change_type"}
    insert_map.update({"__is_deleted": "false", "__etl_processed_at": "current_timestamp()"})
    policy = get_runtime_policy()
    if surrogate_key_strategy == "identity" and (surrogate_key_col or "surrogate_key") in insert_map and not policy.should_include_sk_in_insert():
        insert_map.pop(surrogate_key_col or "surrogate_key", None)
    merge_builder = merge_builder.whenMatchedUpdate(condition=update_cond, set=update_map)
    merge_builder = merge_builder.whenNotMatchedInsert(condition=update_cond, values=insert_map)
    merge_builder.execute()


def merge_scd2(source_df: DataFrame, *, target_table_name: str, join_keys: list[str], track_history_columns: list[str], surrogate_key_col: str, surrogate_key_strategy: str = "identity", schema_evolution: bool = False, effective_at_column: str | None = None) -> None:
    if not track_history_columns:
        raise ValueError("track_history_columns must be provided for SCD Type 2")
    upserts, deletes = _filter_cdf_deletes(source_df)
    if deletes is not None and not deletes.isEmpty():
        delete_count = deletes.count()
        logger.info(f"SCD2: Processing {delete_count} delete(s) - expiring current versions")
        delta_table = DeltaTable.forName(get_spark(), target_table_name)
        vcol = f"source.{effective_at_column}" if effective_at_column and effective_at_column in source_df.columns else "current_timestamp()"
        delta_table.alias("target").merge(deletes.alias("source"), _build_merge_condition(join_keys, current_only=True)).whenMatchedUpdate(set={"__is_current": "false", "__valid_to": vcol, "__etl_processed_at": "current_timestamp()", "__is_deleted": "true"}).execute()
        logger.info("SCD2: Deleted keys expired successfully")
        upserts = source_df.filter(col("_change_type") != "delete")
    else:
        delta_table = DeltaTable.forName(get_spark(), target_table_name)
        current_target = delta_table.toDF().filter("__is_current = true")
        missing_in_source = current_target.join(upserts.select(*join_keys).distinct(), join_keys, "left_anti").dropDuplicates(join_keys)
        if not missing_in_source.isEmpty():
            missing_count = missing_in_source.count()
            logger.info(f"SCD2 full CDC: Detected {missing_count} delete(s) via anti-join - expiring current versions")
            vcol = "current_timestamp()"
            keys_expr = " AND ".join([f"target.{k} <=> source.{k}" for k in join_keys]) + " AND target.__is_current = true"
            delta_table.alias("target").merge(missing_in_source.alias("source"), keys_expr).whenMatchedUpdate(set={"__is_current": "false", "__valid_to": vcol, "__etl_processed_at": "current_timestamp()", "__is_deleted": "true"}).execute()
            logger.info("SCD2: Full CDC deletes expired successfully")
    _apply_schema_evolution(target_table_name, schema_evolution, upserts)
    delta_table = DeltaTable.forName(get_spark(), target_table_name)
    target_has_skeleton_col = "__is_skeleton" in [f.name for f in delta_table.toDF().schema.fields]
    upserts = upserts.withColumn("hashdiff", compute_hashdiff(track_history_columns))
    SYSTEM_COLS = set(_CDF_METADATA) | {"__etl_processed_at", "__etl_batch_id", "__is_current", "__valid_from", "__valid_to", "__is_deleted", "hashdiff"}
    source_cols = set(upserts.columns) - SYSTEM_COLS - set(join_keys or [])
    tracked_cols = set(track_history_columns or [])
    untracked = source_cols - tracked_cols
    if untracked and schema_evolution:
        logger.info(f"WARNING: Schema drift detected. Columns {sorted(untracked)} are NOT tracked for SCD2 history changes.")
    source_keys = broadcast(upserts.select(*join_keys).distinct())
    target_df = delta_table.toDF().filter("__is_current = true").join(source_keys, join_keys, "semi")
    join_conditions = [upserts[k].eqNullSafe(target_df[k]) for k in join_keys]
    combined_join_cond = reduce(lambda a, b: a & b, join_conditions) if join_conditions else None
    joined_df = upserts.alias("s").join(target_df.alias("t"), combined_join_cond, "left").select("s.*", col("t.hashdiff").alias("target_hashdiff"), col("t." + surrogate_key_col).alias("target_sk"), col("t.__is_skeleton").alias("target_is_skeleton") if target_has_skeleton_col else lit(False).alias("target_is_skeleton"))
    rows_new = joined_df.filter(col("target_sk").isNull()).drop("target_hashdiff", "target_sk", "target_is_skeleton").withColumn("__merge_action", lit("INSERT_NEW"))
    rows_changed = joined_df.filter(col("target_sk").isNotNull() & ~col("target_is_skeleton") & ((col("hashdiff") != col("target_hashdiff")) | col("hashdiff").isNull() | col("target_hashdiff").isNull())).drop("target_hashdiff", "target_sk", "target_is_skeleton")
    rows_to_hydrate = None
    if target_has_skeleton_col:
        rows_to_hydrate = joined_df.filter(col("target_sk").isNotNull() & col("target_is_skeleton") & (col("hashdiff") != col("target_hashdiff"))).drop("target_hashdiff", "target_sk", "target_is_skeleton").withColumn("__merge_action", lit("HYDRATE"))
    rows_to_expire = rows_changed.withColumn("__merge_action", lit("UPDATE_EXPIRE"))
    rows_to_insert_version = rows_changed.withColumn("__merge_action", lit("INSERT_VERSION"))
    staged_source = rows_new.union(rows_to_expire).union(rows_to_insert_version)
    rows_needing_keys = staged_source.filter(col("__merge_action").isin("INSERT_NEW", "INSERT_VERSION"))
    rows_no_keys = staged_source.filter(col("__merge_action") == "UPDATE_EXPIRE")
    rows_with_keys = _generate_keys(rows_needing_keys, surrogate_key_strategy, join_keys, surrogate_key_col)
    if surrogate_key_strategy == "identity" and get_runtime_policy().should_include_sk_in_insert() and surrogate_key_col not in rows_with_keys.columns:
        from pyspark.sql.functions import lit as _lit, max as _spark_max, row_number as _row_number
        from pyspark.sql.window import Window as _Window
        current_max = 0
        try:
            existing_df = get_spark().table(target_table_name)
            if surrogate_key_col in existing_df.columns:
                max_val = existing_df.agg(_spark_max(col(surrogate_key_col))).collect()[0][0]
                if max_val is not None:
                    current_max = max_val
        except Exception:
            pass
        window = _Window.orderBy(_lit(1))
        rows_with_keys = rows_with_keys.withColumn("_rn", _row_number().over(window))
        rows_with_keys = rows_with_keys.withColumn(surrogate_key_col, (_lit(int(current_max)) + rows_with_keys["_rn"]).cast("bigint")).drop("_rn")
    if rows_to_hydrate is not None:
        rows_with_keys = rows_with_keys.unionByName(rows_to_hydrate, allowMissingColumns=True)
    if surrogate_key_col in rows_with_keys.columns and surrogate_key_col not in rows_no_keys.columns:
        rows_no_keys = rows_no_keys.withColumn(surrogate_key_col, lit(None))
    final_source = rows_with_keys.unionByName(rows_no_keys, allowMissingColumns=True)
    from pyspark.sql.functions import when as _when
    for k in join_keys:
        final_source = final_source.withColumn(f"__orig_{k}", col(k))
        final_source = final_source.withColumn(k, _when(col("__merge_action") == "INSERT_NEW", lit(None)).when(col("__merge_action") == "INSERT_VERSION", lit(None)).otherwise(col(k)))
    merge_condition = _build_merge_condition(join_keys, current_only=True)
    validity_col, validity_note = _get_validity_col(effective_at_column, upserts, target_table_name)
    logger.info(f"SCD2 time semantics: using {validity_note} for history boundaries")
    insert_values = _build_insert_values(upserts, join_keys, surrogate_key_col, validity_col, include_history=True)
    if surrogate_key_col in final_source.columns:
        insert_values[surrogate_key_col] = f"source.{surrogate_key_col}"
    if surrogate_key_strategy == "identity" and surrogate_key_col in insert_values and not get_runtime_policy().should_include_sk_in_insert():
        del insert_values[surrogate_key_col]
    if target_has_skeleton_col:
        skeleton_hydration_set = {c: (f"source.__orig_{c}" if c in join_keys else f"source.{c}") for c in upserts.columns if c not in _CDF_METADATA}
        skeleton_hydration_set.update({"__is_skeleton": "false", "__valid_from": f"COALESCE({validity_col}, current_timestamp())", "__is_current": "true", "__etl_processed_at": "current_timestamp()", "__is_deleted": "false"})
        skeleton_hydration_set.pop(surrogate_key_col, None)
        delta_table.alias("target").merge(final_source.alias("source"), merge_condition).whenMatchedUpdate(condition="target.__is_skeleton = true AND source.__merge_action = 'HYDRATE'", set=skeleton_hydration_set).whenMatchedUpdate(condition="source.__merge_action = 'UPDATE_EXPIRE'", set={"__is_current": "false", "__valid_to": validity_col, "__etl_processed_at": "current_timestamp()"}).whenNotMatchedInsert(values=insert_values).execute()
    else:
        delta_table.alias("target").merge(final_source.alias("source"), merge_condition).whenMatchedUpdate(condition="source.__merge_action = 'UPDATE_EXPIRE'", set={"__is_current": "false", "__valid_to": validity_col, "__etl_processed_at": "current_timestamp()"}).whenNotMatchedInsert(values=insert_values).execute()


def merge_scd4(source_df: DataFrame, *, target_table_name: str, history_table_name: str, join_keys: list[str], track_history_columns: list[str], surrogate_key_col: str = "surrogate_key", surrogate_key_strategy: str = "identity", schema_evolution: bool = False, effective_at_column: str | None = None) -> None:
    merge_scd1(source_df, target_table_name=target_table_name, join_keys=join_keys, delete_strategy="hard", schema_evolution=schema_evolution, surrogate_key_col=surrogate_key_col, surrogate_key_strategy=surrogate_key_strategy)
    _merge_history(source_df, target_table_name, history_table_name, join_keys, track_history_columns, surrogate_key_col, surrogate_key_strategy, effective_at_column or "__etl_processed_at")


def _merge_history(source_df: DataFrame, target_table_name: str, history_table_name: str, join_keys: list[str], track_history_columns: list[str], surrogate_key_col: str, surrogate_key_strategy: str, effective_at_column: str) -> None:
    spark = source_df.sparkSession
    if track_history_columns == ["*"]:
        exclude = {surrogate_key_col, effective_at_column, "_change_type", "__etl_processed_at", "__etl_batch_id"}
        track_cols = [c for c in source_df.columns if c not in exclude and not c.startswith("__")]
    else:
        track_cols = track_history_columns
    if surrogate_key_strategy == "identity":
        cols_to_select = list(set(track_cols) | set(join_keys)) + [effective_at_column] + (["_change_type"] if "_change_type" in source_df.columns else [])
        source_subset = source_df.select(*cols_to_select)
        current_dim = spark.table(target_table_name).select(*join_keys, surrogate_key_col)
        source_with_sk = source_subset.join(current_dim, on=join_keys, how="inner")
    else:
        source_with_sk = source_df
    stack_expr = ", ".join([f"'{c}', cast({c} as string)" for c in track_cols])
    select_cols = [surrogate_key_col, f"stack({len(track_cols)}, {stack_expr}) as (field, value)", f"{effective_at_column} as effective_at"]
    if "_change_type" in source_with_sk.columns:
        select_cols.append("_change_type")
    else:
        select_cols.append(lit("insert").alias("_change_type"))
    unpivoted = source_with_sk.selectExpr(*select_cols)
    history = spark.table(history_table_name).filter("__is_current = true")
    compared = broadcast(unpivoted).alias("src").join(history.alias("tgt"), (col("src." + surrogate_key_col) == col("tgt.surrogate_key")) & (col("src.field") == col("tgt.field")), "left").filter(col("tgt.value").isNull() | (col("tgt.value") != col("src.value")) | (col("src._change_type") == "delete")).select(col("src.*"), col("tgt.value").alias("old_value"))
    from pyspark.sql.functions import expr
    inserts = compared.filter(col("_change_type") != "delete").select(col(surrogate_key_col).alias("surrogate_key"), col("field"), col("value"), col("effective_at").alias("valid_from"), lit("9999-12-31 23:59:59").cast("timestamp").alias("valid_to"), lit(True).alias("__is_current"), lit("INSERT").alias("__action"))
    expires = compared.filter(col("old_value").isNotNull()).select(col(surrogate_key_col).alias("surrogate_key"), col("field"), col("old_value").alias("value"), lit(None).cast("timestamp").alias("valid_from"), (col("effective_at") - expr("INTERVAL 1 MICROSECOND")).alias("valid_to"), lit(False).alias("__is_current"), lit("EXPIRE").alias("__action"))
    staged = inserts.unionByName(expires)
    DeltaTable.forName(spark, history_table_name).alias("target").merge(staged.alias("source"), "target.surrogate_key = source.surrogate_key AND target.field = source.field AND target.value <=> source.value AND target.__is_current = true AND source.__action = 'EXPIRE'").whenMatchedUpdate(set={"valid_to": "source.valid_to", "__is_current": "source.__is_current"}).whenNotMatchedInsert(condition="source.__action = 'INSERT'", values={"surrogate_key": "source.surrogate_key", "field": "source.field", "value": "source.value", "valid_from": "source.valid_from", "valid_to": "source.valid_to", "__is_current": "source.__is_current"}).execute()


def merge_scd6(source_df: DataFrame, *, target_table_name: str, join_keys: list[str], track_history_columns: list[str], current_value_columns: list[str], surrogate_key_col: str = "surrogate_key", surrogate_key_strategy: str = "identity", schema_evolution: bool = False, effective_at_column: str | None = None) -> None:
    spark = source_df.sparkSession
    current_cols = current_value_columns
    upserts, deletes = _filter_cdf_deletes(source_df)
    upserts = upserts.withColumn("hashdiff", compute_hashdiff(track_history_columns))
    changed_keys = broadcast(upserts.select(*join_keys).distinct())
    all_existing = spark.table(target_table_name).join(changed_keys, join_keys, "inner")
    current_values = broadcast(upserts.select(*join_keys, "hashdiff", *[col(c).alias(f"new_current_{c}") for c in current_cols]))
    from pyspark.sql.functions import when
    staged_updates = all_existing.alias("old").join(current_values, join_keys).select(col(f"old.{surrogate_key_col}"), *[col(f"old.{c}") for c in all_existing.columns if c != surrogate_key_col and not c.startswith("current_")], *[col(f"new_current_{c}").alias(f"current_{c}") for c in current_cols], when(col("old.__is_current") & (col("old.hashdiff") != col("current_values.hashdiff")), lit("EXPIRE")).otherwise(lit("UPDATE")).alias("__action"))
    target_current = spark.table(target_table_name).filter("__is_current = true").select(*join_keys, "hashdiff").alias("tgt")
    source_with_flag = upserts.alias("src").join(target_current, join_keys, "left").select(col("src.*"), when(col("tgt.hashdiff").isNull(), lit("INSERT_NEW")).when(col("tgt.hashdiff") != col("src.hashdiff"), lit("INSERT_VERSION")).otherwise(lit("NO_OP")).alias("__insert_action"))
    rows_to_insert = source_with_flag.filter(col("__insert_action") != "NO_OP").drop("__insert_action").select(*[col(c) for c in upserts.columns], *[col(c).alias(f"current_{c}") for c in current_cols], lit("INSERT").alias("__action"))
    staged_upserts = rows_to_insert.unionByName(staged_updates.select(*rows_to_insert.columns), allowMissingColumns=True)
    effective_col = effective_at_column or "__etl_processed_at"
    if deletes is not None and not deletes.isEmpty():
        delete_targets = spark.table(target_table_name).filter("__is_current = true").join(deletes, join_keys).select(col(surrogate_key_col), col(effective_col).alias("_effective_at"), lit("EXPIRE_DELETE").alias("__action"))
        staged_final = staged_upserts.unionByName(delete_targets, allowMissingColumns=True)
    else:
        staged_final = staged_upserts
    DeltaTable.forName(spark, target_table_name).alias("t").merge(staged_final.alias("s"), f"t.{surrogate_key_col} = s.{surrogate_key_col}").whenMatchedUpdate(condition="s.__action = 'EXPIRE'", set={"__is_current": "false", "__valid_to": f"s.{effective_col}", **{f"current_{c}": f"s.current_{c}" for c in current_cols}}).whenMatchedUpdate(condition="s.__action = 'EXPIRE_DELETE'", set={"__is_current": "false", "__valid_to": f"s.{effective_col}", "__is_deleted": "true"}).whenMatchedUpdate(condition="s.__action = 'UPDATE'", set={f"current_{c}": f"s.current_{c}" for c in current_cols}).whenNotMatchedInsert(condition="s.__action = 'INSERT'", values={c: f"s.{c}" for c in staged_final.columns if c != "__action"}).execute()


def _is_concurrent_exception(e: Exception) -> bool:
    try:
        from pyspark.errors.exceptions.base import ConcurrentModificationException
        if isinstance(e, ConcurrentModificationException):
            return True
    except ImportError:
        pass
    error_str = str(e)
    return any(x in error_str for x in ["ConcurrentAppendException", "WriteConflictException", "ConcurrentDeleteReadException", "ConcurrentModificationException"])


def merge(source_df: DataFrame, *, target_table_name: str, join_keys: list[str], delete_strategy: str = "hard", batch_id: str | None = None, scd_type: int = 1, track_history_columns: list[str] | None = None, surrogate_key_col: str = "surrogate_key", surrogate_key_strategy: str = "identity", schema_evolution: bool = False, effective_at_column: str | None = None, history_table: str | None = None, current_value_columns: list[str] | None = None, max_retries: int = 3) -> None:
    enriched_df = source_df.withColumn("__etl_processed_at", current_timestamp())
    if batch_id:
        enriched_df = enriched_df.withColumn("__etl_batch_id", lit(batch_id))
    merge_fn: Callable[[DataFrame], None]
    if scd_type == 1:
        merge_fn = lambda df: merge_scd1(df, target_table_name=target_table_name, join_keys=join_keys, delete_strategy=delete_strategy, schema_evolution=schema_evolution, surrogate_key_col=surrogate_key_col, surrogate_key_strategy=surrogate_key_strategy)
    elif scd_type == 2:
        merge_fn = lambda df: merge_scd2(df, target_table_name=target_table_name, join_keys=join_keys, track_history_columns=track_history_columns or [], surrogate_key_col=surrogate_key_col, surrogate_key_strategy=surrogate_key_strategy, schema_evolution=schema_evolution, effective_at_column=effective_at_column)
    elif scd_type == 4:
        if not history_table:
            raise ValueError("scd_type=4 requires history_table parameter")
        merge_fn = lambda df: merge_scd4(df, target_table_name=target_table_name, history_table_name=history_table, join_keys=join_keys, track_history_columns=track_history_columns or ["*"], surrogate_key_col=surrogate_key_col, surrogate_key_strategy=surrogate_key_strategy, schema_evolution=schema_evolution, effective_at_column=effective_at_column)
    elif scd_type == 6:
        if not current_value_columns:
            raise ValueError("scd_type=6 requires current_value_columns parameter")
        merge_fn = lambda df: merge_scd6(df, target_table_name=target_table_name, join_keys=join_keys, track_history_columns=track_history_columns or [], current_value_columns=current_value_columns, surrogate_key_col=surrogate_key_col, surrogate_key_strategy=surrogate_key_strategy, schema_evolution=schema_evolution, effective_at_column=effective_at_column)
    else:
        raise ValueError(f"Unsupported SCD type: {scd_type}. Supported: 1, 2, 4, 6")
    for attempt in range(max_retries + 1):
        try:
            merge_fn(enriched_df)
            return
        except PYSPARK_EXCEPTION_BASE as e:
            if _is_concurrent_exception(e) and attempt < max_retries:
                wait_time = 2 ** attempt
                logger.info(f"Concurrent write detected, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries + 1})")
                time.sleep(wait_time)
                continue
            raise


def get_last_merge_metrics(table_name: str, batch_id: str | None = None) -> dict[str, Any]:
    try:
        delta_table = DeltaTable.forName(get_spark(), table_name)
        if batch_id:
            history = delta_table.history(10)
            matching = history.filter(col("userMetadata") == batch_id).first()
            if matching and matching.operationMetrics:
                return dict(matching.operationMetrics)
            logger.info(f"Warning: Could not find commit with batch_id={batch_id}. Using latest commit metrics (may be inaccurate if concurrent pipelines).")
        history = delta_table.history(1).select("operationMetrics").first()
        if history and history.operationMetrics:
            return dict(history.operationMetrics)
    except Exception as e:
        logger.debug(f"Could not fetch merge metrics for {table_name}: {e}")
    return {}


def _seed_default_rows(target_table_name: str, schema: StructType, surrogate_key: str, default_values: dict[str, Any] | None = None, include_history_fields: bool = False) -> None:
    spark = get_spark()
    if not spark.catalog.tableExists(target_table_name):
        logger.info(f"ensure_defaults: table {target_table_name} does not exist. Skipping.")
        return
    try:
        DeltaTable.forName(spark, target_table_name)
    except Exception:
        raise ValueError(f"ensure_defaults: {target_table_name} exists but is not a Delta table.")
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
                    elif "int" in dtype_str or "long" in dtype_str or "short" in dtype_str:
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
        logger.info(f"Seeding {len(rows_to_insert)} default rows into {target_table_name}...")
        df = spark.createDataFrame(rows_to_insert, schema)
        delta_table.alias("target").merge(df.alias("source"), f"target.{surrogate_key} = source.{surrogate_key}").whenNotMatchedInsertAll().execute()


def ensure_scd2_defaults(target_table_name: str, schema: StructType, surrogate_key: str, default_values: dict[str, Any] | None = None, surrogate_key_strategy: str = "identity") -> None:
    _seed_default_rows(target_table_name, schema, surrogate_key, default_values, include_history_fields=True)


def ensure_scd1_defaults(target_table_name: str, schema: StructType, surrogate_key: str, default_values: dict[str, Any] | None = None, surrogate_key_strategy: str = "identity") -> None:
    _seed_default_rows(target_table_name, schema, surrogate_key, default_values, include_history_fields=False)


def optimize_table(table_name: str, cluster_by: list[str] | None = None) -> None:
    logger.info(f"Optimizing table {table_name}...")
    quoted_table_name = quote_table_name(table_name)
    get_spark().sql(f"OPTIMIZE {quoted_table_name}")
    if cluster_by:
        logger.info(f"Optimized {table_name} using Liquid Clustering on {cluster_by}")
    else:
        logger.info(f"Optimized {table_name}")
