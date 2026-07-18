from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.errors import PySparkException as PYSPARK_EXCEPTION_BASE
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, row_number, when
from pyspark.sql.window import Window

from kimball.common.spark_session import get_spark
from kimball.common.utils import quote_table_name
from kimball.processing.key_generator import HashKeyGenerator

logger = logging.getLogger(__name__)

_CDF_METADATA = frozenset(
    {
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
        "__merge_action",
        "__scd2_intermediate",
        "__scd2_seq",
        "__scd2_total",
    }
)


def dedup_cdf(source_df: DataFrame, join_keys: list[str]) -> DataFrame:
    if "_change_type" not in source_df.columns:
        return source_df
    order_col = next(
        (
            c
            for c in ("_commit_version", "_commit_timestamp", "__etl_processed_at")
            if c in source_df.columns
        ),
        None,
    )
    if order_col is None:
        raise ValueError(
            "CDF deduplication requires an ordering column. "
            "Source has _change_type but none of _commit_version, "
            "_commit_timestamp, or __etl_processed_at."
        )
    w = Window.partitionBy(*join_keys).orderBy(
        col(order_col).desc(),
        # Tie-breaker: when two rows share the same order value, prefer
        # the non-delete row (more recent semantics) then fall back to
        # any stable ordering by _change_type.
        when(col("_change_type") == "delete", lit(0)).otherwise(lit(1)).desc(),
    )
    return (
        source_df.withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )


def filter_cdf_deletes(source_df: DataFrame) -> tuple[DataFrame, DataFrame | None]:
    if "_change_type" not in source_df.columns:
        return source_df, None
    return source_df.filter(col("_change_type") != "delete"), source_df.filter(
        col("_change_type") == "delete"
    )


def apply_schema_evolution(
    table_name: str, enabled: bool, source_df: DataFrame | None = None
) -> None:
    if not enabled:
        return
    if source_df is None:
        return
    try:
        target_cols = {
            f.name: f.dataType
            for f in DeltaTable.forName(get_spark(), table_name).toDF().schema.fields
        }
        new_cols = []
        for field in source_df.schema.fields:
            cn = field.name
            if (
                cn.startswith("__")
                or cn == "_change_type"
                or cn in target_cols
                or cn in {"hashdiff", "__merge_action"}
            ):
                continue
            new_cols.append(f"{cn} {field.dataType.simpleString()}")
            target_cols[cn] = field.dataType
        if new_cols:
            cols_sql = ", ".join(new_cols)
            get_spark().sql(
                f"ALTER TABLE {quote_table_name(table_name)} ADD COLUMNS ({cols_sql})"
            )
            logger.info(
                f"Schema evolution: added {len(new_cols)} column(s) to {table_name}"
            )
    except PYSPARK_EXCEPTION_BASE as e:
        logger.warning(f"Schema evolution check failed for {table_name}: {e}")


def build_merge_condition(join_keys: list[str], current_only: bool = False) -> str:
    cond = " AND ".join([f"target.{k} <=> source.{k}" for k in join_keys])
    if current_only:
        cond += " AND target.__is_current = true"
    return cond


def generate_keys(
    source_df: DataFrame,
    join_keys: list[str],
    col_name: str,
    scd_type: int = 1,
    effective_at_column: str | None = None,
    durable_key_col: str | None = None,
) -> DataFrame:
    version_col = effective_at_column if scd_type in (2, 7) else None
    if scd_type in (2, 7) and not version_col:
        if "__etl_processed_at" not in source_df.columns:
            source_df = source_df.withColumn("__etl_processed_at", current_timestamp())
        version_col = "__etl_processed_at"
    generator = HashKeyGenerator(join_keys, version_column=version_col)
    if scd_type == 7:
        if not durable_key_col:
            raise ValueError("SCD Type 7 requires durable_key_col")
        return generator.generate_type7_keys(source_df, col_name, durable_key_col)
    return generator.generate_keys(source_df, col_name)


def get_validity_col(
    effective_at_column: str | None, source_df: DataFrame, target_table_name: str
) -> tuple[str, str]:
    if effective_at_column and effective_at_column in source_df.columns:
        return f"source.{effective_at_column}", f"business time ({effective_at_column})"
    # This should never trigger — config validation enforces effective_at for SCD2.
    # But get_validity_col may be called with None from older code paths.
    if effective_at_column and effective_at_column not in source_df.columns:
        raise ValueError(
            f"effective_at column '{effective_at_column}' not found in source columns: {source_df.columns}"
        )
    # Last-resort fallback: use __etl_processed_at (processing time, not idempotent)
    return "source.__etl_processed_at", "processing time (__etl_processed_at)"


def build_expire_set(validity_col: str) -> dict[str, str]:
    return {
        "__is_current": "false",
        "__valid_to": validity_col,
        "__etl_processed_at": "current_timestamp()",
    }


def get_current_df(table_or_df) -> DataFrame:
    if hasattr(table_or_df, "toDF"):
        result: DataFrame = table_or_df.toDF().filter("__is_current = true")
        return result
    result = table_or_df.filter("__is_current = true")
    return result


def build_insert_values(
    source_df: DataFrame,
    join_keys: list[str],
    surrogate_key_col: str,
    validity_col: str,
    include_history: bool = True,
) -> dict[str, str]:
    from kimball.common.constants import SQL_DEFAULT_VALID_TO

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
