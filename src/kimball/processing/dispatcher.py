from __future__ import annotations

import logging
import time

from pyspark.errors import PySparkException as PYSPARK_EXCEPTION_BASE
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit

from kimball.processing.scd1 import merge_scd1
from kimball.processing.scd2 import merge_scd2
from kimball.processing.scd4 import merge_scd4
from kimball.processing.scd6 import merge_scd6

logger = logging.getLogger(__name__)


def _is_concurrent_exception(e: Exception) -> bool:
    get_condition = getattr(e, "getCondition", None)
    condition = str(get_condition() or "") if callable(get_condition) else ""
    if "CONCURRENT" in condition:
        return True
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
    surrogate_key_col: str | None = None,
    schema_evolution: bool = False,
    effective_at_column: str | None = None,
    history_table: str | None = None,
    current_value_columns: list[str] | None = None,
    max_retries: int = 3,
    append_only: bool = False,
) -> None:
    enriched_df = source_df.withColumn("__etl_processed_at", current_timestamp())
    if batch_id:
        enriched_df = enriched_df.withColumn("__etl_batch_id", lit(batch_id))

    if scd_type == 1:

        def merge_fn(df: DataFrame) -> None:
            merge_scd1(
                df,
                target_table_name=target_table_name,
                join_keys=join_keys,
                delete_strategy=delete_strategy,
                schema_evolution=schema_evolution,
                surrogate_key_col=surrogate_key_col,
                append_only=append_only,
            )
    elif scd_type == 2:
        if surrogate_key_col is None:
            raise ValueError("SCD2 requires surrogate_key_col")

        def merge_fn(df: DataFrame) -> None:
            merge_scd2(
                df,
                target_table_name=target_table_name,
                join_keys=join_keys,
                track_history_columns=track_history_columns or [],
                surrogate_key_col=surrogate_key_col,
                schema_evolution=schema_evolution,
                effective_at_column=effective_at_column,
            )
    elif scd_type == 4:
        if surrogate_key_col is None:
            raise ValueError("SCD4 requires surrogate_key_col")
        if not history_table:
            raise ValueError("scd_type=4 requires history_table parameter")

        def merge_fn(df: DataFrame) -> None:
            merge_scd4(
                df,
                target_table_name=target_table_name,
                history_table_name=history_table,
                join_keys=join_keys,
                track_history_columns=track_history_columns or ["*"],
                surrogate_key_col=surrogate_key_col,
                schema_evolution=schema_evolution,
                effective_at_column=effective_at_column,
            )
    elif scd_type == 6:
        if surrogate_key_col is None:
            raise ValueError("SCD6 requires surrogate_key_col")
        if not current_value_columns:
            raise ValueError("scd_type=6 requires current_value_columns parameter")

        def merge_fn(df: DataFrame) -> None:
            merge_scd6(
                df,
                target_table_name=target_table_name,
                join_keys=join_keys,
                track_history_columns=track_history_columns or [],
                current_value_columns=current_value_columns,
                surrogate_key_col=surrogate_key_col,
                schema_evolution=schema_evolution,
                effective_at_column=effective_at_column,
            )
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
