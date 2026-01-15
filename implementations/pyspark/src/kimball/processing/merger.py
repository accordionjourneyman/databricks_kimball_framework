from __future__ import annotations

import os
import time
from collections.abc import Callable
from datetime import date, datetime
from functools import reduce, wraps
from typing import Any, Protocol, cast

from databricks.sdk.runtime import spark
from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
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
from kimball.common.exceptions import PYSPARK_EXCEPTION_BASE
from kimball.common.utils import quote_table_name
from kimball.processing.hashing import compute_hashdiff
from kimball.processing.key_generator import (
    HashKeyGenerator,
    IdentityKeyGenerator,
    KeyGenerator,
    SequenceKeyGenerator,
)


def retry_on_concurrent_exception(
    max_retries: int = 3, backoff_base: int = 2
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to retry merge operations on ConcurrentAppendException with exponential backoff.
    Updated for Databricks Runtime 13+ compatibility.
    """
    # FINDING-005: Try to import specific exception types for more reliable detection
    try:
        from pyspark.errors.exceptions.base import ConcurrentModificationException

        CONCURRENT_EXCEPTIONS: tuple[type[Exception], ...] = (
            ConcurrentModificationException,
        )
    except ImportError:
        CONCURRENT_EXCEPTIONS = ()

    def is_concurrent_exception(e: Exception) -> bool:
        """Check if exception is a concurrent modification error."""
        # First try type-based detection (more reliable)
        if CONCURRENT_EXCEPTIONS and isinstance(e, CONCURRENT_EXCEPTIONS):
            return True
        # Fallback to string matching (fragile but necessary for older runtimes)
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

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except PYSPARK_EXCEPTION_BASE as e:
                    # Only retry on concurrent exceptions, not all PySpark exceptions
                    if is_concurrent_exception(e) and attempt < max_retries:
                        wait_time = backoff_base**attempt
                        print(
                            f"Concurrent write detected, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries + 1})"
                        )
                        time.sleep(wait_time)
                        continue
                    raise
            return func(*args, **kwargs)

        return wrapper

    return decorator


def set_table_auto_merge(table_name: str, enabled: bool) -> None:
    """
    Set the table property 'delta.schema.autoMerge.enabled' on a Delta table.
    """
    val = "true" if enabled else "false"
    quoted_table_name = quote_table_name(table_name)
    sql = f"ALTER TABLE {quoted_table_name} SET TBLPROPERTIES ('delta.schema.autoMerge.enabled' = '{val}')"
    spark.sql(sql)


class MergeStrategy(Protocol):
    """Protocol defining the interface for SCD merge strategies."""

    def merge(self, source_df: DataFrame) -> None: ...


# Strategy Registry for Open/Closed Principle compliance
# New SCD types can be added without modifying existing code
_STRATEGY_REGISTRY: dict[int, type] = {}


def register_strategy(scd_type: int) -> Callable[[type], type]:
    """Decorator to register a merge strategy for an SCD type."""

    def decorator(cls: type) -> type:
        _STRATEGY_REGISTRY[scd_type] = cls
        return cls

    return decorator


def create_merge_strategy(
    scd_type: int,
    target_table_name: str,
    join_keys: list[str],
    delete_strategy: str = "hard",
    track_history_columns: list[str] | None = None,
    surrogate_key_col: str = "surrogate_key",
    surrogate_key_strategy: str = "identity",
    schema_evolution: bool = False,
    effective_at_column: str | None = None,
) -> MergeStrategy:
    """
    Factory function to create the appropriate merge strategy.

    This follows the Open/Closed Principle - new SCD types can be added
    by registering new strategy classes without modifying this function.

    Args:
        scd_type: SCD type (1 or 2)
        target_table_name: Target Delta table name
        join_keys: Natural key columns for matching
        delete_strategy: "hard" or "soft" (SCD1 only)
        track_history_columns: Columns to track for changes (SCD2 only)
        surrogate_key_col: Surrogate key column name
        surrogate_key_strategy: "identity", "hash", or "sequence"
        schema_evolution: Enable schema auto-merge
        effective_at_column: Column containing business effective date for SCD2.
                            If None, uses processing time (__etl_processed_at).

    Returns:
        MergeStrategy implementation for the specified SCD type.

    Raises:
        ValueError: If SCD type is not supported.
    """
    if scd_type == 2:
        return SCD2Strategy(
            target_table_name,
            join_keys,
            track_history_columns or [],
            surrogate_key_col,
            surrogate_key_strategy,
            schema_evolution,
            effective_at_column,
        )
    elif scd_type == 1:
        return SCD1Strategy(
            target_table_name,
            join_keys,
            delete_strategy,
            schema_evolution,
            surrogate_key_col,
            surrogate_key_strategy,
        )
    else:
        # Check registry for custom strategies
        if scd_type in _STRATEGY_REGISTRY:
            strategy_cls = _STRATEGY_REGISTRY[scd_type]
            return strategy_cls(
                target_table_name,
                join_keys,
                track_history_columns or [],
                surrogate_key_col,
                surrogate_key_strategy,
                schema_evolution,
            )
        raise ValueError(
            f"Unsupported SCD type: {scd_type}. Supported: 1, 2, or register custom via @register_strategy"
        )


class SCD1Strategy:
    """
    Implements SCD Type 1 (Overwrite) merge logic.
    Updates existing records in place and inserts new records.
    Supports hard and soft deletes.
    """

    def __init__(
        self,
        target_table_name: str,
        join_keys: list[str],
        delete_strategy: str,
        schema_evolution: bool,
        surrogate_key_col: str | None = None,
        surrogate_key_strategy: str = "identity",
    ):
        if not join_keys:
            raise ValueError(
                "join_keys must be provided for SCD1 MERGE. "
                "Ensure 'natural_keys' (Dimensions) or 'merge_keys' (Facts) are defined in config."
            )
        self.target_table_name = target_table_name
        self.join_keys = join_keys
        self.delete_strategy = delete_strategy
        self.schema_evolution = schema_evolution
        self.surrogate_key_col = surrogate_key_col
        self.surrogate_key_strategy = surrogate_key_strategy

    def merge(self, source_df: DataFrame) -> None:
        # Robustness: Deduplicate source data for SCD1
        # If CDF is used ("_change_type" exists), we might have multiple ops per key (e.g. Delete then Insert).
        # We must keep only the LATEST operation per key to prevent "Multiple source rows matched" error.
        if "_change_type" in source_df.columns:
            # Partition by join keys, order by commit version/timestamp desc
            # Note: _commit_version is best, but if missing (unlikely in CDF), we might need fallback.
            dedup_cols = (
                ["_commit_version"] if "_commit_version" in source_df.columns else []
            )
            if dedup_cols:
                window_spec = Window.partitionBy(*self.join_keys).orderBy(
                    *[col(c).desc() for c in dedup_cols]
                )
                source_df = (
                    source_df.withColumn("_rn", row_number().over(window_spec))
                    .filter(col("_rn") == 1)
                    .drop("_rn")
                )

        # Construct Merge Condition
        merge_condition = " AND ".join(
            [f"target.{k} <=> source.{k}" for k in self.join_keys]
        )

        delta_table = DeltaTable.forName(spark, self.target_table_name)

        if self.schema_evolution:
            try:
                set_table_auto_merge(self.target_table_name, True)
            except Exception:
                pass

        # Generate surrogate keys
        if self.surrogate_key_col and self.surrogate_key_strategy:
            max_key = 0
            key_gen: KeyGenerator | None = None
            if self.surrogate_key_strategy == "identity":
                key_gen = IdentityKeyGenerator()
            elif self.surrogate_key_strategy == "hash":
                key_gen = HashKeyGenerator(self.join_keys)
            elif self.surrogate_key_strategy == "sequence":
                # FINDING-006: Block sequence strategy with clear error
                raise ValueError(
                    "surrogate_key_strategy='sequence' is blocked due to OOM risk with row_number(). "
                    "Use 'identity' (recommended) or 'hash'. See KNOWN_LIMITATIONS.md."
                )

            if key_gen and self.surrogate_key_strategy != "identity":
                source_df = key_gen.generate_keys(
                    source_df, self.surrogate_key_col, existing_max_key=max_key
                )

        merge_builder = delta_table.alias("target").merge(
            source_df.alias("source"), merge_condition
        )

        if "_change_type" in source_df.columns:
            if self.delete_strategy == "hard":
                merge_builder = merge_builder.whenMatchedDelete(
                    condition="source._change_type = 'delete'"
                )
            elif self.delete_strategy == "soft":
                update_set: dict[str, str | Column] = {
                    "__is_deleted": "true",
                    "__etl_processed_at": "current_timestamp()",
                }
                merge_builder = merge_builder.whenMatchedUpdate(
                    condition="source._change_type = 'delete'",
                    set=update_set,
                )

        update_condition_col: str | Column | None = (
            "source._change_type != 'delete'"
            if "_change_type" in source_df.columns
            else None
        )

        source_cols_map = {c: f"source.{c}" for c in source_df.columns}

        update_map: dict[str, str | Column] = {
            c: f"source.{c}" for c in source_df.columns if c != self.surrogate_key_col
        }
        update_map["__is_deleted"] = "false"
        update_map["__etl_processed_at"] = "current_timestamp()"

        insert_map: dict[str, str | Column] = {
            **source_cols_map,
            "__is_deleted": "false",
            "__etl_processed_at": "current_timestamp()",
        }

        if (
            self.surrogate_key_strategy == "identity"
            and self.surrogate_key_col in insert_map
        ):
            del insert_map[self.surrogate_key_col]

        merge_builder = merge_builder.whenMatchedUpdate(
            condition=cast(Column, update_condition_col), set=update_map
        )

        merge_builder = merge_builder.whenNotMatchedInsert(
            condition=cast(Column, update_condition_col), values=insert_map
        )

        merge_builder.execute()


class SCD2Strategy:
    """
    Implements SCD Type 2 (History Tracking) merge logic.
    Tracks changes to `track_history_columns` by closing the current record
    and inserting a new active record.
    Preserves history with `__valid_from`, `__valid_to`, and `__is_current`.

    Time Semantics:
        - If effective_at_column is provided, uses business time for history.
        - Otherwise, uses processing time (__etl_processed_at) as fallback.
    """

    def __init__(
        self,
        target_table_name: str,
        join_keys: list[str],
        track_history_columns: list[str],
        surrogate_key_col: str,
        surrogate_key_strategy: str,
        schema_evolution: bool = False,
        effective_at_column: str | None = None,
    ):
        if not join_keys:
            raise ValueError(
                "join_keys must be provided for SCD2 MERGE. "
                "Ensure 'natural_keys' are defined in config."
            )
        self.target_table_name = target_table_name
        self.join_keys = join_keys
        self.track_history_columns = track_history_columns
        self.surrogate_key_col = surrogate_key_col
        self.surrogate_key_strategy = surrogate_key_strategy
        self.schema_evolution = schema_evolution
        self.effective_at_column = effective_at_column

    def merge(self, source_df: DataFrame) -> None:
        """
        Executes SCD Type 2 Merge.

        Algorithm:
        1. Handles deletes (expires current version for deleted keys).
        2. Deduplicates source data (intra-batch) using window functions.
        3. Detects changes by comparing HashDiffs between Source and Target.
        4. Classifies rows into:
           - INSERT_NEW: New business keys.
           - UPDATE_EXPIRE: Existing keys with changed data (to expire old version).
           - INSERT_VERSION: New version of changed keys (to become current).
        5. Performs a single MERGE operation using a synthesized 'staged_source'.
        """
        if not self.track_history_columns:
            raise ValueError("track_history_columns must be provided for SCD Type 2")

        # FINDING-003: Handle deletes first - expire current version for deleted keys
        if "_change_type" in source_df.columns:
            delete_rows = source_df.filter(col("_change_type") == "delete")
            # Debug: show delete detection
            if not delete_rows.isEmpty():
                from pyspark.sql import functions as F

                print(
                    f"SCD2: Processing {delete_rows.count()} delete(s) - expiring current versions"
                )
                delta_table = DeltaTable.forName(spark, self.target_table_name)

                # Determine validity column for __valid_to
                if (
                    self.effective_at_column
                    and self.effective_at_column in source_df.columns
                ):
                    validity_col = f"source.{self.effective_at_column}"
                else:
                    validity_col = "current_timestamp()"

                # Build merge condition for deletes
                delete_merge_cond = (
                    " AND ".join([f"target.{k} <=> source.{k}" for k in self.join_keys])
                    + " AND target.__is_current = true"
                )

                # Expire current versions for deleted keys
                delta_table.alias("target").merge(
                    delete_rows.alias("source"), delete_merge_cond
                ).whenMatchedUpdate(
                    set={
                        "__is_current": "false",
                        "__valid_to": validity_col,
                        "__etl_processed_at": "current_timestamp()",
                        "__is_deleted": "true",
                    }
                ).execute()
                print(f"SCD2: Deleted keys expired successfully")

            # Filter out deletes from main processing
            source_df = source_df.filter(col("_change_type") != "delete")

        if self.schema_evolution:
            try:
                set_table_auto_merge(self.target_table_name, True)
            except Exception:
                pass

        delta_table = DeltaTable.forName(spark, self.target_table_name)

        source_df = source_df.withColumn(
            "hashdiff", compute_hashdiff(self.track_history_columns)
        )

        # Schema drift warning: Alert if source has columns not tracked for history
        # This prevents silent loss of change detection when new columns are added
        SYSTEM_COLS = {
            "__etl_processed_at",
            "__etl_batch_id",
            "__is_current",
            "__valid_from",
            "__valid_to",
            "__is_deleted",
            "_change_type",
            "_commit_version",
            "_commit_timestamp",
            "hashdiff",
        }
        source_cols = set(source_df.columns) - SYSTEM_COLS - set(self.join_keys or [])
        tracked_cols = set(self.track_history_columns or [])
        untracked = source_cols - tracked_cols
        if untracked and self.schema_evolution:
            print(
                f"WARNING: Schema drift detected. Columns {sorted(untracked)} are NOT tracked "
                f"for SCD2 history changes. Changes to these columns will NOT trigger new versions. "
                f"Add to track_history_columns in config if history tracking is needed."
            )

        if self.join_keys and "__etl_processed_at" in source_df.columns:
            # FINDING-007: Always validate grain - Kimball methodology requires unique natural keys per batch
            # This prevents "Multiple source rows matched" merge errors and ensures data integrity
            duplicates_check = (
                source_df.groupBy(*self.join_keys).count().filter(col("count") > 1)
            )
            if not duplicates_check.limit(1).isEmpty():
                # Get sample duplicates for error message
                dup_sample = duplicates_check.limit(5).collect()
                dup_keys = [str(row.asDict()) for row in dup_sample]
                raise ValueError(
                    f"GRAIN VIOLATION: Source data contains duplicate natural keys within batch. "
                    f"Kimball methodology requires unique keys per batch. "
                    f"Sample duplicates: {dup_keys[:3]}... "
                    f"Fix the source data or adjust the grain definition in config."
                )

        source_keys = source_df.select(*self.join_keys).distinct()
        target_df = (
            delta_table.toDF()
            .filter("__is_current = true")
            .join(source_keys, self.join_keys, "semi")
        )

        join_conditions = [
            source_df[k].eqNullSafe(target_df[k]) for k in self.join_keys
        ]
        if join_conditions:
            combined_join_cond = reduce(lambda a, b: a & b, join_conditions)
        else:
            combined_join_cond = None

        joined_df = (
            source_df.alias("s")
            .join(target_df.alias("t"), combined_join_cond, "left")
            .select(
                "s.*",
                col("t.hashdiff").alias("target_hashdiff"),
                col("t." + self.surrogate_key_col).alias("target_sk"),
            )
        )

        rows_new = (
            joined_df.filter(col("target_sk").isNull())
            .drop("target_hashdiff", "target_sk")
            .withColumn("__merge_action", lit("INSERT_NEW"))
        )

        rows_changed = joined_df.filter(
            col("target_sk").isNotNull() & (col("hashdiff") != col("target_hashdiff"))
        ).drop("target_hashdiff", "target_sk")

        rows_to_expire = rows_changed.withColumn("__merge_action", lit("UPDATE_EXPIRE"))
        rows_to_insert_version = rows_changed.withColumn(
            "__merge_action", lit("INSERT_VERSION")
        )

        staged_source = rows_new.union(rows_to_expire).union(rows_to_insert_version)

        rows_needing_keys = staged_source.filter(
            col("__merge_action").isin("INSERT_NEW", "INSERT_VERSION")
        )
        rows_no_keys = staged_source.filter(col("__merge_action") == "UPDATE_EXPIRE")

        max_key = 0
        key_gen: KeyGenerator
        if self.surrogate_key_strategy == "identity":
            key_gen = IdentityKeyGenerator()
        elif self.surrogate_key_strategy == "hash":
            key_gen = HashKeyGenerator(self.join_keys)
        elif self.surrogate_key_strategy == "sequence":
            # FINDING-006: Block sequence strategy with clear error
            raise ValueError(
                "surrogate_key_strategy='sequence' is blocked due to OOM risk at scale. "
                "Use 'identity' (recommended) or 'hash'. See KNOWN_LIMITATIONS.md."
            )
        else:
            raise ValueError(f"Unknown key strategy: {self.surrogate_key_strategy}")

        rows_with_keys = key_gen.generate_keys(
            rows_needing_keys,
            self.surrogate_key_col,
            existing_max_key=max_key
            if self.surrogate_key_strategy == "sequence"
            else 0,
        )

        if (
            self.surrogate_key_col in rows_with_keys.columns
            and self.surrogate_key_col not in rows_no_keys.columns
        ):
            rows_no_keys = rows_no_keys.withColumn(self.surrogate_key_col, lit(None))

        final_source = rows_with_keys.unionByName(
            rows_no_keys, allowMissingColumns=True
        )

        from pyspark.sql.functions import when as _when

        for k in self.join_keys:
            final_source = final_source.withColumn(f"__orig_{k}", col(k))
            final_source = final_source.withColumn(
                k,
                _when(col("__merge_action") == "UPDATE_EXPIRE", col(k)).otherwise(
                    lit(None)
                ),
            )

        merge_condition = (
            " AND ".join([f"target.{k} <=> source.{k}" for k in self.join_keys])
            + " AND target.__is_current = true"
        )

        insert_values = {}
        for c in source_df.columns:
            if c == "__merge_action":
                continue
            if c in self.join_keys:
                insert_values[c] = f"source.__orig_{c}"
            else:
                insert_values[c] = f"source.{c}"

        # Determine validity column: use business time if available, else processing time
        # FINDING-004: Warn when SCD2 uses processing time for history
        if self.effective_at_column and self.effective_at_column in source_df.columns:
            validity_col = f"source.{self.effective_at_column}"
            validity_note = f"business time ({self.effective_at_column})"
        else:
            validity_col = "source.__etl_processed_at"
            validity_note = "processing time (__etl_processed_at)"
            import warnings

            warnings.warn(
                f"SCD2 table {self.target_table_name} using processing time for history. "
                "Configure 'effective_at' in YAML for correct business time semantics. "
                "Late-arriving dimensions will have incorrect __valid_from dates.",
                UserWarning,
                stacklevel=2,
            )

        print(f"SCD2 time semantics: using {validity_note} for history boundaries")

        insert_values.update(
            {
                "__is_current": "true",
                # Use validity_col for all inserts; fallback to default only if NULL
                "__valid_from": f"COALESCE({validity_col}, {SQL_DEFAULT_VALID_FROM})",
                "__valid_to": SQL_DEFAULT_VALID_TO,
                "__etl_processed_at": "current_timestamp()",
                "__is_deleted": "false",
                "__is_skeleton": "false",  # New rows are not skeletons
            }
        )

        if self.surrogate_key_col in final_source.columns:
            insert_values[self.surrogate_key_col] = f"source.{self.surrogate_key_col}"

        if (
            self.surrogate_key_strategy == "identity"
            and self.surrogate_key_col in insert_values
        ):
            del insert_values[self.surrogate_key_col]

        # Check if target table has __is_skeleton column (for skeleton hydration support)
        target_has_skeleton_col = "__is_skeleton" in [
            f.name for f in delta_table.toDF().schema.fields
        ]

        if target_has_skeleton_col:
            # Build skeleton hydration update set - update all source columns in place
            # This keeps the SK but replaces placeholder NULLs with real data
            skeleton_hydration_set = {}
            for c in source_df.columns:
                if c == "__merge_action":
                    continue
                if c in self.join_keys:
                    skeleton_hydration_set[c] = f"source.__orig_{c}"
                else:
                    skeleton_hydration_set[c] = f"source.{c}"

            # Add system columns for hydration
            skeleton_hydration_set.update(
                {
                    "__is_skeleton": "false",  # No longer a skeleton
                    "__valid_from": f"COALESCE({validity_col}, {SQL_DEFAULT_VALID_FROM})",
                    "__is_current": "true",
                    "__etl_processed_at": "current_timestamp()",
                    "__is_deleted": "false",
                }
            )
            # Remove SK from hydration (keep original skeleton SK)
            if self.surrogate_key_col in skeleton_hydration_set:
                del skeleton_hydration_set[self.surrogate_key_col]

            # Merge with skeleton hydration support
            delta_table.alias("target").merge(
                final_source.alias("source"), merge_condition
            ).whenMatchedUpdate(
                # SKELETON HYDRATION: If target is skeleton and source has real data,
                # update in-place (keep SK, update attributes). This prevents the
                # "same customer, two SKs" bug from skeleton getting SCD2 versioned.
                condition="target.__is_skeleton = true AND source.__merge_action = 'INSERT_NEW'",
                set=cast(dict[str, str | Column], skeleton_hydration_set),
            ).whenMatchedUpdate(
                # Normal SCD2 expiration for non-skeleton rows
                condition="source.__merge_action = 'UPDATE_EXPIRE'",
                set={
                    "__is_current": "false",
                    "__valid_to": validity_col,
                    "__etl_processed_at": "current_timestamp()",
                },
            ).whenNotMatchedInsert(
                values=cast(dict[str, str | Column], insert_values)
            ).execute()
        else:
            # Legacy table without __is_skeleton column - use standard merge
            delta_table.alias("target").merge(
                final_source.alias("source"), merge_condition
            ).whenMatchedUpdate(
                condition="source.__merge_action = 'UPDATE_EXPIRE'",
                set={
                    "__is_current": "false",
                    "__valid_to": validity_col,
                    "__etl_processed_at": "current_timestamp()",
                },
            ).whenNotMatchedInsert(
                values=cast(dict[str, str | Column], insert_values)
            ).execute()


class DeltaMerger:
    """
    Handles the MERGE operation into the target Delta table.
    Includes Audit Column injection and Delete handling.

    C-01: Supports SparkSession injection for testability.
    """

    def __init__(self, spark_session: Any | None = None) -> None:
        """
        Initialize DeltaMerger with optional SparkSession injection.

        Args:
            spark_session: Optional SparkSession. If None, uses global Databricks spark.
        """
        self._spark = spark_session

    @property
    def spark(self) -> Any:
        """Lazy SparkSession accessor with fallback to global."""
        if self._spark is None:
            from databricks.sdk.runtime import spark

            self._spark = spark
        return self._spark

    @retry_on_concurrent_exception()
    def merge(
        self,
        target_table_name: str,
        source_df: DataFrame,
        join_keys: list[str],
        delete_strategy: str = "hard",
        batch_id: str | None = None,
        scd_type: int = 1,
        track_history_columns: list[str] | None = None,
        surrogate_key_col: str = "surrogate_key",
        surrogate_key_strategy: str = "identity",
        schema_evolution: bool = False,
        effective_at_column: str | None = None,
    ) -> None:
        """
        Executes the MERGE operation.

        Args:
            target_table_name: Target Delta table name.
            source_df: Source DataFrame with changes.
            join_keys: Natural key columns for matching.
            delete_strategy: "hard" or "soft" delete handling.
            batch_id: Optional batch ID for audit.
            scd_type: SCD type (1 or 2).
            track_history_columns: Columns to track for SCD2 changes.
            surrogate_key_col: Surrogate key column name.
            surrogate_key_strategy: "identity", "hash", or "sequence".
            schema_evolution: Enable schema auto-merge.
            effective_at_column: Column containing business effective date for SCD2.
                                If None, uses processing time (__etl_processed_at).
        """
        # 1. Inject Audit Columns into Source DataFrame
        enriched_df = source_df.withColumn("__etl_processed_at", current_timestamp())
        if batch_id:
            enriched_df = enriched_df.withColumn("__etl_batch_id", lit(batch_id))

        # Use factory function for OCP compliance
        strategy = create_merge_strategy(
            scd_type=scd_type,
            target_table_name=target_table_name,
            join_keys=join_keys,
            delete_strategy=delete_strategy,
            track_history_columns=track_history_columns,
            surrogate_key_col=surrogate_key_col,
            surrogate_key_strategy=surrogate_key_strategy,
            schema_evolution=schema_evolution,
            effective_at_column=effective_at_column,
        )

        strategy.merge(enriched_df)

    def _is_delta_table(self, table_name: str) -> bool:
        """
        Check if a managed/external table is a Delta table by inspecting its provider.
        DeltaTable.isDeltaTable() requires a path, not a table name.
        """
        try:
            DeltaTable.forName(self.spark, table_name)
            return True
        except Exception:
            return False

    def get_last_merge_metrics(
        self, table_name: str, batch_id: str | None = None
    ) -> dict[str, Any]:
        """
        Get metrics from a MERGE operation on a table.

        C-02: Fix race condition by optionally querying by batch_id.
        Without batch_id, returns history(1) which may be from a concurrent pipeline.
        With batch_id, searches recent history to find the exact commit.

        Args:
            table_name: Target table name.
            batch_id: Optional batch ID to find exact commit metrics.

        Returns:
            Dict with numSourceRows, numTargetRowsInserted, numTargetRowsUpdated, etc.
        """
        try:
            delta_table = DeltaTable.forName(self.spark, table_name)

            if batch_id:
                # Query by batch_id in userMetadata for exact commit
                from pyspark.sql.functions import col

                history = delta_table.history(10)  # Check last 10 commits
                matching = history.filter(col("userMetadata") == batch_id).first()
                if matching and matching.operationMetrics:
                    return dict(matching.operationMetrics)
                # Fallback to latest if batch_id not found (Serverless may not tag)
                print(
                    f"Warning: Could not find commit with batch_id={batch_id}. "
                    "Using latest commit metrics (may be inaccurate if concurrent pipelines)."
                )

            # Default: return latest commit metrics
            history = delta_table.history(1).select("operationMetrics").first()
            if history and history.operationMetrics:
                return dict(history.operationMetrics)
        except Exception:
            pass
        return {}

    def ensure_scd2_defaults(
        self,
        target_table_name: str,
        schema: StructType,
        surrogate_key: str,
        default_values: dict[str, Any] | None = None,
        surrogate_key_strategy: str = "identity",
    ) -> None:
        """
        Ensures that the standard SCD2 default rows (-1, -2, -3) exist in the table.
        """
        # Table must exist (Orchestrator creates it as Delta before calling this)
        if not self.spark.catalog.tableExists(target_table_name):
            print(
                f"ensure_scd2_defaults: table {target_table_name} does not exist. Skipping."
            )
            return

        # Verify it's a Delta table (use our helper for managed tables)
        if not self._is_delta_table(target_table_name):
            raise ValueError(
                f"ensure_scd2_defaults: {target_table_name} exists but is not a Delta table. "
                "The Orchestrator should create it as Delta before calling this method."
            )

        delta_table = DeltaTable.forName(self.spark, target_table_name)

        # Define the standard defaults
        # -1: Unknown
        # -2: Not Applicable
        # -3: Error

        standard_defaults = {-1: "Unknown", -2: "Not Applicable", -3: "Error"}

        rows_to_insert = []

        for key, label in standard_defaults.items():
            # Construct row with heterogeneous types
            row: dict[str, Any] = {surrogate_key: key}

            # Fill other columns
            for field in schema.fields:
                col_name = field.name
                if col_name == surrogate_key:
                    continue

                # System columns
                if col_name == "__is_current":
                    row[col_name] = True
                elif col_name == "__valid_from":
                    row[col_name] = DEFAULT_VALID_FROM  # Standard start
                elif col_name == "__valid_to":
                    row[col_name] = DEFAULT_VALID_TO  # Standard end for default rows
                elif col_name.startswith("__"):
                    # Other system cols: provide a fallback if the target field is non-nullable
                    if not field.nullable:
                        dtype = field.dataType
                        # Timestamp/Date -> use safe historical date/time
                        if isinstance(dtype, TimestampType):
                            row[col_name] = DEFAULT_VALID_FROM
                        elif isinstance(dtype, DateType):
                            row[col_name] = DEFAULT_START_DATE
                        elif isinstance(
                            dtype,
                            (
                                IntegerType,
                                LongType,
                                ShortType,
                                DoubleType,
                                FloatType,
                                DecimalType,
                            ),
                        ):
                            row[col_name] = -1
                        elif isinstance(dtype, BooleanType):
                            row[col_name] = False
                        else:
                            # Default to empty string for string-like types
                            row[col_name] = ""
                    else:
                        row[col_name] = None  # Other system cols (nullable)
                else:
                    # User columns
                    # Check if user provided a specific default in config
                    if default_values and col_name in default_values:
                        row[col_name] = default_values[col_name]
                    else:
                        # Infer based on type
                        dtype_str = field.dataType.simpleString()
                        if "string" in dtype_str:
                            row[col_name] = label
                        elif any(x in dtype_str for x in ["int", "long", "double"]):
                            row[col_name] = -1
                        elif "timestamp" in dtype_str:
                            row[col_name] = DEFAULT_VALID_FROM
                        elif "date" in dtype_str:
                            row[col_name] = DEFAULT_START_DATE
                        else:
                            row[col_name] = None

            rows_to_insert.append(row)

        if rows_to_insert:
            print(
                f"Seeding {len(rows_to_insert)} default rows into {target_table_name}..."
            )
            df = self.spark.createDataFrame(rows_to_insert, schema)

            # Use atomic MERGE operation to prevent duplicates in concurrent environments
            delta_table.alias("target").merge(
                df.alias("source"), f"target.{surrogate_key} = source.{surrogate_key}"
            ).whenNotMatchedInsertAll().execute()

    def ensure_scd1_defaults(
        self,
        target_table_name: str,
        schema: StructType,
        surrogate_key: str,
        default_values: dict[str, Any] | None = None,
        surrogate_key_strategy: str = "identity",
    ) -> None:
        """
        Ensures that the standard SCD1 default rows (-1, -2, -3) exist in the table.
        Similar to SCD2 but without the SCD2-specific system columns.
        """
        # Table must exist (Orchestrator creates it as Delta before calling this)
        if not self.spark.catalog.tableExists(target_table_name):
            print(
                f"ensure_scd1_defaults: table {target_table_name} does not exist. Skipping."
            )
            return

        # Verify it's a Delta table (use our helper for managed tables)
        if not self._is_delta_table(target_table_name):
            raise ValueError(
                f"ensure_scd1_defaults: {target_table_name} exists but is not a Delta table."
            )

        delta_table = DeltaTable.forName(self.spark, target_table_name)

        rows_to_insert = []

        standard_defaults = {-1: "Unknown", -2: "Not Applicable", -3: "Error"}

        for key, label in standard_defaults.items():
            row: dict[str, Any] = {surrogate_key: key}

            for field in schema.fields:
                col_name = field.name
                if col_name == surrogate_key:
                    continue

                # System columns (SCD1 has fewer than SCD2)
                if col_name.startswith("__"):
                    if not field.nullable:
                        dtype = field.dataType
                        if isinstance(dtype, TimestampType):
                            row[col_name] = datetime(1900, 1, 1, 0, 0, 0)
                        elif isinstance(dtype, DateType):
                            row[col_name] = date(1900, 1, 1)
                        elif isinstance(
                            dtype,
                            (
                                IntegerType,
                                LongType,
                                ShortType,
                                DoubleType,
                                FloatType,
                                DecimalType,
                            ),
                        ):
                            row[col_name] = -1
                        elif isinstance(dtype, BooleanType):
                            row[col_name] = False
                        else:
                            row[col_name] = ""
                    else:
                        row[col_name] = None
                else:
                    # User columns
                    if default_values and col_name in default_values:
                        row[col_name] = default_values[col_name]
                    else:
                        dtype_str = field.dataType.simpleString()
                        if "string" in dtype_str:
                            row[col_name] = label
                        elif any(x in dtype_str for x in ["int", "long", "double"]):
                            row[col_name] = -1
                        elif "timestamp" in dtype_str:
                            row[col_name] = DEFAULT_VALID_FROM
                        elif "date" in dtype_str:
                            row[col_name] = DEFAULT_START_DATE
                        else:
                            row[col_name] = None

            rows_to_insert.append(row)

        if rows_to_insert:
            print(
                f"Seeding {len(rows_to_insert)} default rows into {target_table_name}..."
            )
            df = self.spark.createDataFrame(rows_to_insert, schema)

            # Use atomic MERGE operation to prevent duplicates in concurrent environments
            delta_table.alias("target").merge(
                df.alias("source"), f"target.{surrogate_key} = source.{surrogate_key}"
            ).whenNotMatchedInsertAll().execute()

    def optimize_table(
        self, table_name: str, cluster_by: list[str] | None = None
    ) -> None:
        """
        Runs OPTIMIZE on the target table.

        Args:
            table_name: The table to optimize
            cluster_by: Optional list of columns for clustering (Liquid Clustering)
        """
        print(f"Optimizing table {table_name}...")

        if cluster_by:
            # If cluster_by is specified, ensure the table is configured for Liquid Clustering
            # Note: This assumes the table was created with CLUSTER BY clause
            # We just run OPTIMIZE, which will use the existing clustering spec
            quoted_table_name = quote_table_name(table_name)
            self.spark.sql(f"OPTIMIZE {quoted_table_name}")
            print(f"Optimized {table_name} using Liquid Clustering on {cluster_by}")
        else:
            # Standard OPTIMIZE without clustering
            quoted_table_name = quote_table_name(table_name)
            self.spark.sql(f"OPTIMIZE {quoted_table_name}")
            print(f"Optimized {table_name}")
