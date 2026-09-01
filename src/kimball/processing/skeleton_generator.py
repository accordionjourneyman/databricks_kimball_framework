"""Generate skeleton placeholder rows for early-arriving facts.

When a fact table references a dimension key that does not yet exist in the
dimension, a *skeleton* row is created as a placeholder. Skeletons are
flagged with ``__is_skeleton = True`` and receive substitute values for
all non-key columns. When real data arrives later, the skeleton is
hydrated (replaced or updated in place) via
:class:`LateArrivingDimensionProcessor`.
"""

from __future__ import annotations

import logging
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType

from kimball.common.constants import DEFAULT_VALID_FROM, DEFAULT_VALID_TO
from kimball.processing.key_generator import HashKeyGenerator

logger = logging.getLogger(__name__)

_RESERVED_SKS = (-1, -2, -3, -4)


def _replacement_for_type(data_type: Any) -> Any:
    """Return the deterministic Kimball substitute for a NOT NULL column.

    Wraps :func:`replacement_for_type` with skeleton-specific defaults for
    the audit/validity columns a dimension table can carry.
    """
    if isinstance(data_type, TimestampType):
        return DEFAULT_VALID_FROM
    if isinstance(data_type, DateType):
        return DEFAULT_VALID_FROM.date()
    from kimball.processing.dimension_nulls import replacement_for_type as _rft

    return _rft(data_type)


class SkeletonGenerator:
    """Create skeleton placeholder rows for missing dimension keys.

    Usage::

        gen = SkeletonGenerator(spark)
        gen.generate_skeletons(
            fact_df=fact_df,
            dim_table_name="warehouse.dim_customer",
            fact_join_key="customer_id",
            dim_join_key="customer_id",
            surrogate_key_col="customer_sk",
            batch_id="batch-001",
        )
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def generate_skeletons(
        self,
        fact_df: DataFrame,
        dim_table_name: str,
        fact_join_key: str,
        dim_join_key: str,
        surrogate_key_col: str,
        batch_id: str | None = None,
        *,
        version_column: str | None = None,
        durable_key_col: str | None = None,
    ) -> None:
        """Insert skeleton rows for *fact_df* keys missing from *dim_table_name*.

        No-op if the dimension table does not exist or lacks ``__is_skeleton``.
        Also no-ops on an empty dimension (defaults only): the subsequent merge
        owns the initial load, and skeletonizing every source key would race it.
        """
        if not self.spark.catalog.tableExists(dim_table_name):
            logger.debug(
                "Skeleton generation skipped: %s does not exist", dim_table_name
            )
            return
        dim = self.spark.table(dim_table_name)
        if "__is_skeleton" not in dim.columns:
            logger.debug(
                "Skeleton generation skipped: %s has no __is_skeleton column",
                dim_table_name,
            )
            return

        # Initial load: target only has reserved defaults (or is empty).
        # The merge will insert real members; skeletonizing the full source
        # would insert placeholder rows with wrong validity and race the merge.
        if surrogate_key_col in dim.columns:
            real_members = dim.filter(
                ~F.col(surrogate_key_col).isin(list(_RESERVED_SKS))
                & F.col(surrogate_key_col).isNotNull()
            )
        else:
            real_members = dim
        if real_members.limit(1).count() == 0:
            logger.info(
                "Skeleton generation skipped: %s has no real members yet "
                "(initial load; merge owns population)",
                dim_table_name,
            )
            return

        source_keys = (
            fact_df.select(fact_join_key)
            .filter(F.col(fact_join_key).isNotNull())
            .distinct()
        )
        dim_keys = dim.select(dim_join_key).distinct()
        missing = source_keys.join(
            dim_keys,
            source_keys[fact_join_key] == dim_keys[dim_join_key],
            "left_anti",
        )
        if missing.limit(1).count() == 0:
            return

        skeletons = missing.select(F.col(fact_join_key).alias(dim_join_key))

        # Deterministic SK from the natural key (and optional effective-time).
        # Must be set before the attribute loop so it is never nullified.
        key_gen = HashKeyGenerator([dim_join_key], version_column=version_column)
        if (
            version_column
            and version_column in fact_df.columns
            and version_column not in skeletons.columns
        ):
            # Attach a stable effective-time for Type-7 style keys when available.
            min_effective = (
                fact_df.filter(F.col(fact_join_key).isNotNull())
                .groupBy(fact_join_key)
                .agg(F.min(F.col(version_column)).alias(version_column))
            )
            skeletons = skeletons.join(
                min_effective,
                skeletons[dim_join_key] == min_effective[fact_join_key],
                "left",
            ).drop(min_effective[fact_join_key])
        skeletons = key_gen.generate_keys(skeletons, surrogate_key_col)

        # SCD7 dimensions carry a durable key and hash fingerprints. Their
        # columns are NOT NULL (a Type 7 table is created under the kimball
        # null policy), so the skeleton must carry a real durable key derived
        # the same way the merge will derive it. Later hydration in scd2.py
        # overwrites these with the real row's values.
        dim_columns = {f.name for f in dim.schema.fields}
        if durable_key_col and durable_key_col in dim_columns:
            from kimball.processing.key_generator import stamp_type7_columns

            # __skeleton_valid_from is a stable effective-time input so the
            # Type 7 key derivation has a version column; the derivation is
            # dropped again after stamping (never projected to the target).
            skeletons = stamp_type7_columns(
                skeletons.withColumn(
                    "__skeleton_valid_from",
                    F.lit(DEFAULT_VALID_FROM).cast(TimestampType()),
                ),
                [dim_join_key],
                "__skeleton_valid_from",
                durable_key_col,
            ).drop("__skeleton_valid_from")

        # Lookup table: column name -> (needs_column, apply_fn(skeletons, field, batch_id)).
        # Reduces 23 if/elif branches to a single dict lookup.
        _COLUMN_HANDLERS: dict[str, tuple[bool, Any]] = {
            "__is_skeleton": (False, lambda s, f, b: s.withColumn(f.name, F.lit(True))),
            "__is_current": (False, lambda s, f, b: s.withColumn(f.name, F.lit(True))),
            "__is_deleted": (False, lambda s, f, b: s.withColumn(f.name, F.lit(False))),
            "__valid_from": (
                False,
                lambda s, f, b: s.withColumn(
                    f.name, F.lit(DEFAULT_VALID_FROM).cast(TimestampType())
                ),
            ),
            "__valid_to": (
                False,
                lambda s, f, b: s.withColumn(
                    f.name, F.lit(DEFAULT_VALID_TO).cast(TimestampType())
                ),
            ),
            "__etl_processed_at": (
                False,
                lambda s, f, b: s.withColumn(f.name, F.current_timestamp()),
            ),
            "__etl_batch_id": (
                False,
                lambda s, f, b: s.withColumn(f.name, F.lit(b or "skeleton")),
            ),
            "__skeleton_created_at": (
                False,
                lambda s, f, b: s.withColumn(f.name, F.current_timestamp()),
            ),
            "__member_status": (
                False,
                lambda s, f, b: s.withColumn(f.name, F.lit("NOT_YET_AVAILABLE")),
            ),
            "__key_origin": (
                False,
                lambda s, f, b: s.withColumn(f.name, F.lit("skeleton")),
            ),
        }
        _SKIP_COLUMNS = frozenset(
            {
                "__merge_action",
                "__scd2_intermediate",
                "__scd2_seq",
                "__scd2_total",
            }
        )

        for field in dim.schema.fields:
            name = field.name
            if (
                name == dim_join_key
                or name == surrogate_key_col
                or name in _SKIP_COLUMNS
            ):
                continue
            if name in skeletons.columns:
                continue
            handler = _COLUMN_HANDLERS.get(name)
            if handler is not None:
                _, apply_fn = handler
                skeletons = apply_fn(skeletons, field, batch_id)
            elif field.nullable:
                skeletons = skeletons.withColumn(name, F.lit(None).cast(field.dataType))
            else:
                # The target column carries a NOT NULL constraint (e.g. a
                # dimension created under the "kimball" null policy stamps
                # every attribute column NOT NULL).  Inserting NULL here
                # would violate DELTA_NOT_NULL_CONSTRAINT_VIOLATED, so use
                # the framework's type-appropriate substitute instead.
                skeletons = skeletons.withColumn(
                    name,
                    F.lit(_replacement_for_type(field.dataType)).cast(field.dataType),
                )

        # Project to target column order; drop helper columns not in dim.
        target_cols = [f.name for f in dim.schema.fields]
        skeletons = skeletons.select(
            *[
                F.col(c).cast(dim.schema[c].dataType).alias(c)
                if c in skeletons.columns
                else F.lit(None).cast(dim.schema[c].dataType).alias(c)
                for c in target_cols
            ]
        )

        condition = f"target.`{dim_join_key}` = source.`{dim_join_key}`"
        DeltaTable.forName(self.spark, dim_table_name).alias("target").merge(
            skeletons.alias("source"), condition
        ).whenNotMatchedInsertAll().execute()
        logger.info("Inserted skeleton row(s) into %s", dim_table_name)
