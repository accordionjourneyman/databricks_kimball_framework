"""Pure staging algebra for the single-pass SCD2 merge.

Everything here is DataFrame-in / DataFrame-out: no DeltaTable access, no
MERGE builders, no SparkSession lookups. The write layer
(``scd2._merge_single_pass``) orchestrates: stage -> derive keys -> hand to
its (unchanged) conditional MERGE builder.

Extracted per ADR-004 so the staging algebra is unit-testable without a
JVM and so plan-shape decisions (which buckets exist) are computed at one
seam instead of being coupled to eager ``isEmpty()`` inspections scattered
through the write path.

Behavioral contract: every function here is a byte-for-byte extraction of
the logic that lived inline in ``scd2._merge_single_pass``. The comments
documenting deliberate fixes are preserved; they are the record of why
the algebra is shaped this way.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, expr, lag, lead, lit, row_number, when

from kimball.common.constants import DEFAULT_VALID_TO


@dataclass(frozen=True)
class StagedChanges:
    """Staged change buckets for one SCD2 merge, keyed by __merge_action.

    A bucket key is absent when that action produced no rows. Also carries
    the plan-shape flags the MERGE builder needs (``has_changed`` etc.) so
    eager ``isEmpty()`` inspections live at exactly one seam, plus the
    chosen validity column (SQL-qualified, bare name, and semantics note).
    """

    buckets: dict[str, DataFrame]
    validity_col: str
    validity_col_name: str
    validity_note: str
    has_changed: bool
    has_older: bool
    has_hydrate: bool
    upserts: DataFrame


def choose_order_column(upserts: DataFrame) -> tuple[str, DataFrame]:
    """Pick the commit-order column, stamping ``__etl_processed_at`` if absent.

    Preference: ``_commit_version`` > ``_commit_timestamp`` >
    ``__etl_processed_at``. When none exists, ``__etl_processed_at`` is
    stamped with the current time (first-seen ordering).
    """
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
        from pyspark.sql.functions import current_timestamp

        upserts = upserts.withColumn("__etl_processed_at", current_timestamp())
    return order_col, upserts


def rank_source_versions(
    upserts: DataFrame,
    join_keys: list[str],
    validity_name: str,
    order_col: str,
) -> tuple[DataFrame, DataFrame]:
    """Rank incoming versions per key; return (latest, older).

    Computes ``__scd2_next_valid_from`` across the complete incoming chain
    *before* splitting latest from historical: deriving ``lead`` over only
    historical rows leaves the newest historical row open-ended whenever a
    newer current row exists in the same batch (documented in scd2.py).

    ``KIMBALL_SINGLE_WINDOW_SCD2=1`` switches to a single-window variant
    using ``lag`` over the descending order (behavioral escape hatch kept
    identical to the original implementation).
    """
    w_desc = Window.partitionBy(*join_keys).orderBy(col(order_col).desc())
    if os.environ.get("KIMBALL_SINGLE_WINDOW_SCD2") == "1":
        ranked = upserts.withColumn("_rn", row_number().over(w_desc)).withColumn(
            "__scd2_next_valid_from", lag(validity_name, 1).over(w_desc)
        )
    else:
        w_asc = Window.partitionBy(*join_keys).orderBy(col(order_col).asc())
        ranked = upserts.withColumn("_rn", row_number().over(w_desc)).withColumn(
            "__scd2_next_valid_from", lead(validity_name, 1).over(w_asc)
        )
    latest = ranked.filter(col("_rn") == 1).drop("_rn")
    older = ranked.filter(col("_rn") > 1).drop("_rn")
    return latest, older


def classify_joined(
    joined: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Split target-joined latest rows into (new, changed, hydrate).

    NEW: no matching target row. CHANGED: matched non-skeleton row whose
    recomputed hashdiff differs (null-aware on both sides). HYDRATE:
    matched skeleton whose hashdiff differs — keeps ``target_sk`` so the
    SK-based MERGE hydrates the skeleton in place, preserving the SK that
    fact FKs already point at. (Previously target_sk was dropped on this
    bucket and the rows inserted as null-SK duplicates while the skeleton
    stayed unfilled: silent data loss / orphaned FKs.)
    """
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
    rows_to_hydrate = joined.filter(
        col("target_sk").isNotNull()
        & col("target_is_skeleton")
        & (
            (col("hashdiff") != col("target_hashdiff"))
            | (col("hashdiff").isNull() != col("target_hashdiff").isNull())
        )
    ).drop("target_hashdiff", "target_is_skeleton")
    return rows_new, rows_changed, rows_to_hydrate


def build_expire_row(
    rows_changed: DataFrame,
    upserts: DataFrame,
    join_keys: list[str],
    validity_name: str,
) -> DataFrame:
    """EXPIRE rows: exclusive upper bound = oldest incoming valid_from.

    The bound is the min over the *complete incoming chain* (``upserts``),
    not just the changed rows: the old current target row must be expired
    at oldest_new_valid_from, NOT at the latest version's valid_from.
    Expiring at the latest value makes the old row's [t0, latest_from]
    interval overlap the back-filled intermediate versions, so a
    point-in-time read between the oldest and latest new version returns
    the stale old row instead of the correct intermediate version.
    """
    oldest_valid_from = upserts.groupBy(*join_keys).agg(
        expr(f"min(CAST(`{validity_name}` AS TIMESTAMP)) as __scd2_oldest_valid_from")
    )
    rows_changed = rows_changed.join(oldest_valid_from, join_keys, "left")
    return rows_changed.withColumn("__merge_action", lit("EXPIRE")).withColumn(
        "__valid_to", col("__scd2_oldest_valid_from")
    )


def build_insert_latest(rows_changed: DataFrame) -> DataFrame:
    """INSERT_LATEST rows: current with a concrete high-date boundary."""
    return (
        rows_changed.withColumn("__merge_action", lit("INSERT_LATEST"))
        .withColumn("__is_current", lit(True))
        .withColumn("__valid_to", lit(DEFAULT_VALID_TO).cast("timestamp"))
    )


def build_insert_new(rows_new: DataFrame) -> DataFrame:
    """INSERT_NEW rows: current with the same high-date boundary."""
    return (
        rows_new.withColumn("__merge_action", lit("INSERT_NEW"))
        .withColumn("__is_current", lit(True))
        .withColumn("__valid_to", lit(DEFAULT_VALID_TO).cast("timestamp"))
    )


def build_insert_older(older: DataFrame) -> DataFrame:
    """INSERT_OLDER rows: historical, chained to the next incoming version."""
    return (
        older.withColumn("__merge_action", lit("INSERT_OLDER"))
        .withColumn("__is_current", lit(False))
        .withColumn(
            "__valid_to",
            when(
                col("__scd2_next_valid_from").isNull(),
                lit(DEFAULT_VALID_TO).cast("timestamp"),
            ).otherwise(col("__scd2_next_valid_from").cast("timestamp")),
        )
        .drop("__scd2_next_valid_from")
    )


def build_hydrate(rows_to_hydrate: DataFrame) -> DataFrame:
    """HYDRATE rows: keep target_sk for in-place skeleton hydration."""
    return rows_to_hydrate.withColumn("__merge_action", lit("HYDRATE"))


def stage_scd2_changes(
    *,
    joined: DataFrame,
    upserts: DataFrame,
    older: DataFrame,
    join_keys: list[str],
    validity_col: str,
    validity_col_name: str,
    validity_note: str,
    lazy_eval: bool,
) -> StagedChanges:
    """Union the classified buckets into the staged change set.

    Plan-shape flags (``has_changed`` / ``has_older`` / ``has_hydrate``)
    gate which buckets are unioned. They mirror the original eager
    ``isEmpty()`` gating: the EXPIRE whenMatchedUpdate branch references
    ``source.__scd2_oldest_valid_from``, which only exists on expire rows
    (derived from rows_changed). When the only matched target is a
    skeleton (routed to HYDRATE) there are no expire rows and the column
    is absent from the staged source; adding the EXPIRE branch
    unconditionally then fails Delta plan resolution with
    DELTA_MERGE_UNRESOLVED_EXPRESSION. With ``KIMBALL_OPTIMIZE_SCD2_LAZY_EVAL``
    the union is unconditional and the column is always present.
    """
    rows_new, rows_changed, rows_to_hydrate = classify_joined(joined)

    expire_rows = build_expire_row(rows_changed, upserts, join_keys, validity_col_name)
    latest_version = build_insert_latest(rows_changed)
    new_rows = build_insert_new(rows_new)

    has_older = True if lazy_eval else not older.isEmpty()
    older_versions = build_insert_older(older) if has_older else None

    has_changed = True if lazy_eval else not rows_changed.isEmpty()

    staged = new_rows
    if has_changed:
        staged = staged.unionByName(expire_rows, allowMissingColumns=True)
        staged = staged.unionByName(latest_version, allowMissingColumns=True)
    if older_versions is not None:
        staged = staged.unionByName(older_versions, allowMissingColumns=True)

    has_hydrate = True if lazy_eval else not rows_to_hydrate.isEmpty()
    if has_hydrate:
        staged = staged.unionByName(
            build_hydrate(rows_to_hydrate), allowMissingColumns=True
        )

    buckets = {"*": staged}
    return StagedChanges(
        buckets=buckets,
        validity_col=validity_col,
        validity_col_name=validity_col_name,
        validity_note=validity_note,
        has_changed=has_changed,
        has_older=has_older,
        has_hydrate=has_hydrate,
        upserts=upserts,
    )
