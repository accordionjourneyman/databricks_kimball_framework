"""Polars Kimball Framework - SCD Logic with Delta support."""

from datetime import datetime
from pathlib import Path

import polars as pl
from deltalake import DeltaTable, write_deltalake


def apply_scd2(
    target_path: str | Path,
    source_df: pl.DataFrame,
    natural_keys: list[str],
    track_columns: list[str],
    surrogate_key: str = "sk",
) -> pl.DataFrame:
    """
    Apply SCD Type 2 logic.

    Creates history rows when tracked columns change.

    Args:
        target_path: Path to Delta table
        source_df: New/updated records from source
        natural_keys: Columns that identify unique business entities
        track_columns: Columns to track for history
        surrogate_key: Name of surrogate key column

    Returns:
        Updated dimension DataFrame
    """
    now = datetime.now()
    far_future = datetime(2099, 12, 31, 23, 59, 59)
    path = Path(target_path)

    # Add audit columns
    source_ready = source_df.with_columns(
        [
            pl.lit(now).alias("__valid_from"),
            pl.lit(far_future).alias("__valid_to"),
            pl.lit(True).alias("__is_current"),
        ]
    )

    if not path.exists():
        # First load - assign surrogate keys and write
        result = source_ready.with_row_index(surrogate_key, offset=1).cast(
            {surrogate_key: pl.Int64}
        )
        write_deltalake(str(path), result.to_arrow(), mode="overwrite")
        return result

    # Read current target
    dt = DeltaTable(str(path))
    target_df = pl.from_arrow(dt.to_pyarrow_table())

    # Get current records
    current_target = target_df.filter(pl.col("__is_current"))

    # Find changed records by comparing track columns
    matched = source_df.join(
        current_target.select(natural_keys + track_columns + [surrogate_key]),
        on=natural_keys,
        how="inner",
        suffix="_old",
    )

    # Check for changes in any track column
    change_conditions = [
        pl.col(col).cast(pl.String) != pl.col(f"{col}_old").cast(pl.String)
        for col in track_columns
    ]
    has_changes = pl.any_horizontal(change_conditions)
    changed_keys = matched.filter(has_changes).select(natural_keys)

    # New records (not in target)
    new_record_keys = source_df.join(
        current_target.select(natural_keys), on=natural_keys, how="anti"
    ).select(natural_keys)

    # Build result:
    # 1. All historical rows unchanged
    historical = target_df.filter(~pl.col("__is_current"))

    # 2. Expire changed current rows
    expired = current_target.join(
        changed_keys, on=natural_keys, how="inner"
    ).with_columns(
        [
            pl.lit(now).alias("__valid_to"),
            pl.lit(False).alias("__is_current"),
        ]
    )

    # 3. Keep unchanged current rows
    unchanged = current_target.join(changed_keys, on=natural_keys, how="anti").join(
        new_record_keys, on=natural_keys, how="anti"
    )

    # 4. New versions of changed records
    max_sk = int(target_df[surrogate_key].max() or 0)

    new_versions = (
        source_ready.join(changed_keys, on=natural_keys, how="inner")
        .with_row_index("__temp_idx", offset=max_sk + 1)
        .with_columns(pl.col("__temp_idx").cast(pl.Int64).alias(surrogate_key))
        .drop("__temp_idx")
    )

    # 5. Completely new records
    next_sk = max_sk + len(new_versions) + 1
    new_inserts = (
        source_ready.join(
            current_target.select(natural_keys), on=natural_keys, how="anti"
        )
        .with_row_index("__temp_idx", offset=next_sk)
        .with_columns(pl.col("__temp_idx").cast(pl.Int64).alias(surrogate_key))
        .drop("__temp_idx")
    )

    # Combine all
    result = pl.concat(
        [
            historical,
            expired,
            unchanged,
            new_versions,
            new_inserts,
        ],
        how="diagonal",
    ).sort(natural_keys + ["__valid_from"])

    # Write back
    write_deltalake(str(path), result.to_arrow(), mode="overwrite")

    return result


def apply_scd1(
    target_path: str | Path,
    source_df: pl.DataFrame,
    natural_keys: list[str],
    surrogate_key: str = "sk",
) -> pl.DataFrame:
    """
    Apply SCD Type 1 logic using Delta MERGE.

    Updates in place, no history preserved.
    """
    now = datetime.now()
    path = Path(target_path)

    source_ready = source_df.with_columns(pl.lit(now).alias("__updated_at"))

    if not path.exists():
        result = source_ready.with_row_index(surrogate_key, offset=1).cast(
            {surrogate_key: pl.Int64}
        )
        write_deltalake(str(path), result.to_arrow(), mode="overwrite")
        return result

    # Use Delta MERGE
    predicate = " AND ".join([f"s.{k} = t.{k}" for k in natural_keys])

    source_ready.write_delta(
        target=str(path),
        mode="merge",
        delta_merge_options={
            "predicate": predicate,
            "source_alias": "s",
            "target_alias": "t",
        },
    ).when_matched_update_all().when_not_matched_insert_all().execute()

    # Read back result
    dt = DeltaTable(str(path))
    return pl.from_arrow(dt.to_pyarrow_table())


def read_dimension(path: str | Path) -> pl.DataFrame:
    """Read a dimension table from Delta Lake."""
    dt = DeltaTable(str(path))
    return pl.from_arrow(dt.to_pyarrow_table())
