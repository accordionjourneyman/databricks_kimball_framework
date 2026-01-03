"""CDF (Change Data Feed) reader for Polars via delta-rs."""

from pathlib import Path

import polars as pl
from deltalake import DeltaTable


def read_cdf(
    path: str | Path,
    starting_version: int,
    ending_version: int | None = None,
) -> pl.DataFrame:
    """
    Read Change Data Feed from a Delta table.

    Uses the delta-rs Python bindings to read CDF, then converts
    to Polars via zero-copy PyArrow bridge.

    Args:
        path: Path to Delta table
        starting_version: First version to read (inclusive)
        ending_version: Last version to read (inclusive), None for latest

    Returns:
        DataFrame with CDF columns: _change_type, _commit_version, _commit_timestamp
    """
    dt = DeltaTable(str(path))

    if ending_version is None:
        ending_version = dt.version()

    arrow_table = dt.load_cdf(
        starting_version=starting_version,
        ending_version=ending_version,
    )

    return pl.from_arrow(arrow_table)


def get_cdf_inserts(cdf_df: pl.DataFrame) -> pl.DataFrame:
    """Filter CDF to only INSERT operations."""
    return cdf_df.filter(pl.col("_change_type") == "insert")


def get_cdf_updates(cdf_df: pl.DataFrame) -> pl.DataFrame:
    """Filter CDF to only UPDATE operations (pre and post images)."""
    return cdf_df.filter(
        pl.col("_change_type").is_in(["update_preimage", "update_postimage"])
    )


def get_cdf_deletes(cdf_df: pl.DataFrame) -> pl.DataFrame:
    """Filter CDF to only DELETE operations."""
    return cdf_df.filter(pl.col("_change_type") == "delete")
