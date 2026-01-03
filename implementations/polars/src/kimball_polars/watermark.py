"""Watermark tracking for incremental processing."""

from datetime import datetime
from pathlib import Path

import polars as pl
from deltalake import DeltaTable, write_deltalake


def get_watermark(control_path: str | Path, table_name: str) -> int | None:
    """
    Get the last processed Delta version for a table.

    Args:
        control_path: Path to watermark control table
        table_name: Name of the table to get watermark for

    Returns:
        Last processed version, or None if never processed
    """
    path = Path(control_path)
    if not path.exists():
        return None

    try:
        dt = DeltaTable(str(path))
        df = pl.from_arrow(dt.to_pyarrow_table())

        result = df.filter(pl.col("table_name") == table_name)
        if result.is_empty():
            return None

        return result["last_version"][0]
    except Exception:
        return None


def update_watermark(
    control_path: str | Path,
    table_name: str,
    version: int,
) -> None:
    """
    Update the watermark for a table after successful processing.

    Args:
        control_path: Path to watermark control table
        table_name: Name of the table
        version: Delta version that was processed
    """
    path = Path(control_path)
    now = datetime.now()

    new_record = pl.DataFrame(
        {
            "table_name": [table_name],
            "last_version": [version],
            "updated_at": [now],
        }
    )

    if not path.exists():
        # Create new control table
        write_deltalake(str(path), new_record.to_arrow(), mode="overwrite")
    else:
        # Merge update
        new_record.write_delta(
            target=str(path),
            mode="merge",
            delta_merge_options={
                "predicate": "s.table_name = t.table_name",
                "source_alias": "s",
                "target_alias": "t",
            },
        ).when_matched_update_all().when_not_matched_insert_all().execute()


def get_table_version(path: str | Path) -> int:
    """Get current version of a Delta table."""
    dt = DeltaTable(str(path))
    return dt.version()
