"""Public merge facade for SCD strategies and table operations."""

from kimball.processing.defaults import ensure_scd1_defaults, ensure_scd2_defaults
from kimball.processing.dispatcher import merge
from kimball.processing.table_ops import (
    get_last_merge_metrics,
    optimize_table,
    vacuum_table,
)

__all__ = [
    "ensure_scd1_defaults",
    "ensure_scd2_defaults",
    "get_last_merge_metrics",
    "merge",
    "optimize_table",
    "vacuum_table",
]
