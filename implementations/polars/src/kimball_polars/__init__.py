"""Polars Kimball Framework - Delta Lake implementation."""

from kimball_polars.scd import apply_scd1, apply_scd2
from kimball_polars.cdf import (
    read_cdf,
    get_cdf_inserts,
    get_cdf_updates,
    get_cdf_deletes,
)
from kimball_polars.watermark import get_watermark, update_watermark, get_table_version
from kimball_polars.facts import temporal_scd2_join, fill_unknown_fk, build_fact_table

__all__ = [
    # SCD operations
    "apply_scd1",
    "apply_scd2",
    # CDF operations
    "read_cdf",
    "get_cdf_inserts",
    "get_cdf_updates",
    "get_cdf_deletes",
    # Watermark tracking
    "get_watermark",
    "update_watermark",
    "get_table_version",
    # Fact table helpers
    "temporal_scd2_join",
    "fill_unknown_fk",
    "build_fact_table",
]

__version__ = "0.2.0"
