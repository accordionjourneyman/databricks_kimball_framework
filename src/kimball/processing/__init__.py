"""Kimball Framework Processing Module.

Contains core data processing components for ETL operations:
SCD merge strategies, key generation, hashing, PII masking,
skeleton generation, table creation, and default row seeding.
"""

from kimball.processing.defaults import (
    ensure_scd1_defaults,
    ensure_scd2_defaults,
    seed_default_rows,
)
from kimball.processing.dispatcher import merge
from kimball.processing.hashing import compute_hashdiff
from kimball.processing.key_generator import HashKeyGenerator
from kimball.processing.late_arriving_dimension import LateArrivingDimensionProcessor
from kimball.processing.loader import DataLoader
from kimball.processing.pii import apply_pii_masking
from kimball.processing.scd1 import merge_scd1
from kimball.processing.scd2 import merge_scd2
from kimball.processing.scd4 import merge_scd4
from kimball.processing.scd6 import merge_scd6
from kimball.processing.skeleton_generator import SkeletonGenerator
from kimball.processing.table_creator import TableCreator
from kimball.processing.table_ops import get_last_merge_metrics, optimize_table

__all__ = [
    "DataLoader",
    "HashKeyGenerator",
    "LateArrivingDimensionProcessor",
    "SkeletonGenerator",
    "TableCreator",
    "apply_pii_masking",
    "compute_hashdiff",
    "ensure_scd1_defaults",
    "ensure_scd2_defaults",
    "get_last_merge_metrics",
    "merge",
    "merge_scd1",
    "merge_scd2",
    "merge_scd4",
    "merge_scd6",
    "optimize_table",
    "seed_default_rows",
]
