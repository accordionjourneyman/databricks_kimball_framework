"""Merge module — thin facade re-exporting from decomposed submodules.

Historical note: this was a 1493-line god object. It has been split into:
  - merge_helpers: shared infrastructure (CDF utils, key gen, merge conditions)
  - scd1: SCD Type 1 strategy
  - scd2: SCD Type 2 strategy (classic + two-phase)
  - scd4: SCD Type 4 strategy (mini-dimension / EAV history)
  - scd6: SCD Type 6 strategy (hybrid SCD1+SCD2)
  - defaults: default row seeding
  - table_ops: optimize, metrics
  - dispatcher: merge() entry point with retry
"""
from kimball.processing.defaults import (
    ensure_scd1_defaults,
    ensure_scd2_defaults,
)
from kimball.processing.dispatcher import merge
from kimball.processing.table_ops import (
    get_last_merge_metrics,
    optimize_table,
)