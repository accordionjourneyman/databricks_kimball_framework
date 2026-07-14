from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import xxhash64

from kimball.common.config import PIIPolicy

logger = logging.getLogger(__name__)


def apply_pii_masking(df: DataFrame, policy: PIIPolicy) -> DataFrame:
    """Apply PII masking to a DataFrame per the given policy.

    Called by the orchestrator after ``transformation_sql`` executes and
    before FK fill, validation, and merge — so PII never lands in the
    target Delta table (local Spark) or is masked at read time via Delta
    column masks (Databricks, see ``TableCreator._apply_pii_masks``).

    Strategies (per column):
        hash   — ``xxhash64(cast(col as string), col_name)``.
                 Irreversible, deterministic.  Same input always produces
                 the same 64-bit integer, so hashed columns can still
                 be used for joins and equality checks.  Uses xxhash64
                 (non-cryptographic, ~1.8x faster than SHA-256).
        mask   — Replace value with ``mask_char * 10``.  Optionally
                 reveal the first ``reveal_prefix`` characters (e.g.,
                 ``12345**********``).  Useful for partial visibility
                 such as ZIP-code-level address data.
        null   — Set column to NULL.  Column is retained for schema
                 compatibility but the value is removed.
        drop   — Remove the column entirely from the DataFrame.
                 Use when downstream consumers should never see the
                 column at all (data minimization).

    Args:
        df: Transformed DataFrame from ``transformation_sql``.
        policy: PII policy from the YAML ``pii`` block.

    Returns:
        DataFrame with PII columns masked/dropped.
    """
    col_map = policy.column_map
    drop_cols = policy.drop_columns

    for col_name, cfg in col_map.items():
        if col_name not in df.columns:
            logger.warning(f"PII column '{col_name}' not in DataFrame, skipping")
            continue

        strategy = cfg.strategy

        if strategy == "drop":
            if col_name in df.columns:
                df = df.drop(col_name)
            continue

        if strategy == "null":
            df = df.withColumn(col_name, F.lit(None).cast(df.schema[col_name].dataType))
            continue

        if strategy == "hash":
            df = df.withColumn(
                col_name, xxhash64(F.col(col_name).cast("string"), F.lit(col_name))
            )
            continue

        if strategy == "mask":
            prefix = cfg.reveal_prefix
            mask_char = cfg.mask_char
            col_type = df.schema[col_name].dataType

            if prefix > 0:
                masked_expr = F.concat(
                    F.substring(F.col(col_name).cast("string"), 1, prefix),
                    F.lit(mask_char * 10),
                )
            else:
                masked_expr = F.lit(mask_char * 10)

            df = df.withColumn(col_name, masked_expr.cast(col_type))
            continue

        logger.warning(f"Unknown PII strategy '{strategy}' for column '{col_name}'")

    return df