from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kimball.common.constants import RESERVED_DIMENSION_KEYS
from kimball.common.errors import DataQualityError


def validate_type7_keys(
    source: DataFrame,
    target: DataFrame,
    *,
    surrogate_key: str,
    durable_key: str,
) -> None:
    """Fail before MERGE if a hash maps to two canonical payloads.

    Both key domains are checked in one Spark action. The SHA-256 fingerprints
    are evidence only; facts continue to store compact BIGINT keys.
    """

    definitions = (
        ("row", surrogate_key, "__row_key_fingerprint"),
        ("durable", durable_key, "__durable_key_fingerprint"),
    )
    pairs = None
    for kind, key_column, fingerprint_column in definitions:
        frames = []
        for incoming, frame in ((True, source), (False, target)):
            if (
                key_column not in frame.columns
                or fingerprint_column not in frame.columns
            ):
                continue
            frames.append(
                frame.select(
                    F.lit(kind).alias("__key_kind"),
                    F.col(key_column).cast("long").alias("__key"),
                    F.col(fingerprint_column).alias("__fingerprint"),
                    F.lit(incoming).alias("__incoming"),
                )
            )
        if not frames:
            continue
        domain = frames[0]
        for frame in frames[1:]:
            domain = domain.unionByName(frame)
        pairs = domain if pairs is None else pairs.unionByName(domain)
    if pairs is None:
        raise DataQualityError("Type 7 key fingerprints are missing")

    violations = (
        pairs.filter(
            F.col("__key").isNotNull()
            & F.col("__fingerprint").isNotNull()
            & (F.col("__fingerprint") != "")
        )
        .groupBy("__key_kind", "__key")
        .agg(
            F.count_distinct("__fingerprint").alias("__payloads"),
            F.max(F.col("__incoming").cast("int")).alias("__incoming"),
        )
        .filter(
            (F.col("__payloads") > 1)
            | (
                (F.col("__incoming") == 1)
                & F.col("__key").isin(*sorted(RESERVED_DIMENSION_KEYS))
            )
        )
        .limit(1)
        .collect()
    )
    if violations:
        row = violations[0]
        raise DataQualityError(
            f"Type 7 {row['__key_kind']} key collision/reserved-key violation "
            f"for BIGINT {row['__key']}"
        )
