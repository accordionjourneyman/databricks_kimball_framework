"""PII minimization, masking, fast equality hashes, and keyed tokenization."""

from __future__ import annotations

import hashlib
import hmac
import logging
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import xxhash64
from pyspark.sql.types import StringType

from kimball.common.config import PIIPolicy
from kimball.common.secrets import SecretResolver

logger = logging.getLogger(__name__)


def _hmac_sha256(value: Any | None, key: bytes) -> str | None:
    """Return a lower-case HMAC-SHA-256 token while preserving nulls."""
    if value is None:
        return None
    return hmac.new(key, str(value).encode("utf-8"), hashlib.sha256).hexdigest()


def apply_pii_masking(
    df: DataFrame,
    policy: PIIPolicy,
    *,
    secret_resolver: SecretResolver | None = None,
) -> DataFrame:
    """Apply declared PII handling before validation and target persistence.

    ``tokenize`` is deterministic keyed HMAC-SHA-256 pseudonymization.
    ``fast_hash`` (and legacy alias ``hash``) is non-cryptographic xxhash64
    equality encoding and must not be represented as a security boundary.
    Masked values are strings, including when the source column is numeric.
    """
    resolver = secret_resolver or SecretResolver.for_runtime()
    resolved_keys: dict[str, bytes] = {}

    for col_name, cfg in policy.column_map.items():
        if col_name not in df.columns:
            logger.warning("PII column '%s' not in DataFrame, skipping", col_name)
            continue

        strategy = cfg.strategy
        if strategy == "drop":
            df = df.drop(col_name)
            continue
        if strategy == "null":
            df = df.withColumn(col_name, F.lit(None).cast(df.schema[col_name].dataType))
            continue
        if strategy in {"fast_hash", "hash"}:
            if strategy == "hash":
                logger.warning(
                    "PII strategy 'hash' is a legacy non-cryptographic alias; "
                    "use 'fast_hash' or keyed 'tokenize'"
                )
            df = df.withColumn(
                col_name, xxhash64(F.col(col_name).cast("string"), F.lit(col_name))
            )
            continue
        if strategy == "tokenize":
            reference = cfg.secret_ref
            if reference is None:  # defensive guard for non-Pydantic callers
                raise ValueError(
                    f"PII tokenization for '{col_name}' requires secret_ref"
                )
            if reference not in resolved_keys:
                resolved_keys[reference] = resolver.resolve(reference).encode("utf-8")
            key = resolved_keys[reference]
            tokenize = F.udf(
                lambda value, hmac_key=key: _hmac_sha256(value, hmac_key),
                StringType(),
            )
            df = df.withColumn(col_name, tokenize(F.col(col_name).cast("string")))
            continue
        if strategy == "mask":
            if cfg.reveal_prefix > 0:
                masked = F.concat(
                    F.substring(F.col(col_name).cast("string"), 1, cfg.reveal_prefix),
                    F.lit(cfg.mask_char * 10),
                )
            else:
                masked = F.lit(cfg.mask_char * 10)
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(), F.lit(None).cast("string")).otherwise(
                    masked
                ),
            )
            continue
        raise ValueError(f"Unsupported PII strategy '{strategy}'")
    return df
