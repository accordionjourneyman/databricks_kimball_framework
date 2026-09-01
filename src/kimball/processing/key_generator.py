from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import coalesce, col, concat_ws, lit, sha2, xxhash64

_NULL_TOKEN = "__KIMBALL_NULL__"


def _canonical_payload(columns: list[str]) -> Column:
    """Build an unambiguous, type-normalized payload for deterministic keys."""

    normalized = [
        concat_ws(
            ":",
            lit(name),
            coalesce(col(name).cast("string"), lit(_NULL_TOKEN)),
        )
        for name in columns
    ]
    return concat_ws("\x00", *normalized)


def type7_key_columns(natural_keys: list[str], effective_at: str) -> dict[str, Column]:
    """Return BIGINT Type 7 keys plus cryptographic collision evidence."""

    durable_payload = _canonical_payload(natural_keys)
    durable_fingerprint = sha2(durable_payload, 256)
    row_payload = concat_ws(
        "\x00", durable_fingerprint, col(effective_at).cast("string")
    )
    return {
        "durable_key": xxhash64(durable_payload),
        "row_key": xxhash64(row_payload),
        "durable_fingerprint": durable_fingerprint,
        "row_fingerprint": sha2(row_payload, 256),
    }


class HashKeyGenerator:
    """Generate deterministic BIGINT surrogate keys via xxhash64.

    For SCD1/SCD4/SCD6: hashes the natural keys only (one SK per entity).
    For SCD2: hashes the natural keys + validity timestamp so each
    historical version gets a unique, deterministic SK.
    """

    def __init__(
        self,
        natural_keys: list[str],
        version_column: str | None = None,
    ):
        self.natural_keys = natural_keys
        self.version_column = version_column

    def generate_keys(self, df: DataFrame, key_col_name: str) -> DataFrame:
        payload_columns = list(self.natural_keys)
        if self.version_column and self.version_column in df.columns:
            payload_columns.append(self.version_column)
        return df.withColumn(
            key_col_name, xxhash64(_canonical_payload(payload_columns))
        )

    def generate_type7_keys(
        self,
        df: DataFrame,
        surrogate_key: str,
        durable_key: str,
    ) -> DataFrame:
        if not self.version_column or self.version_column not in df.columns:
            raise ValueError("Type 7 key generation requires the effective-time column")
        keys = type7_key_columns(self.natural_keys, self.version_column)
        return (
            df.withColumn(durable_key, keys["durable_key"])
            .withColumn(surrogate_key, keys["row_key"])
            .withColumn("__durable_key_fingerprint", keys["durable_fingerprint"])
            .withColumn("__row_key_fingerprint", keys["row_fingerprint"])
        )


def stamp_type7_columns(
    df: DataFrame,
    natural_keys: list[str],
    effective_at: str,
    durable_key_col: str,
    *,
    fingerprint_columns: tuple[str, str] = (
        "__durable_key_fingerprint",
        "__row_key_fingerprint",
    ),
) -> DataFrame:
    """Add the Type 7 durable key and fingerprint columns to *df*.

    Single derivation point shared by insert-row key generation, skeleton
    placeholders, and HYDRATE rows: all three must produce the same values
    for the same natural key/effective-time so NOT NULL constraints hold and
    collision evidence stays consistent across the pipeline.
    """
    keys = type7_key_columns(natural_keys, effective_at)
    return (
        df.withColumn(durable_key_col, keys["durable_key"])
        .withColumn(fingerprint_columns[0], keys["durable_fingerprint"])
        .withColumn(fingerprint_columns[1], keys["row_fingerprint"])
    )
