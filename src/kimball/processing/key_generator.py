from pyspark.sql import DataFrame
from pyspark.sql.functions import col, xxhash64


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
        hash_cols = [col(c) for c in self.natural_keys]
        if self.version_column and self.version_column in df.columns:
            hash_cols.append(col(self.version_column))
        return df.withColumn(key_col_name, xxhash64(*hash_cols))
