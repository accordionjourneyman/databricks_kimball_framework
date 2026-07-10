from abc import ABC, abstractmethod

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, monotonically_increasing_id, xxhash64


class KeyGenerator(ABC):
    @abstractmethod
    def generate_keys(
        self, df: DataFrame, key_col_name: str, existing_max_key: int = 0
    ) -> DataFrame:
        pass


class IdentityKeyGenerator(KeyGenerator):
    def generate_keys(
        self, df: DataFrame, key_col_name: str, existing_max_key: int = 0
    ) -> DataFrame:
        return df


class HashKeyGenerator(KeyGenerator):
    def __init__(
        self, natural_keys: list[str], allow_for_dimension_surrogates: bool = False
    ):
        import warnings

        if not allow_for_dimension_surrogates:
            warnings.warn(
                "HashKeyGenerator should not be used for dimension surrogate keys.",
                UserWarning,
                stacklevel=2,
            )
        self.natural_keys = natural_keys

    def generate_keys(
        self, df: DataFrame, key_col_name: str, existing_max_key: int = 0
    ) -> DataFrame:
        return df.withColumn(
            key_col_name, xxhash64(*[col(c) for c in self.natural_keys])
        )


class SequenceKeyGenerator(KeyGenerator):
    def generate_keys(
        self, df: DataFrame, key_col_name: str, existing_max_key: int = 0
    ) -> DataFrame:
        import os
        import warnings

        if os.environ.get("KIMBALL_ALLOW_UNSAFE_SEQUENCE_KEY", "0") != "1":
            raise RuntimeError("BLOCKED: unsafe sequence key generation")
        warnings.warn(
            "SequenceKeyGenerator is deprecated", DeprecationWarning, stacklevel=2
        )
        return df.withColumn(
            key_col_name,
            monotonically_increasing_id() + lit(existing_max_key),
        )
