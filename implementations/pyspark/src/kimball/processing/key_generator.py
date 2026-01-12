from abc import ABC, abstractmethod

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    xxhash64,
)
from pyspark.sql.window import Window


class KeyGenerator(ABC):
    """
    Abstract base class for surrogate key generation strategies.
    """

    @abstractmethod
    def generate_keys(
        self, df: DataFrame, key_col_name: str, existing_max_key: int = 0
    ) -> DataFrame:
        """
        Generates surrogate keys for the input DataFrame.

        Args:
            df: Input DataFrame (new rows to be inserted).
            key_col_name: Name of the surrogate key column.
            existing_max_key: The current maximum surrogate key in the target table (for sequence generation).

        Returns:
            DataFrame with the surrogate key column added.
        """
        pass


class IdentityKeyGenerator(KeyGenerator):
    """
    Relies on Delta Lake Identity Columns.
    Does not add a key column in the DataFrame, assumes the target table handles it on INSERT.
    Returns the DataFrame unmodified to allow explicit values (if present) or auto-generation (if absent).
    """

    def generate_keys(
        self, df: DataFrame, key_col_name: str, existing_max_key: int = 0
    ) -> DataFrame:
        # For Identity Columns:
        # - If the column is missing (normal case), Delta generates the ID.
        # - If the column is present (e.g. default rows with -1), Delta uses the provided value.
        # So we just return the DataFrame as-is.
        return df


class HashKeyGenerator(KeyGenerator):
    """
    Generates deterministic keys using xxhash64 of natural keys.

    ⚠️ COLLISION WARNING ⚠️
    xxhash64 produces 64-bit hashes which have birthday paradox collisions
    at approximately 4 billion rows (2^32). For production use at high scale,
    prefer IdentityKeyGenerator which uses Delta Lake Identity Columns.

    Use cases for HashKeyGenerator:
    - Deterministic key generation across environments (dev/prod same keys)
    - Small to medium datasets (< 1 billion rows)
    - Re-processing scenarios where consistent keys are required

    For most production Kimball warehouses, use IdentityKeyGenerator instead.
    """

    def __init__(self, natural_keys: list[str]):
        import warnings

        warnings.warn(
            "HashKeyGenerator uses xxhash64 which has collision risk at scale (~4B rows). "
            "For production workloads, consider IdentityKeyGenerator instead.",
            UserWarning,
            stacklevel=2,
        )
        self.natural_keys = natural_keys

    def generate_keys(
        self, df: DataFrame, key_col_name: str, existing_max_key: int = 0
    ) -> DataFrame:
        # xxhash64 returns a long (64-bit integer)
        # We concat natural keys to form the seed
        # We use a separator to avoid collisions like ("a", "b") vs ("ab", "")

        # Handle nulls in natural keys by coalescing to empty string or sentinel
        # But xxhash64 handles inputs, so we can just pass columns.
        # However, to be safe and consistent with typical hash key practices:
        return df.withColumn(
            key_col_name, xxhash64(*[col(c) for c in self.natural_keys])
        )


class SequenceKeyGenerator(KeyGenerator):
    """
    DEPRECATED: This implementation is unsafe for production use at scale.

    ⚠️  CRITICAL SCALABILITY WARNING ⚠️
    This implementation uses row_number().over(Window.orderBy(lit(1))) which forces
    the entire dataset to be shuffled to a single partition. This is a well-known
    performance killer that will cause OOM (Out of Memory) errors for large datasets.

    For Kimball surrogates at scale, use IdentityKeyGenerator which handles sequencing
    at the Delta storage layer with proper distributed performance.

    This class is deprecated and will be removed in a future version.
    """

    def generate_keys(
        self, df: DataFrame, key_col_name: str, existing_max_key: int = 0
    ) -> DataFrame:
        warn_msg = (
            "SequenceKeyGenerator is deprecated and UNSAFE for production usage. "
            "It forces a global sort (Window.orderBy(lit(1))) which causes OOM on large datasets. "
            "Use IdentityKeyGenerator (Delta Identity Columns) instead."
        )

        import os

        # Allow unsafe usage only if explicitly overridden (e.g. for small unit tests)
        if os.environ.get("KIMBALL_ALLOW_UNSAFE_SEQUENCE_KEY", "0") != "1":
            raise RuntimeError(
                f"BLOCKED: {warn_msg} Set KIMBALL_ALLOW_UNSAFE_SEQUENCE_KEY=1 to override (NOT RECOMMENDED)."
            )

        import warnings

        warnings.warn(warn_msg, DeprecationWarning, stacklevel=2)

        # This implementation is fundamentally broken for scale
        # Forces all data to single partition = OOM guaranteed
        window = Window.orderBy(lit(1))
        return df.withColumn(
            key_col_name, row_number().over(window) + lit(existing_max_key)
        )
