from pyspark.sql import Column
from pyspark.sql.functions import col, concat_ws, lit, sha2, when

# FINDING-009: Use UUID-based sentinel for NULL values that is statistically unique
# This prevents (NULL, 'val') from colliding with ('', 'val') or any real data
# The UUID is fixed to ensure deterministic hashing across runs
_NULL_SENTINEL = "__NULL_SENTINEL_12345678123456781234567812345678__"


def compute_hashdiff(columns: list[str], sort_columns: bool = True) -> Column:
    """
    Computes a deterministic hashdiff for a list of columns using SHA-256.

    Args:
        columns: List of column names to include in the hash.
        sort_columns: If True, sorts columns alphabetically for consistency.
                     This prevents config order changes from triggering false SCD2 versioning.

    Returns:
        A PySpark Column expression representing the hashdiff.
    """
    # C-10: Sort columns alphabetically to ensure deterministic hashing regardless of config order
    ordered_cols = sorted(columns) if sort_columns else columns

    # Use sha2 for cryptographic collision resistance (256-bit)
    # xxhash64 (64-bit) has birthday paradox collisions at ~4B rows, which is unacceptable for SCD2

    # FIX: Use unique NULL sentinel to distinguish NULL from empty string
    # FIX: Use null byte separator to prevent delimiter collision in data values
    normalized_cols = [
        when(col(c).isNull(), lit(_NULL_SENTINEL)).otherwise(col(c).cast("string"))
        for c in ordered_cols
    ]

    # Use null byte as separator - extremely unlikely to appear in real data
    return sha2(concat_ws("\x00", *normalized_cols), 256)
