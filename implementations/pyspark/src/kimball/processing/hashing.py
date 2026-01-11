from pyspark.sql import Column
from pyspark.sql.functions import col, concat_ws, lit, sha2, when

# Unique sentinel for NULL values to distinguish from empty string
# This prevents (NULL, 'val') from colliding with ('', 'val')
_NULL_SENTINEL = "<\x00NULL\x00>"


def compute_hashdiff(columns: list[str]) -> Column:
    """
    Computes a deterministic hashdiff for a list of columns using SHA-256.

    Args:
        columns: List of column names to include in the hash.

    Returns:
        A PySpark Column expression representing the hashdiff.
    """
    # Use sha2 for cryptographic collision resistance (256-bit)
    # xxhash64 (64-bit) has birthday paradox collisions at ~4B rows, which is unacceptable for SCD2

    # FIX: Use unique NULL sentinel to distinguish NULL from empty string
    # FIX: Use null byte separator to prevent delimiter collision in data values
    normalized_cols = [
        when(col(c).isNull(), lit(_NULL_SENTINEL)).otherwise(col(c).cast("string"))
        for c in columns
    ]

    # Use null byte as separator - extremely unlikely to appear in real data
    return sha2(concat_ws("\x00", *normalized_cols), 256)
