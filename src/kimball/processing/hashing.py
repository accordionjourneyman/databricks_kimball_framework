from pyspark.sql import Column
from pyspark.sql.functions import coalesce, col, concat_ws, lit, xxhash64

_NULL_SENTINEL = "__NULL_SENTINEL_12345678123456781234567812345678__"


def compute_hashdiff(
    columns: list[str],
    sort_columns: bool = True,
) -> Column:
    """Compute a deterministic 64-bit hashdiff for a list of columns.

    Uses ``xxhash64`` (non-cryptographic, ~1.8x faster than SHA-256 in
    Spark 4.0).  Returns a BIGINT column.  Collision risk is negligible
    below ~600M distinct values; a full reload self-heals any miss.

    Nulls are mapped to a sentinel string via ``coalesce`` before hashing,
    so ``NULL = NULL`` for hashdiff comparison.

    Args:
        columns: Column names to include in the hash.
        sort_columns: Sort alphabetically for config-order independence.

    Returns:
        A PySpark Column expression (BIGINT) representing the hashdiff.
    """
    ordered_cols = sorted(columns) if sort_columns else columns

    normalized_cols = [
        coalesce(col(c).cast("string"), lit(_NULL_SENTINEL)) for c in ordered_cols
    ]

    return xxhash64(concat_ws("\x00", *normalized_cols))
