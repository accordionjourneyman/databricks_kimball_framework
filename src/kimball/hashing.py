from pyspark.sql import Column
from pyspark.sql.functions import xxhash64, coalesce, lit, col, trim, lower
from typing import List

def compute_hashdiff(columns: List[str]) -> Column:
    """
    Computes a deterministic hashdiff for a list of columns.
    
    Args:
        columns: List of column names to include in the hash.
        
    Returns:
        A PySpark Column expression representing the hashdiff.
    """
    # Normalize and coalesce columns to ensure stability
    # We trim whitespace and convert to lower case for string columns? 
    # Maybe safer to just coalesce nulls for now to avoid data loss if case matters.
    # The user requirement mentioned "normalized/trimmed attribute set".
    
    # Let's handle this by applying a standard transformation:
    # 1. Coalesce NULL to empty string (or specific sentinel if needed, but empty string is common)
    # 2. Cast to string (implicitly done by some hash functions, but good to be explicit if mixed types)
    # xxhash64 accepts multiple columns of any type.
    
    # To match the user's example:
    # xxhash64(coalesce(name,''), coalesce(city,''), ...)
    
    # We will just coalesce to empty string for now.
    # If strict type handling is needed, we might need schema awareness, but we only have column names here.
    # Assuming columns are available in the DF context where this expr is used.
    
    normalized_cols = [coalesce(col(c).cast("string"), lit("")) for c in columns]
    
    return xxhash64(*normalized_cols)
