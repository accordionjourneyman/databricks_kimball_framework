from pyspark.sql import Column
from pyspark.sql.functions import xxhash64, coalesce, lit, col, struct
from typing import List

def compute_hashdiff(columns: List[str]) -> Column:
    """
    Computes a deterministic hashdiff for a list of columns using vectorized struct.
    
    Args:
        columns: List of column names to include in the hash.
        
    Returns:
        A PySpark Column expression representing the hashdiff.
    """
    # Use struct for better vectorization and Photon compatibility
    normalized_cols = [coalesce(col(c).cast("string"), lit("")) for c in columns]
    
    return xxhash64(struct(*normalized_cols))
