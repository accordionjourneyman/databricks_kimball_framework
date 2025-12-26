from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, xxhash64, lit, concat_ws, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from databricks.sdk.runtime import spark

class KeyGenerator(ABC):
    """
    Abstract base class for surrogate key generation strategies.
    """
    
    @abstractmethod
    def generate_keys(self, df: DataFrame, key_col_name: str, existing_max_key: int = 0) -> DataFrame:
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
    """
    def generate_keys(self, df: DataFrame, key_col_name: str, existing_max_key: int = 0) -> DataFrame:
        # For Identity Columns, we typically DO NOT provide the column in the INSERT statement
        # or we provide it as DEFAULT.
        # However, if the dataframe already has the column (e.g. from source), we might need to drop it
        # or ensure it's not mapped in the MERGE insert.
        # For this implementation, we'll assume the caller handles the MERGE mapping,
        # so we just return the DF as is, potentially ensuring the key col is NOT present or is NULL.
        if key_col_name in df.columns:
            return df.drop(key_col_name)
        return df

class HashKeyGenerator(KeyGenerator):
    """
    Generates deterministic keys using xxhash64 of natural keys.
    """
    def __init__(self, natural_keys: list[str]):
        self.natural_keys = natural_keys

    def generate_keys(self, df: DataFrame, key_col_name: str, existing_max_key: int = 0) -> DataFrame:
        # xxhash64 returns a long (64-bit integer)
        # We concat natural keys to form the seed
        # We use a separator to avoid collisions like ("a", "b") vs ("ab", "")
        
        # Handle nulls in natural keys by coalescing to empty string or sentinel
        # But xxhash64 handles inputs, so we can just pass columns.
        # However, to be safe and consistent with typical hash key practices:
        return df.withColumn(key_col_name, xxhash64(*[col(c) for c in self.natural_keys]))

class UniqueKeyGenerator(KeyGenerator):
    """
    Generates unique identifiers using monotonically_increasing_id.
    Produces unique values across partitions but may have gaps.
    Suitable for cases where uniqueness is required but sequentiality is not.
    """
    def generate_keys(self, df: DataFrame, key_col_name: str, existing_max_key: int = 0) -> DataFrame:
        # Use monotonically_increasing_id for distributed, parallel key generation
        # Adds offset for existing max key, may have gaps but scales across executors
        return df.withColumn(key_col_name, monotonically_increasing_id() + lit(existing_max_key + 1))

class SequenceKeyGenerator(KeyGenerator):
    """
    Generates sequential integers using row_number() over a window.
    Produces dense, sequential surrogate keys as expected in Kimball methodology.
    Note: Requires collecting data to a single partition, may not scale for very large datasets.
    """
    def generate_keys(self, df: DataFrame, key_col_name: str, existing_max_key: int = 0) -> DataFrame:
        # Use row_number for sequential generation
        # This requires sorting/ordering, so we use a dummy ordering
        window = Window.orderBy(lit(1))  # Dummy ordering since we just want sequential numbers
        return df.withColumn(key_col_name, row_number().over(window) + lit(existing_max_key))
