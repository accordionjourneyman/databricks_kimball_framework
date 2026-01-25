"""Lazy SparkSession accessor for Databricks/local compatibility.

This module provides a function to get the SparkSession lazily,
allowing the Kimball framework to be imported outside of Databricks
(e.g., for testing, type checking, or documentation generation).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Get SparkSession lazily.

    In Databricks: Returns the runtime-provided spark session.
    Outside Databricks: Falls back to SparkSession.builder.getOrCreate().

    Returns:
        Active SparkSession instance.

    Raises:
        RuntimeError: If no SparkSession can be obtained.
    """
    try:
        # Try Databricks runtime first
        from databricks.sdk.runtime import spark

        return spark
    except ImportError:
        # Fallback for non-Databricks environments
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()
    except Exception as e:
        # Databricks SDK installed but not in Databricks runtime
        try:
            from pyspark.sql import SparkSession

            return SparkSession.builder.getOrCreate()
        except ImportError:
            raise RuntimeError(
                "No SparkSession available. Either run in Databricks or install pyspark."
            ) from e
