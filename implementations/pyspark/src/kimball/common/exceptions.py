"""
Common exception handling for Kimball Framework.

FINDING-025: This module re-exports exceptions from errors.py to maintain
backward compatibility while eliminating duplicate class definitions.

Provides:
- Custom exception hierarchy (imported from errors.py)
- PySpark exception base class compatibility across Runtime versions
"""

from __future__ import annotations

from typing import Any

# Handle PySpark exception location changes between Runtime versions
# Databricks Runtime 13+ moved errors to pyspark.errors
PYSPARK_EXCEPTION_BASE: type[Exception]

try:
    from pyspark.errors import PySparkException

    PYSPARK_EXCEPTION_BASE = PySparkException
except ImportError:
    # Fallback for older Databricks Runtime versions
    import pyspark.sql.utils

    PYSPARK_EXCEPTION_BASE = pyspark.sql.utils.AnalysisException


# FINDING-025: Re-export exceptions from errors.py to avoid duplicate definitions
# This maintains backward compatibility for code that imports from exceptions.py
from kimball.common.errors import (
    KimballError,
    RetriableError,
    NonRetriableError,
    DataQualityError,
    ConfigurationError,
)

# Also export SchemaEvolutionError for backward compatibility
# (it was defined here but not in errors.py, so we create it here)


class SchemaEvolutionError(RetriableError):
    """Raised when schema evolution fails but might succeed on retry."""

    pass


__all__ = [
    "PYSPARK_EXCEPTION_BASE",
    "KimballError",
    "RetriableError",
    "NonRetriableError",
    "DataQualityError",
    "ConfigurationError",
    "SchemaEvolutionError",
]
