"""Common exception handling for Kimball Framework.

Provides:
- Custom exception hierarchy (imported from errors.py)
- PySpark exception base class compatibility across Runtime versions

The PYSPARK_EXCEPTION_BASE is determined at import time via feature detection,
providing a stable base for catching PySpark exceptions regardless of the
Databricks Runtime version.
"""

from __future__ import annotations

# Handle PySpark exception location changes between Runtime versions
# Databricks Runtime 13+ moved errors to pyspark.errors
# We use Exception as fallback to ensure the module is always importable
PYSPARK_EXCEPTION_BASE: type[Exception]

try:
    # Databricks Runtime 13+ / PySpark 3.4+
    from pyspark.errors import PySparkException

    PYSPARK_EXCEPTION_BASE = PySparkException
except ImportError:
    try:
        # Fallback for older Databricks Runtime versions (11.x, 12.x)
        from pyspark.sql.utils import AnalysisException

        PYSPARK_EXCEPTION_BASE = AnalysisException
    except ImportError:
        # If PySpark isn't installed at all (e.g., during type checking),
        # use Exception as the base class - this is stable
        PYSPARK_EXCEPTION_BASE = Exception


# Re-export exceptions from errors.py (single canonical location)
from kimball.common.errors import (  # noqa: E402
    ConfigurationError,
    DataQualityError,
    KimballError,
    NonRetriableError,
    RetriableError,
    SchemaEvolutionError,
)

__all__ = [
    "PYSPARK_EXCEPTION_BASE",
    "KimballError",
    "RetriableError",
    "NonRetriableError",
    "DataQualityError",
    "ConfigurationError",
    "SchemaEvolutionError",
]
