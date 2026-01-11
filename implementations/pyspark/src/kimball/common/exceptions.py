"""
Common exception handling for Kimball Framework.

Provides:
- Custom exception hierarchy
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


class KimballError(Exception):
    """Base exception for all Kimball Framework errors."""

    pass


class RetriableError(KimballError):
    """
    Exception for errors that can be retried (e.g., transient network issues).
    The Orchestrator can catch these and retry the operation.
    """

    pass


class NonRetriableError(KimballError):
    """
    Exception for errors that should NOT be retried (e.g., data quality failures).
    These indicate a fundamental problem that requires intervention.
    """

    pass


class DataQualityError(NonRetriableError):
    """
    Raised when data quality validation fails.
    Contains details about which tests failed and sample data.
    """

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.details = details or {}


class ConfigurationError(NonRetriableError):
    """Raised when configuration is invalid or missing."""

    pass


class SchemaEvolutionError(RetriableError):
    """Raised when schema evolution fails but might succeed on retry."""

    pass
