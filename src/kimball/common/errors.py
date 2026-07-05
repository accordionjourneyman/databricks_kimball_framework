"""Kimball Framework Error Classification

Errors are divided into two categories:
- RetriableError: Transient issues that may succeed on retry (environment issues)
- NonRetriableError: Permanent issues that won't succeed on retry (code/data issues)

The principle is: Retry is the exception, not the norm.
"""

from collections.abc import Mapping
from typing import Any


class KimballError(Exception):
    """Base exception for all Kimball framework errors."""

    retriable = False

    def __init__(self, message: str, details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.details: Mapping[str, Any] = details or {}


# =============================================================================
# RETRIABLE ERRORS - May succeed on retry (transient environment issues)
# =============================================================================


class RetriableError(KimballError):
    """Errors that may succeed on retry (transient environment issues)."""

    retriable = True


class SourceTableBusyError(RetriableError):
    """Source table is being updated (OPTIMIZE, MERGE, VACUUM in progress).

    This happens when trying to read from a table that's currently being
    modified by another process. Usually resolves in seconds/minutes.
    """

    pass


class DeltaConcurrentModificationError(RetriableError):
    """Concurrent write conflict on target table.

    Another process is writing to the same table. Delta's optimistic
    concurrency control will retry automatically, but in some cases
    manual retry is needed.
    """

    pass


class TransientSparkError(RetriableError):
    """Transient Spark cluster issues (executor lost, shuffle failure, etc).

    These are infrastructure issues that usually resolve with a retry.
    """

    pass


class ETLControlConflictError(RetriableError):
    """Concurrent ETL control table update conflict.

    Another instance is updating the same control table entry.
    Should be rare with partitioned ETL control tables.
    """

    pass


# =============================================================================
# NON-RETRIABLE ERRORS - Will NOT succeed on retry (code/data issues)
# =============================================================================


class NonRetriableError(KimballError):
    """Errors that will NOT succeed on retry (code/data issues)."""

    retriable = False


class ConfigurationError(NonRetriableError):
    """Invalid YAML configuration.

    Examples:
    - Missing required fields (surrogate_key for dimensions)
    - Invalid table_type
    - Malformed YAML syntax
    """

    pass


class TransformationSQLError(NonRetriableError):
    """Bad SQL in transformation_sql.

    Examples:
    - Syntax error in SQL
    - Reference to non-existent column
    - Invalid function usage
    """

    pass


class SchemaMismatchError(NonRetriableError):
    """Source or target schema doesn't match expected.

    Examples:
    - Column type changed in source
    - Required column missing
    - Target table has incompatible schema
    """

    pass


class DataQualityError(NonRetriableError):
    """Data quality check failed.

    Examples:
    - NULL values in required columns
    - Duplicate natural keys
    - Referential integrity violation
    """

    pass


class ETLControlNotFoundError(NonRetriableError):
    """ETL control table or entry not accessible.

    Usually indicates a permissions issue or missing table.
    """

    pass
