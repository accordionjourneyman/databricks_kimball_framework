"""
Kimball Framework Error Classification

Errors are divided into two categories:
- RetriableError: Transient issues that may succeed on retry (environment issues)
- NonRetriableError: Permanent issues that won't succeed on retry (code/data issues)

The principle is: Retry is the exception, not the norm.
"""


class KimballError(Exception):
    """Base exception for all Kimball framework errors."""

    retriable = False

    def __init__(self, message: str, details: dict[str, str] | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


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


class MissingDependencyError(NonRetriableError):
    """Required dimension table doesn't exist.

    For fact tables that reference dimensions which haven't been loaded yet.
    This indicates a pipeline ordering issue.
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


# Backward compatibility alias
WatermarkNotFoundError = ETLControlNotFoundError


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def is_retriable(error: Exception) -> bool:
    """Check if an error is retriable."""
    if isinstance(error, KimballError):
        return error.retriable

    # Check for known Spark/Delta exceptions that are retriable
    error_msg = str(error).lower()
    retriable_patterns = [
        "concurrent",
        "transaction conflict",
        "optimistic concurrency",
        "executor lost",
        "shuffle",
        "timeout",
        "connection reset",
    ]
    return any(pattern in error_msg for pattern in retriable_patterns)


def wrap_exception(original: Exception) -> KimballError:
    """Wrap a standard exception in the appropriate KimballError type."""
    error_msg = str(original).lower()

    # Try to classify based on error message patterns
    if "concurrent" in error_msg or "transaction conflict" in error_msg:
        return DeltaConcurrentModificationError(
            f"Delta concurrency conflict: {original}", {"original_error": str(original)}
        )

    if "executor lost" in error_msg or "shuffle" in error_msg:
        return TransientSparkError(
            f"Spark transient error: {original}", {"original_error": str(original)}
        )

    if "schema" in error_msg or "type mismatch" in error_msg:
        return SchemaMismatchError(
            f"Schema issue: {original}", {"original_error": str(original)}
        )

    if "cannot resolve" in error_msg or "syntax" in error_msg:
        return TransformationSQLError(
            f"SQL error: {original}", {"original_error": str(original)}
        )

    if "table or view not found" in error_msg:
        return MissingDependencyError(
            f"Missing table: {original}", {"original_error": str(original)}
        )

    # Default to non-retriable
    return NonRetriableError(
        f"Unknown error: {original}", {"original_error": str(original)}
    )
