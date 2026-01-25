"""
Data Quality Validator - dbt-like data quality testing for Kimball Framework.

Provides post-merge data quality assertions:
- Unique constraint validation
- Not null validation
- Accepted values validation
- Referential integrity validation (FK checks)

Usage:
    from kimball.validation import DataQualityValidator

    validator = DataQualityValidator()

    # Individual tests
    result = validator.validate_unique(df, ["customer_sk"])
    result = validator.validate_not_null(df, ["customer_id", "email"])
    result = validator.validate_accepted_values(df, "status", ["active", "inactive"])

    # Run all tests from config
    results = validator.run_config_tests(config, df)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from kimball.common.errors import DataQualityError

# Import PySpark functions at module level for testability
# This allows tests to patch kimball.validation.F
try:
    from pyspark.sql import functions as F
except ImportError:
    F = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from kimball.config import TableConfig


class TestSeverity(str, Enum):
    """Severity level for data quality tests."""

    WARN = "warn"
    ERROR = "error"


@dataclass
class TestResult:
    """Result of a data quality test."""

    test_name: str
    passed: bool
    failed_rows: int = 0
    total_rows: int = 0
    severity: TestSeverity = TestSeverity.ERROR
    details: str | None = None
    sample_failures: list[dict[str, Any]] = field(default_factory=list)

    def __str__(self) -> str:
        icon = (
            "✅"
            if self.passed
            else ("⚠️" if self.severity == TestSeverity.WARN else "❌")
        )
        status = "PASSED" if self.passed else f"FAILED ({self.failed_rows} rows)"
        return f"{icon} {self.test_name}: {status}"


@dataclass
class ValidationReport:
    """Aggregated report of all validation tests."""

    results: list[TestResult]

    @property
    def passed(self) -> bool:
        """True if all ERROR-severity tests passed."""
        return all(r.passed or r.severity == TestSeverity.WARN for r in self.results)

    @property
    def all_passed(self) -> bool:
        """True if all tests (including warnings) passed."""
        return all(r.passed for r in self.results)

    @property
    def error_count(self) -> int:
        """Count of failed ERROR-severity tests."""
        return sum(
            1 for r in self.results if not r.passed and r.severity == TestSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        """Count of failed WARN-severity tests."""
        return sum(
            1 for r in self.results if not r.passed and r.severity == TestSeverity.WARN
        )

    def __str__(self) -> str:
        lines = ["Data Quality Validation Report", "=" * 40]
        for result in self.results:
            lines.append(str(result))
        lines.append("=" * 40)
        lines.append(
            f"Summary: {self.error_count} errors, {self.warning_count} warnings"
        )
        return "\n".join(lines)

    def raise_on_failure(self) -> None:
        """Raise DataQualityError if any ERROR-severity tests failed."""
        if not self.passed:
            failed = [
                r
                for r in self.results
                if not r.passed and r.severity == TestSeverity.ERROR
            ]
            messages = [f"{r.test_name}: {r.failed_rows} failures" for r in failed]
            raise DataQualityError(
                f"Data quality validation failed: {'; '.join(messages)}",
                details={"failed_tests": [r.test_name for r in failed]},
            )


class DataQualityValidator:
    """Run data quality tests on DataFrames.

    Provides dbt-like data quality assertions for the Kimball Framework.
    Tests can be run individually or automatically based on TableConfig.
    """

    def __init__(self, spark: SparkSession | None = None):
        """Initialize validator.

        Args:
            spark: SparkSession for table lookups. If None, will attempt
                   to get from databricks.sdk.runtime.
        """
        self._spark = spark

    @property
    def spark(self) -> SparkSession:
        """Lazy-load SparkSession."""
        if self._spark is None:
            from databricks.sdk.runtime import spark

            self._spark = spark
        return self._spark

    def validate_unique(
        self,
        df: DataFrame,
        columns: list[str],
        severity: TestSeverity = TestSeverity.ERROR,
        sample_size: int = 5,
    ) -> TestResult:
        """Validate that column(s) contain unique values.

        Args:
            df: DataFrame to validate.
            columns: Column(s) that should be unique together.
            severity: Test severity level.
            sample_size: Number of sample failures to include.

        Returns:
            TestResult with validation outcome.
        """
        test_name = f"unique({', '.join(columns)})"

        try:
            # Performance: Only compute total_rows in dev mode
            is_dev_mode = os.environ.get("KIMBALL_ENABLE_DEV_CHECKS") == "1"

            if is_dev_mode:
                total_rows = df.count()
            else:
                total_rows = -1  # Skip expensive count in production mode

            # Find duplicates: group by columns and count > 1
            duplicates = (
                df.groupBy(*columns)
                .agg(F.count("*").alias("_dq_count"))
                .filter(F.col("_dq_count") > 1)
            )

            if is_dev_mode:
                dup_count = duplicates.count()
            else:
                # Fast existence check (limit 1)
                # We save the shuffle of counting all duplicates
                if duplicates.limit(1).isEmpty():
                    dup_count = 0
                else:
                    dup_count = -1  # Flag as failed but count unknown

            # Get sample of duplicate values
            sample_failures: list[dict[str, Any]] = []
            if dup_count != 0 and sample_size > 0:
                samples = duplicates.limit(sample_size).collect()
                sample_failures = [row.asDict() for row in samples]

            # Construct failure details
            if dup_count > 0:
                details = f"Found {dup_count} duplicate key combinations"
            elif dup_count == -1:
                details = "Found duplicate key combinations (count skipped for speed)"
            else:
                details = None

            return TestResult(
                test_name=test_name,
                passed=dup_count == 0,
                failed_rows=dup_count,
                total_rows=total_rows,
                severity=severity,
                details=details,
                sample_failures=sample_failures,
            )
        except Exception as e:
            return TestResult(
                test_name=test_name,
                passed=False,
                severity=severity,
                details=f"Test error: {e}",
            )

    def validate_not_null(
        self,
        df: DataFrame,
        columns: list[str],
        severity: TestSeverity = TestSeverity.ERROR,
        sample_size: int = 5,
    ) -> TestResult:
        """Validate that column(s) do not contain NULL values.

        Args:
            df: DataFrame to validate.
            columns: Column(s) that should not be null.
            severity: Test severity level.
            sample_size: Number of sample failures to include.

        Returns:
            TestResult with validation outcome.
        """
        test_name = f"not_null({', '.join(columns)})"

        try:
            # Performance: Only compute total_rows in dev mode
            is_dev_mode = os.environ.get("KIMBALL_ENABLE_DEV_CHECKS") == "1"

            if is_dev_mode:
                total_rows = df.count()
            else:
                total_rows = -1  # Skip expensive count in production mode

            # Build filter for any null in specified columns
            null_condition = F.lit(False)
            for col in columns:
                null_condition = null_condition | F.col(col).isNull()

            null_rows = df.filter(null_condition)

            if is_dev_mode:
                null_count = null_rows.count()
            else:
                # Fast existence check
                if null_rows.limit(1).isEmpty():
                    null_count = 0
                else:
                    null_count = -1

            # Get sample of null rows
            sample_failures: list[dict[str, Any]] = []
            if null_count != 0 and sample_size > 0:
                samples = null_rows.select(*columns).limit(sample_size).collect()
                sample_failures = [row.asDict() for row in samples]

            return TestResult(
                test_name=test_name,
                passed=null_count == 0,
                failed_rows=null_count,
                total_rows=total_rows,
                severity=severity,
                details=f"Found {null_count} rows with NULL values"
                if null_count > 0
                else (
                    "Found rows with NULL values (count skipped for speed)"
                    if null_count == -1
                    else None
                ),
                sample_failures=sample_failures,
            )
        except Exception as e:
            return TestResult(
                test_name=test_name,
                passed=False,
                severity=severity,
                details=f"Test error: {e}",
            )

    def validate_accepted_values(
        self,
        df: DataFrame,
        column: str,
        values: list[Any],
        severity: TestSeverity = TestSeverity.ERROR,
        sample_size: int = 5,
    ) -> TestResult:
        """Validate that column contains only accepted values.

        Args:
            df: DataFrame to validate.
            column: Column to check.
            values: List of accepted values.
            severity: Test severity level.
            sample_size: Number of sample failures to include.

        Returns:
            TestResult with validation outcome.
        """
        test_name = f"accepted_values({column})"

        try:
            # Performance: Only compute total_rows in dev mode
            is_dev_mode = os.environ.get("KIMBALL_ENABLE_DEV_CHECKS") == "1"

            if is_dev_mode:
                total_rows = df.count()
            else:
                total_rows = -1  # Skip expensive count in production mode

            # Find rows with values not in accepted list
            invalid_rows = df.filter(~F.col(column).isin(values))

            if is_dev_mode:
                invalid_count = invalid_rows.count()
            else:
                if invalid_rows.limit(1).isEmpty():
                    invalid_count = 0
                else:
                    invalid_count = -1

            # Get sample of invalid values
            sample_failures: list[dict[str, Any]] = []
            if invalid_count != 0 and sample_size > 0:
                # Get distinct invalid values
                samples = (
                    invalid_rows.select(column).distinct().limit(sample_size).collect()
                )
                sample_failures = [row.asDict() for row in samples]

            return TestResult(
                test_name=test_name,
                passed=invalid_count == 0,
                failed_rows=invalid_count,
                total_rows=total_rows,
                severity=severity,
                details=f"Found {invalid_count} rows with values not in {values}"
                if invalid_count > 0
                else (
                    f"Found rows with values not in {values} (count skipped)"
                    if invalid_count == -1
                    else None
                ),
                sample_failures=sample_failures,
            )
        except Exception as e:
            return TestResult(
                test_name=test_name,
                passed=False,
                severity=severity,
                details=f"Test error: {e}",
            )

    def validate_relationships(
        self,
        df: DataFrame,
        fk_column: str,
        reference_table: str,
        reference_column: str,
        severity: TestSeverity = TestSeverity.ERROR,
        sample_size: int = 5,
    ) -> TestResult:
        """Validate referential integrity (FK references exist in dimension).

        Args:
            df: Fact DataFrame to validate.
            fk_column: Foreign key column in the fact table.
            reference_table: Dimension table name.
            reference_column: Column in dimension table that FK references.
            severity: Test severity level.
            sample_size: Number of sample failures to include.

        Returns:
            TestResult with validation outcome.
        """
        test_name = (
            f"relationships({fk_column} -> {reference_table}.{reference_column})"
        )

        try:
            # Performance: Only compute total_rows in dev mode
            is_dev_mode = os.environ.get("KIMBALL_ENABLE_DEV_CHECKS") == "1"

            if is_dev_mode:
                total_rows = df.count()
            else:
                total_rows = -1  # Skip expensive count in production mode

            # Load dimension table
            dim_df = self.spark.table(reference_table)

            # Find orphan FKs using LEFT ANTI JOIN
            # These are FK values that don't exist in the dimension
            orphans = df.join(
                dim_df.select(F.col(reference_column).alias("_ref_col")),
                df[fk_column] == F.col("_ref_col"),
                "left_anti",
            )

            # Exclude nulls (nulls are handled by not_null test)
            orphans = orphans.filter(F.col(fk_column).isNotNull())

            if is_dev_mode:
                orphan_count = orphans.count()
            else:
                if orphans.limit(1).isEmpty():
                    orphan_count = 0
                else:
                    orphan_count = -1

            # Get sample of orphan FK values
            sample_failures: list[dict[str, Any]] = []
            if orphan_count != 0 and sample_size > 0:
                samples = (
                    orphans.select(fk_column).distinct().limit(sample_size).collect()
                )
                sample_failures = [row.asDict() for row in samples]

            return TestResult(
                test_name=test_name,
                passed=orphan_count == 0,
                failed_rows=orphan_count,
                total_rows=total_rows,
                severity=severity,
                details=f"Found {orphan_count} orphan FK values not in {reference_table}"
                if orphan_count > 0
                else (
                    f"Found orphan FK values not in {reference_table} (count skipped)"
                    if orphan_count == -1
                    else None
                ),
                sample_failures=sample_failures,
            )
        except Exception as e:
            return TestResult(
                test_name=test_name,
                passed=False,
                severity=severity,
                details=f"Test error: {e}",
            )

    def validate_expression(
        self,
        df: DataFrame,
        expression: str,
        test_name: str | None = None,
        severity: TestSeverity = TestSeverity.ERROR,
        sample_size: int = 5,
    ) -> TestResult:
        """Validate a custom SQL expression.

        The expression should evaluate to true for valid rows.

        FINDING-027: Validates expression for forbidden SQL patterns to prevent injection.

        Args:
            df: DataFrame to validate.
            expression: SQL expression that should be true for all rows.
            test_name: Optional name for the test.
            severity: Test severity level.
            sample_size: Number of sample failures to include.

        Returns:
            TestResult with validation outcome.
        """
        test_name = test_name or f"expression({expression[:30]}...)"

        try:
            # FINDING-027: Validate expression for forbidden SQL patterns
            forbidden_patterns = [
                "select",
                "insert",
                "update",
                "delete",
                "drop",
                "create",
                "alter",
                "truncate",
                "grant",
                "revoke",
                "exec",
                "execute",
            ]
            expr_lower = expression.lower()
            for pattern in forbidden_patterns:
                # Check for pattern as a word boundary (not part of column name)
                import re

                if re.search(rf"\b{pattern}\b", expr_lower):
                    return TestResult(
                        test_name=test_name,
                        passed=False,
                        severity=severity,
                        details=f"Expression contains forbidden SQL keyword: '{pattern}'. "
                        f"Only boolean filter expressions are allowed.",
                    )

            # Performance: Only compute total_rows in dev mode
            is_dev_mode = os.environ.get("KIMBALL_ENABLE_DEV_CHECKS") == "1"

            if is_dev_mode:
                total_rows = df.count()
            else:
                total_rows = -1  # Skip expensive count in production mode

            # Find rows where expression is false - wrap in parens for safety
            invalid_rows = df.filter(~F.expr(f"({expression})"))

            if is_dev_mode:
                invalid_count = invalid_rows.count()
            else:
                if invalid_rows.limit(1).isEmpty():
                    invalid_count = 0
                else:
                    invalid_count = -1

            # Get sample of failing rows
            sample_failures: list[dict[str, Any]] = []
            if invalid_count != 0 and sample_size > 0:
                samples = invalid_rows.limit(sample_size).collect()
                sample_failures = [row.asDict() for row in samples]

            return TestResult(
                test_name=test_name,
                passed=invalid_count == 0,
                failed_rows=invalid_count,
                total_rows=total_rows,
                severity=severity,
                details=f"Found {invalid_count} rows failing: {expression}"
                if invalid_count > 0
                else (
                    f"Found rows failing: {expression} (count skipped)"
                    if invalid_count == -1
                    else None
                ),
                sample_failures=sample_failures,
            )
        except Exception as e:
            return TestResult(
                test_name=test_name,
                passed=False,
                severity=severity,
                details=f"Test error: {e}",
            )

    def run_config_tests(
        self,
        config: TableConfig,
        df: DataFrame,
    ) -> ValidationReport:
        """Run all tests defined in a TableConfig.

        Automatically runs:
        - Unique test on surrogate_key (for dimensions)
        - Not null test on natural_keys
        - Relationship tests based on foreign_keys
        - Any custom tests defined in config.tests

        Args:
            config: TableConfig with test definitions.
            df: DataFrame to validate.

        Returns:
            ValidationReport with all test results.
        """
        results: list[TestResult] = []

        # Auto-test: Dimension surrogate key should be unique
        if config.table_type == "dimension" and config.surrogate_key:
            results.append(self.validate_unique(df, [config.surrogate_key]))

        # Auto-test: Natural keys should not be null
        if config.natural_keys:
            results.append(self.validate_not_null(df, config.natural_keys))

        # Auto-test: Foreign key relationships
        # FINDING-026: Use dimension_key if specified, otherwise fall back to column name
        if config.foreign_keys:
            for fk in config.foreign_keys:
                if fk.references:
                    # Use explicit dimension_key if provided, otherwise assume same name
                    ref_column = (
                        fk.dimension_key if fk.dimension_key else fk.column
                    )
                    results.append(
                        self.validate_relationships(
                            df,
                            fk_column=fk.column,
                            reference_table=fk.references,
                            reference_column=ref_column,
                        )
                    )

        # Custom tests from config.tests (if present)
        if hasattr(config, "tests") and config.tests:
            for test_def in config.tests:
                results.extend(self._run_test_definition(df, test_def))

        return ValidationReport(results=results)

    def _run_test_definition(self, df: DataFrame, test_def: Any) -> list[TestResult]:
        """Run a single test definition from config.

        Handles both string tests ("unique", "not_null") and dict tests
        ({"accepted_values": [1, 2, 3]}).
        """
        results: list[TestResult] = []

        # test_def should have 'column' and 'tests' attributes
        column = getattr(test_def, "column", None)
        tests = getattr(test_def, "tests", [])

        if not column or not tests:
            return results

        for test in tests:
            if isinstance(test, str):
                # Simple test: "unique", "not_null"
                if test == "unique":
                    results.append(self.validate_unique(df, [column]))
                elif test == "not_null":
                    results.append(self.validate_not_null(df, [column]))
            elif isinstance(test, dict):
                # Parameterized test
                if "accepted_values" in test:
                    results.append(
                        self.validate_accepted_values(
                            df, column, test["accepted_values"]
                        )
                    )
                elif "relationships" in test:
                    rel = test["relationships"]
                    results.append(
                        self.validate_relationships(
                            df,
                            fk_column=column,
                            reference_table=rel.get("to", ""),
                            reference_column=rel.get("field", column),
                        )
                    )
                elif "expression" in test:
                    results.append(self.validate_expression(df, test["expression"]))

        return results

    def validate_natural_key_uniqueness(
        self,
        df: DataFrame,
        natural_keys: list[str],
        table_name: str | None = None,
        severity: TestSeverity = TestSeverity.ERROR,
    ) -> TestResult:
        """Validate that natural keys are unique in source data (pre-load gate).

        This is a critical Kimball integrity check that prevents SK corruption.
        For SCD1: count(distinct NK) must equal row count
        For SCD2: count(distinct NK) must equal row count (per batch)

        Args:
            df: Source DataFrame to validate BEFORE loading.
            natural_keys: Natural key columns that must be unique together.
            table_name: Optional table name for clearer error messages.
            severity: Test severity level.

        Returns:
            TestResult with validation outcome.
        """
        test_name = f"natural_key_uniqueness({', '.join(natural_keys)})"
        if table_name:
            test_name = f"{table_name}: {test_name}"

        try:
            is_dev_mode = os.environ.get("KIMBALL_ENABLE_DEV_CHECKS") == "1"
            if is_dev_mode:
                total_rows = df.count()
                distinct_keys = df.select(*natural_keys).distinct().count()
                duplicate_count = total_rows - distinct_keys
            else:
                # Fast path: Check for duplicates by grouping
                duplicates_check = (
                    df.groupBy(*natural_keys).count().filter(F.col("count") > 1)
                )
                if duplicates_check.limit(1).isEmpty():
                    duplicate_count = 0
                else:
                    duplicate_count = -1
                total_rows = -1  # Unknown in fast mode

            details = None
            sample_failures: list[dict[str, Any]] = []

            if duplicate_count != 0:
                # Find the actual duplicates for debugging
                duplicates = (
                    df.groupBy(*natural_keys)
                    .agg(F.count("*").alias("_dup_count"))
                    .filter(F.col("_dup_count") > 1)
                    .orderBy(F.col("_dup_count").desc())
                    .limit(5)
                )
                sample_failures = [row.asDict() for row in duplicates.collect()]
                details = (
                    f"CRITICAL: {duplicate_count if duplicate_count > 0 else 'Found'} duplicate natural keys found! "
                    f"Total rows: {total_rows if total_rows > 0 else 'Skipped'}, Distinct keys: {distinct_keys if is_dev_mode else 'Skipped'}. "
                    f"This will corrupt surrogate keys."
                )

            return TestResult(
                test_name=test_name,
                passed=duplicate_count == 0,
                failed_rows=duplicate_count,
                total_rows=total_rows,
                severity=severity,
                details=details,
                sample_failures=sample_failures,
            )
        except Exception as e:
            return TestResult(
                test_name=test_name,
                passed=False,
                severity=severity,
                details=f"Test error: {e}",
            )

    def validate_fact_fk_integrity(
        self,
        df: DataFrame,
        foreign_keys: list[dict[str, Any]],
        exclude_seeds: bool = True,
        severity: TestSeverity = TestSeverity.ERROR,
    ) -> ValidationReport:
        """Validate all FK columns in a fact table reference valid dimension SKs.

        This is a critical integrity check for fact tables. Each FK column
        is validated separately so you know WHICH dimension has the problem.

        Args:
            df: Fact DataFrame to validate BEFORE loading.
            foreign_keys: List of FK definitions, each with:
                - column: FK column name in the fact table
                - dimension_table: Target dimension table name
                - dimension_key: SK column in the dimension (default: same as column)
            exclude_seeds: If True, excludes seed values (-1, -2, -3) from validation.
            severity: Test severity level.

        Returns:
            ValidationReport with per-dimension validation results.

        Example:
            foreign_keys = [
                {"column": "customer_sk", "dimension_table": "gold.dim_customer"},
                {"column": "product_sk", "dimension_table": "gold.dim_product"},
            ]
            report = validator.validate_fact_fk_integrity(fact_df, foreign_keys)
            report.raise_on_failure()  # Fails with "customer_sk: 15 orphan FKs"
        """
        results: list[TestResult] = []

        # Cache removed for Serverless compatibility (PERSIST TABLE not supported)
        # df_cached = df.cache()

        try:
            for fk in foreign_keys:
                fk_column = fk.get("column")
                dim_table = fk.get("dimension_table")
                dim_key = fk.get("dimension_key", fk_column)

                if not fk_column or not dim_table:
                    continue

                test_name = f"fk_integrity({fk_column} -> {dim_table}.{dim_key})"

                try:
                    # Get distinct FK values from fact (using df directly)
                    fact_fks = df.select(fk_column).distinct()
                    if exclude_seeds:
                        fact_fks = fact_fks.filter(F.col(fk_column) > 0)

                    # Get valid SKs from dimension (current rows only for SCD2)
                    dim_df = self.spark.table(dim_table)
                    if "__is_current" in dim_df.columns:
                        dim_df = dim_df.filter(F.col("__is_current") == True)  # noqa: E712
                    if exclude_seeds:
                        dim_df = dim_df.filter(F.col(dim_key) > 0)

                    valid_sks = dim_df.select(dim_key).distinct()

                    # Left anti-join to find orphan FKs
                    orphans = fact_fks.join(
                        valid_sks,
                        fact_fks[fk_column] == valid_sks[dim_key],
                        "left_anti",
                    )

                    # Performance: Use lightweight existence check unless in DEV/Strict mode
                    is_dev_mode = os.environ.get("KIMBALL_ENABLE_DEV_CHECKS") == "1"

                    if is_dev_mode:
                        orphan_count = orphans.count()
                        fk_total = fact_fks.count()
                    else:
                        if orphans.limit(1).isEmpty():
                            orphan_count = 0
                        else:
                            orphan_count = -1
                        fk_total = -1  # Skip count

                    sample_failures: list[dict[str, Any]] = []
                    if orphan_count != 0:
                        samples = orphans.limit(5).collect()
                        sample_failures = [row.asDict() for row in samples]

                    results.append(
                        TestResult(
                            test_name=test_name,
                            passed=orphan_count == 0,
                            failed_rows=orphan_count,
                            total_rows=fk_total,
                            severity=severity,
                            details=f"Found {orphan_count} FK values with no matching dimension SK"
                            if orphan_count > 0
                            else (
                                "Found FK values with no matching dimension SK (count skipped)"
                                if orphan_count == -1
                                else None
                            ),
                            sample_failures=sample_failures,
                        )
                    )
                except Exception as e:
                    results.append(
                        TestResult(
                            test_name=test_name,
                            passed=False,
                            severity=severity,
                            details=f"Test error: {e}",
                        )
                    )
        finally:
            pass  # No unpersist needed

        return ValidationReport(results=results)
