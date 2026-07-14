"""
Data Quality Validator - dbt-like data quality testing for Kimball Framework.

Provides post-merge data quality assertions:
- Unique constraint validation
- Not null validation
- Accepted values validation
- Referential integrity validation (FK checks)

Usage:
    from kimball.orchestration.validation import DataQualityValidator

    validator = DataQualityValidator()

    # Individual tests
    result = validator.validate_unique(df, ["customer_sk"])
    result = validator.validate_not_null(df, ["customer_id", "email"])
    result = validator.validate_accepted_values(df, "status", ["active", "inactive"])

    # Run all tests from config
    results = validator.run_config_tests(config, df)
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from kimball.common.errors import DataQualityError

try:
    from pyspark.sql import functions as F
except ImportError:
    F = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from kimball.config import TableConfig

logger = logging.getLogger(__name__)


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
    """Run data quality tests on DataFrames."""

    def __init__(self, spark: SparkSession | None = None):
        self._spark = spark

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            from databricks.sdk.runtime import spark

            self._spark = spark
        return self._spark

    def _dev_total_rows(self, df: DataFrame) -> int:
        is_dev_mode = os.environ.get("KIMBALL_ENABLE_DEV_CHECKS") == "1"
        return df.count() if is_dev_mode else -1

    def _count_bad_rows(self, bad_df: DataFrame) -> int:
        is_dev_mode = os.environ.get("KIMBALL_ENABLE_DEV_CHECKS") == "1"
        if is_dev_mode:
            return bad_df.count()
        return 0 if bad_df.limit(1).isEmpty() else -1

    def _sample_failures(
        self,
        bad_df: DataFrame,
        sample_size: int,
        sample_fn: Callable[[DataFrame, int], list[dict[str, Any]]] | None = None,
    ) -> list[dict[str, Any]]:
        if sample_size <= 0:
            return []
        fn = sample_fn or (lambda d, n: [row.asDict() for row in d.limit(n).collect()])
        return fn(bad_df, sample_size)

    def _build_test_result(
        self,
        df: DataFrame,
        bad_df: DataFrame,
        test_name: str,
        severity: TestSeverity,
        sample_size: int,
        details_fn: Callable[[int], str | None],
        sample_fn: Callable[[DataFrame, int], list[dict[str, Any]]] | None = None,
    ) -> TestResult:
        total_rows = self._dev_total_rows(df)
        bad_count = self._count_bad_rows(bad_df)
        samples = (
            self._sample_failures(bad_df, sample_size, sample_fn)
            if bad_count != 0
            else []
        )
        return TestResult(
            test_name=test_name,
            passed=bad_count == 0,
            failed_rows=bad_count,
            total_rows=total_rows,
            severity=severity,
            details=details_fn(bad_count),
            sample_failures=samples,
        )

    def _test_error(
        self, test_name: str, severity: TestSeverity, e: Exception
    ) -> TestResult:
        return TestResult(
            test_name=test_name,
            passed=False,
            severity=severity,
            details=f"Test error: {e}",
        )

    def validate_unique(
        self,
        df: DataFrame,
        columns: list[str],
        severity: TestSeverity = TestSeverity.ERROR,
        sample_size: int = 5,
    ) -> TestResult:
        test_name = f"unique({', '.join(columns)})"
        try:
            bad_df = (
                df.groupBy(*columns)
                .agg(F.count("*").alias("_dq_count"))
                .filter(F.col("_dq_count") > 1)
            )

            def details(count):
                if count > 0:
                    return f"Found {count} duplicate key combinations"
                if count == -1:
                    return "Found duplicate key combinations (count skipped for speed)"
                return None

            return self._build_test_result(
                df, bad_df, test_name, severity, sample_size, details
            )
        except Exception as e:
            return self._test_error(test_name, severity, e)

    def validate_not_null(
        self,
        df: DataFrame,
        columns: list[str],
        severity: TestSeverity = TestSeverity.ERROR,
        sample_size: int = 5,
    ) -> TestResult:
        test_name = f"not_null({', '.join(columns)})"
        try:
            null_condition = F.lit(False)
            for c in columns:
                null_condition = null_condition | F.col(c).isNull()
            bad_df = df.filter(null_condition)

            def details(count):
                if count > 0:
                    return f"Found {count} rows with NULL values"
                if count == -1:
                    return "Found rows with NULL values (count skipped for speed)"
                return None

            return self._build_test_result(
                df, bad_df, test_name, severity, sample_size, details
            )
        except Exception as e:
            return self._test_error(test_name, severity, e)

    def validate_accepted_values(
        self,
        df: DataFrame,
        column: str,
        values: list[Any],
        severity: TestSeverity = TestSeverity.ERROR,
        sample_size: int = 5,
    ) -> TestResult:
        test_name = f"accepted_values({column})"
        try:
            bad_df = df.filter(~F.col(column).isin(values))

            def details(count):
                if count > 0:
                    return f"Found {count} rows with values not in {values}"
                if count == -1:
                    return f"Found rows with values not in {values} (count skipped)"
                return None

            def sample_fn(d, n):
                return [
                    row.asDict()
                    for row in d.select(column).distinct().limit(n).collect()
                ]

            return self._build_test_result(
                df, bad_df, test_name, severity, sample_size, details, sample_fn
            )
        except Exception as e:
            return self._test_error(test_name, severity, e)

    def validate_relationships(
        self,
        df: DataFrame,
        fk_column: str,
        reference_table: str,
        reference_column: str,
        severity: TestSeverity = TestSeverity.ERROR,
        sample_size: int = 5,
    ) -> TestResult:
        test_name = (
            f"relationships({fk_column} -> {reference_table}.{reference_column})"
        )
        try:
            dim_df = self.spark.table(reference_table)
            if "__is_current" in dim_df.columns:
                dim_df = dim_df.filter(F.col("__is_current") == True)  # noqa: E712
            orphans = df.join(
                dim_df.select(F.col(reference_column).alias("_ref_col")),
                df[fk_column] == F.col("_ref_col"),
                "left_anti",
            ).filter(F.col(fk_column).isNotNull())

            def details(count):
                if count > 0:
                    return f"Found {count} orphan FK values not in {reference_table}"
                if count == -1:
                    return f"Found orphan FK values not in {reference_table} (count skipped)"
                return None

            def sample_fn(d, n):
                return [
                    row.asDict()
                    for row in d.select(fk_column).distinct().limit(n).collect()
                ]

            return self._build_test_result(
                df, orphans, test_name, severity, sample_size, details, sample_fn
            )
        except Exception as e:
            return self._test_error(test_name, severity, e)

    def validate_expression(
        self,
        df: DataFrame,
        expression: str,
        test_name: str | None = None,
        severity: TestSeverity = TestSeverity.ERROR,
        sample_size: int = 5,
    ) -> TestResult:
        test_name = test_name or f"expression({expression[:30]}...)"
        try:
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
                if re.search(rf"\b{pattern}\s", expr_lower):
                    return TestResult(
                        test_name=test_name,
                        passed=False,
                        severity=severity,
                        details=f"Expression contains forbidden SQL keyword: '{pattern}'. Only boolean filter expressions are allowed.",
                    )

            bad_df = df.filter(~F.expr(f"({expression})"))

            def details(count):
                if count > 0:
                    return f"Found {count} rows failing: {expression}"
                if count == -1:
                    return f"Found rows failing: {expression} (count skipped)"
                return None

            return self._build_test_result(
                df, bad_df, test_name, severity, sample_size, details
            )
        except Exception as e:
            return self._test_error(test_name, severity, e)

    def validate_unique_approximate(
        self,
        df: DataFrame,
        columns: list[str],
        severity: TestSeverity = TestSeverity.WARN,
        sample_size: int = 5,
    ) -> TestResult:
        """
        Probabilistic uniqueness check using HLL (HyperLogLog).

        O(n) time and memory instead of O(n log n) for exact groupBy.
        Returns WARN severity by default since it's probabilistic.
        Use for large datasets where exact uniqueness is too expensive.
        """
        test_name = f"unique_approx({', '.join(columns)})"
        try:
            stats = (
                df.select(*[F.col(c) for c in columns])
                .agg(
                    F.count("*").alias("_total"),
                    F.approx_count_distinct(*columns).alias("_distinct"),
                )
                .first()
            )
            total = int(stats["_total"]) if stats else 0
            distinct = int(stats["_distinct"]) if stats else 0
            if distinct < 0:
                return self._test_error(
                    test_name, severity, "approx_count_distinct failed"
                )
            ratio = distinct / total if total > 0 else 1.0
            passed = ratio >= 0.99
            details = (
                f"Approximate uniqueness: {distinct} distinct vs {total} total "
                f"(ratio={ratio:.4f}). HLL error ~1.5%."
            )
            return TestResult(
                test_name=test_name,
                passed=passed,
                failed_rows=0 if passed else -1,
                total_rows=total,
                severity=severity,
                details=details,
            )
        except Exception as e:
            return self._test_error(test_name, severity, e)

    def run_config_tests(
        self,
        config: TableConfig,
        df: DataFrame,
        use_approximate_unique: bool = False,
    ) -> ValidationReport:
        results: list[TestResult] = []
        if (
            config.table_type == "dimension"
            and config.surrogate_key
            and config.surrogate_key in df.columns
        ):
            results.append(
                self.validate_unique(
                    df, [config.surrogate_key], severity=TestSeverity.ERROR
                )
            )
        if config.natural_keys:
            nat_cols = [k for k in config.natural_keys if k in df.columns]
            if nat_cols:
                if use_approximate_unique and len(nat_cols) <= 1:
                    results.append(
                        self.validate_unique_approximate(
                            df, nat_cols, severity=TestSeverity.WARN
                        )
                    )
                else:
                    results.append(
                        self.validate_not_null(
                            df, nat_cols, severity=TestSeverity.ERROR
                        )
                    )
        if config.foreign_keys:
            for fk in config.foreign_keys:
                if fk.references and fk.column in df.columns:
                    ref_column = fk.dimension_key if fk.dimension_key else fk.column
                    results.append(
                        self.validate_relationships(
                            df,
                            fk_column=fk.column,
                            reference_table=fk.references,
                            reference_column=ref_column,
                            severity=TestSeverity.ERROR,
                        )
                    )
        if hasattr(config, "tests") and config.tests:
            for test_def in config.tests:
                results.extend(self._run_test_definition(df, test_def))
        return ValidationReport(results=results)

    def _run_test_definition(self, df: DataFrame, test_def: Any) -> list[TestResult]:
        results: list[TestResult] = []
        column = getattr(test_def, "column", None)
        tests = getattr(test_def, "tests", [])
        severity = (
            TestSeverity.WARN
            if getattr(test_def, "severity", "error") == "warn"
            else TestSeverity.ERROR
        )
        if not column or not tests or column not in df.columns:
            return results
        for test in tests:
            if isinstance(test, str):
                if test == "unique":
                    results.append(
                        self.validate_unique(df, [column], severity=severity)
                    )
                elif test == "not_null":
                    results.append(
                        self.validate_not_null(df, [column], severity=severity)
                    )
            elif isinstance(test, dict):
                if "accepted_values" in test:
                    results.append(
                        self.validate_accepted_values(
                            df, column, test["accepted_values"], severity=severity
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
                            severity=severity,
                        )
                    )
                elif "expression" in test:
                    results.append(
                        self.validate_expression(
                            df, test["expression"], severity=severity
                        )
                    )
        return results

    def validate_natural_key_uniqueness(
        self,
        df: DataFrame,
        natural_keys: list[str],
        table_name: str | None = None,
        severity: TestSeverity = TestSeverity.ERROR,
    ) -> TestResult:
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
                duplicates_check = (
                    df.groupBy(*natural_keys).count().filter(F.col("count") > 1)
                )
                duplicate_count = 0 if duplicates_check.limit(1).isEmpty() else -1
                total_rows = -1
            details = None
            sample_failures: list[dict[str, Any]] = []
            if duplicate_count != 0:
                duplicates = (
                    df.groupBy(*natural_keys)
                    .agg(F.count("*").alias("_dup_count"))
                    .filter(F.col("_dup_count") > 1)
                    .orderBy(F.col("_dup_count").desc())
                    .limit(5)
                )
                sample_failures = [row.asDict() for row in duplicates.collect()]
                details = f"CRITICAL: {duplicate_count if duplicate_count > 0 else 'Found'} duplicate natural keys found! Total rows: {total_rows if total_rows > 0 else 'Skipped'}, Distinct keys: {distinct_keys if is_dev_mode else 'Skipped'}. This will corrupt surrogate keys."
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
            return self._test_error(test_name, severity, e)

    def _check_single_fk(
        self,
        df: DataFrame,
        fk: dict[str, Any],
        exclude_seeds: bool,
        severity: TestSeverity,
    ) -> TestResult | None:
        fk_column = fk.get("column")
        dim_table = fk.get("dimension_table")
        dim_key = fk.get("dimension_key", fk_column)
        if not fk_column or not dim_table:
            return None
        test_name = f"fk_integrity({fk_column} -> {dim_table}.{dim_key})"
        try:
            raw_default = fk.get("default_value")
            accepted_defaults: set[Any] = set()
            if raw_default is not None:
                if isinstance(raw_default, (list, tuple, set)):
                    accepted_defaults.update(raw_default)
                else:
                    accepted_defaults.add(raw_default)
            if exclude_seeds:
                accepted_defaults.update({-1, -2, -3})
            fact_fks = df.select(fk_column).distinct()
            if accepted_defaults:
                from pyspark.sql.functions import col as _col

                defaults_literal = list(accepted_defaults)
                fact_fks = fact_fks.filter(~_col(fk_column).isin(defaults_literal))
            dim_df = self.spark.table(dim_table)
            if "__is_current" in dim_df.columns:
                dim_df = dim_df.filter(F.col("__is_current") == True)  # noqa: E712
            valid_sks = dim_df.select(dim_key).distinct()
            orphans = fact_fks.join(
                valid_sks, fact_fks[fk_column] == valid_sks[dim_key], "left_anti"
            )
            is_dev_mode = os.environ.get("KIMBALL_ENABLE_DEV_CHECKS") == "1"
            if is_dev_mode:
                orphans.count()
                fact_fks.count()

            def details(count):
                if count > 0:
                    return f"Found {count} FK values with no matching dimension SK"
                if count == -1:
                    return (
                        "Found FK values with no matching dimension SK (count skipped)"
                    )
                return None

            def sample_fn(d, n):
                return [row.asDict() for row in d.limit(n).collect()]

            return self._build_test_result(
                df, orphans, test_name, severity, 5, details, sample_fn
            )
        except Exception as e:
            return self._test_error(test_name, severity, e)

    def validate_fact_fk_integrity(
        self,
        df: DataFrame,
        foreign_keys: list[dict[str, Any]],
        exclude_seeds: bool = True,
        severity: TestSeverity = TestSeverity.ERROR,
    ) -> ValidationReport:
        results: list[TestResult] = []
        try:
            for fk in foreign_keys:
                result = self._check_single_fk(df, fk, exclude_seeds, severity)
                if result:
                    results.append(result)
        finally:
            pass
        return ValidationReport(results=results)
