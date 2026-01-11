"""Unit tests for DataQualityValidator.

Tests cover:
- Unique constraint validation
- Not null validation
- Accepted values validation
- Referential integrity validation
- Expression validation
- ValidationReport aggregation

Note: These tests use mocked DataFrames to avoid requiring a real Spark session.
The validation logic is tested through the mock return values.
"""

from unittest.mock import MagicMock, patch

import pytest

from kimball.validation import (
    DataQualityValidator,
    TestResult,
    TestSeverity,
    ValidationReport,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_spark():
    """Create a mock SparkSession."""
    spark = MagicMock()
    return spark


@pytest.fixture
def validator(mock_spark):
    """Create a DataQualityValidator with mocked SparkSession."""
    return DataQualityValidator(spark=mock_spark)


# =============================================================================
# Test: Unique Validation
# =============================================================================


@patch("kimball.validation.F")
def test_validate_unique_pass(mock_F, validator):
    """Test unique validation passes when no duplicates exist."""
    # Setup mock DataFrame
    df = MagicMock()
    df.count.return_value = 100

    # Mock the chain: groupBy().agg().filter()
    grouped = MagicMock()
    agged = MagicMock()
    filtered = MagicMock()

    df.groupBy.return_value = grouped
    grouped.agg.return_value = agged
    agged.filter.return_value = filtered
    filtered.count.return_value = 0
    filtered.limit.return_value.collect.return_value = []

    # Configure F.col() to return a mock that supports > comparison
    mock_col = MagicMock()
    mock_col.__gt__ = MagicMock(return_value=MagicMock())
    mock_F.col.return_value = mock_col
    mock_F.count.return_value = MagicMock()

    result = validator.validate_unique(df, ["id"])

    assert result.passed is True
    assert result.failed_rows == 0
    assert result.total_rows == 100
    assert "unique" in result.test_name


@patch("kimball.validation.F")
def test_validate_unique_fail(mock_F, validator):
    """Test unique validation fails when duplicates exist."""
    df = MagicMock()
    df.count.return_value = 100

    grouped = MagicMock()
    agged = MagicMock()
    filtered = MagicMock()

    df.groupBy.return_value = grouped
    grouped.agg.return_value = agged
    agged.filter.return_value = filtered
    filtered.count.return_value = 3

    # Mock sample rows
    mock_row = MagicMock()
    mock_row.asDict.return_value = {"id": 1, "_dq_count": 2}
    filtered.limit.return_value.collect.return_value = [mock_row]

    # Configure F.col() to return a mock that supports > comparison
    mock_col = MagicMock()
    mock_col.__gt__ = MagicMock(return_value=MagicMock())
    mock_F.col.return_value = mock_col
    mock_F.count.return_value = MagicMock()

    result = validator.validate_unique(df, ["id"])

    assert result.passed is False
    assert result.failed_rows == 3
    assert "duplicate" in result.details.lower()


@patch("kimball.validation.F")
def test_validate_unique_multiple_columns(mock_F, validator):
    """Test unique validation with composite key."""
    df = MagicMock()
    df.count.return_value = 100

    grouped = MagicMock()
    agged = MagicMock()
    filtered = MagicMock()

    df.groupBy.return_value = grouped
    grouped.agg.return_value = agged
    agged.filter.return_value = filtered
    filtered.count.return_value = 0
    filtered.limit.return_value.collect.return_value = []

    # Configure F.col() to return a mock that supports > comparison
    mock_col = MagicMock()
    mock_col.__gt__ = MagicMock(return_value=MagicMock())
    mock_F.col.return_value = mock_col
    mock_F.count.return_value = MagicMock()

    result = validator.validate_unique(df, ["id", "version"])

    assert result.passed is True
    assert "id, version" in result.test_name


# =============================================================================
# Test: Not Null Validation
# =============================================================================


@patch("kimball.validation.F")
def test_validate_not_null_pass(mock_F, validator):
    """Test not_null validation passes when no nulls exist."""
    df = MagicMock()
    df.count.return_value = 100

    filtered = MagicMock()
    df.filter.return_value = filtered
    filtered.count.return_value = 0
    filtered.select.return_value.limit.return_value.collect.return_value = []

    result = validator.validate_not_null(df, ["col1"])

    assert result.passed is True
    assert result.failed_rows == 0


@patch("kimball.validation.F")
def test_validate_not_null_fail(mock_F, validator):
    """Test not_null validation fails when nulls exist."""
    df = MagicMock()
    df.count.return_value = 100

    filtered = MagicMock()
    df.filter.return_value = filtered
    filtered.count.return_value = 5

    mock_row = MagicMock()
    mock_row.asDict.return_value = {"col1": None}
    filtered.select.return_value.limit.return_value.collect.return_value = [mock_row]

    result = validator.validate_not_null(df, ["col1"])

    assert result.passed is False
    assert result.failed_rows == 5
    assert "NULL" in result.details


@patch("kimball.validation.F")
def test_validate_not_null_multiple_columns(mock_F, validator):
    """Test not_null validation with multiple columns."""
    df = MagicMock()
    df.count.return_value = 100

    filtered = MagicMock()
    df.filter.return_value = filtered
    filtered.count.return_value = 0
    filtered.select.return_value.limit.return_value.collect.return_value = []

    result = validator.validate_not_null(df, ["col1", "col2", "col3"])

    assert result.passed is True
    assert "col1, col2, col3" in result.test_name


# =============================================================================
# Test: Accepted Values Validation
# =============================================================================


@patch("kimball.validation.F")
def test_validate_accepted_values_pass(mock_F, validator):
    """Test accepted_values validation passes for valid values."""
    df = MagicMock()
    df.count.return_value = 100

    # filter() returns empty (all values valid)
    filtered = MagicMock()
    df.filter.return_value = filtered
    filtered.count.return_value = 0
    filtered.select.return_value.distinct.return_value.limit.return_value.collect.return_value = []

    result = validator.validate_accepted_values(df, "status", ["active", "inactive"])

    assert result.passed is True
    assert result.failed_rows == 0


@patch("kimball.validation.F")
def test_validate_accepted_values_fail(mock_F, validator):
    """Test accepted_values validation fails for invalid values."""
    df = MagicMock()
    df.count.return_value = 100

    # filter() returns 10 invalid rows
    filtered = MagicMock()
    df.filter.return_value = filtered
    filtered.count.return_value = 10

    mock_row = MagicMock()
    mock_row.asDict.return_value = {"status": "unknown"}
    filtered.select.return_value.distinct.return_value.limit.return_value.collect.return_value = [
        mock_row
    ]

    result = validator.validate_accepted_values(df, "status", ["active", "inactive"])

    assert result.passed is False
    assert result.failed_rows == 10
    assert "not in" in result.details


# =============================================================================
# Test: Relationships Validation
# =============================================================================


@patch("kimball.validation.F")
def test_validate_relationships_pass(mock_F, validator, mock_spark):
    """Test relationships validation passes when all FKs exist."""
    fact_df = MagicMock()
    fact_df.count.return_value = 1000

    # Mock dimension table
    dim_df = MagicMock()
    mock_spark.table.return_value = dim_df

    # LEFT ANTI JOIN returns empty (no orphans) after filtering nulls
    joined = MagicMock()
    filtered = MagicMock()
    joined.filter.return_value = filtered
    filtered.count.return_value = 0
    filtered.select.return_value.distinct.return_value.limit.return_value.collect.return_value = []
    fact_df.join.return_value = joined

    result = validator.validate_relationships(
        fact_df,
        fk_column="customer_sk",
        reference_table="dim_customer",
        reference_column="customer_sk",
    )

    assert result.passed is True
    assert result.failed_rows == 0


@patch("kimball.validation.F")
def test_validate_relationships_fail(mock_F, validator, mock_spark):
    """Test relationships validation fails when orphan FKs exist."""
    fact_df = MagicMock()
    fact_df.count.return_value = 1000

    # Mock dimension table
    dim_df = MagicMock()
    mock_spark.table.return_value = dim_df

    # LEFT ANTI JOIN returns 5 orphans after filtering nulls
    joined = MagicMock()
    filtered = MagicMock()
    joined.filter.return_value = filtered
    filtered.count.return_value = 5

    mock_row = MagicMock()
    mock_row.asDict.return_value = {"customer_sk": 999}
    filtered.select.return_value.distinct.return_value.limit.return_value.collect.return_value = [
        mock_row
    ]
    fact_df.join.return_value = joined

    result = validator.validate_relationships(
        fact_df,
        fk_column="customer_sk",
        reference_table="dim_customer",
        reference_column="customer_sk",
    )

    assert result.passed is False
    assert result.failed_rows == 5
    assert "orphan" in result.details.lower()


# =============================================================================
# Test: Expression Validation
# =============================================================================


@patch("kimball.validation.F")
def test_validate_expression_pass(mock_F, validator):
    """Test expression validation passes for valid expression."""
    df = MagicMock()
    df.count.return_value = 100

    # filter() returns empty (all rows pass expression)
    filtered = MagicMock()
    df.filter.return_value = filtered
    filtered.count.return_value = 0
    filtered.limit.return_value.collect.return_value = []

    result = validator.validate_expression(df, "amount > 0")

    assert result.passed is True


@patch("kimball.validation.F")
def test_validate_expression_fail(mock_F, validator):
    """Test expression validation fails for invalid rows."""
    df = MagicMock()
    df.count.return_value = 100

    # filter() returns 15 failing rows
    filtered = MagicMock()
    df.filter.return_value = filtered
    filtered.count.return_value = 15
    filtered.limit.return_value.collect.return_value = []

    result = validator.validate_expression(df, "amount > 0")

    assert result.passed is False
    assert result.failed_rows == 15


# =============================================================================
# Test: TestResult
# =============================================================================


def test_test_result_str_passed():
    """Test TestResult string representation when passed."""
    result = TestResult(
        test_name="unique(id)",
        passed=True,
        failed_rows=0,
        total_rows=100,
    )

    output = str(result)
    assert "✅" in output
    assert "unique(id)" in output
    assert "PASSED" in output


def test_test_result_str_failed_error():
    """Test TestResult string representation when failed with error severity."""
    result = TestResult(
        test_name="not_null(email)",
        passed=False,
        failed_rows=5,
        severity=TestSeverity.ERROR,
    )

    output = str(result)
    assert "❌" in output
    assert "FAILED" in output
    assert "5" in output


def test_test_result_str_failed_warn():
    """Test TestResult string representation when failed with warn severity."""
    result = TestResult(
        test_name="accepted_values(status)",
        passed=False,
        failed_rows=2,
        severity=TestSeverity.WARN,
    )

    output = str(result)
    assert "⚠️" in output


# =============================================================================
# Test: ValidationReport
# =============================================================================


def test_validation_report_all_passed():
    """Test ValidationReport when all tests pass."""
    results = [
        TestResult(test_name="test1", passed=True),
        TestResult(test_name="test2", passed=True),
    ]
    report = ValidationReport(results=results)

    assert report.passed is True
    assert report.all_passed is True
    assert report.error_count == 0
    assert report.warning_count == 0


def test_validation_report_with_errors():
    """Test ValidationReport with failed error-severity tests."""
    results = [
        TestResult(test_name="test1", passed=True),
        TestResult(test_name="test2", passed=False, severity=TestSeverity.ERROR),
    ]
    report = ValidationReport(results=results)

    assert report.passed is False
    assert report.error_count == 1


def test_validation_report_with_warnings_only():
    """Test ValidationReport with only warning-severity failures."""
    results = [
        TestResult(test_name="test1", passed=True),
        TestResult(test_name="test2", passed=False, severity=TestSeverity.WARN),
    ]
    report = ValidationReport(results=results)

    assert report.passed is True  # Warnings don't block
    assert report.all_passed is False
    assert report.warning_count == 1


def test_validation_report_raise_on_failure():
    """Test ValidationReport.raise_on_failure raises on errors."""
    from kimball.errors import DataQualityError

    results = [
        TestResult(
            test_name="test1", passed=False, failed_rows=5, severity=TestSeverity.ERROR
        ),
    ]
    report = ValidationReport(results=results)

    with pytest.raises(DataQualityError) as excinfo:
        report.raise_on_failure()

    assert "test1" in str(excinfo.value)


def test_validation_report_str():
    """Test ValidationReport string representation."""
    results = [
        TestResult(test_name="test1", passed=True),
        TestResult(test_name="test2", passed=False, severity=TestSeverity.ERROR),
    ]
    report = ValidationReport(results=results)

    output = str(report)
    assert "Data Quality Validation Report" in output
    assert "1 errors" in output
