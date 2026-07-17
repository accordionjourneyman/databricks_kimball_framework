"""Tests for validation.py FK integrity and helper functions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.validation import (
    DataQualityValidator,
    TestResult,
    TestSeverity,
    ValidationReport,
)


def _make_validator():
    spark = MagicMock()
    return DataQualityValidator(spark=spark), spark


class TestCheckSingleFk:
    def test_returns_none_when_no_column(self):
        validator, _ = _make_validator()
        df = MagicMock()
        result = validator._check_single_fk(
            df, {"dimension_table": "dim"}, True, TestSeverity.ERROR
        )
        assert result is None

    def test_returns_none_when_no_dim_table(self):
        validator, _ = _make_validator()
        df = MagicMock()
        result = validator._check_single_fk(
            df, {"column": "sk"}, True, TestSeverity.ERROR
        )
        assert result is None

    def test_returns_result_when_column_and_dim_table_provided(self):
        # _check_single_fk only returns None when the FK column or dimension
        # table is missing. When both are present (dimension_key defaults to
        # the FK column), it proceeds and returns a TestResult -- it does NOT
        # short-circuit on a missing/empty dimension_key.
        validator, spark = _make_validator()
        df = MagicMock()
        df.select.return_value = MagicMock()
        df.select.return_value.distinct.return_value = MagicMock()
        df.select.return_value.distinct.return_value.filter.return_value = MagicMock()
        dim_df = MagicMock()
        dim_df.columns = []
        dim_df.select.return_value = MagicMock()
        dim_df.select.return_value.distinct.return_value = MagicMock()
        spark.table.return_value = dim_df
        result = validator._check_single_fk(
            df,
            {"column": "sk", "dimension_table": "dim", "dimension_key": "sk"},
            True,
            TestSeverity.ERROR,
        )
        assert result is not None

    def test_excludes_seed_values_from_fact_fks(self):
        validator, spark = _make_validator()
        df = MagicMock()
        fact_fks = MagicMock()
        fact_fks.filter.return_value = MagicMock()
        df.select.return_value = fact_fks
        dim_df = MagicMock()
        dim_df.columns = ["sk", "__is_current"]
        dim_df.filter.return_value = MagicMock()
        dim_df.filter.return_value.select.return_value = MagicMock()
        spark.table.return_value = dim_df
        result = validator._check_single_fk(
            df,
            {
                "column": "sk",
                "dimension_table": "dim",
                "dimension_key": "sk",
                "default_value": -1,
            },
            True,
            TestSeverity.ERROR,
        )
        assert result is not None

    def test_handles_list_default_values(self):
        validator, spark = _make_validator()
        df = MagicMock()
        fact_fks = MagicMock()
        fact_fks.filter.return_value = MagicMock()
        df.select.return_value = fact_fks
        dim_df = MagicMock()
        dim_df.columns = []
        dim_df.select.return_value = MagicMock()
        dim_df.select.return_value.distinct.return_value = MagicMock()
        spark.table.return_value = dim_df
        result = validator._check_single_fk(
            df,
            {"column": "sk", "dimension_table": "dim", "default_value": [-1, -2]},
            True,
            TestSeverity.ERROR,
        )
        assert result is not None

    def test_error_handling_returns_test_error(self):
        validator, spark = _make_validator()
        df = MagicMock()
        spark.table.side_effect = Exception("table not found")
        result = validator._check_single_fk(
            df,
            {"column": "sk", "dimension_table": "dim", "dimension_key": "sk"},
            True,
            TestSeverity.ERROR,
        )
        assert result is not None
        assert result.passed is False
        assert "Test error" in result.details

    def test_details_count_positive(self):
        validator, spark = _make_validator()
        df = MagicMock()
        fact_fks = MagicMock()
        fact_fks.filter.return_value = MagicMock()
        df.select.return_value = fact_fks
        dim_df = MagicMock()
        dim_df.columns = []
        dim_df.select.return_value = MagicMock()
        dim_df.select.return_value.distinct.return_value = MagicMock()
        spark.table.return_value = dim_df
        with patch.object(validator, "_count_bad_rows", return_value=5):
            with patch.object(validator, "_dev_total_rows", return_value=-1):
                result = validator._check_single_fk(
                    df,
                    {"column": "sk", "dimension_table": "dim", "dimension_key": "sk"},
                    True,
                    TestSeverity.ERROR,
                )
        assert result is not None
        assert "Found 5 FK values" in result.details


class TestValidateFactFkIntegrity:
    def test_returns_report_with_results(self):
        validator, _ = _make_validator()
        df = MagicMock()
        with patch.object(validator, "_check_single_fk") as mock_check:
            mock_check.return_value = TestResult(
                "test", True, 0, 10, TestSeverity.ERROR
            )
            report = validator.validate_fact_fk_integrity(
                df, [{"column": "sk", "dimension_table": "dim"}]
            )
        assert len(report.results) == 1
        assert report.results[0].passed is True

    def test_skips_none_results(self):
        validator, _ = _make_validator()
        df = MagicMock()
        with patch.object(validator, "_check_single_fk", return_value=None):
            report = validator.validate_fact_fk_integrity(df, [{"column": "sk"}])
        assert len(report.results) == 0

    def test_empty_fk_list(self):
        validator, _ = _make_validator()
        df = MagicMock()
        report = validator.validate_fact_fk_integrity(df, [])
        assert len(report.results) == 0


class TestValidationReport:
    def test_raise_on_failure_passes_when_all_passed(self):
        results = [TestResult("test1", True, 0, 10, TestSeverity.ERROR)]
        report = ValidationReport(results=results)
        report.raise_on_failure()

    def test_raise_on_failure_raises_when_failed(self):
        results = [TestResult("test1", False, 5, 10, TestSeverity.ERROR)]
        report = ValidationReport(results=results)
        with pytest.raises(Exception, match="test1"):
            report.raise_on_failure()

    def test_raise_on_failure_skips_warnings(self):
        results = [TestResult("test1", False, 5, 10, TestSeverity.WARN)]
        report = ValidationReport(results=results)
        report.raise_on_failure()

    def test_all_passed_true(self):
        results = [TestResult("test1", True, 0, 10, TestSeverity.ERROR)]
        report = ValidationReport(results=results)
        assert report.all_passed is True

    def test_all_passed_false(self):
        results = [TestResult("test1", False, 5, 10, TestSeverity.ERROR)]
        report = ValidationReport(results=results)
        assert report.all_passed is False

    def test_error_count(self):
        results = [
            TestResult("e1", False, 5, 10, TestSeverity.ERROR),
            TestResult("w1", False, 5, 10, TestSeverity.WARN),
            TestResult("e2", True, 0, 10, TestSeverity.ERROR),
        ]
        report = ValidationReport(results=results)
        assert report.error_count == 1

    def test_warning_count(self):
        results = [
            TestResult("e1", False, 5, 10, TestSeverity.ERROR),
            TestResult("w1", False, 5, 10, TestSeverity.WARN),
            TestResult("w2", True, 0, 10, TestSeverity.WARN),
        ]
        report = ValidationReport(results=results)
        assert report.warning_count == 1

    def test_str_contains_summary(self):
        results = [TestResult("test1", False, 5, 10, TestSeverity.ERROR)]
        report = ValidationReport(results=results)
        s = str(report)
        assert "Data Quality Validation Report" in s
        assert "1 errors" in s
        assert "0 warnings" in s


class TestCountBadRows:
    def test_returns_none_when_orphans_exist(self):
        validator, _ = _make_validator()
        bad_df = MagicMock()
        bad_df.limit.return_value.isEmpty.return_value = False
        assert validator._count_bad_rows(bad_df) is None

    def test_returns_zero_when_no_orphans(self):
        validator, _ = _make_validator()
        bad_df = MagicMock()
        bad_df.limit.return_value.isEmpty.return_value = True
        assert validator._count_bad_rows(bad_df) == 0

    def test_returns_count_in_dev_mode(self):
        with patch.dict("os.environ", {"KIMBALL_ENABLE_DEV_CHECKS": "1"}):
            validator, _ = _make_validator()
            bad_df = MagicMock()
            bad_df.count.return_value = 42
            assert validator._count_bad_rows(bad_df) == 42


class TestDevTotalRows:
    def test_returns_none_non_dev(self):
        validator, _ = _make_validator()
        assert validator._dev_total_rows(MagicMock()) is None

    def test_returns_count_in_dev_mode(self):
        with patch.dict("os.environ", {"KIMBALL_ENABLE_DEV_CHECKS": "1"}):
            validator, _ = _make_validator()
            df = MagicMock()
            df.count.return_value = 100
            assert validator._dev_total_rows(df) == 100


class TestValidateRelationships:
    def test_filters_current_when_is_current_exists(self):
        validator, spark = _make_validator()
        df = MagicMock()
        dim_df = MagicMock()
        dim_df.columns = ["sk", "__is_current"]
        spark.table.return_value = dim_df
        with patch("kimball.orchestration.validation.F") as mock_F:
            mock_F.col.return_value = MagicMock()
            col_mock = MagicMock()
            col_mock.__eq__ = MagicMock(return_value=MagicMock())
            mock_F.col.return_value = col_mock
            with patch.object(validator, "_build_test_result") as mock_build:
                mock_build.return_value = TestResult(
                    "test", True, 0, 10, TestSeverity.ERROR
                )
                validator.validate_relationships(df, "sk", "dim", "sk")
        dim_df.filter.assert_called_once()

    def test_details_count_positive(self):
        validator, spark = _make_validator()
        df = MagicMock()
        dim_df = MagicMock()
        dim_df.columns = ["sk"]
        spark.table.return_value = dim_df
        joined = MagicMock()
        joined.filter.return_value = MagicMock()
        df.join.return_value = joined
        with patch("kimball.orchestration.validation.F") as mock_F:
            col_mock = MagicMock()
            col_mock.alias.return_value = MagicMock()
            col_mock.isNotNull.return_value = MagicMock()
            mock_F.col.return_value = col_mock
            with patch.object(validator, "_build_test_result") as mock_build:

                def capture(
                    df,
                    bad_df,
                    test_name,
                    severity,
                    sample_size,
                    details_fn,
                    sample_fn=None,
                ):
                    details = details_fn(3)
                    return TestResult(test_name, True, 0, 10, severity, details=details)

                mock_build.side_effect = capture
                result = validator.validate_relationships(df, "sk", "dim", "sk")
        assert "Found 3 orphan" in result.details

    def test_details_count_negative_one(self):
        validator, spark = _make_validator()
        df = MagicMock()
        dim_df = MagicMock()
        dim_df.columns = ["sk"]
        spark.table.return_value = dim_df
        joined = MagicMock()
        joined.filter.return_value = MagicMock()
        df.join.return_value = joined
        with patch("kimball.orchestration.validation.F") as mock_F:
            col_mock = MagicMock()
            col_mock.alias.return_value = MagicMock()
            col_mock.isNotNull.return_value = MagicMock()
            mock_F.col.return_value = col_mock
            with patch.object(validator, "_build_test_result") as mock_build:

                def capture(
                    df,
                    bad_df,
                    test_name,
                    severity,
                    sample_size,
                    details_fn,
                    sample_fn=None,
                ):
                    details = details_fn(-1)
                    return TestResult(test_name, True, 0, 10, severity, details=details)

                mock_build.side_effect = capture
                result = validator.validate_relationships(df, "sk", "dim", "sk")
        assert "count skipped" in result.details

    def test_error_handling(self):
        validator, spark = _make_validator()
        df = MagicMock()
        spark.table.side_effect = Exception("boom")
        result = validator.validate_relationships(df, "sk", "dim", "sk")
        assert result.passed is False
        assert "Test error" in result.details


class TestValidateUnique:
    def test_passes_when_no_duplicates(self):
        validator, _ = _make_validator()
        df = MagicMock()
        bad_df = MagicMock()
        bad_df.isEmpty.return_value = True
        grouped = MagicMock()
        grouped.agg.return_value = grouped
        grouped.filter.return_value = bad_df
        df.groupBy.return_value = grouped
        with patch.object(validator, "_build_test_result") as mock_build:
            mock_build.return_value = TestResult(
                "test", True, 0, 10, TestSeverity.ERROR
            )
            result = validator.validate_unique(df, ["id"])
        # The validator must actually run groupBy/agg/filter and delegate the
        # (empty) violation set to _build_test_result with the right test name
        # -- not short-circuit to a stub. And the returned result must be a
        # PASS, not merely non-None (which would pass even if it returned a
        # failing result).
        mock_build.assert_called_once()
        assert mock_build.call_args.args[1] is bad_df
        assert mock_build.call_args.args[2] == "unique(id)"
        assert mock_build.call_args.args[3] is TestSeverity.ERROR
        assert result.passed is True
        assert result.failed_rows == 0

    def test_details_count_positive(self):
        validator, _ = _make_validator()
        df = MagicMock()
        with patch.object(validator, "_build_test_result") as mock_build:

            def capture(df, bad_df, test_name, severity, sample_size, details_fn):
                details = details_fn(3)
                return TestResult(test_name, True, 0, 10, severity, details=details)

            mock_build.side_effect = capture
            result = validator.validate_unique(df, ["id"])
        assert "Found 3 duplicate" in result.details

    def test_details_count_negative_one(self):
        validator, _ = _make_validator()
        df = MagicMock()
        with patch.object(validator, "_build_test_result") as mock_build:

            def capture(df, bad_df, test_name, severity, sample_size, details_fn):
                details = details_fn(-1)
                return TestResult(test_name, True, 0, 10, severity, details=details)

            mock_build.side_effect = capture
            result = validator.validate_unique(df, ["id"])
        assert "count skipped" in result.details

    def test_error_handling(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.groupBy.side_effect = Exception("boom")
        result = validator.validate_unique(df, ["id"])
        assert result.passed is False
        assert "Test error" in result.details


class TestValidateNotNull:
    def test_passes_when_no_nulls(self):
        validator, _ = _make_validator()
        df = MagicMock()
        bad_df = MagicMock()
        bad_df.isEmpty.return_value = True
        df.filter.return_value = bad_df
        with patch.object(validator, "_build_test_result") as mock_build:
            mock_build.return_value = TestResult(
                "test", True, 0, 10, TestSeverity.ERROR
            )
            result = validator.validate_not_null(df, ["id"])
        mock_build.assert_called_once()
        assert mock_build.call_args.args[1] is bad_df
        assert mock_build.call_args.args[2] == "not_null(id)"
        assert result.passed is True
        assert result.failed_rows == 0

    def test_details_count_positive(self):
        validator, _ = _make_validator()
        df = MagicMock()
        with patch.object(validator, "_build_test_result") as mock_build:

            def capture(df, bad_df, test_name, severity, sample_size, details_fn):
                details = details_fn(3)
                return TestResult(test_name, True, 0, 10, severity, details=details)

            mock_build.side_effect = capture
            result = validator.validate_not_null(df, ["id"])
        assert "Found 3 rows with NULL" in result.details

    def test_details_count_negative_one(self):
        validator, _ = _make_validator()
        df = MagicMock()
        with patch.object(validator, "_build_test_result") as mock_build:

            def capture(df, bad_df, test_name, severity, sample_size, details_fn):
                details = details_fn(-1)
                return TestResult(test_name, True, 0, 10, severity, details=details)

            mock_build.side_effect = capture
            result = validator.validate_not_null(df, ["id"])
        assert "count skipped" in result.details

    def test_error_handling(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.filter.side_effect = Exception("boom")
        result = validator.validate_not_null(df, ["id"])
        assert result.passed is False
        assert "Test error" in result.details


class TestValidateAcceptedValues:
    def test_passes_when_all_valid(self):
        validator, _ = _make_validator()
        df = MagicMock()
        bad_df = MagicMock()
        bad_df.isEmpty.return_value = True
        df.filter.return_value = bad_df
        with patch.object(validator, "_build_test_result") as mock_build:
            mock_build.return_value = TestResult(
                "test", True, 0, 10, TestSeverity.ERROR
            )
            result = validator.validate_accepted_values(df, "status", ["a", "b"])
        mock_build.assert_called_once()
        assert mock_build.call_args.args[1] is bad_df
        assert mock_build.call_args.args[2] == "accepted_values(status)"
        assert result.passed is True
        assert result.failed_rows == 0

    def test_details_count_positive(self):
        validator, _ = _make_validator()
        df = MagicMock()
        with patch.object(validator, "_build_test_result") as mock_build:

            def capture(
                df, bad_df, test_name, severity, sample_size, details_fn, sample_fn=None
            ):
                details = details_fn(3)
                return TestResult(test_name, True, 0, 10, severity, details=details)

            mock_build.side_effect = capture
            result = validator.validate_accepted_values(df, "status", ["a", "b"])
        assert "Found 3 rows with values" in result.details

    def test_details_count_negative_one(self):
        validator, _ = _make_validator()
        df = MagicMock()
        with patch.object(validator, "_build_test_result") as mock_build:

            def capture(
                df, bad_df, test_name, severity, sample_size, details_fn, sample_fn=None
            ):
                details = details_fn(-1)
                return TestResult(test_name, True, 0, 10, severity, details=details)

            mock_build.side_effect = capture
            result = validator.validate_accepted_values(df, "status", ["a", "b"])
        assert "count skipped" in result.details

    def test_error_handling(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.filter.side_effect = Exception("boom")
        result = validator.validate_accepted_values(df, "status", ["a", "b"])
        assert result.passed is False
        assert "Test error" in result.details


class TestValidateExpression:
    def test_passes_when_all_valid(self):
        validator, _ = _make_validator()
        df = MagicMock()
        bad_df = MagicMock()
        bad_df.isEmpty.return_value = True
        df.filter.return_value = bad_df
        with patch.object(validator, "_build_test_result") as mock_build:
            mock_build.return_value = TestResult(
                "test", True, 0, 10, TestSeverity.ERROR
            )
            result = validator.validate_expression(df, "amount > 0")
        mock_build.assert_called_once()
        assert mock_build.call_args.args[1] is bad_df
        assert mock_build.call_args.args[2] == "expression(amount > 0...)"
        assert result.passed is True
        assert result.failed_rows == 0

    def test_rejects_forbidden_sql_keywords(self):
        validator, _ = _make_validator()
        df = MagicMock()
        result = validator.validate_expression(df, "select * from t")
        assert result.passed is False
        assert "forbidden SQL keyword" in result.details

    def test_details_count_positive(self):
        validator, _ = _make_validator()
        df = MagicMock()
        with patch.object(validator, "_build_test_result") as mock_build:

            def capture(df, bad_df, test_name, severity, sample_size, details_fn):
                details = details_fn(3)
                return TestResult(test_name, True, 0, 10, severity, details=details)

            mock_build.side_effect = capture
            result = validator.validate_expression(df, "amount > 0")
        assert "Found 3 rows failing" in result.details

    def test_details_count_negative_one(self):
        validator, _ = _make_validator()
        df = MagicMock()
        with patch.object(validator, "_build_test_result") as mock_build:

            def capture(df, bad_df, test_name, severity, sample_size, details_fn):
                details = details_fn(-1)
                return TestResult(test_name, True, 0, 10, severity, details=details)

            mock_build.side_effect = capture
            result = validator.validate_expression(df, "amount > 0")
        assert "count skipped" in result.details

    def test_error_handling(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.filter.side_effect = Exception("boom")
        result = validator.validate_expression(df, "amount > 0")
        assert result.passed is False
        assert "Test error" in result.details


class TestValidateUniqueApproximate:
    def test_passes_when_high_uniqueness(self):
        validator, _ = _make_validator()
        df = MagicMock()
        stats = MagicMock()
        stats.__getitem__.side_effect = lambda k: {"_total": 100, "_distinct": 100}[k]
        df.select.return_value.agg.return_value.first.return_value = stats
        result = validator.validate_unique_approximate(df, ["id"])
        assert result.passed is True

    def test_fails_when_low_uniqueness(self):
        validator, _ = _make_validator()
        df = MagicMock()
        stats = MagicMock()
        stats.__getitem__.side_effect = lambda k: {"_total": 100, "_distinct": 50}[k]
        df.select.return_value.agg.return_value.first.return_value = stats
        result = validator.validate_unique_approximate(df, ["id"])
        assert result.passed is False

    def test_handles_empty_df(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.select.return_value.agg.return_value.first.return_value = None
        result = validator.validate_unique_approximate(df, ["id"])
        assert result.passed is True

    def test_handles_negative_distinct(self):
        validator, _ = _make_validator()
        df = MagicMock()
        stats = MagicMock()
        stats.__getitem__.side_effect = lambda k: {"_total": 100, "_distinct": -1}[k]
        df.select.return_value.agg.return_value.first.return_value = stats
        result = validator.validate_unique_approximate(df, ["id"])
        assert result.passed is False
        assert "Test error" in result.details

    def test_error_handling(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.select.side_effect = Exception("boom")
        result = validator.validate_unique_approximate(df, ["id"])
        assert result.passed is False
        assert "Test error" in result.details


class TestSampleFailures:
    def test_returns_empty_when_sample_size_zero(self):
        validator, _ = _make_validator()
        assert validator._sample_failures(MagicMock(), 0) == []

    def test_returns_empty_when_sample_size_negative(self):
        validator, _ = _make_validator()
        assert validator._sample_failures(MagicMock(), -1) == []

    def test_uses_custom_sample_fn(self):
        validator, _ = _make_validator()
        df = MagicMock()
        result = validator._sample_failures(df, 5, sample_fn=lambda d, n: ["custom"])
        assert result == ["custom"]


class TestBuildTestResult:
    def test_includes_samples_when_bad_count_nonzero(self):
        validator, _ = _make_validator()
        df = MagicMock()
        bad_df = MagicMock()
        with patch.object(validator, "_dev_total_rows", return_value=100):
            with patch.object(validator, "_count_bad_rows", return_value=5):
                with patch.object(
                    validator, "_sample_failures", return_value=[{"id": 1}]
                ):
                    result = validator._build_test_result(
                        df,
                        bad_df,
                        "test",
                        TestSeverity.ERROR,
                        5,
                        lambda c: f"Found {c}",
                    )
        assert result.failed_rows == 5
        assert result.total_rows == 100
        assert result.sample_failures == [{"id": 1}]

    def test_skips_samples_when_bad_count_zero(self):
        validator, _ = _make_validator()
        df = MagicMock()
        bad_df = MagicMock()
        with patch.object(validator, "_dev_total_rows", return_value=100):
            with patch.object(validator, "_count_bad_rows", return_value=0):
                result = validator._build_test_result(
                    df, bad_df, "test", TestSeverity.ERROR, 5, lambda c: None
                )
        assert result.sample_failures == []


class TestRunConfigTests:
    def test_runs_unique_for_surrogate_key(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.columns = ["sk", "id", "name"]
        config = MagicMock()
        config.table_type = "dimension"
        config.surrogate_key = "sk"
        config.natural_keys = []
        config.foreign_keys = None
        config.tests = None
        with patch.object(validator, "validate_unique") as mock_unique:
            mock_unique.return_value = TestResult(
                "unique", True, 0, 10, TestSeverity.ERROR
            )
            validator.run_config_tests(config, df)
        mock_unique.assert_called_once()

    def test_runs_not_null_for_natural_keys(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.columns = ["sk", "id", "name"]
        config = MagicMock()
        config.table_type = "dimension"
        config.surrogate_key = "sk"
        config.natural_keys = ["id"]
        config.foreign_keys = None
        config.tests = None
        with patch.object(validator, "validate_not_null") as mock_nn:
            mock_nn.return_value = TestResult("nn", True, 0, 10, TestSeverity.ERROR)
            validator.run_config_tests(config, df)
        mock_nn.assert_called_once()

    def test_runs_approximate_unique_when_configured(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.columns = ["sk", "id", "name"]
        config = MagicMock()
        config.table_type = "dimension"
        config.surrogate_key = "sk"
        config.natural_keys = ["id"]
        config.foreign_keys = None
        config.tests = None
        with patch.object(validator, "validate_unique_approximate") as mock_approx:
            mock_approx.return_value = TestResult(
                "approx", True, 0, 10, TestSeverity.WARN
            )
            validator.run_config_tests(config, df, use_approximate_unique=True)
        mock_approx.assert_called_once()

    def test_runs_fk_validation(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.columns = ["sk", "customer_sk"]
        config = MagicMock()
        config.table_type = "fact"
        config.surrogate_key = None
        config.natural_keys = []
        config.foreign_keys = [MagicMock()]
        config.foreign_keys[0].references = "dim_customer"
        config.foreign_keys[0].column = "customer_sk"
        config.foreign_keys[0].dimension_key = "customer_sk"
        config.tests = None
        with patch.object(validator, "validate_relationships") as mock_rel:
            mock_rel.return_value = TestResult("rel", True, 0, 10, TestSeverity.ERROR)
            validator.run_config_tests(config, df)
        mock_rel.assert_called_once()

    def test_runs_custom_tests(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.columns = ["id", "name"]
        config = MagicMock()
        config.table_type = "dimension"
        config.surrogate_key = None
        config.natural_keys = []
        config.foreign_keys = None
        config.tests = [MagicMock()]
        config.tests[0].column = "id"
        config.tests[0].tests = ["not_null"]
        config.tests[0].severity = "error"
        with patch.object(validator, "validate_not_null") as mock_nn:
            mock_nn.return_value = TestResult("nn", True, 0, 10, TestSeverity.ERROR)
            validator.run_config_tests(config, df)
        mock_nn.assert_called_once()


class TestRunTestDefinition:
    def test_skips_when_no_column(self):
        validator, _ = _make_validator()
        df = MagicMock()
        test_def = MagicMock()
        test_def.column = None
        test_def.tests = ["not_null"]
        assert validator._run_test_definition(df, test_def) == []

    def test_skips_when_no_tests(self):
        validator, _ = _make_validator()
        df = MagicMock()
        test_def = MagicMock()
        test_def.column = "id"
        test_def.tests = []
        assert validator._run_test_definition(df, test_def) == []

    def test_skips_when_column_not_in_df(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.columns = ["name"]
        test_def = MagicMock()
        test_def.column = "id"
        test_def.tests = ["not_null"]
        assert validator._run_test_definition(df, test_def) == []

    def test_runs_unique_test(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.columns = ["id"]
        test_def = MagicMock()
        test_def.column = "id"
        test_def.tests = ["unique"]
        test_def.severity = "error"
        with patch.object(validator, "validate_unique") as mock_u:
            mock_u.return_value = TestResult("u", True, 0, 10, TestSeverity.ERROR)
            results = validator._run_test_definition(df, test_def)
        assert len(results) == 1

    def test_runs_not_null_test(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.columns = ["id"]
        test_def = MagicMock()
        test_def.column = "id"
        test_def.tests = ["not_null"]
        test_def.severity = "error"
        with patch.object(validator, "validate_not_null") as mock_nn:
            mock_nn.return_value = TestResult("nn", True, 0, 10, TestSeverity.ERROR)
            results = validator._run_test_definition(df, test_def)
        assert len(results) == 1

    def test_runs_accepted_values_test(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.columns = ["status"]
        test_def = MagicMock()
        test_def.column = "status"
        test_def.tests = [{"accepted_values": ["a", "b"]}]
        test_def.severity = "error"
        with patch.object(validator, "validate_accepted_values") as mock_av:
            mock_av.return_value = TestResult("av", True, 0, 10, TestSeverity.ERROR)
            results = validator._run_test_definition(df, test_def)
        assert len(results) == 1

    def test_runs_relationships_test(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.columns = ["sk"]
        test_def = MagicMock()
        test_def.column = "sk"
        test_def.tests = [{"relationships": {"to": "dim", "field": "sk"}}]
        test_def.severity = "error"
        with patch.object(validator, "validate_relationships") as mock_rel:
            mock_rel.return_value = TestResult("rel", True, 0, 10, TestSeverity.ERROR)
            results = validator._run_test_definition(df, test_def)
        assert len(results) == 1

    def test_runs_expression_test(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.columns = ["amount"]
        test_def = MagicMock()
        test_def.column = "amount"
        test_def.tests = [{"expression": "amount > 0"}]
        test_def.severity = "error"
        with patch.object(validator, "validate_expression") as mock_expr:
            mock_expr.return_value = TestResult("expr", True, 0, 10, TestSeverity.ERROR)
            results = validator._run_test_definition(df, test_def)
        assert len(results) == 1

    def test_uses_warn_severity(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.columns = ["id"]
        test_def = MagicMock()
        test_def.column = "id"
        test_def.tests = ["not_null"]
        test_def.severity = "warn"
        with patch.object(validator, "validate_not_null") as mock_nn:
            mock_nn.return_value = TestResult("nn", True, 0, 10, TestSeverity.WARN)
            results = validator._run_test_definition(df, test_def)
        assert len(results) == 1


class TestValidateNaturalKeyUniqueness:
    def test_passes_when_no_duplicates(self):
        validator, _ = _make_validator()
        df = MagicMock()
        grouped = MagicMock()
        grouped.count.return_value = grouped
        grouped.filter.return_value = grouped
        grouped.limit.return_value.isEmpty.return_value = True
        df.groupBy.return_value = grouped
        result = validator.validate_natural_key_uniqueness(df, ["id"])
        assert result.passed is True

    def test_fails_when_duplicates_exist(self):
        validator, _ = _make_validator()
        df = MagicMock()
        grouped = MagicMock()
        grouped.count.return_value = grouped
        grouped.filter.return_value = grouped
        grouped.limit.return_value.isEmpty.return_value = False
        grouped.orderBy.return_value = grouped
        grouped.limit.return_value.collect.return_value = [{"id": 1, "_dup_count": 2}]
        df.groupBy.return_value = grouped
        result = validator.validate_natural_key_uniqueness(df, ["id"])
        assert result.passed is False
        assert "CRITICAL" in result.details

    def test_dev_mode_counts(self):
        with patch.dict("os.environ", {"KIMBALL_ENABLE_DEV_CHECKS": "1"}):
            validator, _ = _make_validator()
            df = MagicMock()
            df.count.return_value = 10
            df.select.return_value.distinct.return_value.count.return_value = 8
            row = MagicMock()
            row.asDict.return_value = {"id": 1, "_dup_count": 2}
            grouped = MagicMock()
            grouped.agg.return_value = grouped
            grouped.filter.return_value = grouped
            grouped.orderBy.return_value = grouped
            grouped.limit.return_value.collect.return_value = [row]
            df.groupBy.return_value = grouped
            result = validator.validate_natural_key_uniqueness(df, ["id"])
            assert result.passed is False
            assert "Total rows: 10" in result.details

    def test_includes_table_name(self):
        validator, _ = _make_validator()
        df = MagicMock()
        grouped = MagicMock()
        grouped.count.return_value = grouped
        grouped.filter.return_value = grouped
        grouped.limit.return_value.isEmpty.return_value = True
        df.groupBy.return_value = grouped
        result = validator.validate_natural_key_uniqueness(df, ["id"], table_name="dim")
        assert "dim:" in result.test_name

    def test_error_handling(self):
        validator, _ = _make_validator()
        df = MagicMock()
        df.groupBy.side_effect = Exception("boom")
        result = validator.validate_natural_key_uniqueness(df, ["id"])
        assert result.passed is False
        assert "Test error" in result.details


class TestSparkProperty:
    def test_uses_provided_spark(self):
        spark = MagicMock()
        validator = DataQualityValidator(spark=spark)
        assert validator.spark is spark

    def test_falls_back_to_databricks_runtime(self):
        validator = DataQualityValidator(spark=None)
        mock_runtime = MagicMock()
        mock_runtime.spark = MagicMock()
        with patch.dict("sys.modules", {"databricks.sdk.runtime": mock_runtime}):
            s = validator.spark
        assert s is mock_runtime.spark
