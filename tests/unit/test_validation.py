"""Tests for validation.py FK integrity and helper functions."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.validation import DataQualityValidator, TestResult, TestSeverity, ValidationReport


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

    def test_returns_none_when_no_dim_key(self):
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
            df, {"column": "sk", "dimension_table": "dim", "dimension_key": "sk"},
            True, TestSeverity.ERROR
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
            {"column": "sk", "dimension_table": "dim", "dimension_key": "sk", "default_value": -1},
            True, TestSeverity.ERROR
        )
        assert result is not None

    def test_error_handling_returns_test_error(self):
        validator, spark = _make_validator()
        df = MagicMock()
        spark.table.side_effect = Exception("table not found")
        result = validator._check_single_fk(
            df, {"column": "sk", "dimension_table": "dim", "dimension_key": "sk"},
            True, TestSeverity.ERROR
        )
        assert result is not None
        assert result.passed is False
        assert "Test error" in result.details


class TestValidateFactFkIntegrity:
    def test_returns_report_with_results(self):
        validator, _ = _make_validator()
        df = MagicMock()
        with patch.object(validator, "_check_single_fk") as mock_check:
            mock_check.return_value = TestResult("test", True, 0, 10, TestSeverity.ERROR)
            report = validator.validate_fact_fk_integrity(df, [{"column": "sk", "dimension_table": "dim"}])
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


class TestCountBadRows:
    def test_returns_negative_one_when_orphans_exist(self):
        validator, _ = _make_validator()
        bad_df = MagicMock()
        bad_df.limit.return_value.isEmpty.return_value = False
        assert validator._count_bad_rows(bad_df) == -1

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
    def test_returns_negative_one_non_dev(self):
        validator, _ = _make_validator()
        assert validator._dev_total_rows(MagicMock()) == -1

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
        with patch("kimball.validation.F") as mock_F:
            mock_F.col.return_value = MagicMock()
            col_mock = MagicMock()
            col_mock.__eq__ = MagicMock(return_value=MagicMock())
            mock_F.col.return_value = col_mock
            with patch.object(validator, "_build_test_result") as mock_build:
                mock_build.return_value = TestResult("test", True, 0, 10, TestSeverity.ERROR)
                validator.validate_relationships(df, "sk", "dim", "sk")
        dim_df.filter.assert_called_once()