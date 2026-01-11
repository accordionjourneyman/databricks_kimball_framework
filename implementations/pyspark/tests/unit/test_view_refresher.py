"""
Unit tests for ViewRefresher.

Tests error handling, SQL injection protection, and view creation.
"""

import pytest
from unittest.mock import MagicMock, patch

from kimball.view_refresher import ViewRefresher, ViewRefreshError


@pytest.fixture
def mock_spark():
    """Create a mock SparkSession."""
    spark = MagicMock()
    return spark


@pytest.fixture
def refresher(mock_spark):
    """Create ViewRefresher with mocked Spark."""
    return ViewRefresher(mock_spark)


class TestQuoteIdentifier:
    """Tests for _quote_identifier method."""

    def test_simple_name(self, refresher):
        """Simple name should be backtick-quoted."""
        assert refresher._quote_identifier("column_name") == "`column_name`"

    def test_name_with_backticks(self, refresher):
        """Backticks in name should be escaped."""
        assert refresher._quote_identifier("col`name") == "`col``name`"

    def test_empty_name(self, refresher):
        """Empty name should still be quoted."""
        assert refresher._quote_identifier("") == "``"


class TestValidateTableExists:
    """Tests for _validate_table_exists method."""

    def test_table_exists(self, refresher, mock_spark):
        """Should not raise when table exists."""
        mock_spark.catalog.tableExists.return_value = True
        refresher._validate_table_exists("gold.dim_test")  # Should not raise

    def test_table_not_exists(self, refresher, mock_spark):
        """Should raise ViewRefreshError when table doesn't exist."""
        mock_spark.catalog.tableExists.return_value = False

        with pytest.raises(ViewRefreshError) as exc_info:
            refresher._validate_table_exists("gold.nonexistent")

        assert "does not exist" in str(exc_info.value)

    def test_catalog_error(self, refresher, mock_spark):
        """Should raise ViewRefreshError on catalog errors."""
        mock_spark.catalog.tableExists.side_effect = Exception("Connection failed")

        with pytest.raises(ViewRefreshError) as exc_info:
            refresher._validate_table_exists("gold.dim_test")

        assert "Error checking table" in str(exc_info.value)


class TestValidateColumnExists:
    """Tests for _validate_column_exists method."""

    def test_column_exists(self, refresher):
        """Should not raise when column exists."""
        columns = ["id", "__is_current", "name"]
        refresher._validate_column_exists("table", "__is_current", columns)

    def test_column_not_exists(self, refresher):
        """Should raise ViewRefreshError when column doesn't exist."""
        columns = ["id", "name"]

        with pytest.raises(ViewRefreshError) as exc_info:
            refresher._validate_column_exists("table", "__is_current", columns)

        assert "not found" in str(exc_info.value)
        assert "Available columns" in str(exc_info.value)


class TestRefreshCurrentView:
    """Tests for refresh_current_view method."""

    def test_successful_refresh(self, refresher, mock_spark):
        """Should create view when table exists with correct column."""
        # Setup mocks
        mock_spark.catalog.tableExists.return_value = True

        col1 = MagicMock()
        col1.name = "id"
        col2 = MagicMock()
        col2.name = "__is_current"
        mock_spark.catalog.listColumns.return_value = [col1, col2]

        # Execute
        refresher.refresh_current_view("gold.dim_test")

        # Verify SQL was called
        mock_spark.sql.assert_called_once()
        sql = mock_spark.sql.call_args[0][0]
        assert "CREATE OR REPLACE VIEW" in sql
        assert "`gold.dim_test_current`" in sql
        assert "WHERE `__is_current` = true" in sql

    def test_custom_view_name(self, refresher, mock_spark):
        """Should use custom view name when provided."""
        mock_spark.catalog.tableExists.return_value = True

        col1 = MagicMock()
        col1.name = "__is_current"
        mock_spark.catalog.listColumns.return_value = [col1]

        refresher.refresh_current_view("gold.dim_test", "gold.v_current_dim_test")

        sql = mock_spark.sql.call_args[0][0]
        assert "`gold.v_current_dim_test`" in sql

    def test_missing_table(self, refresher, mock_spark):
        """Should raise error when table doesn't exist."""
        mock_spark.catalog.tableExists.return_value = False

        with pytest.raises(ViewRefreshError) as exc_info:
            refresher.refresh_current_view("gold.nonexistent")

        assert "does not exist" in str(exc_info.value)

    def test_missing_current_column(self, refresher, mock_spark):
        """Should raise error when current column doesn't exist (SCD1 table)."""
        mock_spark.catalog.tableExists.return_value = True

        col1 = MagicMock()
        col1.name = "id"
        mock_spark.catalog.listColumns.return_value = [col1]

        with pytest.raises(ViewRefreshError) as exc_info:
            refresher.refresh_current_view("gold.dim_test")

        assert "__is_current" in str(exc_info.value)
        assert "not found" in str(exc_info.value)

    def test_sql_injection_prevention(self, refresher, mock_spark):
        """Column names should be properly quoted to prevent injection."""
        mock_spark.catalog.tableExists.return_value = True

        # Malicious column name
        col1 = MagicMock()
        col1.name = "id; DROP TABLE users; --"
        col2 = MagicMock()
        col2.name = "__is_current"
        mock_spark.catalog.listColumns.return_value = [col1, col2]

        refresher.refresh_current_view("gold.dim_test")

        sql = mock_spark.sql.call_args[0][0]
        # Should be escaped with backticks
        assert "`id; DROP TABLE users; --`" in sql


class TestRefreshAllDimensionViews:
    """Tests for refresh_all_dimension_views method."""

    def test_multiple_dimensions(self, refresher, mock_spark):
        """Should refresh views for multiple dimensions."""
        mock_spark.catalog.tableExists.return_value = True

        col = MagicMock()
        col.name = "__is_current"
        mock_spark.catalog.listColumns.return_value = [col]

        results = refresher.refresh_all_dimension_views(["gold.dim_a", "gold.dim_b"])

        assert results == {"gold.dim_a": True, "gold.dim_b": True}
        assert mock_spark.sql.call_count == 2

    def test_partial_failure(self, refresher, mock_spark):
        """Should continue after individual failures."""
        # First table succeeds, second fails
        mock_spark.catalog.tableExists.side_effect = [True, False]

        col = MagicMock()
        col.name = "__is_current"
        mock_spark.catalog.listColumns.return_value = [col]

        results = refresher.refresh_all_dimension_views(
            ["gold.dim_good", "gold.dim_bad"]
        )

        assert results["gold.dim_good"] is True
        assert results["gold.dim_bad"] is False


class TestCreateVersionedView:
    """Tests for create_versioned_view method."""

    def test_version_number(self, refresher, mock_spark):
        """Should create view with VERSION AS OF."""
        mock_spark.catalog.tableExists.return_value = True

        refresher.create_versioned_view(
            "gold.dim_test", "gold.v_version", as_of_version=100
        )

        sql = mock_spark.sql.call_args[0][0]
        assert "VERSION AS OF 100" in sql

    def test_timestamp(self, refresher, mock_spark):
        """Should create view with TIMESTAMP AS OF."""
        mock_spark.catalog.tableExists.return_value = True

        refresher.create_versioned_view(
            "gold.dim_test", "gold.v_timestamp", as_of_timestamp="2024-01-01"
        )

        sql = mock_spark.sql.call_args[0][0]
        assert "TIMESTAMP AS OF '2024-01-01'" in sql

    def test_neither_version_nor_timestamp(self, refresher):
        """Should raise ValueError when neither is provided."""
        with pytest.raises(ValueError) as exc_info:
            refresher.create_versioned_view("gold.dim_test", "gold.v_test")

        assert "Either as_of_version or as_of_timestamp is required" in str(
            exc_info.value
        )

    def test_invalid_version_type(self, refresher, mock_spark):
        """Should raise ValueError for non-integer version."""
        mock_spark.catalog.tableExists.return_value = True

        with pytest.raises(ValueError) as exc_info:
            refresher.create_versioned_view(
                "gold.dim_test",
                "gold.v_test",
                as_of_version="100",  # String instead of int
            )

        assert "must be an integer" in str(exc_info.value)

    def test_invalid_timestamp_format(self, refresher, mock_spark):
        """Should raise ValueError for invalid timestamp format."""
        mock_spark.catalog.tableExists.return_value = True

        with pytest.raises(ValueError) as exc_info:
            refresher.create_versioned_view(
                "gold.dim_test", "gold.v_test", as_of_timestamp="invalid-date"
            )

        assert "YYYY-MM-DD" in str(exc_info.value)
