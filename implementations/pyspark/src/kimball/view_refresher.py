"""
View Refresher - Auto-refresh SCD Type 2 current views after dimension updates.

Provides:
- Automatic view refresh that adapts to schema evolution
- Creates "current" views for SCD2 dimensions (WHERE is_current = true)
- Safe view creation using Spark Catalog API
- Proper error handling for missing tables/columns

Usage:
    from kimball.view_refresher import ViewRefresher

    refresher = ViewRefresher(spark)
    refresher.refresh_current_view("gold.dim_customer", "gold.v_dim_customer_current")
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class ViewRefreshError(Exception):
    """Error during view refresh operation."""

    pass


class ViewRefresher:
    """Manages view refresh for SCD Type 2 dimensions."""

    def __init__(self, spark: "SparkSession"):
        """Initialize with Spark session.

        Args:
            spark: Active SparkSession.
        """
        self.spark = spark

    def _quote_identifier(self, name: str) -> str:
        """Quote an identifier to prevent SQL injection.

        Args:
            name: Column or table name.

        Returns:
            Backtick-quoted identifier.
        """
        # Escape any backticks in the name and wrap with backticks
        return f"`{name.replace('`', '``')}`"

    def _validate_table_exists(self, table_name: str) -> None:
        """Validate that a table exists.

        Args:
            table_name: Fully qualified table name.

        Raises:
            ViewRefreshError: If table doesn't exist.
        """
        try:
            if not self.spark.catalog.tableExists(table_name):
                raise ViewRefreshError(f"Table '{table_name}' does not exist")
        except Exception as e:
            if "does not exist" in str(e).lower():
                raise ViewRefreshError(f"Table '{table_name}' does not exist") from e
            raise ViewRefreshError(f"Error checking table '{table_name}': {e}") from e

    def _validate_column_exists(
        self, table_name: str, column_name: str, columns: list[str]
    ) -> None:
        """Validate that a column exists in the table.

        Args:
            table_name: Table name (for error message).
            column_name: Column to check.
            columns: List of column names from table.

        Raises:
            ViewRefreshError: If column doesn't exist.
        """
        if column_name not in columns:
            raise ViewRefreshError(
                f"Column '{column_name}' not found in table '{table_name}'. "
                f"Available columns: {columns[:10]}{'...' if len(columns) > 10 else ''}"
            )

    def refresh_current_view(
        self,
        table_name: str,
        view_name: str | None = None,
        current_column: str = "__is_current",
    ) -> None:
        """
        Refresh a "current" view for an SCD Type 2 dimension.

        Automatically includes any new columns added by schema evolution.

        Args:
            table_name: Source Delta table (e.g., 'gold.dim_customer').
            view_name: Target view name. Defaults to '{table_name}_current'.
            current_column: Column indicating current row (default: __is_current).

        Raises:
            ViewRefreshError: If table doesn't exist or column is missing.
        """
        if view_name is None:
            view_name = f"{table_name}_current"

        # Validate table exists
        self._validate_table_exists(table_name)

        # Get all columns from the Delta table via Spark Catalog
        try:
            columns = [c.name for c in self.spark.catalog.listColumns(table_name)]
        except Exception as e:
            raise ViewRefreshError(
                f"Failed to list columns for '{table_name}': {e}"
            ) from e

        # Validate current_column exists
        self._validate_column_exists(table_name, current_column, columns)

        # Quote all column names to prevent SQL injection
        quoted_cols = [self._quote_identifier(c) for c in columns]
        col_str = ", ".join(quoted_cols)

        # Use quoted identifiers for table and view names
        quoted_table = self._quote_identifier(table_name)
        quoted_view = self._quote_identifier(view_name)
        quoted_current_col = self._quote_identifier(current_column)

        # Create or replace the view
        sql = f"""
            CREATE OR REPLACE VIEW {quoted_view} AS
            SELECT {col_str}
            FROM {quoted_table}
            WHERE {quoted_current_col} = true
        """

        try:
            self.spark.sql(sql)
            print(f"Refreshed view: {view_name} (columns: {len(columns)})")
        except Exception as e:
            raise ViewRefreshError(f"Failed to create view '{view_name}': {e}") from e

    def refresh_all_dimension_views(
        self,
        dimensions: list[str],
        view_suffix: str = "_current",
    ) -> dict[str, bool]:
        """
        Refresh current views for multiple dimensions.

        Args:
            dimensions: List of dimension table names.
            view_suffix: Suffix for view names (default: '_current').

        Returns:
            Dict mapping table -> success status.
        """
        results = {}
        for dim in dimensions:
            try:
                view_name = f"{dim}{view_suffix}"
                self.refresh_current_view(dim, view_name)
                results[dim] = True
            except ViewRefreshError as e:
                print(f"Failed to refresh view for {dim}: {e}")
                results[dim] = False

        return results

    def create_versioned_view(
        self,
        table_name: str,
        view_name: str,
        as_of_version: int | None = None,
        as_of_timestamp: str | None = None,
    ) -> None:
        """
        Create a view for a specific Delta table version (time travel).

        Args:
            table_name: Source Delta table.
            view_name: Target view name.
            as_of_version: Delta version number.
            as_of_timestamp: Timestamp string (e.g., '2024-01-01').

        Raises:
            ValueError: If neither version nor timestamp is provided.
            ViewRefreshError: If view creation fails.
        """
        # Validate table exists
        self._validate_table_exists(table_name)

        quoted_table = self._quote_identifier(table_name)
        quoted_view = self._quote_identifier(view_name)

        if as_of_version is not None:
            # Version must be an integer (no injection risk)
            if not isinstance(as_of_version, int):
                raise ValueError("as_of_version must be an integer")
            sql = f"""
                CREATE OR REPLACE VIEW {quoted_view} AS
                SELECT * FROM {quoted_table} VERSION AS OF {as_of_version}
            """
        elif as_of_timestamp is not None:
            # Validate timestamp format to prevent injection
            import re

            if not re.match(r"^\d{4}-\d{2}-\d{2}", as_of_timestamp):
                raise ValueError("as_of_timestamp must start with YYYY-MM-DD format")
            sql = f"""
                CREATE OR REPLACE VIEW {quoted_view} AS
                SELECT * FROM {quoted_table} TIMESTAMP AS OF '{as_of_timestamp}'
            """
        else:
            raise ValueError("Either as_of_version or as_of_timestamp is required")

        try:
            self.spark.sql(sql)
            print(f"Created versioned view: {view_name}")
        except Exception as e:
            raise ViewRefreshError(
                f"Failed to create versioned view '{view_name}': {e}"
            ) from e
