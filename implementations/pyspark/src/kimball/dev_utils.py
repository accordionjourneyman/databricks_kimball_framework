"""
Development Utilities - Tools for local development and testing.

Provides:
- Delta Shallow Clone for dev environment data provisioning
- Table comparison utilities
- Quick data exploration helpers

Usage:
    from kimball.dev_utils import shallow_clone_table, compare_tables

    # Clone prod data to dev (zero storage cost)
    shallow_clone_table(spark, "prod.dim_customer", "dev.dim_customer")

    # Compare two table versions
    diff = compare_tables(spark, "dev.dim_customer", "prod.dim_customer")
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def shallow_clone_table(
    spark: "SparkSession",
    source_table: str,
    target_table: str,
    replace: bool = False,
) -> None:
    """
    Create a Delta shallow clone for zero-cost dev copies.

    Shallow clone copies metadata only, pointing to source data files.
    Allows independent MERGE/UPDATE on target without affecting source.

    Args:
        spark: Active SparkSession.
        source_table: Source table to clone (e.g., 'prod.dim_customer').
        target_table: Target table name (e.g., 'dev.dim_customer').
        replace: If True, replace existing target table.
    """
    replace_clause = "OR REPLACE " if replace else ""
    sql = f"CREATE {replace_clause}TABLE {target_table} SHALLOW CLONE {source_table}"

    spark.sql(sql)
    print(f"Created shallow clone: {target_table} <- {source_table}")


def deep_clone_table(
    spark: "SparkSession",
    source_table: str,
    target_table: str,
    replace: bool = False,
) -> None:
    """
    Create a Delta deep clone (full data copy).

    Use for isolated testing where you need complete data independence.

    Args:
        spark: Active SparkSession.
        source_table: Source table to clone.
        target_table: Target table name.
        replace: If True, replace existing target table.
    """
    replace_clause = "OR REPLACE " if replace else ""
    sql = f"CREATE {replace_clause}TABLE {target_table} DEEP CLONE {source_table}"

    spark.sql(sql)
    print(f"Created deep clone: {target_table} <- {source_table}")


def compare_tables(
    spark: "SparkSession",
    table_a: str,
    table_b: str,
    key_columns: list[str] | None = None,
) -> dict[str, int]:
    """
    Compare two tables and return difference counts.

    Args:
        spark: Active SparkSession.
        table_a: First table name.
        table_b: Second table name.
        key_columns: Optional key columns for matching. If None, compares row-by-row.

    Returns:
        Dict with counts: only_in_a, only_in_b, in_both, total_a, total_b.
    """
    df_a = spark.table(table_a)
    df_b = spark.table(table_b)

    total_a = df_a.count()
    total_b = df_b.count()

    if key_columns:
        # Key-based comparison
        only_in_a = df_a.join(df_b, key_columns, "left_anti").count()
        only_in_b = df_b.join(df_a, key_columns, "left_anti").count()
        in_both = df_a.join(df_b, key_columns, "inner").count()
    else:
        # Row-based comparison (EXCEPT)
        only_in_a = df_a.exceptAll(df_b).count()
        only_in_b = df_b.exceptAll(df_a).count()
        in_both = df_a.intersectAll(df_b).count()

    return {
        "only_in_a": only_in_a,
        "only_in_b": only_in_b,
        "in_both": in_both,
        "total_a": total_a,
        "total_b": total_b,
    }


def restore_table_version(
    spark: "SparkSession",
    table_name: str,
    version: int,
) -> None:
    """
    Restore a Delta table to a specific version.

    Args:
        spark: Active SparkSession.
        table_name: Table to restore.
        version: Version number to restore to.
    """
    spark.sql(f"RESTORE TABLE {table_name} TO VERSION AS OF {version}")
    print(f"Restored {table_name} to version {version}")


def get_table_history(
    spark: "SparkSession",
    table_name: str,
    limit: int = 10,
) -> "DataFrame":
    """
    Get Delta table history.

    Args:
        spark: Active SparkSession.
        table_name: Table to get history for.
        limit: Max rows to return.

    Returns:
        DataFrame with table history.
    """
    return spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT {limit}")


def setup_dev_environment(
    spark: "SparkSession",
    prod_tables: list[str],
    dev_schema: str = "dev",
    use_shallow: bool = True,
) -> dict[str, bool]:
    """
    Set up a dev environment by cloning production tables.

    Args:
        spark: Active SparkSession.
        prod_tables: List of production table names.
        dev_schema: Target dev schema (default: 'dev').
        use_shallow: Use shallow clone (default: True).

    Returns:
        Dict mapping table -> success status.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {dev_schema}")

    results = {}
    for table in prod_tables:
        try:
            # Extract table name from fully qualified name
            table_suffix = table.split(".")[-1]
            target = f"{dev_schema}.{table_suffix}"

            if use_shallow:
                shallow_clone_table(spark, table, target, replace=True)
            else:
                deep_clone_table(spark, table, target, replace=True)

            results[table] = True
        except Exception as e:
            print(f"Failed to clone {table}: {e}")
            results[table] = False

    return results
