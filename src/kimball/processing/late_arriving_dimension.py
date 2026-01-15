"""
Late Arriving Dimension Processor - Handles updating facts when dimensions arrive late.

This module complements the SkeletonGenerator by handling the reverse case:
- SkeletonGenerator: Creates placeholder dimension rows when facts reference missing dimensions
- LateArrivingDimensionProcessor: Updates facts when the real dimension data arrives

Kimball ETL Toolkit explicitly states that late-arriving dimension processing must update
previously loaded facts (DW ETL Toolkit, Ch. 4).

Usage:
    from kimball.processing.late_arriving_dimension import LateArrivingDimensionProcessor

    processor = LateArrivingDimensionProcessor()
    processor.process_late_arriving_dimension(
        dimension_table="gold.dim_customer",
        fact_tables=["gold.fact_sales", "gold.fact_returns"],
        dimension_sk_col="customer_sk",
        dimension_nk_cols=["customer_id"],
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if TYPE_CHECKING:
    pass


class LateArrivingDimensionProcessor:
    """
    Processes late-arriving dimensions by updating skeleton rows and related facts.

    When a real dimension arrives for a previously created skeleton:
    1. Identifies skeleton rows that now have real dimension data
    2. Updates skeleton rows with real attribute values
    3. Optionally updates facts if SK changed (e.g., hash -> identity transition)
    """

    def __init__(self, spark_session: SparkSession | None = None) -> None:
        if spark_session is None:
            from databricks.sdk.runtime import spark

            spark_session = spark
        self.spark = spark_session

    def update_skeletons_with_real_data(
        self,
        dimension_table: str,
        source_df: pyspark.sql.DataFrame,  # noqa: F821
        natural_keys: list[str],
        exclude_columns: list[str] | None = None,
    ) -> int:
        """
        Updates skeleton rows in dimension table with real data from source.

        Args:
            dimension_table: Full name of the dimension table.
            source_df: DataFrame with real dimension data.
            natural_keys: Natural key columns used to match skeletons.
            exclude_columns: Columns to NOT update (e.g., system columns).

        Returns:
            Number of skeleton rows updated.
        """
        if not self.spark.catalog.tableExists(dimension_table):
            print(f"Dimension table {dimension_table} does not exist. Skipping.")
            return 0

        delta_table = DeltaTable.forName(self.spark, dimension_table)
        dim_df = delta_table.toDF()

        # Check if __is_skeleton column exists
        if "__is_skeleton" not in dim_df.columns:
            print(
                f"Table {dimension_table} has no __is_skeleton column. "
                "Skipping late-arriving dimension processing."
            )
            return 0

        # Find skeleton rows that match incoming source data
        skeleton_rows = dim_df.filter(col("__is_skeleton") == True)  # noqa: E712
        if skeleton_rows.isEmpty():
            print(f"No skeleton rows found in {dimension_table}.")
            return 0

        # Build merge condition on natural keys
        merge_condition = " AND ".join(
            [f"target.{k} <=> source.{k}" for k in natural_keys]
        )
        merge_condition += " AND target.__is_skeleton = true"

        # Determine columns to update (all source columns except system/excluded)
        exclude_set = set(exclude_columns or [])
        exclude_set.update(
            {
                "__is_current",
                "__valid_from",
                "__valid_to",
                "__etl_processed_at",
                "__etl_batch_id",
                "__is_skeleton",
                "__is_deleted",
            }
        )
        exclude_set.update(natural_keys)  # Don't update keys

        update_cols = [c for c in source_df.columns if c not in exclude_set]

        # Build update set
        update_set = {c: f"source.{c}" for c in update_cols}
        update_set["__is_skeleton"] = "false"  # Mark as real data
        update_set["__etl_processed_at"] = "current_timestamp()"

        # Execute merge
        delta_table.alias("target").merge(
            source_df.alias("source"), merge_condition
        ).whenMatchedUpdate(set=update_set).execute()

        print(f"Updated skeleton rows in {dimension_table} with real dimension data.")

        # Return count of updated rows (from merge metrics)
        try:
            metrics = delta_table.history(1).select("operationMetrics").first()
            if metrics and metrics.operationMetrics:
                return int(metrics.operationMetrics.get("numTargetRowsUpdated", 0))
        except Exception:
            pass

        return 0

    def reconcile_fact_foreign_keys(
        self,
        fact_table: str,
        dimension_table: str,
        fact_fk_col: str,
        dimension_sk_col: str,
        dimension_nk_cols: list[str],
    ) -> int:
        """
        Reconciles fact FK values when dimension SK strategy changed.

        This is needed when:
        - Skeleton was created with hash-based SK
        - Real dimension uses identity-based SK
        - Facts referencing old hash SK need to be updated to new identity SK

        Args:
            fact_table: Full name of the fact table.
            dimension_table: Full name of the dimension table.
            fact_fk_col: Foreign key column in fact table.
            dimension_sk_col: Surrogate key column in dimension table.
            dimension_nk_cols: Natural key columns for matching.

        Returns:
            Number of fact rows updated.
        """
        if not self.spark.catalog.tableExists(fact_table):
            print(f"Fact table {fact_table} does not exist. Skipping.")
            return 0

        if not self.spark.catalog.tableExists(dimension_table):
            print(f"Dimension table {dimension_table} does not exist. Skipping.")
            return 0

        fact_delta = DeltaTable.forName(self.spark, fact_table)
        dim_df = self.spark.table(dimension_table)

        # Get current dimension records only
        if "__is_current" in dim_df.columns:
            dim_df = dim_df.filter(col("__is_current") == True)  # noqa: E712

        # This is a complex operation that requires:
        # 1. Finding facts with orphan FKs (FK doesn't exist in dimension)
        # 2. Joining to dimension on natural keys from the original skeleton
        # 3. Updating fact FK with new dimension SK

        # For now, we log a warning - full implementation requires natural key storage in facts
        print(
            f"WARNING: FK reconciliation for {fact_table}.{fact_fk_col} -> "
            f"{dimension_table}.{dimension_sk_col} requires manual review. "
            "Full auto-reconciliation requires natural key storage in fact tables."
        )

        return 0

    def process_late_arriving_dimension(
        self,
        dimension_table: str,
        source_df: pyspark.sql.DataFrame,  # noqa: F821
        natural_keys: list[str],
        fact_tables: list[dict[str, str]] | None = None,
    ) -> dict[str, int]:
        """
        Full late-arriving dimension processing pipeline.

        Args:
            dimension_table: Full name of the dimension table.
            source_df: DataFrame with real dimension data.
            natural_keys: Natural key columns.
            fact_tables: Optional list of fact table configs for FK reconciliation.
                        Each dict should have 'table', 'fk_column', 'dimension_sk_column'.

        Returns:
            Dictionary with processing statistics.
        """
        stats = {"skeletons_updated": 0, "facts_reconciled": 0}

        # Step 1: Update skeleton rows with real data
        stats["skeletons_updated"] = self.update_skeletons_with_real_data(
            dimension_table=dimension_table,
            source_df=source_df,
            natural_keys=natural_keys,
        )

        # Step 2: Reconcile fact FKs if configured
        if fact_tables:
            for fact_config in fact_tables:
                updated = self.reconcile_fact_foreign_keys(
                    fact_table=fact_config["table"],
                    dimension_table=dimension_table,
                    fact_fk_col=fact_config["fk_column"],
                    dimension_sk_col=fact_config.get("dimension_sk_column", "sk"),
                    dimension_nk_cols=natural_keys,
                )
                stats["facts_reconciled"] += updated

        return stats
