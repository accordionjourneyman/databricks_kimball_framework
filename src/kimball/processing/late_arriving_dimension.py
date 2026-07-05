from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)


class LateArrivingDimensionProcessor:
    def __init__(self, spark_session: SparkSession | None = None) -> None:
        self.spark = (
            spark_session
            or __import__("databricks.sdk.runtime", fromlist=["spark"]).spark
        )

    def update_skeletons_with_real_data(
        self,
        dimension_table: str,
        source_df,
        natural_keys: list[str],
        exclude_columns: list[str] | None = None,
    ) -> int:
        if not self.spark.catalog.tableExists(dimension_table):
            logger.info(f"Dimension table {dimension_table} does not exist. Skipping.")
            return 0
        delta_table = DeltaTable.forName(self.spark, dimension_table)
        dim_df = delta_table.toDF()
        if "__is_skeleton" not in dim_df.columns:
            logger.info(
                f"Table {dimension_table} has no __is_skeleton column. Skipping late-arriving dimension processing."
            )
            return 0
        skeleton_rows = dim_df.filter(col("__is_skeleton"))
        if skeleton_rows.isEmpty():
            logger.info(f"No skeleton rows found in {dimension_table}.")
            return 0
        merge_condition = (
            " AND ".join([f"target.{k} <=> source.{k}" for k in natural_keys])
            + " AND target.__is_skeleton = true"
        )
        exclude_set = (
            set(exclude_columns or [])
            | {
                "__is_current",
                "__valid_from",
                "__valid_to",
                "__etl_processed_at",
                "__etl_batch_id",
                "__is_skeleton",
                "__is_deleted",
            }
            | set(natural_keys)
        )
        update_set = {
            c: f"source.{c}" for c in source_df.columns if c not in exclude_set
        }
        update_set.update(
            {"__is_skeleton": "false", "__etl_processed_at": "current_timestamp()"}
        )
        delta_table.alias("target").merge(
            source_df.alias("source"), merge_condition
        ).whenMatchedUpdate(set=update_set).execute()
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
        if not self.spark.catalog.tableExists(
            fact_table
        ) or not self.spark.catalog.tableExists(dimension_table):
            return 0
        try:
            DeltaTable.forName(self.spark, fact_table).alias("target").merge(
                self.spark.table(dimension_table).alias("source"),
                " AND ".join([f"target.{k} <=> source.{k}" for k in dimension_nk_cols]),
            ).whenMatchedUpdate(
                set={fact_fk_col: f"source.{dimension_sk_col}"}
            ).execute()
            return 1
        except Exception as e:
            logger.warning(f"FK reconciliation failed: {e}")
            return 0

    def process_late_arriving_dimension(
        self,
        dimension_table: str,
        source_df,
        natural_keys: list[str],
        fact_tables: list[dict[str, str]] | None = None,
    ) -> dict[str, int]:
        stats = {
            "skeletons_updated": self.update_skeletons_with_real_data(
                dimension_table, source_df, natural_keys
            ),
            "facts_reconciled": 0,
        }
        if fact_tables:
            for fact_config in fact_tables:
                stats["facts_reconciled"] += self.reconcile_fact_foreign_keys(
                    fact_config["table"],
                    dimension_table,
                    fact_config["fk_column"],
                    fact_config.get("dimension_sk_column", "sk"),
                    natural_keys,
                )
        return stats
