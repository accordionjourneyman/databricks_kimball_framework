from __future__ import annotations

import logging
import re
from typing import Any

from pyspark.sql import DataFrame

from kimball.common.constants import SPARK_CONF_AUTO_MERGE
from kimball.common.spark_session import get_spark
from kimball.common.utils import quote_table_name

logger = logging.getLogger(__name__)


def _is_valid_identifier(name: str) -> bool:
    """Validate that a name is a safe SQL identifier."""
    return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name))


def _is_safe_sql_expression(expr: str) -> bool:
    """
    Validate that a SQL expression contains only safe characters.
    Allows alphanumeric, whitespace, and common SQL operators/syntax: (), =<> .
    FINDING-012: Removed single quotes to prevent SQL injection.
    Rejecting semicolons -- and /* is handled implicitly by the whitelist.
    """
    # Check for quotes which could enable injection
    if "'" in expr or '"' in expr:
        raise ValueError(
            f"SQL expression contains quotes, which are not allowed: {expr}. "
            "Use column references only, no string literals."
        )
    # Whitelist: alphanumeric, underscore, whitespace, parens, dot, comparison ops
    return bool(re.match(r"^[a-zA-Z0-9_().=<>\s]+$", expr))


class TableCreator:
    """
    Handles creation of Delta tables with Liquid Clustering support.
    """

    def add_system_columns(
        self,
        df: DataFrame,
        scd_type: int,
        surrogate_key: str | None,
        surrogate_key_strategy: str,
    ) -> DataFrame:
        """
        Add system/audit columns to a DataFrame for table creation.
        SCD1: __etl_processed_at, __etl_batch_id, __is_deleted (for soft deletes)
        SCD2: above + __is_current, __valid_from, __valid_to, hashdiff, __is_skeleton
        """
        from pyspark.sql.functions import current_timestamp, lit
        from pyspark.sql.types import LongType, StringType, TimestampType

        # Common audit columns
        result_df = df.withColumn("__etl_processed_at", current_timestamp())
        result_df = result_df.withColumn("__etl_batch_id", lit(None).cast(StringType()))
        result_df = result_df.withColumn("__is_deleted", lit(False))

        if scd_type == 2:
            # SCD2 specific columns
            result_df = result_df.withColumn("__is_current", lit(True))
            result_df = result_df.withColumn("__valid_from", current_timestamp())
            result_df = result_df.withColumn(
                "__valid_to", lit(None).cast(TimestampType())
            )
            result_df = result_df.withColumn("hashdiff", lit(None).cast(StringType()))
            result_df = result_df.withColumn(
                "__is_skeleton", lit(False)
            )  # For skeleton hydration

        # Add surrogate key column according to strategy
        if surrogate_key:
            if surrogate_key_strategy in ("identity", "sequence"):
                # numeric surrogate key
                result_df = result_df.withColumn(
                    surrogate_key, lit(None).cast(LongType())
                )
            else:
                # default to string for hash or unknown strategies
                result_df = result_df.withColumn(
                    surrogate_key, lit(None).cast(StringType())
                )

        return result_df

    def create_table_with_clustering(
        self,
        table_name: str,
        schema_df: DataFrame,
        config: dict[str, Any] | None = None,
        cluster_by: list[str] | None = None,
        partition_by: list[str] | None = None,
        surrogate_key_col: str | None = None,
        surrogate_key_strategy: str | None = None,
    ) -> None:
        """
        Creates a Delta table with optional Liquid Clustering.

        Args:
            table_name: Full table name (catalog.schema.table)
            schema_df: DataFrame with the desired schema
            config: Table configuration from YAML
            cluster_by: Columns for Liquid Clustering (deprecated, use config)
            partition_by: Columns for partitioning (optional, usually not needed with clustering)
            surrogate_key_col: Name of the surrogate key column (if any)
            surrogate_key_strategy: Strategy for SK generation ('identity', 'hash', 'sequence')
        """
        if get_spark().catalog.tableExists(table_name):
            logger.info(f"Table {table_name} already exists. Skipping creation.")
            return

        # Check config for liquid clustering
        if config and "cluster_by" in config:
            cluster_by = config["cluster_by"]
            partition_by = None  # Don't use partitioning with liquid clustering
            logger.info(f"Using Liquid Clustering from config: {cluster_by}")
        elif cluster_by:
            logger.info(f"Using provided cluster_by: {cluster_by}")

        # Validate clustering columns to prevent SQL injection
        if cluster_by:
            formatted_cluster_cols = []
            for col_name in cluster_by:
                if not _is_valid_identifier(col_name):
                    raise ValueError(f"Invalid clustering column name: {col_name}")
                formatted_cluster_cols.append(f"`{col_name}`")
            cluster_by = formatted_cluster_cols

        # Build CREATE TABLE statement
        # We'll use the DataFrame schema to infer column definitions
        columns = []

        # CDF metadata columns should not be part of the table schema
        # They are internal to Delta Lake's CDF feature
        CDF_METADATA = {
            "_change_type",
            "_commit_version",
            "_commit_timestamp",
            "__merge_action",
        }

        for field in schema_df.schema.fields:
            col_name = field.name

            # Skip CDF internal columns
            if col_name in CDF_METADATA:
                continue

            # Check if this is the surrogate key column with identity strategy
            if col_name == surrogate_key_col and surrogate_key_strategy == "identity":
                # Use GENERATED BY DEFAULT AS IDENTITY - allows explicit values for default rows (-1, -2, -3)
                # while auto-generating for normal inserts
                col_def = (
                    f"{col_name} BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1)"
                )
            else:
                col_def = f"{col_name} {field.dataType.simpleString()}"
                if not field.nullable:
                    col_def += " NOT NULL"
            columns.append(col_def)

        columns_sql = ",\n  ".join(columns)

        # Properly quote multi-part table names for Unity Catalog compatibility
        quoted_table_name = quote_table_name(table_name)

        create_sql = f"""
        CREATE TABLE {quoted_table_name} (
          {columns_sql}
        )
        USING DELTA
        """

        if cluster_by:
            cluster_cols = ", ".join(cluster_by)
            create_sql += f"\nCLUSTER BY ({cluster_cols})"
        elif partition_by:
            partition_cols = ", ".join(partition_by)
            create_sql += f"\nPARTITIONED BY ({partition_cols})"

        # Enable Change Data Feed by default
        create_sql += "\nTBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"

        if surrogate_key_col and surrogate_key_strategy == "identity":
            logger.info(
                f"  - Surrogate key '{surrogate_key_col}' using IDENTITY column"
            )
        if cluster_by:
            logger.info(f"  - Liquid Clustering on {cluster_by}")
        get_spark().sql(create_sql)
        logger.info(f"Table {table_name} created successfully.")

        # Enable Delta optimizations after table creation (optional features, may fail on Free Edition)
        # Phase 1 optimization: Batch TBLPROPERTIES into single ALTER TABLE
        try:
            self.enable_delta_features(table_name)
        except Exception as e:
            error_str = str(e).lower()
            # Serverless/edition limitations - suppress verbose output
            if any(
                x in error_str
                for x in [
                    "not supported",
                    "premium",
                    "serverless",
                    "not enabled",
                    "unknown configuration",
                    "delta_unknown_configuration",
                ]
            ):
                pass  # Silently skip - known serverless limitation
            else:
                # Unknown error - print first line only, not full JVM trace
                first_line = str(e).split("\n")[0][:200]
                logger.info(f"Warning: Delta features failed: {first_line}")

        # Apply basic Delta constraints after table creation
        self.apply_basic_constraints(table_name, surrogate_key_col, schema_df)

        # Apply additional constraints from config
        if config:
            self.apply_delta_constraints(table_name, config)

    def apply_basic_constraints(
        self,
        table_name: str,
        surrogate_key_col: str | None = None,
        schema_df: DataFrame | None = None,
    ) -> None:
        """
        Apply basic Delta constraints using ALTER TABLE statements.

        Args:
            table_name: Full table name
            surrogate_key_col: Name of the surrogate key column
            schema_df: DataFrame with schema information
        """
        # Apply surrogate key NOT NULL constraint
        quoted_table_name = quote_table_name(table_name)
        if surrogate_key_col:
            alter_sql = f"ALTER TABLE {quoted_table_name} ADD CONSTRAINT sk_not_null CHECK (`{surrogate_key_col}` IS NOT NULL)"
            try:
                get_spark().sql(alter_sql)
                logger.info("Applied surrogate key NOT NULL constraint")
            except Exception as e:
                logger.info(f"Warning: Could not apply surrogate key constraint: {e}")

        # Apply is_current boolean constraint for SCD2 tables
        if schema_df and "__is_current" in [f.name for f in schema_df.schema.fields]:
            alter_sql = f"ALTER TABLE {quoted_table_name} ADD CONSTRAINT is_current_check CHECK (__is_current IN (true, false))"
            try:
                get_spark().sql(alter_sql)
                logger.info("Applied is_current boolean constraint")
            except Exception as e:
                logger.info(f"Warning: Could not apply is_current constraint: {e}")

    def apply_delta_constraints(self, table_name: str, config: dict[str, Any]) -> None:
        """
        Apply Delta constraints based on YAML configuration.

        Args:
            table_name: Full table name
            config: Table configuration from YAML
        """
        # Apply NOT NULL constraints for natural keys
        quoted_table_name = quote_table_name(table_name)
        # Handle both flat and nested config structures for natural keys
        # Use safe navigation to avoid AttributeError if 'keys' is None
        keys_config = config.get("keys") or {}
        natural_keys = config.get("natural_keys") or keys_config.get("natural_keys", [])
        for key in natural_keys:
            alter_sql = (
                f"ALTER TABLE {quoted_table_name} ALTER COLUMN `{key}` SET NOT NULL"
            )
            try:
                get_spark().sql(alter_sql)
                logger.info(f"Applied NOT NULL constraint to {key}")
            except Exception as e:
                logger.error(f"Failed to apply NOT NULL to {key}: {e}")

        # Apply NOT NULL constraints for foreign keys (fact tables only)
        if config.get("table_type") == "fact":
            foreign_keys = config.get("foreign_keys") or []
            for fk in foreign_keys:
                fk_col = (
                    fk.get("column")
                    if isinstance(fk, dict)
                    else getattr(fk, "column", None)
                )
                if fk_col:
                    if not _is_valid_identifier(fk_col):
                        logger.info(f"Skipping invalid FK column name: {fk_col}")
                        continue
                    constraint_name = f"fk_{fk_col}_not_null"
                    alter_sql = f"ALTER TABLE {quoted_table_name} ADD CONSTRAINT `{constraint_name}` CHECK (`{fk_col}` IS NOT NULL)"
                    try:
                        get_spark().sql(alter_sql)
                        logger.info(
                            f"Applied FK NOT NULL constraint: {constraint_name}"
                        )
                    except Exception as e:
                        logger.info(
                            f"Warning: Could not apply FK constraint {constraint_name}: {e}"
                        )

        # Apply business domain constraints
        constraints = config.get("constraints") or []
        for constraint in constraints:
            constraint_name = constraint.get("name")
            constraint_expr = constraint.get("expression")

            if constraint_name and constraint_expr:
                if not _is_valid_identifier(constraint_name):
                    logger.info(f"Skipping invalid constraint name: {constraint_name}")
                    continue

                # Strict whitelist validation for constraints
                if not _is_safe_sql_expression(constraint_expr):
                    raise ValueError(
                        f"Invalid characters in constraint expression: {constraint_expr}"
                    )

                alter_sql = f"ALTER TABLE {quoted_table_name} ADD CONSTRAINT `{constraint_name}` CHECK ({constraint_expr})"
                try:
                    get_spark().sql(alter_sql)
                    logger.info(f"Applied constraint {constraint_name}")
                except Exception as e:
                    logger.error(f"Failed to apply constraint {constraint_name}: {e}")

    def enable_schema_auto_merge(self) -> None:
        """
        Enable schema auto-merge for the current session.
        """
        get_spark().conf.set(SPARK_CONF_AUTO_MERGE, "true")
        logger.info("Enabled schema auto-merge for current session")

    def enable_delta_features(self, table_name: str) -> None:
        """
        Enable multiple Delta features in a single ALTER TABLE call.
        Phase 1 optimization: Batches TBLPROPERTIES to reduce round-trips.

        Features enabled:
        - Deletion Vectors (improves MERGE performance)
        - Predictive Optimization (auto table layout optimization)
        """
        quoted_table_name = quote_table_name(table_name)
        features = [
            "'delta.enableDeletionVectors' = 'true'",
            "'delta.enablePredictiveOptimization' = 'true'",
        ]
        alter_sql = (
            f"ALTER TABLE {quoted_table_name} SET TBLPROPERTIES ({', '.join(features)})"
        )
        get_spark().sql(alter_sql)
        logger.info(f"Delta features enabled for {table_name}")

    def enable_predictive_optimization(self, table_name: str) -> None:
        """
        Enables Predictive Optimization for a Delta table.
        DEPRECATED: Use enable_delta_features() for batched operations.
        """
        quoted_table_name = quote_table_name(table_name)
        alter_sql = f"ALTER TABLE {quoted_table_name} SET TBLPROPERTIES ('delta.enablePredictiveOptimization' = 'true')"
        get_spark().sql(alter_sql)
        logger.info(f"Predictive Optimization enabled for {table_name}")

    def enable_deletion_vectors(self, table_name: str) -> None:
        """
        Enables Deletion Vectors for a Delta table.
        DEPRECATED: Use enable_delta_features() for batched operations.
        """
        quoted_table_name = quote_table_name(table_name)
        alter_sql = f"ALTER TABLE {quoted_table_name} SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')"
        get_spark().sql(alter_sql)
        logger.info(f"Deletion Vectors enabled for {table_name}")
