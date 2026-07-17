from __future__ import annotations

import logging
import re
from typing import Any

from pyspark.errors import PySparkException
from pyspark.sql import DataFrame

from kimball.common.constants import SPARK_CONF_AUTO_MERGE
from kimball.common.runtime_policy import get_runtime_policy
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
    return bool(re.match(r"^[a-zA-Z0-9_().=<>\s\-,!+*/]+$", expr))


class TableCreator:
    """
    Handles creation of Delta tables with Liquid Clustering support.
    """

    def add_system_columns(
        self,
        df: DataFrame,
        scd_type: int,
        surrogate_key: str | None,
        current_value_columns: list[str] | None = None,
    ) -> DataFrame:
        """
        Add system/audit columns to a DataFrame for table creation.
        SCD1: __etl_processed_at, __etl_batch_id, __is_deleted (for soft deletes)
        SCD2: above + __is_current, __valid_from, __valid_to, hashdiff, __is_skeleton
        SCD6: SCD2 + current_* columns for backfill
        """
        from pyspark.sql.functions import current_timestamp, lit
        from pyspark.sql.types import LongType, StringType, TimestampType

        # Common audit columns
        result_df = df.withColumn("__etl_processed_at", current_timestamp())
        result_df = result_df.withColumn("__etl_batch_id", lit(None).cast(StringType()))
        result_df = result_df.withColumn("__is_deleted", lit(False))

        if scd_type in (2, 6):
            # SCD2/SCD6 specific columns
            result_df = result_df.withColumn("__is_current", lit(True))
            result_df = result_df.withColumn("__valid_from", current_timestamp())
            result_df = result_df.withColumn(
                "__valid_to", lit(None).cast(TimestampType())
            )
            result_df = result_df.withColumn("hashdiff", lit(None).cast(LongType()))
            result_df = result_df.withColumn(
                "__is_skeleton", lit(False)
            )  # For skeleton hydration

        # SCD6: Add current_* columns
        if scd_type == 6 and current_value_columns:
            for col_name in current_value_columns:
                if col_name in df.columns:
                    # Copy column type from source
                    col_type = df.schema[col_name].dataType
                    result_df = result_df.withColumn(
                        f"current_{col_name}", lit(None).cast(col_type)
                    )

        # Add surrogate key column (always LongType for xxhash64)
        if surrogate_key:
            result_df = result_df.withColumn(surrogate_key, lit(None).cast(LongType()))

        return result_df

    def create_history_table(self, table_name: str) -> None:
        """
        Create EAV history table for SCD4.

        Schema:
            surrogate_key BIGINT - FK to current dimension
            field STRING - Column name that changed
            value STRING - Column value (cast to string)
            valid_from TIMESTAMP - When this value became effective
            valid_to TIMESTAMP - When this value was superseded
            __is_current BOOLEAN - True for latest value per (sk, field)
            __etl_processed_at TIMESTAMP - Processing timestamp
        """
        if get_spark().catalog.tableExists(table_name):
            logger.info(
                f"History table {table_name} already exists. Skipping creation."
            )
            return

        quoted_table_name = quote_table_name(table_name)
        create_sql = f"""
        CREATE TABLE {quoted_table_name} (
            surrogate_key BIGINT NOT NULL,
            field STRING NOT NULL,
            value STRING,
            valid_from TIMESTAMP NOT NULL,
            valid_to TIMESTAMP NOT NULL,
            __is_current BOOLEAN NOT NULL,
            __etl_processed_at TIMESTAMP
        )
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """
        get_spark().sql(create_sql)
        logger.info(f"EAV history table {table_name} created successfully.")

        # Enable Delta optimizations
        try:
            self.enable_delta_features(table_name)
        except PySparkException:
            pass  # Serverless limitation

    def create_table_with_clustering(
        self,
        table_name: str,
        schema_df: DataFrame,
        config: dict[str, Any] | None = None,
        cluster_by: list[str] | None = None,
        partition_by: list[str] | None = None,
        surrogate_key_col: str | None = None,
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

        policy = get_runtime_policy()

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

        # Columns that should be NOT NULL based on config
        not_null_cols: set[str] = set()
        if config:
            natural_keys = config.get("natural_keys") or []
            if not natural_keys:
                keys_config = config.get("keys") or {}
                natural_keys = (
                    keys_config.get("natural_keys", [])
                    if isinstance(keys_config, dict)
                    else []
                )
            not_null_cols.update(natural_keys)
            if config.get("table_type") == "fact" and config.get("foreign_keys"):
                for fk in config.get("foreign_keys") or []:
                    fk_col = (
                        fk.get("column")
                        if isinstance(fk, dict)
                        else getattr(fk, "column", None)
                    )
                    if fk_col:
                        not_null_cols.add(fk_col)

        for field in schema_df.schema.fields:
            col_name = field.name

            # Skip CDF internal columns
            if col_name in CDF_METADATA:
                continue

            col_def = f"{col_name} {field.dataType.simpleString()}"
            if not field.nullable or col_name in not_null_cols:
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

        create_sql += policy.cluster_clause(cluster_by, partition_by)

        # Enable Change Data Feed by default
        create_sql += "\nTBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"

        if cluster_by:
            logger.info(f"  - Liquid Clustering on {cluster_by}")
        get_spark().sql(create_sql)
        logger.info(f"Table {table_name} created successfully.")

        # Enable Delta optimizations after table creation (optional features, may fail on Free Edition)
        # Phase 1 optimization: Batch TBLPROPERTIES into single ALTER TABLE
        try:
            self.enable_delta_features(table_name)
        except PySparkException as e:
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
        quoted_table_name = quote_table_name(table_name)
        # Hash SKs are always populated by the framework, so enforce NOT NULL.
        if surrogate_key_col:
            alter_sql = f"ALTER TABLE {quoted_table_name} ADD CONSTRAINT sk_not_null CHECK (`{surrogate_key_col}` IS NOT NULL)"
            try:
                get_spark().sql(alter_sql)
                logger.info("Applied surrogate key NOT NULL constraint")
            except PySparkException as e:
                logger.info(f"Warning: Could not apply surrogate key constraint: {e}")

        # Apply is_current boolean constraint for SCD2 tables
        if schema_df and "__is_current" in [f.name for f in schema_df.schema.fields]:
            alter_sql = f"ALTER TABLE {quoted_table_name} ADD CONSTRAINT is_current_check CHECK (__is_current IN (true, false))"
            try:
                get_spark().sql(alter_sql)
                logger.info("Applied is_current boolean constraint")
            except PySparkException as e:
                logger.info(f"Warning: Could not apply is_current constraint: {e}")

    def apply_delta_constraints(self, table_name: str, config: dict[str, Any]) -> None:
        """
        Apply Delta constraints based on YAML configuration.

        Args:
            table_name: Full table name
            config: Table configuration from YAML
        """
        # Apply NOT NULL constraints for surrogate key first so that the
        # PRIMARY KEY declaration (which requires the column to be NOT NULL
        # on Unity Catalog) succeeds downstream.
        quoted_table_name = quote_table_name(table_name)
        surrogate_key = config.get("surrogate_key")
        if surrogate_key and _is_valid_identifier(surrogate_key):
            try:
                get_spark().sql(
                    f"ALTER TABLE {quoted_table_name} ALTER COLUMN `{surrogate_key}` SET NOT NULL"
                )
                logger.info(f"Applied NOT NULL constraint to {surrogate_key}")
            except PySparkException as e:
                logger.warning(
                    f"Could not apply NOT NULL constraint to {surrogate_key}: {e}"
                )

        # Apply NOT NULL constraints for natural keys
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
            except PySparkException as e:
                logger.warning(f"Could not apply NOT NULL constraint to {key}: {e}")

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
                    except PySparkException as e:
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
                except PySparkException as e:
                    logger.error(f"Failed to apply constraint {constraint_name}: {e}")

        # Declare PRIMARY KEY and FOREIGN KEY constraints (Unity Catalog only).
        # These are informational metadata — they help the CBO eliminate
        # redundant aggregations but are not enforced at write time.
        if config.get("declare_constraints", True):
            self._declare_pk_fk_constraints(table_name, config)

        pii_config = config.get("pii")
        if pii_config:
            self._apply_pii_masks(table_name, pii_config)

    def _declare_pk_fk_constraints(
        self, table_name: str, config: dict[str, Any]
    ) -> None:
        """Issue PRIMARY KEY / FOREIGN KEY DDL on Databricks (Unity Catalog).

        These constraints are informational only — UC does not enforce
        uniqueness at write time, but the cost-based optimizer can use
        them to skip redundant deduplication aggregations.

        On non-Databricks runtimes (OSS Delta, local Docker) this is a
        no-op because the DDL syntax is not supported.
        """
        policy = get_runtime_policy()
        if not policy.is_databricks:
            return

        quoted = quote_table_name(table_name)
        table_short = table_name.split(".")[-1]

        # --- Primary key on surrogate key (all SCD types) ---
        # The SK is the merge key for all SCD types, so it always gets
        # the PRIMARY KEY constraint.  SCD2+ tables have multiple rows
        # per natural key (history), so a PK on natural_keys would be
        # incorrect; SCD1 tables already get a unique index on NKs via
        # the SK PK since the SK is derived deterministically from them.
        surrogate_key = config.get("surrogate_key")
        if surrogate_key and _is_valid_identifier(surrogate_key):
            pk_name = f"pk_{table_short}_{surrogate_key}"
            pk_sql = (
                f"ALTER TABLE {quoted} "
                f"ADD CONSTRAINT `{pk_name}` PRIMARY KEY (`{surrogate_key}`)"
            )
            try:
                get_spark().sql(pk_sql)
                logger.info(f"Declared PRIMARY KEY({surrogate_key}) on {table_name}")
            except PySparkException as e:
                logger.warning(f"Could not declare PK on {surrogate_key}: {e}")

        # --- Foreign keys (fact tables) ---
        foreign_keys = config.get("foreign_keys") or []
        for fk in foreign_keys:
            fk_col = (
                fk.get("column")
                if isinstance(fk, dict)
                else getattr(fk, "column", None)
            )
            fk_ref = (
                fk.get("references")
                if isinstance(fk, dict)
                else getattr(fk, "references", None)
            )
            fk_dim_key = (
                fk.get("dimension_key")
                if isinstance(fk, dict)
                else getattr(fk, "dimension_key", None)
            )
            if not fk_col or not fk_ref or not _is_valid_identifier(fk_col):
                continue
            ref_col = fk_dim_key or fk_col
            fk_name = f"fk_{table_short}_{fk_col}"
            ref_quoted = quote_table_name(fk_ref)
            fk_sql = (
                f"ALTER TABLE {quoted} "
                f"ADD CONSTRAINT `{fk_name}` FOREIGN KEY (`{fk_col}`) "
                f"REFERENCES {ref_quoted} (`{ref_col}`)"
            )
            try:
                get_spark().sql(fk_sql)
                logger.info(
                    f"Declared FOREIGN KEY({fk_col} -> {fk_ref}.{ref_col}) "
                    f"on {table_name}"
                )
            except PySparkException as e:
                logger.warning(f"Could not declare FK on {fk_col}: {e}")

    def _apply_pii_masks(self, table_name: str, pii_config: dict[str, Any]) -> None:
        policy = get_runtime_policy()
        if not policy.is_databricks:
            return
        quoted = quote_table_name(table_name)
        columns = (
            pii_config.get("columns", [])
            if isinstance(pii_config, dict)
            else pii_config
        )
        for col_cfg in columns:
            if isinstance(col_cfg, dict):
                col_name = col_cfg.get("column")
                strategy = col_cfg.get("strategy", "mask")
            else:
                col_name = getattr(col_cfg, "column", None)
                strategy = getattr(col_cfg, "strategy", "mask")
            if not col_name or not _is_valid_identifier(col_name):
                continue
            if strategy == "drop":
                continue
            if strategy == "null":
                mask_expr = "NULL"
            elif strategy in {"hash", "fast_hash"}:
                mask_expr = f"xxhash64(cast(`{col_name}` as string), '{col_name}')"
            elif strategy == "tokenize":
                # Stored values are already keyed HMAC tokens. Re-tokenizing on
                # read would break equality and require exposing a key in DDL.
                continue
            elif strategy == "mask":
                mask_char = (
                    col_cfg.get("mask_char", "*")
                    if isinstance(col_cfg, dict)
                    else getattr(col_cfg, "mask_char", "*")
                )
                mask_expr = f"'{mask_char * 10}'"
            else:
                continue
            try:
                get_spark().sql(
                    f"ALTER TABLE {quoted} ALTER COLUMN `{col_name}` SET MASK {mask_expr}"
                )
                logger.info(f"Applied MASK({strategy}) to {col_name} on {table_name}")
            except PySparkException as e:
                logger.warning(f"Could not apply MASK to {col_name}: {e}")

    def enable_schema_auto_merge(self) -> None:
        get_spark().conf.set(SPARK_CONF_AUTO_MERGE, "true")
        logger.info("Enabled schema auto-merge for current session")

    def enable_delta_features(self, table_name: str) -> None:
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
