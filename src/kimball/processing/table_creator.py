from __future__ import annotations

import logging
from typing import Any

from pyspark.errors import PySparkException
from pyspark.sql import DataFrame

from kimball.common.constants import DEFAULT_VALID_TO
from kimball.common.runtime_policy import get_runtime_policy
from kimball.common.spark_session import get_spark
from kimball.common.utils import quote_table_name
from kimball.processing.ddl import (
    CDF_METADATA_COLUMNS,
    ColumnSpec,
    is_safe_sql_data_type,
    is_safe_sql_expression,
    is_valid_identifier,
    serialize_columns,
)

logger = logging.getLogger(__name__)


def _is_valid_identifier(name: str) -> bool:
    """Validate that a name is a safe SQL identifier (delegates to ddl)."""
    return is_valid_identifier(name)


def _is_safe_sql_expression(expr: str) -> bool:
    """Whitelist check for SQL expressions (delegates to ddl)."""
    return is_safe_sql_expression(expr)


def _is_safe_sql_data_type(data_type: str) -> bool:
    """Whitelist check for Delta data types (delegates to ddl)."""
    return is_safe_sql_data_type(data_type)


class TableCreator:
    """
    Handles creation of Delta tables with Liquid Clustering support.
    """

    def add_system_columns(
        self,
        df: DataFrame,
        scd_type: int,
        surrogate_key: str | None,
        durable_key: str | None = None,
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
        result_df = result_df.withColumn("__etl_batch_id", lit("").cast(StringType()))
        result_df = result_df.withColumn("__is_deleted", lit(False))

        if scd_type in {2, 6, 7}:
            # SCD2/SCD6 specific columns
            result_df = result_df.withColumn("__is_current", lit(True))
            result_df = result_df.withColumn("__valid_from", current_timestamp())
            result_df = result_df.withColumn(
                "__valid_to", lit(DEFAULT_VALID_TO).cast(TimestampType())
            )
            result_df = result_df.withColumn("hashdiff", lit(-1).cast(LongType()))
            result_df = result_df.withColumn(
                "__is_skeleton", lit(False)
            )  # For skeleton hydration
            result_df = result_df.withColumn("__member_status", lit("REAL"))
            result_df = result_df.withColumn("__key_origin", lit("generated"))

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
        if durable_key:
            result_df = result_df.withColumn(durable_key, lit(None).cast(LongType()))
            result_df = result_df.withColumn(
                "__durable_key_fingerprint", lit("").cast(StringType())
            )
            result_df = result_df.withColumn(
                "__row_key_fingerprint", lit("").cast(StringType())
            )

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
        surrogate_key_col: str | None = None,
    ) -> None:
        """
        Creates a Delta table with optional Liquid Clustering.

        Args:
            table_name: Full table name (catalog.schema.table)
            schema_df: DataFrame with the desired schema
            config: Validated table configuration from YAML
            surrogate_key_col: Name of the surrogate key column (if any)
        """
        if get_spark().catalog.tableExists(table_name):
            logger.info(f"Table {table_name} already exists. Skipping creation.")
            if config:
                self._apply_governance(table_name, config)
            return

        config = config or {}
        cluster_by = self._validated_cluster_columns(config.get("cluster_by") or [])
        if cluster_by:
            logger.info(f"Using Liquid Clustering from config: {cluster_by}")

        policy = get_runtime_policy()
        columns_sql = self._build_column_definitions(schema_df, config)
        quoted_table_name = quote_table_name(table_name)

        create_sql = f"""
        CREATE TABLE {quoted_table_name} (
          {columns_sql}
        )
        USING DELTA
        """

        create_sql += policy.cluster_clause(cluster_by)

        # Enable Change Data Feed by default
        create_sql += "\nTBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"

        if cluster_by:
            logger.info(f"  - Liquid Clustering on {cluster_by}")
        get_spark().sql(create_sql)
        logger.info(f"Table {table_name} created successfully.")

        self._enable_delta_features_tolerant(table_name)

        # Apply basic Delta constraints after table creation
        self.apply_basic_constraints(table_name, surrogate_key_col, schema_df)

        # Apply additional constraints and governance from config
        if config:
            self.apply_delta_constraints(table_name, config)
            self._apply_governance(table_name, config)

    @staticmethod
    def _validated_cluster_columns(cluster_by: list[str]) -> list[str]:
        """Validate and backtick-quote clustering columns (anti-injection)."""
        formatted = []
        for col_name in cluster_by:
            if not _is_valid_identifier(col_name):
                raise ValueError(f"Invalid clustering column name: {col_name}")
            formatted.append(f"`{col_name}`")
        return formatted

    @staticmethod
    def _build_column_definitions(schema_df: DataFrame, config: dict[str, Any]) -> str:
        """Build the CREATE TABLE column list as typed ColumnSpecs.

        CDF metadata columns are internal to Delta Lake's CDF feature and
        never part of the table schema. NOT NULL comes from three sources:
        non-nullable fields, natural keys, and (for dimensions under the
        kimball null policy) every column. Generated columns are appended
        and validated fail-closed by ``ColumnSpec.validate``.
        """
        not_null_cols = TableCreator._collect_not_null_columns(schema_df, config)
        column_specs = [
            ColumnSpec(
                name=field.name,
                data_type=field.dataType.simpleString(),
                not_null=not field.nullable or field.name in not_null_cols,
            )
            for field in schema_df.schema.fields
            if field.name not in CDF_METADATA_COLUMNS
        ]
        column_specs.extend(TableCreator._generated_column_specs(schema_df, config))
        return serialize_columns(column_specs)

    @staticmethod
    def _collect_not_null_columns(
        schema_df: DataFrame, config: dict[str, Any]
    ) -> set[str]:
        not_null_cols: set[str] = set()
        if not config:
            return not_null_cols
        natural_keys = config.get("natural_keys") or []
        if not natural_keys:
            keys_config = config.get("keys") or {}
            natural_keys = (
                keys_config.get("natural_keys", [])
                if isinstance(keys_config, dict)
                else []
            )
        not_null_cols.update(natural_keys)
        null_policy = config.get("null_policy") or {}
        if (
            config.get("table_type") == "dimension"
            and null_policy.get("mode", "kimball") == "kimball"
        ):
            not_null_cols.update(
                field.name
                for field in schema_df.schema.fields
                if field.name not in CDF_METADATA_COLUMNS
            )
        if config.get("table_type") == "fact" and config.get("foreign_keys"):
            for fk in config.get("foreign_keys") or []:
                fk_col = (
                    fk.get("column")
                    if isinstance(fk, dict)
                    else getattr(fk, "column", None)
                )
                if fk_col:
                    not_null_cols.add(fk_col)
                durable_col = (
                    fk.get("durable_column")
                    if isinstance(fk, dict)
                    else getattr(fk, "durable_column", None)
                )
                if durable_col:
                    not_null_cols.add(durable_col)
        return not_null_cols

    @staticmethod
    def _generated_column_specs(
        schema_df: DataFrame, config: dict[str, Any]
    ) -> list[ColumnSpec]:
        """Typed specs for generated columns; must not shadow input columns."""
        generated_cols = (config or {}).get("generated_columns") or {}
        if not generated_cols:
            return []
        schema_columns = {field.name for field in schema_df.schema.fields}
        specs: list[ColumnSpec] = []
        for gen_col, definition in generated_cols.items():
            if not is_valid_identifier(gen_col):
                raise ValueError(f"Invalid generated column name: {gen_col}")
            if gen_col in schema_columns:
                raise ValueError(
                    f"Generated column {gen_col} must not also be present in the input schema"
                )
            if isinstance(definition, dict):
                gen_expr = definition.get("expression")
                data_type = definition.get("data_type")
            else:
                gen_expr = getattr(definition, "expression", None)
                data_type = getattr(definition, "data_type", None)
            if not isinstance(gen_expr, str) or not isinstance(data_type, str):
                raise ValueError(
                    f"Generated column {gen_col} must define expression and data_type"
                )
            specs.append(
                ColumnSpec(
                    name=gen_col,
                    data_type=data_type,
                    generated_expression=gen_expr,
                )
            )
        return specs

    def _enable_delta_features_tolerant(self, table_name: str) -> None:
        """Enable Delta optimizations, tolerating edition limitations.

        Optional features may fail on Free/Serverless editions; recognized
        limitation errors are suppressed, unknown errors log the first
        line only (not the full JVM trace).
        """
        try:
            self.enable_delta_features(table_name)
        except PySparkException as e:
            error_str = str(e).lower()
            if all(
                x not in error_str
                for x in [
                    "not supported",
                    "premium",
                    "serverless",
                    "not enabled",
                    "unknown configuration",
                    "delta_unknown_configuration",
                ]
            ):
                first_line = str(e).split("\n")[0][:200]
                logger.info(f"Warning: Delta features failed: {first_line}")

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

    @staticmethod
    def _field_value(item: Any, field: str) -> Any:
        return item.get(field) if isinstance(item, dict) else getattr(item, field, None)

    def _execute_ddl(
        self,
        sql: str,
        *,
        success: str,
        failure: str,
        failure_level: str = "warning",
    ) -> bool:
        try:
            get_spark().sql(sql)
        except PySparkException as exc:
            getattr(logger, failure_level)("%s: %s", failure, exc)
            return False
        logger.info(success)
        return True

    def _not_null_columns(self, config: dict[str, Any]) -> list[str]:
        candidates = [
            config.get("surrogate_key"),
            config.get("durable_key"),
            *(config.get("natural_keys") or []),
        ]
        return list(dict.fromkeys(column for column in candidates if column))

    def _foreign_key_columns(self, config: dict[str, Any]) -> list[str]:
        columns: list[str] = []
        for foreign_key in config.get("foreign_keys") or []:
            columns.extend(
                column
                for field in ("column", "durable_column")
                if (column := self._field_value(foreign_key, field))
            )
        return list(dict.fromkeys(columns))

    def apply_delta_constraints(self, table_name: str, config: dict[str, Any]) -> None:
        """Apply validated Delta constraints and governance configuration."""
        quoted_table_name = quote_table_name(table_name)

        for column in self._not_null_columns(config):
            if not _is_valid_identifier(column):
                logger.info("Skipping invalid NOT NULL column name: %s", column)
                continue
            self._execute_ddl(
                f"ALTER TABLE {quoted_table_name} ALTER COLUMN `{column}` SET NOT NULL",
                success=f"Applied NOT NULL constraint to {column}",
                failure=f"Could not apply NOT NULL constraint to {column}",
            )

        if config.get("table_type") == "fact":
            for column in self._foreign_key_columns(config):
                if not _is_valid_identifier(column):
                    logger.info("Skipping invalid FK column name: %s", column)
                    continue
                constraint_name = f"fk_{column}_not_null"
                self._execute_ddl(
                    f"ALTER TABLE {quoted_table_name} ADD CONSTRAINT `{constraint_name}` "
                    f"CHECK (`{column}` IS NOT NULL)",
                    success=f"Applied FK NOT NULL constraint: {constraint_name}",
                    failure=f"Could not apply FK constraint {constraint_name}",
                    failure_level="info",
                )

        for constraint in config.get("constraints") or []:
            name = self._field_value(constraint, "name")
            expression = self._field_value(constraint, "expression")
            if not name or not expression:
                continue
            if not _is_valid_identifier(name):
                logger.info("Skipping invalid constraint name: %s", name)
                continue
            if not _is_safe_sql_expression(expression):
                raise ValueError(
                    f"Invalid characters in constraint expression: {expression}"
                )
            self._execute_ddl(
                f"ALTER TABLE {quoted_table_name} ADD CONSTRAINT `{name}` CHECK ({expression})",
                success=f"Applied constraint {name}",
                failure=f"Failed to apply constraint {name}",
                failure_level="error",
            )

        if config.get("declare_constraints", True):
            self._declare_pk_fk_constraints(table_name, config)
        if pii_config := config.get("pii"):
            self._apply_pii_masks(table_name, pii_config)

    def _declare_pk_fk_constraints(
        self, table_name: str, config: dict[str, Any]
    ) -> None:
        """Issue PRIMARY KEY / FOREIGN KEY DDL on Databricks (Unity Catalog).

        These constraints are informational only ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â UC does not enforce
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
            fk_col = fk.get("column")
            fk_ref = fk.get("references")
            fk_dim_key = fk.get("dimension_key")
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
            elif strategy == "fast_hash":
                mask_expr = f"xxhash64(cast(`{col_name}` as string), '{col_name}')"
            elif strategy == "tokenize" or strategy != "mask":
                # Stored values are already keyed HMAC tokens. Re-tokenizing on
                # read would break equality and require exposing a key in DDL.
                continue
            else:
                mask_char = (
                    col_cfg.get("mask_char", "*")
                    if isinstance(col_cfg, dict)
                    else getattr(col_cfg, "mask_char", "*")
                )
                mask_expr = f"'{mask_char * 10}'"
            try:
                get_spark().sql(
                    f"ALTER TABLE {quoted} ALTER COLUMN `{col_name}` SET MASK {mask_expr}"
                )
                logger.info(f"Applied MASK({strategy}) to {col_name} on {table_name}")
            except PySparkException as e:
                logger.warning(f"Could not apply MASK to {col_name}: {e}")

    def _apply_row_filter(self, table_name: str, rf_config: dict[str, Any]) -> None:
        """Apply Unity Catalog ROW FILTER via ``ALTER TABLE SET ROW FILTER``."""
        policy = get_runtime_policy()
        if not policy.is_databricks:
            raise RuntimeError(
                "row_filter is configured but Unity Catalog row filters require Databricks"
            )
        quoted = quote_table_name(table_name)
        func_name = rf_config["function_name"]
        func_body = rf_config["function_body"]
        column = rf_config["column"]
        if not _is_valid_identifier(func_name) or not _is_valid_identifier(column):
            raise ValueError("Invalid row filter function or column name")
        try:
            get_spark().sql(
                f"CREATE OR REPLACE FUNCTION {func_name}(region_param STRING) "
                f"RETURN {func_body}"
            )
            logger.info(f"Created row filter function {func_name}")
            get_spark().sql(
                f"ALTER TABLE {quoted} SET ROW FILTER {func_name} ON ({column})"
            )
            logger.info(f"Applied ROW FILTER {func_name} on {table_name}({column})")
            for group in rf_config.get("grant_to") or []:
                if _is_valid_identifier(group):
                    get_spark().sql(
                        f"GRANT ALL PRIVILEGES ON FUNCTION {func_name} TO `{group}`"
                    )
                    logger.info(f"Granted ROW FILTER function to {group}")
        except PySparkException as e:
            raise RuntimeError(f"Could not apply configured ROW FILTER: {e}") from e

    def _apply_governance(self, table_name: str, config: dict[str, Any]) -> None:
        """Apply configured security controls or fail the deployment closed."""
        if row_filter := config.get("row_filter"):
            self._apply_row_filter(table_name, row_filter)

        abac_policies = config.get("abac_policies") or []
        if not abac_policies:
            return
        if not get_runtime_policy().is_databricks:
            raise RuntimeError(
                "abac_policies are configured but Unity Catalog ABAC requires Databricks"
            )
        parts = table_name.split(".")
        if len(parts) != 3:
            raise ValueError(
                "abac_policies require table_name to be catalog.schema.table"
            )
        from kimball.common.config import ABACPolicyConfig
        from kimball.governance.abac import ABACManager

        catalog, schema, _ = parts
        manager = ABACManager(get_spark(), catalog, schema, table_name=table_name)
        for policy in abac_policies:
            manager.create_policy(ABACPolicyConfig.model_validate(policy))

    def enable_delta_features(self, table_name: str) -> None:
        quoted_table_name = quote_table_name(table_name)
        features = [
            "'delta.enableDeletionVectors' = 'true'",
            "'delta.enablePredictiveOptimization' = 'true'",
            "'delta.autoOptimize.optimizeWrite' = 'true'",
            "'delta.autoOptimize.autoCompact' = 'true'",
        ]
        alter_sql = (
            f"ALTER TABLE {quoted_table_name} SET TBLPROPERTIES ({', '.join(features)})"
        )
        get_spark().sql(alter_sql)
        logger.info(f"Delta features enabled for {table_name}")
