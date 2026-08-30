"""ABAC (Attribute-Based Access Control) manager for Unity Catalog.

Creates catalog-, schema-, or table-level row filter and column mask policies that activate
automatically when governed tags are applied to columns.

Usage::

    manager = ABACManager(spark, "main", "my_schema")
    manager.create_policy(row_filter_config)
    manager.apply_tag("customers", "region", "geo_region")
"""

from __future__ import annotations

import logging
import re

from pyspark.sql import SparkSession

from kimball.common.config import ABACPolicyConfig
from kimball.common.utils import quote_table_name

logger = logging.getLogger(__name__)


def _safe_sql_str(value: str) -> str:
    return value.replace("'", "''")


def _quote_identifier_path(value: str, label: str) -> str:
    parts = value.split(".")
    if not value or not all(
        re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in parts
    ):
        raise ValueError(f"Invalid {label}: {value!r}")
    return quote_table_name(value)


class ABACManager:
    """Manage Unity Catalog ABAC policies and governed tags."""

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        schema: str,
        table_name: str | None = None,
    ) -> None:
        self.spark = spark
        self.catalog = catalog
        self.full_schema = f"{catalog}.{schema}"
        self.table_name = table_name

    def create_policy(self, config: ABACPolicyConfig) -> None:
        """Create a UDF and a row filter or column mask policy."""
        # Build the scope and function names before creating the UDF.
        if config.scope == "catalog":
            scope_kind = "CATALOG"
            scope_ref = _quote_identifier_path(self.catalog, "catalog name")
        elif config.scope == "schema":
            scope_kind = "SCHEMA"
            scope_ref = _quote_identifier_path(self.full_schema, "schema name")
        else:
            if not self.table_name:
                raise ValueError("table-scoped ABAC policy requires table_name")
            scope_kind = "TABLE"
            scope_ref = quote_table_name(self.table_name)
        function_name = config.udf_name
        if "." not in function_name:
            function_name = f"{self.full_schema}.{function_name}"
        quoted_function = _quote_identifier_path(function_name, "UDF name")
        if "." in config.function_argument:
            raise ValueError("function_argument must be a simple identifier")
        alias = _quote_identifier_path(config.function_argument, "function argument")
        to_groups = ", ".join(f"`{g.replace('`', '``')}`" for g in config.target_groups)
        safe_tag = _safe_sql_str(config.match_tag)
        match_col = f"has_tag('{safe_tag}')"
        if config.tag_value:
            safe_val = _safe_sql_str(config.tag_value)
            match_col = f"has_tag_value('{safe_tag}', '{safe_val}')"

        self.spark.sql(
            f"CREATE OR REPLACE FUNCTION {quoted_function}({config.function_argument} STRING) "
            f"RETURN {config.udf_body}"
        )
        logger.info("Created ABAC UDF: %s", config.udf_name)
        if config.policy_type == "row_filter":
            policy_fn = "ROW FILTER"
            suffix = f"USING COLUMNS ({alias})"
        elif config.policy_type == "column_mask":
            policy_fn = "COLUMN MASK"
            suffix = f"ON COLUMN {alias} USING COLUMNS ({alias})"
        else:
            logger.warning("Unknown ABAC policy type: %s", config.policy_type)
            return
        sql = (
            f"CREATE OR REPLACE POLICY {config.policy_name} "
            f"ON {scope_kind} {scope_ref} "
            f"{policy_fn} {quoted_function} "
            f"TO ({to_groups}) "
            f"FOR TABLES "
            f"MATCH COLUMNS {match_col} AS {alias} "
            f"{suffix}"
        )

        self.spark.sql(sql)
        logger.info(
            "Created ABAC policy: %s (%s)", config.policy_name, config.policy_type
        )

    def apply_tag(
        self,
        table: str,
        column: str,
        tag: str,
        value: str | None = None,
    ) -> None:
        """Tag a column to activate ABAC policies."""
        quoted_table = quote_table_name(table)
        safe_tag = _safe_sql_str(tag)
        if value:
            safe_val = _safe_sql_str(value)
            tag_expr = f"'{safe_tag}'='{safe_val}'"
        else:
            tag_expr = f"'{safe_tag}'"
        self.spark.sql(
            f"ALTER TABLE {quoted_table} ALTER COLUMN `{column}` SET TAGS({tag_expr})"
        )
        logger.info("Applied tag %s to %s.%s", tag, table, column)

    def drop_policy(self, policy_name: str) -> None:
        """Drop an ABAC policy from the schema."""
        self.spark.sql(f"DROP POLICY {policy_name}")
        logger.info("Dropped ABAC policy: %s", policy_name)
