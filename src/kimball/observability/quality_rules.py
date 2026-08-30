"""Centralized data-quality rules store.

Provides a Delta-backed registry of reusable quality rules that can be
shared across multiple tables.  Rules are organized by *layer* (e.g.
``silver``, ``gold``) and *tag* (e.g. ``customer``, ``order``).

Usage::

    store = QualityRuleStore(spark, "etl_schema")
    store.add_rule("silver", "customer", "valid_email", "email IS NOT NULL")

    rules = store.get_rules("silver", "customer")
    # {"valid_email": "email IS NOT NULL"}
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from kimball.common.utils import quote_table_name

logger = logging.getLogger(__name__)


class QualityRuleStore:
    """Append-only registry of reusable data-quality constraints."""

    _TABLE_NAME = "kimball_quality_rules"

    def __init__(self, spark: SparkSession, etl_schema: str) -> None:
        self.spark = spark
        self.full_table = (
            f"{etl_schema}.{self._TABLE_NAME}"
            if "." not in self._TABLE_NAME
            else self._TABLE_NAME
        )
        if "." not in self._TABLE_NAME:
            self.full_table = f"{etl_schema}.{self._TABLE_NAME}"
        else:
            self.full_table = self._TABLE_NAME
        self._ensure_table()

    def _ensure_table(self) -> None:
        quoted = quote_table_name(self.full_table)
        self.spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {quoted} (
                layer       STRING NOT NULL,
                tag         STRING NOT NULL,
                rule_name   STRING NOT NULL,
                `constraint`  STRING NOT NULL,
                severity    STRING NOT NULL DEFAULT 'error',
                enabled     BOOLEAN NOT NULL DEFAULT true,
                created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
            ) USING DELTA
            TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')"""
        )

    def add_rule(
        self,
        layer: str,
        tag: str,
        name: str,
        constraint: str,
        severity: str = "error",
    ) -> None:
        """Insert a quality rule into the store."""
        quoted = quote_table_name(self.full_table)
        safe_layer = layer.replace("'", "''")
        safe_tag = tag.replace("'", "''")
        safe_name = name.replace("'", "''")
        safe_constraint = constraint.replace("'", "''")
        safe_severity = severity.replace("'", "''")
        self.spark.sql(
            f"INSERT INTO {quoted} (layer, tag, rule_name, `constraint`, severity) "
            f"VALUES ('{safe_layer}', '{safe_tag}', '{safe_name}', "
            f"'{safe_constraint}', '{safe_severity}')"
        )
        logger.info("Added quality rule: %s.%s.%s", layer, tag, name)

    def get_rules(self, layer: str, tag: str) -> dict[str, str]:
        """Return ``{rule_name: constraint}`` for a given layer and tag."""
        quoted = quote_table_name(self.full_table)
        safe_layer = layer.replace("'", "''")
        safe_tag = tag.replace("'", "''")
        rows = self.spark.sql(
            f"SELECT rule_name, `constraint` FROM {quoted} "
            f"WHERE layer = '{safe_layer}' AND tag = '{safe_tag}' AND enabled = true"
        ).collect()
        return {row["rule_name"]: row["constraint"] for row in rows}

    def list_rules(self, layer: str | None = None) -> DataFrame:
        """Return all rules, optionally filtered by layer."""
        quoted = quote_table_name(self.full_table)
        if layer:
            safe_layer = layer.replace("'", "''")
            return self.spark.sql(
                f"SELECT * FROM {quoted} WHERE layer = '{safe_layer}' ORDER BY tag, rule_name"
            )
        return self.spark.sql(f"SELECT * FROM {quoted} ORDER BY layer, tag, rule_name")
