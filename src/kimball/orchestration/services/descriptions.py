"""YAML-owned Delta table and column descriptions."""

from __future__ import annotations

import json
import logging

from pyspark.sql import SparkSession

from kimball.common.utils import quote_table_name

logger = logging.getLogger(__name__)
_MANIFEST_PROPERTY = "kimball.descriptions.manifest"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class DescriptionManager:
    def _manifest(
        self, table_description: str | None, column_descriptions: dict[str, str]
    ) -> str:
        return json.dumps(
            {
                "table": table_description,
                "columns": dict(sorted(column_descriptions.items())),
            },
            sort_keys=True,
        )

    def sync(
        self,
        spark: SparkSession,
        table_name: str,
        table_description: str | None,
        column_descriptions: dict[str, str],
    ) -> bool:
        available = {field.name for field in spark.table(table_name).schema.fields}
        unknown = sorted(set(column_descriptions).difference(available))
        if unknown:
            raise ValueError(
                f"Description columns do not exist on {table_name}: {unknown}"
            )
        quoted = quote_table_name(table_name)
        previous = self._read_manifest(spark, quoted)
        desired = {
            "table": table_description,
            "columns": dict(sorted(column_descriptions.items())),
        }
        if previous == desired:
            return False
        if previous is None and table_description is None and not column_descriptions:
            return False

        previous_table = previous.get("table") if previous else None
        previous_columns = previous.get("columns", {}) if previous else {}
        if previous_table != table_description:
            value = _sql_literal(table_description) if table_description else "NULL"
            spark.sql(f"COMMENT ON TABLE {quoted} IS {value}")
        for column in sorted(set(previous_columns).union(column_descriptions)):
            old = previous_columns.get(column)
            new = column_descriptions.get(column)
            if old == new:
                continue
            value = _sql_literal(new) if new is not None else "NULL"
            spark.sql(f"ALTER TABLE {quoted} ALTER COLUMN `{column}` COMMENT {value}")
        manifest = self._manifest(table_description, column_descriptions)
        spark.sql(
            f"ALTER TABLE {quoted} SET TBLPROPERTIES ('{_MANIFEST_PROPERTY}' = {_sql_literal(manifest)})"
        )
        return True

    @staticmethod
    def _read_manifest(spark: SparkSession, quoted_table: str) -> dict | None:
        row = spark.sql(
            f"SHOW TBLPROPERTIES {quoted_table} ('{_MANIFEST_PROPERTY}')"
        ).first()
        if row is None:
            return None
        try:
            value = row["value"]
            parsed = json.loads(value)
        except (KeyError, TypeError, json.JSONDecodeError):
            return None
        return parsed if isinstance(parsed, dict) else None
