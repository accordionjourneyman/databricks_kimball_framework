from __future__ import annotations

import logging
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql.types import (
    BooleanType, DateType, DecimalType, DoubleType, FloatType,
    IntegerType, LongType, ShortType, StructType, TimestampType,
)

from kimball.common.constants import (
    DEFAULT_START_DATE, DEFAULT_VALID_FROM, DEFAULT_VALID_TO,
)
from kimball.common.spark_session import get_spark

logger = logging.getLogger(__name__)


def seed_default_rows(
    target_table_name: str,
    schema: StructType,
    surrogate_key: str,
    default_values: dict[str, Any] | None = None,
    include_history_fields: bool = False,
) -> None:
    spark = get_spark()
    if not spark.catalog.tableExists(target_table_name):
        logger.info(f"ensure_defaults: table {target_table_name} does not exist. Skipping.")
        return
    delta_table = DeltaTable.forName(spark, target_table_name)
    standard_defaults = {-1: "Unknown", -2: "Not Applicable", -3: "Error"}
    rows_to_insert = []
    for key, label in standard_defaults.items():
        row: dict[str, Any] = {surrogate_key: key}
        for field in schema.fields:
            cn = field.name
            if cn == surrogate_key:
                continue
            if include_history_fields and cn == "__is_current":
                row[cn] = True
            elif include_history_fields and cn == "__valid_from":
                row[cn] = DEFAULT_VALID_FROM
            elif include_history_fields and cn == "__valid_to":
                row[cn] = DEFAULT_VALID_TO
            elif cn.startswith("__"):
                if not field.nullable:
                    dt = field.dataType
                    if isinstance(dt, TimestampType):
                        row[cn] = DEFAULT_VALID_FROM
                    elif isinstance(dt, DateType):
                        row[cn] = DEFAULT_START_DATE
                    elif isinstance(dt, (IntegerType, LongType, ShortType)):
                        row[cn] = -1
                    elif isinstance(dt, DecimalType):
                        from decimal import Decimal
                        row[cn] = Decimal("-1.0")
                    elif isinstance(dt, (DoubleType, FloatType)):
                        row[cn] = -1.0
                    elif isinstance(dt, BooleanType):
                        row[cn] = False
                    else:
                        row[cn] = ""
                else:
                    row[cn] = None
            else:
                if default_values and cn in default_values:
                    row[cn] = default_values[cn]
                else:
                    ds = field.dataType.simpleString()
                    if "string" in ds:
                        row[cn] = label
                    elif "int" in ds or "long" in ds or "short" in ds:
                        row[cn] = -1
                    elif "decimal" in ds:
                        from decimal import Decimal
                        row[cn] = Decimal("-1.0")
                    elif "double" in ds or "float" in ds:
                        row[cn] = -1.0
                    elif "timestamp" in ds:
                        row[cn] = DEFAULT_VALID_FROM
                    elif "date" in ds:
                        row[cn] = DEFAULT_START_DATE
                    else:
                        row[cn] = None
        rows_to_insert.append(row)
    if rows_to_insert:
        logger.info(f"Seeding {len(rows_to_insert)} default rows into {target_table_name}...")
        df = spark.createDataFrame(rows_to_insert, schema)
        delta_table.alias("target").merge(
            df.alias("source"), f"target.{surrogate_key} = source.{surrogate_key}"
        ).whenNotMatchedInsertAll().execute()


def ensure_scd2_defaults(
    target_table_name: str,
    schema: StructType,
    surrogate_key: str,
    default_values: dict[str, Any] | None = None,
) -> None:
    seed_default_rows(target_table_name, schema, surrogate_key, default_values, include_history_fields=True)


def ensure_scd1_defaults(
    target_table_name: str,
    schema: StructType,
    surrogate_key: str,
    default_values: dict[str, Any] | None = None,
) -> None:
    seed_default_rows(target_table_name, schema, surrogate_key, default_values, include_history_fields=False)