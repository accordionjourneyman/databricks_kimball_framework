from __future__ import annotations

import decimal
import logging
from datetime import date, datetime, timedelta
from typing import Any

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructType,
    TimestampType,
)

from kimball.common.constants import (
    DEFAULT_MEMBERS,
    DEFAULT_START_DATE,
    DEFAULT_VALID_FROM,
    DEFAULT_VALID_TO,
)
from kimball.common.spark_session import get_spark

logger = logging.getLogger(__name__)


def _to_iso(value: Any) -> Any:
    """Convert datetime/date values to ISO strings for Databricks Connect compatibility.

    Spark Connect serializes Python datetime/date values client-side before
    sending them to the server.  Some Python/datetime combinations raise
    ``OSError: [Errno 22] Invalid argument`` during that serialization.
    Passing ISO strings and letting Spark parse them via the schema is
    safer and works on both local and remote Spark.
    """
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return value


def sql_literal(value: Any) -> str:
    """Render a Python value as a SQL literal for INSERT VALUES.

    Handles strings (single-quote escaped), numbers, booleans, None,
    and datetime/date via ISO rendering.  Used by ``seed_default_rows``
    to bypass Databricks Connect's broken ``createDataFrame`` for
    timestamp columns.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, decimal.Decimal):
        return str(value)
    iso = _to_iso(value)
    if isinstance(iso, str):
        return "'" + iso.replace("'", "''") + "'"
    return str(iso)


def seed_default_rows(
    target_table_name: str,
    schema: StructType,
    surrogate_key: str,
    default_values: dict[str, Any] | None = None,
    include_history_fields: bool = False,
    durable_key: str | None = None,
) -> None:
    spark = get_spark()
    if not spark.catalog.tableExists(target_table_name):
        logger.info(
            f"ensure_defaults: table {target_table_name} does not exist. Skipping."
        )
        return
    rows_to_insert = []
    for key, (status, label) in DEFAULT_MEMBERS.items():
        row: dict[str, Any] = {surrogate_key: key}
        for field in schema.fields:
            cn = field.name
            if cn == surrogate_key:
                continue
            if durable_key and cn == durable_key:
                row[cn] = key
                continue
            if cn == "__member_status":
                row[cn] = status
                continue
            if cn == "__key_origin":
                row[cn] = "default"
                continue
            if include_history_fields and cn == "__is_current":
                row[cn] = True
            elif include_history_fields and cn == "__valid_from":
                row[cn] = DEFAULT_VALID_FROM
            elif include_history_fields and cn == "__valid_to":
                row[cn] = DEFAULT_VALID_TO
            elif cn.startswith("__"):
                dt = field.dataType
                if isinstance(dt, TimestampType):
                    row[cn] = DEFAULT_VALID_FROM
                elif isinstance(dt, DateType):
                    row[cn] = DEFAULT_START_DATE
                elif isinstance(dt, (IntegerType, LongType, ShortType)):
                    row[cn] = key
                elif isinstance(dt, DecimalType):
                    row[cn] = decimal.Decimal(str(key))
                elif isinstance(dt, (DoubleType, FloatType)):
                    row[cn] = float(key)
                elif isinstance(dt, BooleanType):
                    row[cn] = False
                elif isinstance(dt, StringType):
                    row[cn] = label
                else:
                    raise ValueError(
                        f"Default member requires an explicit value for {cn} ({dt})"
                    )
            else:
                if default_values and cn in default_values:
                    row[cn] = default_values[cn]
                else:
                    ds = field.dataType.simpleString()
                    if "string" in ds:
                        row[cn] = label
                    elif "int" in ds or "long" in ds or "short" in ds:
                        row[cn] = key
                    elif "decimal" in ds:
                        row[cn] = decimal.Decimal(str(key))
                    elif "double" in ds or "float" in ds:
                        row[cn] = float(key)
                    elif "timestamp" in ds:
                        row[cn] = DEFAULT_VALID_FROM + timedelta(days=abs(key) - 1)
                    elif "date" in ds:
                        row[cn] = DEFAULT_START_DATE + timedelta(days=abs(key) - 1)
                    elif isinstance(field.dataType, BooleanType):
                        row[cn] = False
                    else:
                        raise ValueError(
                            f"Default member requires an explicit value for {cn} ({field.dataType})"
                        )
        rows_to_insert.append(row)
    if rows_to_insert:
        logger.info(
            f"Seeding {len(rows_to_insert)} default rows into {target_table_name}..."
        )
        # Use Delta MERGE with a temp view to avoid createDataFrame
        # on Databricks Connect (which fails for timestamp columns).
        col_names = [surrogate_key] + [
            f.name for f in schema.fields if f.name != surrogate_key
        ]
        for row in rows_to_insert:
            values = ", ".join(sql_literal(row.get(c)) for c in col_names)
            col_list = ", ".join(f"`{c}`" for c in col_names)
            insert_sql = (
                f"INSERT INTO {target_table_name} ({col_list}) "
                f"SELECT {values} WHERE NOT EXISTS "
                f"(SELECT 1 FROM {target_table_name} WHERE `{surrogate_key}` = {sql_literal(row[surrogate_key])})"
            )
            spark.sql(insert_sql)


def ensure_scd2_defaults(
    target_table_name: str,
    schema: StructType,
    surrogate_key: str,
    default_values: dict[str, Any] | None = None,
    durable_key: str | None = None,
) -> None:
    seed_default_rows(
        target_table_name,
        schema,
        surrogate_key,
        default_values,
        include_history_fields=True,
        durable_key=durable_key,
    )


def ensure_scd1_defaults(
    target_table_name: str,
    schema: StructType,
    surrogate_key: str,
    default_values: dict[str, Any] | None = None,
) -> None:
    seed_default_rows(
        target_table_name,
        schema,
        surrogate_key,
        default_values,
        include_history_fields=False,
    )
