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
    StructField,
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
    return value.strftime("%Y-%m-%d") if isinstance(value, date) else value


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
    return "'" + iso.replace("'", "''") + "'" if isinstance(iso, str) else str(iso)


def seed_default_rows(
    target_table_name: str,
    schema: StructType,
    surrogate_key: str,
    default_values: dict[str, Any] | None = None,
    include_history_fields: bool = False,
    durable_key: str | None = None,
) -> None:
    """Seed the reserved warehouse members into a dimension table (idempotent).

    One row per reserved SK (MISSING / NOT_APPLICABLE / NOT_YET_AVAILABLE /
    BAD_VALUE). Each column is filled from, in priority order: the
    provenance constants, history-field semantics (when
    ``include_history_fields``), system-column type rules, explicit
    ``default_values``, then the user-column type rules. A column with no
    applicable rule raises — a governed default must never guess.
    """
    spark = get_spark()
    if not spark.catalog.tableExists(target_table_name):
        logger.info(
            f"ensure_defaults: table {target_table_name} does not exist. Skipping."
        )
        return
    rows_to_insert = [
        _default_row(
            key,
            status,
            label,
            schema,
            surrogate_key,
            default_values,
            include_history_fields,
            durable_key,
        )
        for key, (status, label) in DEFAULT_MEMBERS.items()
    ]
    if rows_to_insert:
        logger.info(
            f"Seeding {len(rows_to_insert)} default rows into {target_table_name}..."
        )
        for row in rows_to_insert:
            _insert_default_row_if_absent(
                spark, target_table_name, schema, surrogate_key, row
            )


def _default_row(
    key: int,
    status: str,
    label: str,
    schema: StructType,
    surrogate_key: str,
    default_values: dict[str, Any] | None,
    include_history_fields: bool,
    durable_key: str | None,
) -> dict[str, Any]:
    """Build one reserved-member row for *schema*."""
    row: dict[str, Any] = {surrogate_key: key}
    for field in schema.fields:
        cn = field.name
        if cn == surrogate_key:
            continue
        if durable_key and cn == durable_key:
            row[cn] = key
        elif cn == "__member_status":
            row[cn] = status
        elif cn == "__key_origin":
            row[cn] = "default"
        elif cn.startswith("__"):
            row[cn] = _system_column_value(
                cn, field, key, include_history_fields, label
            )
        elif default_values and cn in default_values:
            row[cn] = default_values[cn]
        else:
            row[cn] = _user_column_value(field, key, label, cn)
    return row


def _system_column_value(
    cn: str,
    field: StructField,
    key: int,
    include_history_fields: bool,
    label: str,
) -> Any:
    """Value for a system (``__``-prefixed) column."""
    dt = field.dataType
    if include_history_fields and cn == "__is_current":
        return True
    if include_history_fields and cn == "__valid_from":
        return DEFAULT_VALID_FROM
    if include_history_fields and cn == "__valid_to":
        return DEFAULT_VALID_TO
    if isinstance(dt, TimestampType):
        return DEFAULT_VALID_FROM
    if isinstance(dt, DateType):
        return DEFAULT_START_DATE
    if isinstance(dt, (IntegerType, LongType, ShortType)):
        return key
    if isinstance(dt, DecimalType):
        return decimal.Decimal(str(key))
    if isinstance(dt, (DoubleType, FloatType)):
        return float(key)
    if isinstance(dt, BooleanType):
        return False
    if isinstance(dt, StringType):
        return label
    raise ValueError(f"Default member requires an explicit value for {cn} ({dt})")


def _user_column_value(field: StructField, key: int, label: str, cn: str) -> Any:
    """Value for a user column with no explicit configured default.

    The offset for temporal types keeps sentinel rows ordered: MISSING
    (-1) gets the base date, deeper sentinels get later days — stable,
    human-readable, and never colliding with real rows.
    """
    ds = field.dataType.simpleString()
    dt = field.dataType
    if "string" in ds:
        return label
    if "int" in ds or "long" in ds or "short" in ds:
        return key
    if "decimal" in ds:
        return decimal.Decimal(str(key))
    if "double" in ds or "float" in ds:
        return float(key)
    if "timestamp" in ds:
        return DEFAULT_VALID_FROM + timedelta(days=abs(key) - 1)
    if "date" in ds:
        return DEFAULT_START_DATE + timedelta(days=abs(key) - 1)
    if isinstance(dt, BooleanType):
        return False
    raise ValueError(
        f"Default member requires an explicit value for {cn} ({field.dataType})"
    )


def _insert_default_row_if_absent(
    spark,
    target_table_name: str,
    schema: StructType,
    surrogate_key: str,
    row: dict[str, Any],
) -> None:
    """Guarded INSERT: only when this sentinel SK is not present.

    Uses SQL literals via a SELECT ... WHERE NOT EXISTS instead of
    ``createDataFrame`` on Databricks Connect (which fails for timestamp
    columns).
    """
    col_names = [surrogate_key] + [
        f.name for f in schema.fields if f.name != surrogate_key
    ]
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
