from __future__ import annotations

import decimal
from datetime import date, datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
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
    TimestampType,
)

from kimball.common.config import NullPolicyConfig
from kimball.common.errors import DataQualityError


def replacement_for_type(data_type: Any) -> Any:
    """Return the deterministic Kimball substitute for a nullable attribute."""
    if isinstance(data_type, StringType):
        return "Missing"
    if isinstance(data_type, (IntegerType, LongType, ShortType)):
        return 0
    if isinstance(data_type, DecimalType):
        return decimal.Decimal("0")
    if isinstance(data_type, (DoubleType, FloatType)):
        return 0.0
    if isinstance(data_type, BooleanType):
        return False
    if isinstance(data_type, TimestampType):
        return datetime(1900, 1, 1)
    if isinstance(data_type, DateType):
        return date(1900, 1, 1)
    raise ValueError(
        f"{data_type} requires an explicit null_policy.attribute_substitutes value"
    )


def apply_dimension_null_policy(
    df: DataFrame,
    policy: NullPolicyConfig,
    *,
    identity_columns: list[str],
) -> DataFrame:
    """Reject null identities and replace null descriptive attributes once."""
    if policy.mode == "legacy":
        return df
    missing = [name for name in identity_columns if name not in df.columns]
    if missing:
        raise DataQualityError(
            "Dimension identity columns are missing: " + ", ".join(sorted(missing))
        )
    invalid_identity = F.lit(False)
    for name in identity_columns:
        invalid_identity = invalid_identity | F.col(name).isNull()
    if df.filter(invalid_identity).limit(1).collect():
        raise DataQualityError(
            "Dimension identity and effective-time columns must not contain NULL"
        )

    result = df
    identity = set(identity_columns)
    substitutes = policy.attribute_substitutes
    for field in df.schema.fields:
        if field.name in identity or field.name.startswith("_"):
            continue
        replacement = (
            substitutes[field.name]
            if field.name in substitutes
            else replacement_for_type(field.dataType)
        )
        result = result.withColumn(
            field.name,
            F.coalesce(F.col(field.name), F.lit(replacement).cast(field.dataType)),
        )
    return result
