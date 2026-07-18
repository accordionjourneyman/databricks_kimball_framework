from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    TimestampType,
)

from kimball.common.config import NullPolicyConfig
from kimball.common.errors import DataQualityError
from kimball.processing.dimension_nulls import (
    apply_dimension_null_policy,
    replacement_for_type,
)


@pytest.mark.parametrize(
    ("data_type", "expected"),
    [
        (StringType(), "Missing"),
        (IntegerType(), 0),
        (BooleanType(), False),
        (DateType(), date(1900, 1, 1)),
        (TimestampType(), datetime(1900, 1, 1)),
    ],
)
def test_kimball_replacement_is_concrete(data_type, expected) -> None:
    assert replacement_for_type(data_type) == expected


def test_unsupported_dimension_attribute_requires_explicit_substitute() -> None:
    from pyspark.sql.types import ArrayType

    with pytest.raises(ValueError, match="explicit null_policy.attribute_substitutes"):
        replacement_for_type(ArrayType(StringType()))


def test_dimension_null_policy_rejects_missing_or_null_identity() -> None:
    frame = MagicMock()
    frame.columns = ["city"]
    with pytest.raises(DataQualityError, match="identity columns are missing"):
        apply_dimension_null_policy(
            frame,
            NullPolicyConfig(),
            identity_columns=["customer_id"],
        )

    frame.columns = ["customer_id", "city"]
    frame.filter.return_value.limit.return_value.collect.return_value = [object()]
    with patch("kimball.processing.dimension_nulls.F"):
        with pytest.raises(DataQualityError, match="must not contain NULL"):
            apply_dimension_null_policy(
                frame,
                NullPolicyConfig(),
                identity_columns=["customer_id"],
            )


def test_dimension_null_policy_builds_substitutions_once() -> None:
    frame = MagicMock()
    frame.columns = ["customer_id", "city", "score"]
    frame.schema.fields = [
        StructField("customer_id", StringType()),
        StructField("city", StringType()),
        StructField("score", IntegerType()),
    ]
    frame.filter.return_value.limit.return_value.collect.return_value = []
    frame.withColumn.return_value = frame

    with patch("kimball.processing.dimension_nulls.F"):
        result = apply_dimension_null_policy(
            frame,
            NullPolicyConfig(attribute_substitutes={"city": "Unknown"}),
            identity_columns=["customer_id"],
        )

    assert result is frame
    assert frame.withColumn.call_count == 2


def test_legacy_null_policy_is_an_explicit_noop() -> None:
    frame = MagicMock()
    assert (
        apply_dimension_null_policy(
            frame,
            NullPolicyConfig(mode="legacy"),
            identity_columns=["customer_id"],
        )
        is frame
    )
