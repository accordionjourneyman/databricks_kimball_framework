from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
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
    projected = MagicMock()
    frame.select.return_value = projected

    with patch("kimball.processing.dimension_nulls.F"):
        result = apply_dimension_null_policy(
            frame,
            NullPolicyConfig(attribute_substitutes={"city": "Unknown"}),
            identity_columns=["customer_id"],
        )

    assert result is projected
    frame.withColumn.assert_not_called()
    frame.select.assert_called_once()
    assert len(frame.select.call_args.args) == len(frame.schema.fields)


@pytest.mark.spark
def test_dimension_null_policy_projection_preserves_values_types_and_metadata(
    spark,
) -> None:
    from pyspark.sql.types import StructType

    schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField("name", StringType(), True, {"source": "fixture"}),
            StructField("quantity", IntegerType(), True),
            StructField("active", BooleanType(), True),
            StructField("launch_date", DateType(), True),
            StructField("_private", StringType(), True),
        ]
    )
    frame = spark.createDataFrame(
        [
            (1, None, None, None, None, None),
            (2, "kept", 3, True, date(2024, 2, 3), None),
        ],
        schema,
    )

    result = apply_dimension_null_policy(
        frame,
        NullPolicyConfig(attribute_substitutes={"name": "Unknown"}),
        identity_columns=["id"],
    )

    assert [row.asDict() for row in result.orderBy("id").collect()] == [
        {
            "id": 1,
            "name": "Unknown",
            "quantity": 0,
            "active": False,
            "launch_date": date(1900, 1, 1),
            "_private": None,
        },
        {
            "id": 2,
            "name": "kept",
            "quantity": 3,
            "active": True,
            "launch_date": date(2024, 2, 3),
            "_private": None,
        },
    ]
    assert result.schema["name"].metadata == {"source": "fixture"}
    assert [
        (field.name, field.dataType, field.nullable) for field in result.schema.fields
    ] == [
        ("id", LongType(), False),
        ("name", StringType(), False),
        ("quantity", IntegerType(), False),
        ("active", BooleanType(), False),
        ("launch_date", DateType(), False),
        ("_private", StringType(), True),
    ]
