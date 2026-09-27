from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from kimball.processing.skeleton_generator import SkeletonGenerator


@pytest.mark.spark
def test_fill_attribute_columns_preserves_skeleton_values_and_schema(spark) -> None:
    dim_schema = StructType(
        [
            StructField("customer_id", LongType(), False),
            StructField("customer_sk", LongType(), False),
            StructField("name", StringType(), True),
            StructField("balance", DecimalType(10, 2), False),
            StructField("active", BooleanType(), False),
            StructField("effective_date", DateType(), False),
            StructField("__is_skeleton", BooleanType(), False),
            StructField("__is_current", BooleanType(), False),
            StructField("__is_deleted", BooleanType(), False),
            StructField("__valid_from", TimestampType(), False),
            StructField("__valid_to", TimestampType(), False),
            StructField("__etl_processed_at", TimestampType(), False),
            StructField("__etl_batch_id", StringType(), False),
            StructField("__skeleton_created_at", TimestampType(), False),
            StructField("__member_status", StringType(), False),
            StructField("__key_origin", StringType(), False),
            StructField("__merge_action", StringType(), True),
        ]
    )
    dim = spark.createDataFrame([], dim_schema)
    skeletons = spark.createDataFrame(
        [(7, -1)],
        StructType(
            [
                StructField("customer_id", LongType(), False),
                StructField("customer_sk", LongType(), False),
            ]
        ),
    )

    result = SkeletonGenerator._fill_attribute_columns(
        skeletons, dim, "customer_id", "customer_sk", "batch-7"
    )
    row = result.first().asDict()

    assert row == {
        "customer_id": 7,
        "customer_sk": -1,
        "name": None,
        "balance": Decimal("0.00"),
        "active": False,
        "effective_date": date(1900, 1, 1),
        "__is_skeleton": True,
        "__is_current": True,
        "__is_deleted": False,
        "__valid_from": datetime(1900, 1, 1),
        "__valid_to": datetime(9999, 12, 31),
        "__etl_processed_at": row["__etl_processed_at"],
        "__etl_batch_id": "batch-7",
        "__skeleton_created_at": row["__skeleton_created_at"],
        "__member_status": "NOT_YET_AVAILABLE",
        "__key_origin": "skeleton",
    }
    assert row["__etl_processed_at"] is not None
    assert row["__skeleton_created_at"] is not None
    assert [field.name for field in result.schema.fields] == [
        field.name for field in dim_schema.fields if field.name != "__merge_action"
    ]
    assert [field.dataType for field in result.schema.fields] == [
        field.dataType for field in dim_schema.fields if field.name != "__merge_action"
    ]
    assert [field.nullable for field in result.schema.fields] == [
        field.nullable for field in dim_schema.fields if field.name != "__merge_action"
    ]
