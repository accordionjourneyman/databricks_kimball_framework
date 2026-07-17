"""Real-Delta coverage for early-arriving-fact operational evidence."""

from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession

from kimball.common.config import ObservabilityConfig
from kimball.orchestration.services.skeleton_manager import SkeletonManager
from kimball.processing.skeleton_generator import SkeletonGenerator

pytestmark = pytest.mark.usefixtures("spark")


def test_skeleton_creation_is_recorded_as_a_durable_dq_event(
    spark: SparkSession, test_db: str
) -> None:
    dimension = f"{test_db}.dim_customer"
    spark.sql(
        f"""CREATE TABLE {dimension} (
            customer_sk BIGINT,
            customer_id BIGINT,
            __is_current BOOLEAN,
            __valid_from TIMESTAMP,
            __valid_to TIMESTAMP,
            __etl_processed_at TIMESTAMP,
            __etl_batch_id STRING,
            __is_skeleton BOOLEAN,
            __skeleton_created_at TIMESTAMP,
            __is_deleted BOOLEAN
        ) USING DELTA"""
    )
    source = SimpleNamespace(name="silver.orders", alias="orders", contract=None)
    config = SimpleNamespace(
        early_arriving_facts=[
            {
                "fact_join_key": "customer_id",
                "dimension_table": dimension,
                "dimension_join_key": "customer_id",
                "surrogate_key_col": "customer_sk",
            }
        ],
        early_arriving_dimensions=[],
        sources=[source],
        effective_at=None,
        table_name=f"{test_db}.fact_orders",
        observability=ObservabilityConfig(event_table="etl_data_quality_events"),
    )
    ctx = SimpleNamespace(
        spark=spark,
        config=config,
        etl_control=SimpleNamespace(schema=test_db),
        batch_id="early-arrival-run",
        source_versions={"silver.orders": 7},
    )
    facts = spark.createDataFrame([(101, 42)], "order_id long, customer_id long")

    SkeletonManager(SkeletonGenerator(spark)).generate_skeletons(
        ctx, {"silver.orders": facts}
    )

    skeleton = spark.table(dimension).where("customer_id = 42").first()
    event = spark.table(f"{test_db}.etl_data_quality_events").first()
    assert skeleton["__is_skeleton"] is True
    assert event["category"] == "early_arriving_fact"
    assert event["action"] == "skeleton_created"
    assert event["failed_rows"] == 1
