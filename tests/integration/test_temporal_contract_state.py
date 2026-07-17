"""Real-Delta coverage for durable cross-batch event-time contracts."""

import pytest
from pyspark.sql import SparkSession

from kimball.common.config import SourceConfig
from kimball.observability.temporal_state import TemporalStateStore
from kimball.orchestration.services.contracts import ContractValidator

pytestmark = pytest.mark.usefixtures("spark")


def _source() -> SourceConfig:
    return SourceConfig.model_validate(
        {
            "name": "silver.customer_events",
            "alias": "events",
            "cdc_strategy": "cdf",
            "primary_keys": ["customer_id"],
            "contract": {
                "id": "customer-events",
                "version": "1.0.0",
                "schema": {
                    "customer_id": {"type": "bigint"},
                    "event_at": {"type": "timestamp"},
                },
                "cdc": {"primary_key": ["customer_id"]},
                "temporal": {
                    "event_time_column": "event_at",
                    "allowed_lateness": "3650 days",
                    "out_of_order_severity": "error",
                },
            },
        }
    )


def test_temporal_state_is_replay_safe_and_detects_cross_batch_disorder(
    spark: SparkSession, test_db: str
) -> None:
    source = _source()
    store = TemporalStateStore(spark, test_db)
    first = spark.createDataFrame(
        [(7, "2030-01-02T12:00:00", 10)],
        "customer_id long, event_at string, _commit_version long",
    )
    update = store.prepare(first, pipeline_table="gold.dim_customer", source=source)

    store.commit(update, "first-run")
    store.commit(update, "replay-of-first-run")

    prior = store.existing(
        "gold.dim_customer", "silver.customer_events", "customer-events"
    )
    assert prior.count() == 1
    assert str(prior.first()["max_event_time"]) == "2030-01-02 12:00:00"

    older = spark.createDataFrame(
        [(7, "2030-01-01T12:00:00", 11)],
        "customer_id long, event_at string, _commit_version long",
    )
    findings = ContractValidator(spark).validate_temporal(
        older, source, prior_state=prior
    )
    cross_batch = next(
        finding
        for finding in findings
        if finding.check_name == "cross_batch_event_order"
    )

    assert cross_batch.passed is False
    assert cross_batch.failed_rows == 1
