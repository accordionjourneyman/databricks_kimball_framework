from __future__ import annotations

from unittest.mock import MagicMock

from kimball.observability.temporal_state import (
    PendingTemporalState,
    TemporalStateStore,
    commit_temporal_state_updates,
)
from kimball.orchestration.services.context import PipelineContext


def test_temporal_state_table_is_a_durable_delta_contract() -> None:
    spark = MagicMock()

    TemporalStateStore(spark, "ops", "etl_contract_temporal_state")

    ddl = spark.sql.call_args.args[0]
    assert "CREATE TABLE IF NOT EXISTS `ops`.`etl_contract_temporal_state`" in ddl
    assert "business_key_hash STRING NOT NULL" in ddl
    assert "max_event_time TIMESTAMP NOT NULL" in ddl
    assert "max_commit_version LONG" in ddl


def test_temporal_state_updates_are_committed_only_when_explicitly_finalized() -> None:
    store = MagicMock()
    pending = PendingTemporalState(store=store, dataframe=MagicMock(), run_id="run-7")
    ctx = MagicMock(spec=PipelineContext)
    ctx.pending_temporal_state = [pending]

    commit_temporal_state_updates(ctx)

    store.commit.assert_called_once_with(pending.dataframe, "run-7")
    assert ctx.pending_temporal_state == []


def test_temporal_state_commit_is_replay_safe_and_monotonic() -> None:
    spark = MagicMock()
    store = TemporalStateStore(spark, "ops", "etl_contract_temporal_state")
    update = MagicMock()

    store.commit(update, "run-7")

    update.createOrReplaceTempView.assert_called_once()
    merge_sql = spark.sql.call_args.args[0]
    assert "source.max_event_time > target.max_event_time" in merge_sql
    assert "source.max_commit_version > target.max_commit_version" in merge_sql
    assert "WHEN NOT MATCHED THEN INSERT" in merge_sql


def test_pipeline_context_starts_without_pending_temporal_state() -> None:
    assert "pending_temporal_state" in PipelineContext.__dataclass_fields__
    assert (
        PipelineContext.__dataclass_fields__["pending_temporal_state"].default_factory()
        == []
    )
    assert (
        PipelineContext.__dataclass_fields__["validation_metrics"].default_factory()
        == []
    )
