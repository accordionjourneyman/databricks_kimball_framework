from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.common.config import SourceConfig
from kimball.orchestration.services.source_loader import SourceLoader
from kimball.orchestration.services.work_plan import (
    SourceWorkItem,
    SourceWorkPlan,
)
from kimball.processing.loader import DataLoader


def _plan(source, start=0, end=0, active=True):
    return SourceWorkPlan(
        (
            SourceWorkItem(
                source=source,
                prior_watermark=None if start == 0 else start - 1,
                latest_version=end,
                starting_version=start,
                ending_version=end,
                active=active,
                delete_mode="explicit_cdf",
            ),
        )
    )


@pytest.fixture
def ctx():
    mock = MagicMock()
    mock.config = MagicMock()
    mock.config.table_name = "fact_events"
    mock.config.scd_type = 1
    mock.config.preserve_all_changes = False
    mock.loader = MagicMock()
    mock.etl_control = MagicMock()
    return mock


def test_data_loader_passes_version_as_of_to_delta_snapshot_reader():
    spark = MagicMock()
    reader = MagicMock()
    reader.option.return_value = reader
    expected = MagicMock()
    reader.table.return_value = expected
    spark.read.format.return_value = reader

    result = DataLoader(spark).load_full_snapshot("silver.customers", version_as_of=42)

    assert result is expected
    spark.read.format.assert_called_once_with("delta")
    reader.option.assert_called_once_with("versionAsOf", 42)
    reader.table.assert_called_once_with("silver.customers")


class TestAppendStrategy:
    def test_append_strategy_loads_cdf_and_drops_metadata(self, ctx):
        source = MagicMock()
        source.name = "silver.events"
        source.alias = "e"
        source.format = "delta"
        source.cdc_strategy = "append"
        source.starting_version = 0
        source.primary_keys = None
        source.options = {}
        ctx.config.sources = [source]

        df = MagicMock()
        df.columns = ["id", "event_time", "_change_type", "_commit_version"]
        dropped_df = MagicMock()
        df.drop.return_value = dropped_df
        dropped_df.createOrReplaceTempView = MagicMock()
        ctx.loader.load_cdf.return_value = df

        versions, active = SourceLoader().load(ctx, _plan(source, 0, 10))

        ctx.loader.load_cdf.assert_called_once_with(
            "silver.events",
            starting_version=0,
            deduplicate_keys=None,
            ending_version=10,
        )
        df.drop.assert_called_once_with("_change_type", "_commit_version")
        assert versions["silver.events"] == 10
        assert active["silver.events"] is dropped_df

    def test_pinned_repair_snapshot_reads_exact_delta_version(self, ctx):
        source = SourceConfig(
            name="silver.customers",
            alias="customers",
            cdc_strategy="cdf",
            primary_keys=["customer_id"],
        )
        ctx.config.sources = [source]
        frame = MagicMock()
        frame.columns = ["customer_id", "name"]
        frame.withColumn.side_effect = lambda *_: frame
        ctx.loader.load_full_snapshot.return_value = frame
        plan = SourceWorkPlan(
            (
                SourceWorkItem(
                    source=source,
                    prior_watermark=None,
                    latest_version=42,
                    starting_version=None,
                    ending_version=None,
                    active=True,
                    delete_mode="full_snapshot",
                    snapshot_version=42,
                ),
            )
        )

        with patch("pyspark.sql.functions.lit", return_value=MagicMock()):
            versions, active = SourceLoader().load(ctx, plan)

        ctx.loader.load_full_snapshot.assert_called_once_with(
            "silver.customers",
            format="delta",
            options={},
            version_as_of=42,
        )
        assert versions["silver.customers"] == 42
        assert active["silver.customers"] is frame
        assert frame.withColumn.call_count == 3
        frame.createOrReplaceTempView.assert_called_once_with("customers")

    def test_temporal_contract_stages_state_without_committing_it(self) -> None:
        source = SourceConfig.model_validate(
            {
                "name": "silver.events",
                "alias": "events",
                "cdc_strategy": "cdf",
                "primary_keys": ["event_id"],
                "contract": {
                    "id": "events",
                    "version": "1.0.0",
                    "schema": {
                        "event_id": {"type": "bigint"},
                        "event_at": {"type": "timestamp"},
                    },
                    "temporal": {"event_time_column": "event_at"},
                },
            }
        )
        ctx = MagicMock()
        ctx.config.sources = [source]
        ctx.config.table_name = "gold.fact_events"
        ctx.config.scd_type = 1
        ctx.config.preserve_all_changes = False
        ctx.config.observability = None
        ctx.batch_id = "run-7"
        ctx.pending_temporal_state = []
        ctx.validation_metrics = []
        df = MagicMock()
        df.columns = ["event_id", "event_at", "_commit_version"]
        deduplicated = MagicMock()
        ctx.loader.load_cdf.return_value = df
        ctx.loader.deduplicate_cdf.return_value = deduplicated
        ctx.etl_control.get_watermark.return_value = 1

        with (
            patch(
                "kimball.orchestration.services.source_loader.DataQualityEventWriter"
            ),
            patch(
                "kimball.orchestration.services.source_loader.ContractValidator"
            ) as validator,
            patch(
                "kimball.orchestration.services.source_loader.TemporalStateStore"
            ) as store_type,
        ):
            validator.return_value.validate_source.return_value = []
            validator.return_value.validate_temporal.return_value = []
            validator.return_value.last_metrics = {
                "stage": "temporal_contract",
                "duration_ms": 12.5,
                "spark_actions": 1,
            }
            prior = MagicMock(name="prior_state")
            update = MagicMock(name="pending_update")
            store_type.return_value.existing.return_value = prior
            store_type.return_value.prepare.return_value = update

            SourceLoader().load(ctx, _plan(source, 2, 3))

        validator.return_value.validate_temporal.assert_called_once_with(
            df,
            source,
            prior_state=prior,
        )
        assert len(ctx.pending_temporal_state) == 1
        pending = ctx.pending_temporal_state[0]
        assert pending.store is store_type.return_value
        assert pending.dataframe is update
        assert pending.run_id == "run-7"
        store_type.return_value.commit.assert_not_called()
        assert ctx.validation_metrics == [
            {
                **validator.return_value.last_metrics,
                "source_table": "silver.events",
                "pipeline_table": "gold.fact_events",
            }
        ]

    def test_append_strategy_skips_when_watermark_caught_up(self, ctx):
        source = MagicMock()
        source.name = "silver.events"
        source.alias = "e"
        source.format = "delta"
        source.cdc_strategy = "append"
        source.starting_version = 0
        source.primary_keys = None
        source.options = {}
        ctx.config.sources = [source]

        versions, active = SourceLoader().load(ctx, _plan(source, 11, 10, False))

        ctx.loader.load_cdf.assert_not_called()
        ctx.etl_control.batch_complete.assert_not_called()
        assert versions["silver.events"] == 10
        assert active == {}

    def test_append_strategy_loads_incremental_from_watermark(self, ctx):
        source = MagicMock()
        source.name = "silver.events"
        source.alias = "e"
        source.format = "delta"
        source.cdc_strategy = "append"
        source.starting_version = 0
        source.primary_keys = None
        source.options = {}
        ctx.config.sources = [source]

        df = MagicMock()
        df.columns = ["id", "event_time"]
        df.drop.return_value = df
        df.createOrReplaceTempView = MagicMock()
        ctx.loader.load_cdf.return_value = df

        versions, active = SourceLoader().load(ctx, _plan(source, 6, 10))

        ctx.loader.load_cdf.assert_called_once_with(
            "silver.events",
            starting_version=6,
            deduplicate_keys=None,
            ending_version=10,
        )
        assert versions["silver.events"] == 10
