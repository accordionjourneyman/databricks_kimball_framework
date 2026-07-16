from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from kimball.orchestration.services.context import PipelineContext
from kimball.orchestration.services.source_loader import SourceLoader


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
        ctx.loader.get_latest_version.return_value = 10
        ctx.loader.load_cdf.return_value = df
        ctx.etl_control.get_watermark.return_value = None

        versions, active = SourceLoader().load(ctx)

        ctx.loader.load_cdf.assert_called_once_with(
            "silver.events",
            starting_version=0,
            deduplicate_keys=None,
            ending_version=10,
        )
        df.drop.assert_called_once_with("_change_type", "_commit_version")
        assert versions["silver.events"] == 10
        assert active["silver.events"] is dropped_df

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

        ctx.loader.get_latest_version.return_value = 10
        ctx.etl_control.get_watermark.return_value = 10

        versions, active = SourceLoader().load(ctx)

        ctx.loader.load_cdf.assert_not_called()
        ctx.etl_control.batch_complete.assert_called_once_with(
            "fact_events", "silver.events", new_version=10, rows_read=0, rows_written=0
        )
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
        ctx.loader.get_latest_version.return_value = 10
        ctx.loader.load_cdf.return_value = df
        ctx.etl_control.get_watermark.return_value = 5

        versions, active = SourceLoader().load(ctx)

        ctx.loader.load_cdf.assert_called_once_with(
            "silver.events",
            6,
            deduplicate_keys=None,
            ending_version=10,
        )
        assert versions["silver.events"] == 10
