"""Tests for the StreamCdfLoader."""

from __future__ import annotations

from unittest.mock import MagicMock

from kimball.common.config import StreamingSourceConfig
from kimball.streaming.loader import StreamCdfLoader


def _build_loader() -> tuple[StreamCdfLoader, MagicMock]:
    spark = MagicMock()
    reader = MagicMock()
    spark.readStream.format.return_value = reader
    reader.option.return_value = reader
    reader.table.return_value = "STREAMING_DF"
    return StreamCdfLoader(spark), reader


def test_stream_cdf_sets_read_change_feed_option() -> None:
    loader, reader = _build_loader()
    cfg = StreamingSourceConfig(enabled=True)

    result = loader.stream_cdf("silver.customers", cfg)

    spark_fmt = reader.option.call_args_list
    read_feed = [c for c in spark_fmt if c.args and c.args[0] == "readChangeFeed"]
    assert read_feed, "readChangeFeed option must be set"
    assert read_feed[0].args[1] == "true"
    assert reader.table.call_args.args == ("silver.customers",)
    assert result == "STREAMING_DF"


def test_stream_cdf_passes_starting_version() -> None:
    loader, reader = _build_loader()
    cfg = StreamingSourceConfig(enabled=True, starting_version=42)

    loader.stream_cdf("silver.orders", cfg)

    seen = {c.args[0]: c.args[1] for c in reader.option.call_args_list}
    assert seen.get("startingVersion") == 42
    assert "startingTimestamp" not in seen


def test_stream_cdf_passes_starting_timestamp_when_no_version() -> None:
    loader, reader = _build_loader()
    cfg = StreamingSourceConfig(enabled=True, starting_timestamp="2025-01-01T00:00:00Z")

    loader.stream_cdf("silver.orders", cfg)

    seen = {c.args[0]: c.args[1] for c in reader.option.call_args_list}
    assert seen.get("startingTimestamp") == "2025-01-01T00:00:00Z"
    assert "startingVersion" not in seen


def test_stream_cdf_honours_ignore_flags() -> None:
    loader, reader = _build_loader()
    cfg = StreamingSourceConfig(enabled=True, ignore_deletes=True, ignore_changes=False)

    loader.stream_cdf("silver.orders", cfg)

    seen = {c.args[0]: c.args[1] for c in reader.option.call_args_list}
    assert seen.get("ignoreDeletes") == "true"
    assert seen.get("ignoreChanges") == "false"
