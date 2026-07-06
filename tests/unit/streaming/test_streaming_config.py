"""Tests for the StreamingSourceConfig pydantic model."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from kimball.common.config import SourceConfig, StreamingSourceConfig


def test_streaming_disabled_by_default() -> None:
    src = SourceConfig(name="silver.customers", alias="c")
    assert src.streaming is None


def test_streaming_config_defaults() -> None:
    cfg = StreamingSourceConfig(enabled=True)
    assert cfg.trigger == "available_now"
    assert cfg.trigger_interval == "30 seconds"
    assert cfg.checkpoint_location is None
    assert cfg.ignore_deletes is False
    assert cfg.ignore_changes is False


def test_streaming_invalid_trigger_raises() -> None:
    with pytest.raises(ValidationError):
        StreamingSourceConfig(enabled=True, trigger="not-a-trigger")  # type: ignore[arg-type]


def test_streaming_processing_time_requires_interval() -> None:
    with pytest.raises(ValidationError):
        StreamingSourceConfig(
            enabled=True, trigger="processing_time", trigger_interval=""
        )


def test_source_carries_streaming_subconfig() -> None:
    src = SourceConfig(
        name="silver.customers",
        alias="c",
        streaming=StreamingSourceConfig(
            enabled=True,
            trigger="processing_time",
            trigger_interval="10 seconds",
            checkpoint_location="/tmp/_ckpt",
        ),
    )
    assert src.streaming is not None
    assert src.streaming.enabled is True
    assert src.streaming.trigger_interval == "10 seconds"
    assert src.streaming.checkpoint_location == "/tmp/_ckpt"
