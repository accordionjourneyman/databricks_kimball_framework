"""Tests for the default-checkpoint-path resolver."""

from __future__ import annotations

import os

from kimball.streaming.checkpoint import default_checkpoint_path


def test_default_root_used(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KIMBALL_STREAMING_CHECKPOINT_ROOT", raising=False)
    path = default_checkpoint_path("silver.customers")
    # The hard-coded default root on every platform is "/tmp/kimball_streaming_checkpoints".
    assert "/tmp/kimball_streaming_checkpoints" in path.replace("\\", "/")
    assert "silver.customers" in path


def test_default_with_etl_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KIMBALL_STREAMING_CHECKPOINT_ROOT", "/ckpt")
    path = default_checkpoint_path("main.silver.orders", etl_schema="etl")
    assert path == os.path.join("/ckpt", "etl__main.silver.orders")


def test_default_sanitises_unsafe_characters() -> None:
    path = default_checkpoint_path("weird/path with spaces", root="/x")
    assert " " not in path
    assert "/" not in os.path.basename(path)


def test_root_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KIMBALL_STREAMING_CHECKPOINT_ROOT", "/env/root")
    root_override = os.path.join("/override", "t")
    assert default_checkpoint_path("t", root="/override").startswith(root_override)
    assert default_checkpoint_path("t").startswith("/env/root")
