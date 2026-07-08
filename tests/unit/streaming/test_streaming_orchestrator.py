"""Tests for StreamingOrchestrator dispatch and lifecycle."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.common.config import SourceConfig, StreamingSourceConfig, TableConfig
from kimball.streaming.orchestrator import StreamingOrchestrator


def _make_config(streaming_enabled: bool) -> TableConfig:
    src = SourceConfig(
        name="silver.customers",
        alias="c",
        cdc_strategy="cdf",
        primary_keys=["customer_id"],
        streaming=StreamingSourceConfig(enabled=streaming_enabled),
    )
    return TableConfig(
        table_name="gold.dim_customer",
        table_type="dimension",
        scd_type=2,
        keys={"surrogate_key": "customer_sk", "natural_keys": ["customer_id"]},
        surrogate_key="customer_sk",
        sources=[src],
    )


def test_is_streaming_detects_enabled_source() -> None:
    spark = MagicMock()
    orch = StreamingOrchestrator(_make_config(True), spark=spark)
    assert orch._is_streaming() is True


def test_is_streaming_false_when_no_sources_stream() -> None:
    spark = MagicMock()
    orch = StreamingOrchestrator(_make_config(False), spark=spark)
    assert orch._is_streaming() is False


def test_run_falls_back_to_batch_orchestrator_when_no_streaming() -> None:
    spark = MagicMock()
    orch = StreamingOrchestrator(_make_config(False), spark=spark)

    fake_result = {"status": "SUCCESS", "rows_written": 10}
    with patch(
        "kimball.orchestration.orchestrator.Orchestrator"
    ) as mock_batch_orch_cls:
        mock_batch_orch = mock_batch_orch_cls.return_value
        mock_batch_orch.run.return_value = fake_result
        result = orch.run()
    assert result is fake_result
    mock_batch_orch_cls.assert_called_once()


def test_stop_calls_query_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    spark = MagicMock()
    orch = StreamingOrchestrator(_make_config(True), spark=spark)

    q1 = MagicMock()
    q2 = MagicMock()
    orch._active_queries = {"silver.customers": q1, "silver.orders": q2}
    orch.stop()
    q1.stop.assert_called_once()
    q2.stop.assert_called_once()


def test_stop_is_resilient_to_query_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark = MagicMock()
    orch = StreamingOrchestrator(_make_config(True), spark=spark)
    bad_query = MagicMock()
    bad_query.stop.side_effect = RuntimeError("already stopped")
    orch._active_queries = {"silver.customers": bad_query}
    # Should not raise.
    orch.stop()


class TestFullReload:
    """StreamingOrchestrator.run(full_reload=True) resets watermarks,
    clears checkpoints, and runs a batch full reload."""

    def test_full_reload_resets_watermarks_and_clears_checkpoints(self) -> None:
        spark = MagicMock()
        cfg = _make_config(True)
        orch = StreamingOrchestrator(cfg, spark=spark)

        with (
            patch.object(orch.etl_control, "reset_watermark") as mock_reset,
            patch("kimball.orchestration.orchestrator.Orchestrator") as mock_batch_cls,
            patch("kimball.streaming.orchestrator.os.path.exists", return_value=True),
            patch("kimball.streaming.orchestrator.shutil.rmtree") as mock_rmtree,
        ):
            mock_batch = mock_batch_cls.return_value
            mock_batch.run.return_value = {"status": "SUCCESS"}

            orch._run_full_reload()

        mock_reset.assert_called_once_with("gold.dim_customer", "silver.customers")
        mock_rmtree.assert_called_once()
        mock_batch.run.assert_called_once_with(full_reload=True)

    def test_full_reload_skips_checkpoint_clear_when_no_streaming_source(self) -> None:
        spark = MagicMock()
        cfg = _make_config(False)  # streaming not enabled
        orch = StreamingOrchestrator(cfg, spark=spark)

        with (
            patch.object(orch.etl_control, "reset_watermark") as mock_reset,
            patch("kimball.orchestration.orchestrator.Orchestrator") as mock_batch_cls,
            patch("kimball.streaming.orchestrator.shutil.rmtree") as mock_rmtree,
        ):
            mock_batch = mock_batch_cls.return_value
            mock_batch.run.return_value = {"status": "SUCCESS"}

            orch._run_full_reload()

        mock_reset.assert_called_once()
        mock_rmtree.assert_not_called()

    def test_run_dispatches_to_full_reload(self) -> None:
        spark = MagicMock()
        orch = StreamingOrchestrator(_make_config(True), spark=spark)

        with patch.object(orch, "_run_full_reload") as mock_reload:
            orch.run(full_reload=True)

        mock_reload.assert_called_once()


class TestWatermarkResume:
    """Watermark-based starting version must not overshoot the source."""

    def test_start_queries_uses_watermark_plus_one_when_behind(self) -> None:
        spark = MagicMock()
        cfg = _make_config(True)
        orch = StreamingOrchestrator(cfg, spark=spark)

        orch.etl_control.get_watermark = MagicMock(return_value=3)
        orch.stream_loader.get_latest_version = MagicMock(return_value=5)

        mock_stream_df = MagicMock()
        mock_stream_df.writeStream = MagicMock()
        writer = mock_stream_df.writeStream.return_value
        writer.queryName.return_value = writer
        writer.foreachBatch.return_value = writer
        writer.option.return_value = writer
        writer.trigger.return_value = writer
        writer.start.return_value = MagicMock()
        orch.stream_loader.stream_cdf = MagicMock(return_value=mock_stream_df)

        orch._start_queries({"queries": {}})

        call_kwargs = orch.stream_loader.stream_cdf.call_args[1]
        assert call_kwargs["config"].starting_version == 4

    def test_start_queries_does_not_overshoot_when_watermark_at_latest(
        self,
    ) -> None:
        spark = MagicMock()
        cfg = _make_config(True)
        orch = StreamingOrchestrator(cfg, spark=spark)

        orch.etl_control.get_watermark = MagicMock(return_value=5)
        orch.stream_loader.get_latest_version = MagicMock(return_value=5)

        mock_stream_df = MagicMock()
        mock_stream_df.writeStream = MagicMock()
        writer = mock_stream_df.writeStream.return_value
        writer.queryName.return_value = writer
        writer.foreachBatch.return_value = writer
        writer.option.return_value = writer
        writer.trigger.return_value = writer
        writer.start.return_value = MagicMock()
        orch.stream_loader.stream_cdf = MagicMock(return_value=mock_stream_df)

        orch._start_queries({"queries": {}})

        call_kwargs = orch.stream_loader.stream_cdf.call_args[1]
        assert call_kwargs["config"].starting_version is None


class TestPerVersionForeachBatch:
    """_execute_microbatch_per_version splits a micro-batch by _commit_version."""

    def test_per_version_processes_each_version_sequentially(self) -> None:
        spark = MagicMock()
        cfg = _make_config(True)
        orch = StreamingOrchestrator(cfg, spark=spark)

        batch_df = MagicMock()
        batch_df.columns = ["customer_id", "_commit_version"]
        batch_df.select.return_value.distinct.return_value.collect.return_value = [
            MagicMock(_commit_version=1),
            MagicMock(_commit_version=3),
            MagicMock(_commit_version=2),
        ]

        calls = []

        def fake_execute_one(version_df, source, batch_id):
            calls.append(source.name)

        with patch.object(orch, "_execute_one_microbatch", side_effect=fake_execute_one):
            orch._execute_microbatch_per_version(batch_df, cfg.sources[0], 7)

        assert calls == ["silver.customers"] * 3
        # Verify filter called for versions 1, 2, 3 in order
        assert batch_df.filter.call_count == 3
        assert batch_df.filter.call_args_list[0][0][0] == "`_commit_version` = 1"
        assert batch_df.filter.call_args_list[1][0][0] == "`_commit_version` = 2"
        assert batch_df.filter.call_args_list[2][0][0] == "`_commit_version` = 3"

    def test_per_version_falls_back_when_no_commit_version(self) -> None:
        spark = MagicMock()
        cfg = _make_config(True)
        orch = StreamingOrchestrator(cfg, spark=spark)

        batch_df = MagicMock()
        batch_df.columns = ["customer_id"]

        with patch.object(orch, "_execute_one_microbatch") as mock_execute:
            orch._execute_microbatch_per_version(batch_df, cfg.sources[0], 5)

        mock_execute.assert_called_once_with(batch_df, cfg.sources[0], 5)

    def test_foreach_uses_per_version_when_enabled(self) -> None:
        spark = MagicMock()
        cfg = _make_config(True)
        cfg.sources[0].streaming = StreamingSourceConfig(
            enabled=True, per_version=True
        )
        orch = StreamingOrchestrator(cfg, spark=spark)

        batch_df = MagicMock()
        batch_df.columns = ["customer_id", "_change_type", "_commit_version"]
        batch_df.select.return_value.distinct.return_value.collect.return_value = [
            MagicMock(_commit_version=5),
        ]

        with patch.object(
            orch, "_execute_microbatch_per_version"
        ) as mock_per_version, patch.object(orch, "_execute_one_microbatch") as mock_single:
            foreach_fn = orch._make_foreach(cfg.sources[0])
            foreach_fn(batch_df, 42)

        mock_per_version.assert_called_once_with(batch_df.filter.return_value, cfg.sources[0], 42)
        mock_single.assert_not_called()

    def test_foreach_uses_single_merge_when_per_version_disabled(self) -> None:
        spark = MagicMock()
        cfg = _make_config(True)
        cfg.sources[0].streaming = StreamingSourceConfig(enabled=True, per_version=False)
        orch = StreamingOrchestrator(cfg, spark=spark)

        batch_df = MagicMock()
        batch_df.columns = ["customer_id", "_change_type"]

        with patch.object(
            orch, "_execute_microbatch_per_version"
        ) as mock_per_version, patch.object(orch, "_execute_one_microbatch") as mock_single:
            foreach_fn = orch._make_foreach(cfg.sources[0])
            foreach_fn(batch_df, 42)

        mock_single.assert_called_once_with(batch_df.filter.return_value, cfg.sources[0], 42)
        mock_per_version.assert_not_called()

