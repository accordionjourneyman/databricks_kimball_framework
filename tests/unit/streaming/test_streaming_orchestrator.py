"""Tests for StreamingOrchestrator dispatch and lifecycle."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.common.config import (
    ForeignKeyConfig,
    PIIColumnConfig,
    PIIPolicy,
    SourceConfig,
    StreamingSourceConfig,
    TableConfig,
)
from kimball.streaming.orchestrator import StreamingOrchestrator

from pyspark.sql.types import StringType


@pytest.fixture(autouse=True)
def _patch_spark_fns():
    """Patch Spark functions that require an active SparkContext."""
    with patch("kimball.streaming.services.microbatch.spark_count", return_value=MagicMock()):
        yield


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

        with patch.object(
            orch, "_execute_one_microbatch", side_effect=fake_execute_one
        ):
            orch._execute_microbatch_per_version(batch_df, cfg.sources[0], 7)

        assert calls == ["silver.customers"] * 3
        # Verify the method reads from the batch_table (already materialised
        # in _foreach) rather than writing its own temp table.
        assert spark.table.call_count == 3  # one per version

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
        cfg.sources[0].streaming = StreamingSourceConfig(enabled=True, per_version=True)
        orch = StreamingOrchestrator(cfg, spark=spark)

        batch_df = MagicMock()
        batch_df.columns = ["customer_id", "_change_type", "_commit_version"]
        batch_df.select.return_value.distinct.return_value.collect.return_value = [
            MagicMock(_commit_version=5),
        ]

        with (
            patch.object(orch, "_execute_microbatch_per_version") as mock_per_version,
            patch.object(orch, "_execute_one_microbatch") as mock_single,
        ):
            foreach_fn = orch._make_foreach(cfg.sources[0])
            foreach_fn(batch_df, 42)

        mock_per_version.assert_called_once_with(
            batch_df.filter.return_value, cfg.sources[0], 42
        )
        mock_single.assert_not_called()

    def test_foreach_uses_single_merge_when_per_version_disabled(self) -> None:
        spark = MagicMock()
        cfg = _make_config(True)
        cfg.sources[0].streaming = StreamingSourceConfig(
            enabled=True, per_version=False
        )
        orch = StreamingOrchestrator(cfg, spark=spark)

        batch_df = MagicMock()
        batch_df.columns = ["customer_id", "_change_type"]

        with (
            patch.object(orch, "_execute_microbatch_per_version") as mock_per_version,
            patch.object(orch, "_execute_one_microbatch") as mock_single,
        ):
            foreach_fn = orch._make_foreach(cfg.sources[0])
            foreach_fn(batch_df, 42)

        mock_single.assert_called_once_with(
            batch_df.filter.return_value, cfg.sources[0], 42
        )
        mock_per_version.assert_not_called()


# ===================================================================
# Streaming feature parity tests (PII, FK validation, grain, target creation)
# ===================================================================

class TestStreamingPIIMasking:
    def test_pii_masking_applied_in_microbatch(self):
        cfg = _make_config(True)
        cfg.transformation_sql = "SELECT customer_id, email FROM c"
        cfg.pii = PIIPolicy(columns=[PIIColumnConfig(column="email", strategy="hash")])
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        orch = StreamingOrchestrator(cfg, spark=spark)

        source_df = MagicMock()
        source_df.columns = ["customer_id", "email"]
        source_df.withColumn.return_value = source_df
        source_df.join.return_value = source_df
        with patch.object(spark, "sql", return_value=source_df):
            with patch("kimball.processing.merger.merge"):
                with patch("kimball.streaming.services.microbatch.StreamingMicroBatchProcessor.ensure_target_table"):
                    with patch("kimball.streaming.services.microbatch.apply_pii_masking", return_value=source_df) as mock_pii:
                        orch._execute_one_microbatch(
                            MagicMock(columns=["customer_id", "email"]),
                            cfg.sources[0],
                            1,
                        )
        mock_pii.assert_called_once()


class TestStreamingFKValidation:
    def test_fk_validation_runs_for_fact(self):
        src = SourceConfig(
            name="silver.orders",
            alias="oi",
            cdc_strategy="cdf",
            primary_keys=["order_id"],
        )
        cfg = TableConfig(
            table_name="gold.fact_sales",
            table_type="fact",
            merge_keys=["order_id"],
            sources=[src],
            foreign_keys=[ForeignKeyConfig(column="customer_sk", references="gold.dim_customer")],
        )
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        orch = StreamingOrchestrator(cfg, spark=spark)

        source_df = MagicMock()
        source_df.columns = ["order_id", "customer_sk"]
        with patch.object(spark, "sql", return_value=source_df):
            with patch("kimball.processing.merger.merge"):
                with patch("kimball.streaming.services.microbatch.StreamingMicroBatchProcessor.ensure_target_table"):
                    with patch("kimball.streaming.services.microbatch.DataQualityValidator") as mock_val_cls:
                        mock_val = mock_val_cls.return_value
                        mock_report = MagicMock()
                        mock_report.results = []
                        mock_val.validate_fact_fk_integrity.return_value = mock_report
                        orch._execute_one_microbatch(
                            MagicMock(columns=["order_id"]), cfg.sources[0], 1
                        )
        mock_val.validate_fact_fk_integrity.assert_called_once()


class TestStreamingGrainValidation:
    def test_grain_violation_raises(self):
        cfg = _make_config(True)
        cfg.transformation_sql = "SELECT customer_id FROM c"
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        orch = StreamingOrchestrator(cfg, spark=spark)

        source_df = MagicMock()
        source_df.columns = ["customer_id"]
        source_df.join.return_value = source_df
        groupby_result = MagicMock()
        agg_result = MagicMock()
        filter_result = MagicMock()
        limit_result = MagicMock()
        limit_result.head.return_value = [{"__grain_count": 2}]
        limit_result.__len__ = lambda self: 1
        filter_result.limit.return_value = limit_result
        agg_result.filter.return_value = filter_result
        groupby_result.agg.return_value = agg_result
        source_df.groupBy.return_value = groupby_result

        with patch.object(spark, "sql", return_value=source_df):
            with patch("kimball.processing.merger.merge"):
                with patch("kimball.streaming.services.microbatch.StreamingMicroBatchProcessor.ensure_target_table"):
                    with pytest.raises(ValueError, match="Grain violation"):
                        orch._execute_one_microbatch(
                            MagicMock(columns=["customer_id"]), cfg.sources[0], 1
                        )


class TestStreamingTargetCreation:
    def test_creates_table_when_missing(self):
        cfg = _make_config(True)
        cfg.transformation_sql = "SELECT customer_id FROM c"
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        orch = StreamingOrchestrator(cfg, spark=spark)

        source_df = MagicMock()
        source_df.columns = ["customer_id"]
        source_df.limit.return_value = source_df
        with patch.object(spark, "sql", return_value=source_df):
            with patch("kimball.streaming.services.microbatch.TableCreator") as mock_tc_cls:
                mock_tc = mock_tc_cls.return_value
                mock_tc.add_system_columns.return_value = source_df
                with patch("kimball.processing.merger.ensure_scd2_defaults"):
                    processor = orch._get_processor()
                    processor.ensure_target_table(source_df)
        mock_tc.create_table_with_clustering.assert_called_once()
