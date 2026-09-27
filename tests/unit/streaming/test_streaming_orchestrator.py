"""Tests for StreamingOrchestrator dispatch and lifecycle."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.common.config import (
    ForeignKeyConfig,
    SourceConfig,
    SourceContractConfig,
    StreamingSourceConfig,
    TableConfig,
    TargetConfig,
)
from kimball.common.runtime import RuntimeOptions
from kimball.streaming.orchestrator import StreamingOrchestrator


@pytest.fixture(autouse=True)
def _patch_spark_fns():
    """Patch Spark functions that require an active SparkContext."""
    with (
        patch(
            "kimball.streaming.services.microbatch.spark_count",
            return_value=MagicMock(),
        ),
        patch(
            "kimball.processing.dimension_nulls.apply_dimension_null_policy",
            side_effect=lambda df, *_args, **_kwargs: df,
        ),
    ):
        yield


def _mock_no_grain_violations(source_df: MagicMock) -> None:
    source_df.groupBy.return_value.agg.return_value.filter.return_value.limit.return_value.collect.return_value = []


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
        effective_at="updated_at",
        surrogate_key="customer_sk",
        natural_keys=["customer_id"],
        sources=[src],
    )


def test_is_streaming_detects_enabled_source() -> None:
    spark = MagicMock()
    orch = StreamingOrchestrator.from_config(_make_config(True), spark=spark)
    assert orch._is_streaming() is True


def test_is_streaming_false_when_no_sources_stream() -> None:
    spark = MagicMock()
    orch = StreamingOrchestrator.from_config(_make_config(False), spark=spark)
    assert orch._is_streaming() is False


def test_run_falls_back_to_batch_orchestrator_when_no_streaming() -> None:
    spark = MagicMock()
    orch = StreamingOrchestrator.from_config(_make_config(False), spark=spark)

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
    orch = StreamingOrchestrator.from_config(_make_config(True), spark=spark)

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
    orch = StreamingOrchestrator.from_config(_make_config(True), spark=spark)
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
        orch = StreamingOrchestrator.from_config(cfg, spark=spark)

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
        orch = StreamingOrchestrator.from_config(cfg, spark=spark)

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
        orch = StreamingOrchestrator.from_config(_make_config(True), spark=spark)

        with patch.object(orch, "_run_full_reload") as mock_reload:
            orch.run(full_reload=True)

        mock_reload.assert_called_once()


class TestWatermarkResume:
    """Watermark-based starting version must not overshoot the source."""

    def test_start_queries_uses_watermark_plus_one_when_behind(self) -> None:
        spark = MagicMock()
        cfg = _make_config(True)
        orch = StreamingOrchestrator.from_config(cfg, spark=spark)

        mock_stream_df = MagicMock()
        mock_stream_df.writeStream = MagicMock()
        writer = mock_stream_df.writeStream.return_value
        writer.queryName.return_value = writer
        writer.foreachBatch.return_value = writer
        writer.option.return_value = writer
        writer.trigger.return_value = writer
        writer.start.return_value = MagicMock()

        with (
            patch.object(type(orch.etl_control), "get_watermark", return_value=3),
            patch.object(
                type(orch.stream_loader), "get_latest_version", return_value=5
            ),
            patch.object(
                type(orch.stream_loader),
                "stream_cdf",
                return_value=mock_stream_df,
            ) as mock_cdf,
        ):
            orch._start_queries({"queries": {}})

        call_kwargs = mock_cdf.call_args[1]
        assert call_kwargs["config"].starting_version == 4

    def test_start_queries_does_not_overshoot_when_watermark_at_latest(
        self,
    ) -> None:
        spark = MagicMock()
        cfg = _make_config(True)
        orch = StreamingOrchestrator.from_config(cfg, spark=spark)

        mock_stream_df = MagicMock()
        mock_stream_df.writeStream = MagicMock()
        writer = mock_stream_df.writeStream.return_value
        writer.queryName.return_value = writer
        writer.foreachBatch.return_value = writer
        writer.option.return_value = writer
        writer.trigger.return_value = writer
        writer.start.return_value = MagicMock()

        with (
            patch.object(type(orch.etl_control), "get_watermark", return_value=5),
            patch.object(
                type(orch.stream_loader), "get_latest_version", return_value=5
            ),
            patch.object(
                type(orch.stream_loader),
                "stream_cdf",
                return_value=mock_stream_df,
            ) as mock_cdf,
        ):
            orch._start_queries({"queries": {}})

        call_kwargs = mock_cdf.call_args[1]
        assert call_kwargs["config"].starting_version is None


class TestPerVersionForeachBatch:
    """_execute_microbatch_per_version splits a micro-batch by _commit_version."""

    def test_per_version_processes_each_version_sequentially(self) -> None:
        spark = MagicMock()
        cfg = _make_config(True)
        orch = StreamingOrchestrator.from_config(cfg, spark=spark)
        spark.reset_mock()

        batch_df = MagicMock()
        batch_df.columns = ["customer_id", "_commit_version"]
        distinct_versions = batch_df.select.return_value.distinct.return_value
        ordered_versions = distinct_versions.orderBy.return_value
        input_versions = (1, 3, 2, 3, 2)
        expected_versions = sorted(set(input_versions))
        yielded_versions = []

        def version_iterator():
            for version in expected_versions:
                yielded_versions.append(version)
                yield MagicMock(_commit_version=version)

        ordered_versions.toLocalIterator.return_value = version_iterator()

        calls = []

        def fake_execute_one(version_df, source, batch_id, *, source_version):
            calls.append(source.name)
            assert source_version == expected_versions[len(calls) - 1]
            assert len(calls) == len(yielded_versions)

        with patch.object(
            orch, "_execute_one_microbatch", side_effect=fake_execute_one
        ):
            orch._execute_microbatch_per_version(batch_df, cfg.sources[0], 7)

        assert calls == ["silver.customers"] * len(expected_versions)
        assert yielded_versions == expected_versions
        # Spark orders distinct versions and streams them without collecting K rows.
        batch_df.select.assert_called_once_with("_commit_version")
        batch_df.select.return_value.distinct.assert_called_once_with()
        distinct_versions.orderBy.assert_called_once_with("_commit_version")
        ordered_versions.toLocalIterator.assert_called_once_with()
        distinct_versions.collect.assert_not_called()
        # Each version is still filtered and processed sequentially.
        assert batch_df.filter.call_args_list == [
            ((f"_commit_version = {version}",), {}) for version in expected_versions
        ]
        assert spark.table.call_count == 0

    def test_per_version_falls_back_when_no_commit_version(self) -> None:
        spark = MagicMock()
        cfg = _make_config(True)
        orch = StreamingOrchestrator.from_config(cfg, spark=spark)

        batch_df = MagicMock()
        batch_df.columns = ["customer_id"]

        with patch.object(orch, "_execute_one_microbatch") as mock_execute:
            orch._execute_microbatch_per_version(batch_df, cfg.sources[0], 5)

        mock_execute.assert_called_once_with(batch_df, cfg.sources[0], 5)

    def test_foreach_uses_per_version_when_enabled(self) -> None:
        spark = MagicMock()
        cfg = _make_config(True)
        cfg.sources[0].streaming = StreamingSourceConfig(enabled=True, per_version=True)
        orch = StreamingOrchestrator.from_config(cfg, spark=spark)

        batch_df = MagicMock()
        batch_df.columns = ["customer_id", "_change_type", "_commit_version"]
        batch_df.filter.return_value.isEmpty.return_value = False
        batch_df.select.return_value.distinct.return_value.orderBy.return_value.toLocalIterator.return_value = [
            MagicMock(_commit_version=5),
        ]

        with (
            patch.object(orch, "_execute_microbatch_per_version") as mock_per_version,
            patch.object(orch, "_execute_one_microbatch") as mock_single,
        ):
            foreach_fn = orch._make_foreach(cfg.sources[0])
            foreach_fn(batch_df, 42)

        # Verify the actual filter predicate used to strip update_preimage rows.
        # batch_df.filter is a MagicMock that returns itself for ANY argument, so
        # asserting only on .return_value would pass even with a wrong/missing
        # filter predicate -- the predicate itself must be checked.
        batch_df.filter.assert_called_once_with("_change_type != 'update_preimage'")
        mock_per_version.assert_called_once_with(
            batch_df.filter.return_value.persist.return_value, cfg.sources[0], 42
        )
        mock_single.assert_not_called()

    def test_foreach_uses_single_merge_when_per_version_disabled(self) -> None:
        spark = MagicMock()
        cfg = _make_config(True)
        cfg.sources[0].streaming = StreamingSourceConfig(
            enabled=True, per_version=False
        )
        orch = StreamingOrchestrator.from_config(cfg, spark=spark)

        batch_df = MagicMock()
        batch_df.columns = ["customer_id", "_change_type"]
        batch_df.filter.return_value.isEmpty.return_value = False

        with (
            patch.object(orch, "_execute_microbatch_per_version") as mock_per_version,
            patch.object(orch, "_execute_one_microbatch") as mock_single,
        ):
            foreach_fn = orch._make_foreach(cfg.sources[0])
            foreach_fn(batch_df, 42)

        # Verify the actual filter predicate (see per_version test for rationale).
        batch_df.filter.assert_called_once_with("_change_type != 'update_preimage'")
        mock_single.assert_called_once_with(
            batch_df.filter.return_value.persist.return_value, cfg.sources[0], 42
        )
        mock_per_version.assert_not_called()


# ===================================================================
# Streaming feature parity tests (PII, FK validation, grain, target creation)
# ===================================================================


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
            foreign_keys=[
                ForeignKeyConfig(column="customer_sk", references="gold.dim_customer")
            ],
        )
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        orch = StreamingOrchestrator.from_config(cfg, spark=spark)

        source_df = MagicMock()
        source_df.columns = ["order_id", "customer_sk"]
        with patch.object(spark, "sql", return_value=source_df):
            with patch("kimball.processing.merger.merge"):
                with patch(
                    "kimball.streaming.services.microbatch.StreamingMicroBatchProcessor.ensure_target_table"
                ):
                    with patch(
                        "kimball.streaming.services.microbatch.DataQualityValidator"
                    ) as mock_val_cls:
                        mock_val = mock_val_cls.return_value
                        mock_report = MagicMock()
                        mock_report.results = []
                        mock_val.validate_fact_fk_integrity.return_value = mock_report
                        batch_df = MagicMock(columns=["order_id"])
                        _mock_no_grain_violations(batch_df)
                        orch._execute_one_microbatch(batch_df, cfg.sources[0], 1)
        mock_val.validate_fact_fk_integrity.assert_called_once()


class TestStreamingGrainValidation:
    def test_grain_violation_raises(self):
        cfg = _make_config(True)
        cfg.transformation_sql = "SELECT customer_id FROM c"
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        orch = StreamingOrchestrator.from_config(cfg, spark=spark)

        source_df = MagicMock()
        source_df.columns = ["customer_id"]
        source_df.join.return_value = source_df
        groupby_result = MagicMock()
        agg_result = MagicMock()
        filter_result = MagicMock()
        limit_result = MagicMock()
        filter_result.limit.return_value = limit_result
        agg_result.filter.return_value = filter_result
        groupby_result.agg.return_value = agg_result
        source_df.groupBy.return_value = groupby_result
        limit_result.collect.return_value = [{"customer_id": 7, "__grain_count": 2}]

        with patch.object(spark, "sql", return_value=source_df):
            with patch("kimball.processing.merger.merge"):
                with patch(
                    "kimball.streaming.services.microbatch.StreamingMicroBatchProcessor.ensure_target_table"
                ):
                    with pytest.raises(ValueError, match="Grain violation"):
                        orch._execute_one_microbatch(
                            MagicMock(columns=["customer_id"]), cfg.sources[0], 1
                        )
        limit_result.collect.assert_called_once_with()
        limit_result.head.assert_not_called()

    def test_grain_violation_warns_after_one_sample_action(self):
        cfg = _make_config(True)
        cfg.grain_validation = "warn"
        spark = MagicMock()
        orch = StreamingOrchestrator.from_config(cfg, spark=spark)
        source_df = MagicMock()
        source_df.columns = ["customer_id"]
        groupby_result = MagicMock()
        violations = groupby_result.agg.return_value.filter.return_value
        violations.limit.return_value.collect.return_value = [
            {"customer_id": 7, "__grain_count": 2}
        ]
        source_df.groupBy.return_value = groupby_result

        processor = orch._get_processor()
        with patch("kimball.streaming.services.microbatch.logger") as logger:
            processor._validate_grain(source_df, ["customer_id"])

        violations.limit.assert_called_once_with(5)
        violations.limit.return_value.collect.assert_called_once_with()
        logger.warning.assert_called_once()
        assert "Grain violation" in str(logger.warning.call_args)

    def test_grain_validation_pass_uses_one_sample_action(self):
        cfg = _make_config(True)
        orch = StreamingOrchestrator.from_config(cfg, spark=MagicMock())
        source_df = MagicMock()
        source_df.columns = ["customer_id"]
        violations = source_df.groupBy.return_value.agg.return_value.filter.return_value
        violations.limit.return_value.collect.return_value = []

        orch._get_processor()._validate_grain(source_df, ["customer_id"])

        violations.limit.assert_called_once_with(5)
        violations.limit.return_value.collect.assert_called_once_with()

    def test_grain_validation_skip_avoids_dataframe_actions(self):
        cfg = _make_config(True)
        cfg.grain_validation = "skip"
        orch = StreamingOrchestrator.from_config(cfg, spark=MagicMock())
        source_df = MagicMock()

        orch._get_processor()._validate_grain(source_df, ["customer_id"])

        source_df.groupBy.assert_not_called()


class TestStreamingTargetCreation:
    def test_creates_table_when_missing(self):
        cfg = _make_config(True)
        cfg.transformation_sql = "SELECT customer_id FROM c"
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        orch = StreamingOrchestrator.from_config(cfg, spark=spark)

        source_df = MagicMock()
        source_df.columns = ["customer_id"]
        source_df.limit.return_value = source_df
        with patch.object(spark, "sql", return_value=source_df):
            with patch(
                "kimball.streaming.services.microbatch.TableCreator"
            ) as mock_tc_cls:
                mock_tc = mock_tc_cls.return_value
                mock_tc.add_system_columns.return_value = source_df
                with patch("kimball.processing.merger.ensure_scd2_defaults"):
                    processor = orch._get_processor()
                    processor.ensure_target_table(source_df)
        mock_tc.create_table_with_clustering.assert_called_once()


class TestStreamingBatchMetadata:
    def test_microbatch_id_is_forwarded_to_merge(self):
        cfg = _make_config(True)
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        orch = StreamingOrchestrator.from_config(cfg, spark=spark)
        source_df = MagicMock(columns=["customer_id"])
        _mock_no_grain_violations(source_df)

        with patch("kimball.streaming.services.microbatch._merger.merge") as merge:
            with patch(
                "kimball.streaming.services.microbatch._merger.get_last_merge_metrics",
                return_value={},
            ):
                orch._execute_one_microbatch(source_df, cfg.sources[0], 42)

        assert merge.call_args.kwargs["batch_id"] == "42"


class TestStreamingTemporalContracts:
    @staticmethod
    def _contracted_config() -> TableConfig:
        cfg = _make_config(True)
        cfg.sources[0].contract = SourceContractConfig.model_validate(
            {
                "id": "customer-events",
                "version": "1.0.0",
                "schema": {
                    "customer_id": {"type": "int"},
                    "updated_at": {"type": "timestamp"},
                },
                "temporal": {
                    "event_time_column": "updated_at",
                    "out_of_order_severity": "error",
                },
            }
        )
        return cfg

    def test_temporal_state_commits_after_successful_merge_and_watermark(self) -> None:
        cfg = self._contracted_config()
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        orchestrator = StreamingOrchestrator.from_config(cfg, spark=spark)
        orchestrator.etl_control = MagicMock()
        batch_df = MagicMock()
        batch_df.columns = ["customer_id", "updated_at", "_commit_version"]
        batch_df.agg.return_value.first.return_value = [9]
        _mock_no_grain_violations(batch_df)

        order: list[str] = []
        orchestrator.etl_control.batch_complete.side_effect = lambda **_kwargs: (
            order.append("watermark")
        )
        with (
            patch(
                "kimball.streaming.services.microbatch.ContractValidator"
            ) as validator,
            patch(
                "kimball.streaming.services.microbatch.TemporalStateStore"
            ) as store_type,
            patch(
                "kimball.processing.merger.merge",
                side_effect=lambda **_kwargs: order.append("merge"),
            ),
        ):
            validator.return_value.validate_data.return_value = []
            validator.return_value.validate_temporal.return_value = []
            store_type.return_value.commit.side_effect = lambda *_args: order.append(
                "temporal_state"
            )
            orchestrator._execute_one_microbatch(batch_df, cfg.sources[0], 3)

        validator.return_value.validate_temporal.assert_called_once_with(
            batch_df,
            cfg.sources[0],
            prior_state=store_type.return_value.existing.return_value,
        )
        assert order == ["merge", "watermark", "temporal_state"]

    def test_failed_merge_does_not_advance_temporal_state(self) -> None:
        cfg = self._contracted_config()
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        orchestrator = StreamingOrchestrator.from_config(cfg, spark=spark)
        orchestrator.etl_control = MagicMock()
        batch_df = MagicMock()
        batch_df.columns = ["customer_id", "updated_at", "_commit_version"]
        _mock_no_grain_violations(batch_df)

        with (
            patch(
                "kimball.streaming.services.microbatch.ContractValidator"
            ) as validator,
            patch(
                "kimball.streaming.services.microbatch.TemporalStateStore"
            ) as store_type,
            patch("kimball.processing.merger.merge", side_effect=RuntimeError("boom")),
        ):
            validator.return_value.validate_data.return_value = []
            validator.return_value.validate_temporal.return_value = []
            with pytest.raises(RuntimeError, match="boom"):
                orchestrator._execute_one_microbatch(batch_df, cfg.sources[0], 3)

        store_type.return_value.commit.assert_not_called()


def test_streaming_query_uses_target_checkpoint_root_over_environment():
    target = TargetConfig(
        name="test",
        catalog="workspace",
        silver_schema="test_silver",
        gold_schema="test_gold",
        etl_schema="test_ops",
        checkpoint_root="/target/checkpoints",
    )
    options = RuntimeOptions.from_environment(
        {"KIMBALL_STREAMING_CHECKPOINT_ROOT": "/environment/checkpoints"}
    )
    spark = MagicMock()
    stream_df = MagicMock()
    stream_df.writeStream = MagicMock()
    writer = stream_df.writeStream
    writer.queryName.return_value = writer
    writer.foreachBatch.return_value = writer
    writer.option.return_value = writer
    writer.trigger.return_value = writer
    writer.start.return_value = MagicMock()
    orch = StreamingOrchestrator.from_config(
        _make_config(True), spark=spark, target=target, runtime_options=options
    )

    with (
        patch.object(type(orch.etl_control), "get_watermark", return_value=None),
        patch.object(type(orch.stream_loader), "get_latest_version", return_value=1),
        patch.object(type(orch.stream_loader), "stream_cdf", return_value=stream_df),
        patch(
            "kimball.streaming.orchestrator.default_checkpoint_path",
            return_value="/target/checkpoints/test_ops__silver.customers",
        ) as make_path,
    ):
        orch._start_queries({"queries": {}})

    assert make_path.call_args.kwargs["root"] == "/target/checkpoints"
    assert make_path.call_args.kwargs["use_environment"] is False
    writer.option.assert_called_once_with(
        "checkpointLocation", "/target/checkpoints/test_ops__silver.customers"
    )
