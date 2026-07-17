from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.orchestration.services.merge_executor import MergeExecutor


@pytest.fixture
def ctx():
    mock = MagicMock()
    mock.config.table_name = "test_table"
    mock.config.scd_type = 1
    mock.config.surrogate_key = "sk"
    mock.config.natural_keys = ["nk"]
    mock.config.merge_keys = ["mk"]
    mock.config.foreign_keys = []
    mock.config.track_history_columns = []
    mock.config.current_value_columns = []
    mock.config.cluster_by = None
    mock.config.table_type = "dimension"
    mock.config.schema_evolution = False
    mock.config.delete_strategy = "hard"
    mock.config.effective_at = None
    mock.config.history_table = None
    mock.config.optimize_after_merge = False
    mock.config.grain_validation = "error"
    mock.config.default_rows = None
    mock.config.enable_lineage_truncation = False
    mock.config.sources = []
    mock.batch_id = "batch-1"
    return mock


@pytest.fixture
def executor():
    return MergeExecutor()


class TestEnsureTargetTable:
    def test_table_exists_returns_false(self, executor, ctx):
        ctx.spark.catalog.tableExists.return_value = True
        assert executor.ensure_target_table(ctx, MagicMock()) is False

    def test_table_not_exists_creates_and_returns_true(self, executor, ctx):
        ctx.spark.catalog.tableExists.return_value = False
        executor._create_target_table = MagicMock()
        executor.table_creator.create_history_table = MagicMock()
        result = executor.ensure_target_table(ctx, MagicMock())
        assert result is True
        executor._create_target_table.assert_called_once()

    def test_scd4_creates_history_table(self, executor, ctx):
        ctx.config.scd_type = 4
        ctx.config.history_table = "test_history"
        ctx.spark.catalog.tableExists.return_value = False
        executor._create_target_table = MagicMock()
        executor.table_creator.create_history_table = MagicMock()
        executor.ensure_target_table(ctx, MagicMock())
        executor.table_creator.create_history_table.assert_called_once_with(
            "test_history"
        )


class TestCreateTargetTable:
    def test_auto_cluster_with_natural_keys(self, executor, ctx):
        ctx.config.cluster_by = None
        ctx.config.natural_keys = ["nk1", "nk2"]
        ctx.config.table_type = "dimension"
        executor.table_creator = MagicMock()
        executor.table_creator.add_system_columns.return_value = MagicMock()
        executor.table_creator.create_table_with_clustering = MagicMock()

        with patch(
            "kimball.orchestration.services.merge_executor._feature_enabled",
            return_value=True,
        ):
            executor._create_target_table(ctx, MagicMock())

        call_kwargs = (
            executor.table_creator.create_table_with_clustering.call_args.kwargs
        )
        assert call_kwargs["cluster_by"] == ["nk1", "nk2"]

    def test_auto_cluster_no_natural_keys(self, executor, ctx):
        ctx.config.cluster_by = None
        ctx.config.natural_keys = None
        ctx.config.table_type = "dimension"
        executor.table_creator = MagicMock()
        executor.table_creator.add_system_columns.return_value = MagicMock()
        executor.table_creator.create_table_with_clustering = MagicMock()

        with patch(
            "kimball.orchestration.services.merge_executor._feature_enabled",
            return_value=True,
        ):
            executor._create_target_table(ctx, MagicMock())

        call_kwargs = (
            executor.table_creator.create_table_with_clustering.call_args.kwargs
        )
        assert call_kwargs["cluster_by"] == []


class TestSeedDefaults:
    def test_skips_when_table_not_created(self, executor, ctx):
        executor.seed_defaults(ctx, table_created=False)
        assert ctx.spark.table.called is False

    def test_skips_when_not_dimension(self, executor, ctx):
        ctx.config.table_type = "fact"
        executor.seed_defaults(ctx, table_created=True)
        assert ctx.spark.table.called is False

    def test_scd2_calls_ensure_scd2_defaults(self, executor, ctx):
        ctx.config.scd_type = 2
        ctx.config.surrogate_key = "sk"
        ctx.spark.table.return_value.schema = MagicMock()
        with patch(
            "kimball.orchestration.services.merge_executor._merger.ensure_scd2_defaults"
        ) as mock_fn:
            executor.seed_defaults(ctx, table_created=True)
            mock_fn.assert_called_once()

    def test_scd1_calls_ensure_scd1_defaults(self, executor, ctx):
        ctx.config.scd_type = 1
        ctx.config.surrogate_key = "sk"
        ctx.spark.table.return_value.schema = MagicMock()
        with patch(
            "kimball.orchestration.services.merge_executor._merger.ensure_scd1_defaults"
        ) as mock_fn:
            executor.seed_defaults(ctx, table_created=True)
            mock_fn.assert_called_once()


class TestPrepareSourceDF:
    def test_lineage_truncation_with_checkpoint_dir(self, executor, ctx):
        ctx.config.enable_lineage_truncation = True
        ctx.spark.sparkContext.getCheckpointDir.return_value = "/checkpoint"
        transformed_df = MagicMock()
        transformed_df.checkpoint.return_value = MagicMock()
        ctx.spark.catalog.tableExists.return_value = False
        result = executor.prepare_source_df(ctx, transformed_df)
        transformed_df.checkpoint.assert_called_once()
        assert result is not None

    def test_lineage_truncation_without_checkpoint_dir(self, executor, ctx):
        ctx.config.enable_lineage_truncation = True
        ctx.spark.sparkContext.getCheckpointDir.return_value = None
        transformed_df = MagicMock()
        transformed_df.localCheckpoint.return_value = MagicMock()
        ctx.spark.catalog.tableExists.return_value = False
        result = executor.prepare_source_df(ctx, transformed_df)
        transformed_df.localCheckpoint.assert_called_once()
        assert result is not None

    def test_lineage_truncation_checkpoint_exception(self, executor, ctx):
        from pyspark.errors import PySparkException

        ctx.config.enable_lineage_truncation = True
        ctx.spark.sparkContext.getCheckpointDir.return_value = "/checkpoint"
        transformed_df = MagicMock()
        transformed_df.checkpoint.side_effect = PySparkException("checkpoint failed")
        transformed_df.localCheckpoint.return_value = MagicMock()
        ctx.spark.catalog.tableExists.return_value = False
        result = executor.prepare_source_df(ctx, transformed_df)
        transformed_df.localCheckpoint.assert_called_once()
        assert result is not None

    def test_no_truncation(self, executor, ctx):
        ctx.config.enable_lineage_truncation = False
        transformed_df = MagicMock()
        ctx.spark.catalog.tableExists.return_value = False
        result = executor.prepare_source_df(ctx, transformed_df)
        assert result is transformed_df

    def test_target_exists_applies_pruning(self, executor, ctx):
        ctx.config.enable_lineage_truncation = False
        transformed_df = MagicMock()
        transformed_df.columns = ["col1", "col2"]
        ctx.spark.catalog.tableExists.return_value = True
        target_schema = MagicMock()
        target_schema.fields = [MagicMock(name="col1")]
        ctx.spark.table.return_value.schema = target_schema
        executor._apply_adaptive_pruning = MagicMock(return_value=transformed_df)
        executor.prepare_source_df(ctx, transformed_df)
        executor._apply_adaptive_pruning.assert_called_once()


class TestApplyAdaptivePruning:
    def test_drops_unused_columns(self, executor, ctx):
        df = MagicMock()
        df.columns = ["col1", "col2", "col3"]
        target_columns = {"col1"}
        executor._apply_adaptive_pruning(ctx, df, target_columns)
        df.select.assert_called_once()
        selected = df.select.call_args[0]
        assert "col1" in selected

    def test_keeps_protected_columns(self, executor, ctx):
        ctx.config.natural_keys = ["nk"]
        ctx.config.merge_keys = ["mk"]
        ctx.config.surrogate_key = "sk"
        ctx.config.track_history_columns = ["thc"]
        ctx.config.current_value_columns = ["cvc"]
        fk = MagicMock()
        fk.column = "fk_col"
        ctx.config.foreign_keys = [fk]
        df = MagicMock()
        df.columns = ["nk", "mk", "sk", "thc", "cvc", "fk_col", "extra"]
        target_columns = set()
        executor._apply_adaptive_pruning(ctx, df, target_columns)
        df.select.assert_called_once()

    def test_schema_evolution_adds_new_columns(self, executor, ctx):
        ctx.config.schema_evolution = True
        ctx.config.sources = [MagicMock(name="src_table")]
        ctx.config.natural_keys = ["new_col"]
        df = MagicMock()
        df.columns = ["new_col", "existing"]
        target_columns = {"existing"}
        executor._evolve_target_schema = MagicMock()
        executor._apply_adaptive_pruning(ctx, df, target_columns)
        executor._evolve_target_schema.assert_called_once()

    def test_no_drop_returns_same_df(self, executor, ctx):
        df = MagicMock()
        df.columns = ["col1"]
        target_columns = {"col1"}
        result = executor._apply_adaptive_pruning(ctx, df, target_columns)
        assert result is df


class TestEvolveTargetSchema:
    def test_adds_new_columns(self, executor, ctx):
        ctx.config.sources = [MagicMock(name="src_table")]
        target_df = MagicMock()
        target_df.schema.fields = [MagicMock(name="existing")]
        ctx.spark.table.return_value = target_df
        src_schema = MagicMock()
        src_field = MagicMock()
        src_field.dataType.simpleString.return_value = "string"
        src_schema.__getitem__ = MagicMock(return_value=src_field)
        ctx.spark.table.return_value.schema = src_schema
        executor._evolve_target_schema(ctx, ["new_col"])
        ctx.spark.sql.assert_called_once()

    def test_skips_existing_columns(self, executor, ctx):
        target_df = MagicMock()
        target_df.schema.fields = [MagicMock()]
        target_df.schema.fields[0].name = "existing"
        ctx.spark.table.return_value = target_df
        executor._evolve_target_schema(ctx, ["existing"])
        ctx.spark.sql.assert_not_called()

    def test_handles_exception_gracefully(self, executor, ctx):
        from pyspark.errors import PySparkException

        ctx.spark.table.side_effect = PySparkException("boom")
        executor._evolve_target_schema(ctx, ["new_col"])


class TestValidateGrain:
    def test_skip_when_no_join_keys(self, executor, ctx):
        source_df = MagicMock()
        executor.validate_grain(ctx, source_df, [])
        # No join keys -> early return; must not run any groupBy/agg work.
        source_df.groupBy.assert_not_called()

    def test_skip_mode(self, executor, ctx):
        ctx.config.grain_validation = "skip"
        source_df = MagicMock()
        executor.validate_grain(ctx, source_df, ["key"])
        # grain_validation=skip -> early return before touching the source df.
        source_df.groupBy.assert_not_called()

    def test_warn_mode(self, executor, ctx):
        ctx.config.grain_validation = "warn"
        source_df = MagicMock()
        grouped = MagicMock()
        grouped.agg.return_value = grouped
        grouped.filter.return_value = grouped
        grouped.limit.return_value = grouped
        grouped.head.return_value = [MagicMock()]  # 1 violation -> non-empty
        grouped.collect.return_value = [MagicMock()]
        source_df.groupBy.return_value = grouped
        # spark_count("*") is a real pyspark call that needs a SparkContext;
        # mock it so this unit test does not depend on a live session.
        with (
            patch(
                "kimball.orchestration.services.merge_executor.spark_count"
            ) as mock_count,
            patch(
                "kimball.orchestration.services.merge_executor.logger"
            ) as mock_logger,
        ):
            mock_count.return_value = MagicMock()
            # Warn mode must log a warning and NOT raise, even with violations.
            executor.validate_grain(ctx, source_df, ["key"])
        mock_logger.warning.assert_called_once()
        assert "Grain violation" in str(mock_logger.warning.call_args)

    def test_error_mode_raises(self, executor, ctx):
        ctx.config.grain_validation = "error"
        source_df = MagicMock()
        grouped = MagicMock()
        grouped.agg.return_value = grouped
        grouped.filter.return_value = grouped
        grouped.limit.return_value = grouped
        grouped.head.return_value = [MagicMock()]
        grouped.collect.return_value = [MagicMock()]
        source_df.groupBy.return_value = grouped
        with patch(
            "kimball.orchestration.services.merge_executor.spark_count"
        ) as mock_count:
            mock_count.return_value = MagicMock()
            with pytest.raises(ValueError, match="Grain violation"):
                executor.validate_grain(ctx, source_df, ["key"])


class TestExecuteMerge:
    def test_execute_merge_calls_merger(self, executor, ctx):
        source_df = MagicMock()
        with patch(
            "kimball.orchestration.services.merge_executor._merger.merge"
        ) as mock_merge:
            executor.execute_merge(ctx, source_df, ["key"])
            mock_merge.assert_called_once()

    def test_optimize_when_enabled(self, executor, ctx):
        ctx.config.optimize_after_merge = True
        source_df = MagicMock()
        with (
            patch("kimball.orchestration.services.merge_executor._merger.merge"),
            patch(
                "kimball.orchestration.services.merge_executor.os.environ.get",
                return_value="1",
            ),
            patch(
                "kimball.orchestration.services.merge_executor._merger.optimize_table"
            ) as mock_opt,
        ):
            executor.execute_merge(ctx, source_df, ["key"])
            mock_opt.assert_called_once()

    def test_optimize_skipped_when_env_not_set(self, executor, ctx):
        ctx.config.optimize_after_merge = True
        source_df = MagicMock()
        with (
            patch(
                "kimball.orchestration.services.merge_executor._merger.merge"
            ) as mock_merge,
            patch(
                "kimball.orchestration.services.merge_executor.os.environ.get",
                return_value="0",
            ),
            patch(
                "kimball.orchestration.services.merge_executor._merger.optimize_table"
            ) as mock_opt,
        ):
            executor.execute_merge(ctx, source_df, ["key"])
        # The merge still runs, but optimize_table must be skipped when
        # KIMBALL_ENABLE_INLINE_OPTIMIZE is not "1" -- otherwise the env gate
        # is meaningless and optimize runs on every merge.
        mock_merge.assert_called_once()
        mock_opt.assert_not_called()


class TestGetMergeMetrics:
    def test_returns_metrics(self, executor, ctx):
        with patch(
            "kimball.orchestration.services.merge_executor._merger.get_last_merge_metrics",
            return_value={
                "numSourceRows": "10",
                "numTargetRowsInserted": "5",
                "numTargetRowsUpdated": "3",
            },
        ):
            result = executor.get_merge_metrics(ctx)
            assert result["rows_read"] == 10
            assert result["rows_written"] == 8
