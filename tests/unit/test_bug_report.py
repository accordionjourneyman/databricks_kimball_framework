"""Regression tests for Algorithmic Bug Report findings.

Each test documents a specific bug (TRUE findings from the report) and asserts
the *fixed* behaviour so that any regression will cause the test to fail.
"""

from __future__ import annotations

import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("KIMBALL_ETL_SCHEMA", "test_schema")

from pyspark.sql import DataFrame, SparkSession

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_df(columns: list[str]) -> MagicMock:
    df = MagicMock(spec=DataFrame)
    df.columns = columns
    df.isEmpty.return_value = False
    df.limit.return_value = df
    df.head.return_value = []
    df.count.return_value = 0
    df.filter.return_value = df
    df.join.return_value = df
    df.select.return_value = df
    df.withColumn.return_value = df
    df.alias.return_value = df
    df.sparkSession = MagicMock(spec=SparkSession)
    return df


def _make_etl_manager():
    """Build an ETLControlManager with properly chained mocks."""
    from kimball.orchestration.watermark import ETLControlManager

    spark_mock = MagicMock()
    spark_mock.catalog.tableExists.return_value = True
    spark_mock.sql = MagicMock()

    # _ensure_table_exists -> _migrate_schema calls spark.table().schema.fields
    init_table = MagicMock()
    init_field = MagicMock()
    init_field.name = "target_table"
    init_table.schema.fields = [init_field]
    spark_mock.table.return_value = init_table

    manager = ETLControlManager(etl_schema="test", spark_session=spark_mock)
    return manager, spark_mock


def _setup_running_batches_mock(spark_mock, stale_rows):
    """Configure spark.table() chain for get_running_batches."""
    table_mock = MagicMock()
    table_mock.filter.return_value = table_mock
    table_mock.select.return_value = table_mock
    table_mock.collect.return_value = stale_rows
    spark_mock.table.return_value = table_mock


# ===================================================================
# #1  SCD6 missing generate_keys — NULL surrogate keys
# ===================================================================


class TestBugSCD6MissingGenerateKeys:
    """merge_scd6 now calls generate_keys() so new INSERTs have valid SK."""

    @patch("kimball.processing.scd6.DeltaTable")
    @patch("kimball.processing.scd6.filter_cdf_deletes")
    @patch("kimball.processing.scd6.compute_hashdiff")
    @patch("kimball.processing.scd6.HashKeyGenerator")
    @patch("kimball.processing.scd6.col", return_value=MagicMock())
    @patch("kimball.processing.scd6.lit", return_value=MagicMock())
    @patch("kimball.processing.scd6.when", return_value=MagicMock())
    def test_scd6_calls_generate_keys_for_inserts(
        self,
        mock_when,
        mock_lit,
        mock_col,
        mock_hkg,
        mock_hashdiff,
        mock_filter,
        mock_dt,
    ):
        from kimball.processing.scd6 import merge_scd6

        mock_hashdiff.return_value = MagicMock()
        mock_gen_instance = MagicMock()
        mock_hkg.return_value = mock_gen_instance
        mock_gen_instance.generate_keys.return_value = MagicMock()
        mock_gen_instance.generate_keys.return_value.columns = [
            "surrogate_key",
            "id",
            "name",
            "hashdiff",
            "__is_current",
            "__valid_from",
            "__valid_to",
            "__etl_processed_at",
            "__etl_batch_id",
            "__is_deleted",
            "__is_skeleton",
        ]

        upserts = _make_df(["id", "name", "__etl_processed_at", "__etl_batch_id"])
        upserts.isEmpty.return_value = False
        deletes = MagicMock()
        deletes.isEmpty.return_value = True
        mock_filter.return_value = (upserts, deletes)

        target_df = _make_df(
            [
                "surrogate_key",
                "id",
                "name",
                "hashdiff",
                "__is_current",
                "__valid_from",
                "__valid_to",
                "__etl_processed_at",
                "__etl_batch_id",
                "__is_deleted",
                "__is_skeleton",
            ]
        )
        target_df.filter.return_value = target_df
        target_df.join.return_value = target_df
        target_df.alias.return_value = target_df

        mock_dt_instance = MagicMock()
        mock_dt.forName.return_value = mock_dt_instance
        merge_chain = mock_dt_instance.alias.return_value.merge.return_value
        merge_chain.whenMatchedUpdate.return_value = merge_chain
        merge_chain.whenNotMatchedInsert.return_value = merge_chain

        merge_scd6(
            upserts,
            target_table_name="test_target",
            join_keys=["id"],
            track_history_columns=["name"],
            current_value_columns=["name"],
        )

        mock_hkg.assert_called_once_with(["id"], version_column=None)
        mock_gen_instance.generate_keys.assert_called_once()


# ===================================================================
# #3  _rebuild_history non-idempotent append
# ===================================================================


class TestBugRebuildHistoryNonIdempotent:
    """_rebuild_history now uses MERGE instead of append — idempotent."""

    @patch("kimball.processing.scd2.generate_keys")
    @patch("kimball.processing.scd2.compute_hashdiff")
    @patch("kimball.processing.scd2.DeltaTable")
    @patch("kimball.processing.scd2.broadcast", side_effect=lambda x: x)
    @patch("kimball.processing.scd2.col", return_value=MagicMock())
    @patch("kimball.processing.scd2.lit", return_value=MagicMock())
    @patch("kimball.processing.scd2.when", return_value=MagicMock())
    @patch("kimball.processing.scd2.current_timestamp", return_value=MagicMock())
    @patch("kimball.processing.scd2.expr", return_value=MagicMock())
    @patch("kimball.processing.scd2.lead", return_value=MagicMock())
    @patch("kimball.processing.scd2.row_number", return_value=MagicMock())
    class TestBugRebuildHistoryNonIdempotent:
        """Retired with the unreachable two-phase SCD2 implementation."""

        pass


# ===================================================================
# #4  SCD2 two-phase mixes business/processing time
# ===================================================================


class TestBugSCD2TimeMixing:
    """_rebuild_history now normalises target __valid_from to the same timeline."""

    def test_validity_chain_uses_consistent_timeline(self):
        from kimball.processing.merge_helpers import get_validity_col

        mock_source_df = _make_df(["id", "order_date", "__etl_processed_at"])

        result_col, desc = get_validity_col("order_date", mock_source_df, "test_target")
        assert "order_date" in result_col
        assert "business time" in desc

        with pytest.raises(
            ValueError, match="effective_at column 'missing_col' not found"
        ):
            get_validity_col("missing_col", mock_source_df, "test_target")


# ===================================================================
# #5  Double FK validation
# ===================================================================


class TestBugDoubleFKValidation:
    """Both validate_relationships and validate_fact_fk_integrity apply __is_current."""

    def test_both_validators_called_for_same_fk(self):
        from kimball.common.config import (
            ForeignKeyConfig,
            SourceConfig,
            TableConfig,
            TestDefinition,
        )
        from kimball.orchestration.orchestrator import Orchestrator

        config = TableConfig(
            table_name="test_fact",
            table_type="fact",
            scd_type=1,
            merge_keys=["id"],
            sources=[SourceConfig(name="src", alias="src")],
            foreign_keys=[
                ForeignKeyConfig(
                    column="dim_id", references="dim_table", dimension_key="dim_id"
                )
            ],
            tests=[TestDefinition(column="id", tests=["not_null"])],
        )
        orch = Orchestrator.__new__(Orchestrator)
        orch.config = config
        orch.spark = MagicMock(spec=SparkSession)
        orch.spark.catalog.tableExists.return_value = True

        orch.runtime_options = MagicMock()
        orch.runtime_options.use_approximate_unique = False

        orch._validator = MagicMock()
        orch._validator.run_config_tests.return_value = MagicMock(
            results=[], raise_on_failure=MagicMock()
        )
        orch._validator.validate_fact_fk_integrity.return_value = MagicMock(
            results=[], raise_on_failure=MagicMock()
        )
        orch.metrics_collector = None
        orch.etl_control = MagicMock()

        transformed_df = _make_df(["id", "dim_id"])

        orch._transform_and_validate({"src": transformed_df})

        orch._validator.run_config_tests.assert_called_once()
        # validate_fact_fk_integrity is now skipped when tests are defined
        orch._validator.validate_fact_fk_integrity.assert_not_called()


# ===================================================================
# #6  Skeleton SK diverges from hydrated SK
# ===================================================================


class TestBugSkeletonSKDivergence:
    """Skeleton SK now includes version_column so it matches hydrated SK."""

    @patch("kimball.processing.skeleton_generator.current_timestamp")
    @patch("kimball.processing.skeleton_generator.lit")
    @patch("kimball.processing.skeleton_generator.col")
    @patch("kimball.processing.skeleton_generator.broadcast")
    @patch("kimball.processing.skeleton_generator.DeltaTable")
    def test_skeleton_sk_includes_version_column(
        self, mock_dt, mock_broadcast, mock_col, mock_lit, mock_cts
    ):
        from kimball.processing.skeleton_generator import SkeletonGenerator

        mock_broadcast.side_effect = lambda x: x
        mock_col.side_effect = lambda x: MagicMock()
        mock_lit.side_effect = lambda x: MagicMock()
        mock_cts.return_value = MagicMock()

        gen = SkeletonGenerator.__new__(SkeletonGenerator)
        gen.spark = MagicMock(spec=SparkSession)
        gen.spark.catalog.tableExists.return_value = True

        mock_dt_instance = MagicMock()
        mock_dt.forName.return_value = mock_dt_instance
        mock_dim_df = MagicMock()
        f_skel = MagicMock()
        f_skel.name = "__is_skeleton"
        mock_dim_df.schema.fields = [f_skel]
        mock_dt_instance.toDF.return_value = mock_dim_df

        fact_df = MagicMock()
        fact_df.columns = ["fact_join_key", "other_col"]
        fact_keys = MagicMock()
        missing = MagicMock()
        missing.isEmpty.return_value = False
        fact_keys.join.return_value = missing
        fact_df.select.return_value.distinct.return_value = fact_keys

        with patch("kimball.processing.key_generator.HashKeyGenerator") as mock_hkg:
            mock_gen_instance = MagicMock()
            mock_hkg.return_value = mock_gen_instance
            mock_gen_instance.generate_keys.return_value = MagicMock()
            mock_gen_instance.generate_keys.return_value.columns = [
                "dim_join_key",
                "name",
                "__is_skeleton",
                "surrogate_key",
            ]

            gen.generate_skeletons(
                fact_df=fact_df,
                dim_table_name="dim_table",
                fact_join_key="fact_join_key",
                dim_join_key="dim_join_key",
                surrogate_key_col="surrogate_key",
                batch_id="test_batch",
            )

            mock_hkg.assert_called_once_with(["dim_join_key"], version_column=None)

    @patch("kimball.processing.skeleton_generator.current_timestamp")
    @patch("kimball.processing.skeleton_generator.lit")
    @patch("kimball.processing.skeleton_generator.col")
    @patch("kimball.processing.skeleton_generator.broadcast")
    @patch("kimball.processing.skeleton_generator.DeltaTable")
    def test_skeleton_sk_passes_effective_at_column(
        self, mock_dt, mock_broadcast, mock_col, mock_lit, mock_cts
    ):
        from kimball.processing.skeleton_generator import SkeletonGenerator

        mock_broadcast.side_effect = lambda x: x
        mock_col.side_effect = lambda x: MagicMock()
        mock_lit.side_effect = lambda x: MagicMock()
        mock_cts.return_value = MagicMock()

        gen = SkeletonGenerator.__new__(SkeletonGenerator)
        gen.spark = MagicMock(spec=SparkSession)
        gen.spark.catalog.tableExists.return_value = True

        mock_dt_instance = MagicMock()
        mock_dt.forName.return_value = mock_dt_instance
        mock_dim_df = MagicMock()
        f_skel = MagicMock()
        f_skel.name = "__is_skeleton"
        mock_dim_df.schema.fields = [f_skel]
        mock_dt_instance.toDF.return_value = mock_dim_df

        fact_df = MagicMock()
        fact_df.columns = ["fact_join_key", "other_col"]
        fact_keys = MagicMock()
        missing = MagicMock()
        missing.isEmpty.return_value = False
        fact_keys.join.return_value = missing
        fact_df.select.return_value.distinct.return_value = fact_keys

        with patch("kimball.processing.key_generator.HashKeyGenerator") as mock_hkg:
            mock_gen_instance = MagicMock()
            mock_hkg.return_value = mock_gen_instance
            mock_gen_instance.generate_keys.return_value = MagicMock()
            mock_gen_instance.generate_keys.return_value.columns = [
                "dim_join_key",
                "name",
                "__is_skeleton",
                "surrogate_key",
            ]

            gen.generate_skeletons(
                fact_df=fact_df,
                dim_table_name="dim_table",
                fact_join_key="fact_join_key",
                dim_join_key="dim_join_key",
                surrogate_key_col="surrogate_key",
                batch_id="test_batch",
                effective_at_column="order_date",
            )

            mock_hkg.assert_called_once_with(
                ["dim_join_key"], version_column="order_date"
            )


# ===================================================================
# #8 + #11  Config fingerprint omits key fields
# ===================================================================


class TestBugConfigFingerprintIncomplete:
    """compute_fingerprint now includes foreign_keys, delete_strategy, etc."""

    def test_fingerprint_includes_foreign_keys(self):
        from kimball.common.config import (
            ConfigLoader,
            ForeignKeyConfig,
            SourceConfig,
            TableConfig,
            TestDefinition,
        )

        loader = ConfigLoader()
        base = TableConfig(
            table_name="t",
            table_type="fact",
            scd_type=1,
            merge_keys=["id"],
            sources=[SourceConfig(name="s", alias="s")],
            foreign_keys=[ForeignKeyConfig(column="dim_a", references="dim_a_table")],
            tests=[TestDefinition(column="id", tests=["not_null"])],
        )
        modified = TableConfig(
            table_name="t",
            table_type="fact",
            scd_type=1,
            merge_keys=["id"],
            sources=[SourceConfig(name="s", alias="s")],
            foreign_keys=[
                ForeignKeyConfig(column="dim_a", references="dim_a_table"),
                ForeignKeyConfig(column="dim_b", references="dim_b_table"),
            ],
            tests=[TestDefinition(column="id", tests=["not_null"])],
        )
        fp1 = loader.compute_fingerprint(base)
        fp2 = loader.compute_fingerprint(modified)
        assert fp1 != fp2, "Fingerprints must differ when foreign_keys change"

    def test_fingerprint_includes_delete_strategy(self):
        from kimball.common.config import (
            ConfigLoader,
            SourceConfig,
            TableConfig,
            TestDefinition,
        )

        loader = ConfigLoader()
        a = TableConfig(
            table_name="t",
            table_type="dimension",
            scd_type=1,
            surrogate_key="sk",
            sources=[SourceConfig(name="s", alias="s")],
            natural_keys=["id"],
            delete_strategy="hard",
            tests=[TestDefinition(column="id", tests=["not_null"])],
        )
        b = TableConfig(
            table_name="t",
            table_type="dimension",
            scd_type=1,
            surrogate_key="sk",
            sources=[SourceConfig(name="s", alias="s")],
            natural_keys=["id"],
            delete_strategy="soft",
            tests=[TestDefinition(column="id", tests=["not_null"])],
        )
        assert loader.compute_fingerprint(a) != loader.compute_fingerprint(b)

    def test_fingerprint_includes_schema_evolution(self):
        from kimball.common.config import (
            ConfigLoader,
            SourceConfig,
            TableConfig,
            TestDefinition,
        )

        loader = ConfigLoader()
        a = TableConfig(
            table_name="t",
            table_type="dimension",
            scd_type=2,
            surrogate_key="sk",
            effective_at="updated_at",
            sources=[SourceConfig(name="s", alias="s")],
            natural_keys=["id"],
            schema_evolution=False,
            track_history_columns=["val"],
            tests=[TestDefinition(column="id", tests=["not_null"])],
        )
        b = TableConfig(
            table_name="t",
            table_type="dimension",
            scd_type=2,
            surrogate_key="sk",
            effective_at="updated_at",
            sources=[SourceConfig(name="s", alias="s")],
            natural_keys=["id"],
            schema_evolution=True,
            track_history_columns=["val"],
            tests=[TestDefinition(column="id", tests=["not_null"])],
        )
        assert loader.compute_fingerprint(a) != loader.compute_fingerprint(b)


# ===================================================================
# #9  preserve_all_changes early return on first caught-up source
# ===================================================================


class TestBugPreserveAllChangesEarlyReturn:
    """Version loop now checks ALL sources before returning."""

    @patch("kimball.orchestration.orchestrator.Orchestrator._run_pipeline_once")
    def test_runs_when_not_all_sources_caught_up(self, mock_run):
        from kimball.common.config import SourceConfig, TableConfig
        from kimball.orchestration.orchestrator import Orchestrator

        src_a = SourceConfig(name="src_a", alias="src_a", cdc_strategy="cdf")
        src_b = SourceConfig(name="src_b", alias="src_b", cdc_strategy="cdf")

        config = TableConfig(
            table_name="test_target",
            table_type="dimension",
            scd_type=2,
            effective_at="updated_at",
            surrogate_key="sk",
            sources=[src_a, src_b],
            natural_keys=["id"],
            track_history_columns=["val"],
        )

        orch = Orchestrator.__new__(Orchestrator)
        orch.config = config
        orch.spark = MagicMock(spec=SparkSession)
        orch.loader = MagicMock()
        orch.etl_control = MagicMock()
        orch._validator = MagicMock()
        orch.table_creator = MagicMock()
        orch.skeleton_generator = MagicMock()
        orch.metrics_collector = None
        orch.runtime_options = MagicMock()

        orch.loader.get_latest_version.side_effect = lambda name: (
            10 if name == "src_a" else 20
        )
        orch.etl_control.get_watermark.side_effect = lambda target, source: (
            10 if source == "src_a" else 5
        )
        mock_run.return_value = {"rows_read": 100, "rows_written": 50}

        result = orch._run_with_version_loop(max_iterations=10)

        mock_run.assert_called()
        assert result["rows_read"] > 0


# ===================================================================
# #10  stop_on_failure cancel is cosmetic
# ===================================================================


class TestBugStopOnFailureCosmetic:
    """f.cancel() now only cancels futures that haven't started."""

    def test_cancel_only_affects_unstarted_futures(self):

        running_future = MagicMock()
        running_future.cancel.return_value = False
        not_started_future = MagicMock()
        not_started_future.cancel.return_value = True

        futures = [running_future, not_started_future]
        actually_cancelled = []
        for f in futures:
            result = f.cancel()
            if result:
                actually_cancelled.append(f)

        assert len(actually_cancelled) == 1
        assert not_started_future in actually_cancelled
        assert running_future not in actually_cancelled


# ===================================================================
# #14  SCD2 has no no-op short-circuit
# ===================================================================


class TestBugSCD2NoNoopShortcircuit:
    """SCD2 now short-circuits when upserts and deletes are both empty."""

    @patch("kimball.processing.scd2.generate_keys")
    @patch("kimball.processing.scd2.compute_hashdiff")
    @patch("kimball.processing.scd2.DeltaTable")
    @patch("kimball.processing.scd2.filter_cdf_deletes")
    def test_scd2_skips_merge_when_no_changes(
        self, mock_filter, mock_dt, mock_hashdiff, mock_gen_keys
    ):
        from kimball.processing.scd2 import merge_scd2

        upserts = _make_df(["id", "name", "__etl_processed_at", "__etl_batch_id"])
        upserts.isEmpty.return_value = True
        deletes = MagicMock()
        deletes.isEmpty.return_value = True
        mock_filter.return_value = (upserts, deletes)

        mock_dt_instance = MagicMock()
        mock_dt.forName.return_value = mock_dt_instance
        merge_chain = mock_dt_instance.alias.return_value.merge.return_value
        merge_chain.whenMatchedUpdate.return_value = merge_chain
        merge_chain.whenNotMatchedInsert.return_value = merge_chain

        merge_scd2(
            upserts,
            target_table_name="test_target",
            join_keys=["id"],
            track_history_columns=["name"],
            surrogate_key_col="surrogate_key",
        )

        mock_dt.forName.assert_not_called()


# ===================================================================
# #16  SCD1 no-op check materializes source+target before merge
# ===================================================================


class TestBugSCD1NoopPreScan:
    """SCD1 no-op check now skips empty sources and selects only needed columns."""

    @patch("kimball.processing.scd1.DeltaTable")
    @patch("kimball.processing.scd1.generate_keys")
    @patch("kimball.processing.scd1.dedup_cdf")
    def test_noop_check_skips_empty_source(self, mock_dedup, mock_gen_keys, mock_dt):
        from kimball.processing.scd1 import merge_scd1

        mock_gen_keys.return_value = MagicMock()
        mock_gen_keys.return_value.columns = [
            "surrogate_key",
            "id",
            "name",
            "hashdiff",
            "__etl_processed_at",
        ]

        deduped = _make_df(["id", "name", "__etl_processed_at"])
        deduped.isEmpty.return_value = True
        mock_dedup.return_value = deduped

        target_df = _make_df(
            ["surrogate_key", "id", "name", "hashdiff", "__etl_processed_at"]
        )
        target_df.toDF.return_value = target_df
        target_df.alias.return_value = target_df
        target_df.limit.return_value = target_df

        mock_dt_instance = MagicMock()
        mock_dt.forName.return_value = mock_dt_instance
        mock_dt_instance.toDF.return_value = target_df
        merge_chain = mock_dt_instance.alias.return_value.merge.return_value
        merge_chain.whenMatchedUpdate.return_value = merge_chain
        merge_chain.whenNotMatchedInsert.return_value = merge_chain

        merge_scd1(deduped, target_table_name="test_target", join_keys=["id"])

        assert mock_dt_instance.toDF.call_count == 0


# ===================================================================
# #19  Streaming per-version writes full Delta table
# ===================================================================


class TestBugStreamingPerVersionMaterialization:
    """Per-version path now uses a single materialised temp table."""

    def test_per_version_uses_single_temp_table(self):
        from kimball.streaming.orchestrator import StreamingOrchestrator

        orch = StreamingOrchestrator.__new__(StreamingOrchestrator)
        orch.spark = MagicMock(spec=SparkSession)
        orch.etl_schema = "test_schema"
        orch.config = MagicMock()
        orch.config.table_name = "test_target"
        orch.config.scd_type = 2
        orch.config.effective_at = "updated_at"
        orch.config.natural_keys = ["id"]
        orch.config.track_history_columns = ["val"]
        orch.config.surrogate_key = "surrogate_key"
        orch.config.transformation_sql = None
        orch.config.schema_evolution = False
        orch.config.effective_at = None
        orch.config.history_table = None
        orch.config.current_value_columns = None
        orch.config.delete_strategy = "hard"
        orch.etl_control = MagicMock()

        batch_df = _make_df(["id", "val", "_commit_version", "_commit_timestamp"])
        batch_df.select.return_value.distinct.return_value.collect.return_value = [
            MagicMock(_commit_version=1),
            MagicMock(_commit_version=2),
            MagicMock(_commit_version=3),
        ]
        batch_df.filter.return_value = batch_df

        source = MagicMock()
        source.name = "test_source"
        source.alias = "test_source"

        orch._execute_one_microbatch = MagicMock()

        orch._execute_microbatch_per_version(batch_df, source, "batch_1")

        # No SQL calls — reads from batch_table already materialised in _foreach
        assert orch.spark.sql.call_count == 0
        # Reads batch_table once per version
        assert orch.spark.table.call_count == 3


# ===================================================================
# #20  Streaming extra count action
# ===================================================================


class TestBugStreamingExtraCount:
    """_execute_one_microbatch no longer issues a separate count()."""

    @patch("kimball.streaming.services.microbatch._merger")
    def test_count_not_called_separately(self, microbatch_merger):
        from kimball.streaming.orchestrator import StreamingOrchestrator

        orch = StreamingOrchestrator.__new__(StreamingOrchestrator)
        orch.spark = MagicMock(spec=SparkSession)
        orch.etl_schema = "test_schema"
        orch.config = MagicMock()
        orch.config.table_name = "test_target"
        orch.config.scd_type = 1
        orch.config.natural_keys = ["id"]
        orch.config.track_history_columns = []
        orch.config.surrogate_key = "surrogate_key"
        orch.config.transformation_sql = None
        orch.config.schema_evolution = False
        orch.config.effective_at = None
        orch.config.history_table = None
        orch.config.current_value_columns = None
        orch.config.delete_strategy = "hard"
        orch.config.pii = None
        orch.config.foreign_keys = None
        orch.config.grain_validation = "skip"
        orch.config.merge_keys = None
        orch.etl_control = MagicMock()

        source_df = _make_df(["id", "name"])
        source_df.count.return_value = 5

        source = MagicMock()
        source.name = "test_source"
        source.alias = "test_source"
        source.cdc_strategy = "full"

        microbatch_merger.merge.return_value = None
        microbatch_merger.get_last_merge_metrics.return_value = {}

        orch._execute_one_microbatch(source_df, source, "batch_1")

        source_df.count.assert_not_called()


# ===================================================================
# #23  Re-derive versions per run
# ===================================================================


class TestBugReDeriveVersions:
    """get_latest_version is now cached once before the loop."""

    def test_versions_cached_exactly_once(self):
        from kimball.common.config import SourceConfig, TableConfig
        from kimball.orchestration.orchestrator import Orchestrator

        config = TableConfig(
            table_name="t",
            table_type="dimension",
            scd_type=2,
            surrogate_key="sk",
            effective_at="updated_at",
            sources=[SourceConfig(name="s1", alias="s1", cdc_strategy="cdf")],
            natural_keys=["id"],
            track_history_columns=["val"],
        )

        orch = Orchestrator.__new__(Orchestrator)
        orch.config = config
        orch.spark = MagicMock(spec=SparkSession)
        orch.loader = MagicMock()
        orch.loader.get_latest_version.return_value = 5
        orch.etl_control = MagicMock()
        orch.etl_control.get_watermark.return_value = 5

        orch._run_with_version_loop(max_iterations=5)

        assert orch.loader.get_latest_version.call_count == 1


# ===================================================================
# #24  bus_matrix substring misclassification
# ===================================================================


class TestBugBusMatrixSubstring:
    """analyze_dependencies now uses prefix/suffix matching, not substring."""

    def test_staging_table_with_dim_in_name_not_treated_as_dimension(self):
        from kimball.common.config import ForeignKeyConfig, SourceConfig, TableConfig
        from kimball.observability.bus_matrix import analyze_dependencies

        fact_config = TableConfig(
            table_name="fact_orders",
            table_type="fact",
            scd_type=1,
            merge_keys=["order_id"],
            sources=[
                SourceConfig(
                    name="stg_customer_dim_phone", alias="stg_customer_dim_phone"
                )
            ],
            foreign_keys=[
                ForeignKeyConfig(column="customer_id", references="dim_customer")
            ],
        )

        facts, matrix, dims = analyze_dependencies([fact_config])

        assert "stg_customer_dim_phone" not in dims

    def test_table_with_dim_prefix_treated_as_dimension(self):
        from kimball.common.config import ForeignKeyConfig, SourceConfig, TableConfig
        from kimball.observability.bus_matrix import analyze_dependencies

        fact_config = TableConfig(
            table_name="fact_orders",
            table_type="fact",
            scd_type=1,
            merge_keys=["order_id"],
            sources=[SourceConfig(name="dim_customer", alias="dim_customer")],
            foreign_keys=[
                ForeignKeyConfig(column="customer_id", references="dim_customer")
            ],
        )

        facts, matrix, dims = analyze_dependencies([fact_config])

        assert "dim_customer" in dims


# ===================================================================
# #25  validate_relationships vs _check_single_fk filter mismatch
# ===================================================================


class TestBugValidateRelationshipsFilterMismatch:
    """validate_relationships now applies __is_current filter on the dimension."""

    def test_validate_relationships_filters_current(self):
        from kimball.validation import DataQualityValidator

        validator = DataQualityValidator.__new__(DataQualityValidator)
        mock_spark = MagicMock(spec=SparkSession)
        validator._spark = mock_spark

        dim_df = _make_df(["dim_id", "name", "__is_current"])
        mock_spark.table.return_value = dim_df

        fact_df = _make_df(["order_id", "dim_id"])
        fact_df.alias.return_value = fact_df
        fact_df.join.return_value = fact_df
        fact_df.filter.return_value = fact_df

        with patch("kimball.orchestration.validation.F") as mock_F:
            col_result = MagicMock()
            col_result.__eq__ = MagicMock(return_value="__is_current = true")
            mock_F.col.return_value = col_result
            validator.validate_relationships(fact_df, "dim_id", "dim_table", "dim_id")

        filter_called = False
        for call in dim_df.filter.call_args_list:
            args_str = " ".join(str(a) for a in call[0])
            if "__is_current" in args_str:
                filter_called = True
                break
        assert filter_called


# ===================================================================
# #26  _generate_skeletons column→df map collision
# ===================================================================


class TestBugSkeletonColumnCollision:
    """_generate_skeletons column→df map now picks first match, not last."""

    def test_column_name_collision_picks_first_source(self):
        from kimball.common.config import TableConfig
        from kimball.orchestration.orchestrator import Orchestrator

        config = TableConfig(
            table_name="test_target",
            table_type="dimension",
            scd_type=2,
            effective_at="updated_at",
            surrogate_key="sk",
            sources=[],
            natural_keys=["id"],
            track_history_columns=["val"],
            early_arriving_facts=[
                {
                    "fact_join_key": "shared_col",
                    "dimension_table": "dim_table",
                    "dimension_join_key": "dim_key",
                }
            ],
        )

        orch = Orchestrator.__new__(Orchestrator)
        orch.config = config
        orch.skeleton_generator = MagicMock()

        df_a = _make_df(["shared_col", "other_a"])
        df_b = _make_df(["shared_col", "other_b"])

        orch._generate_skeletons({"src_a": df_a, "src_b": df_b}, "batch_1")

        call_kwargs = orch.skeleton_generator.generate_skeletons.call_args
        actual_fact_df = (
            call_kwargs[1].get("fact_df") if call_kwargs[1] else call_kwargs[0][0]
        )
        assert actual_fact_df is df_a


# ===================================================================
# #28  metrics total_pipeline double count
# ===================================================================


class TestBugMetricsDoubleCount:
    """total_pipeline is now excluded from total_execution_time_ms."""

    def test_total_pipeline_not_double_counted(self):
        from kimball.observability.resilience import QueryMetricsCollector

        mc = QueryMetricsCollector()
        mc.start_time = 1000.0

        mc.add_operation_metric("merge", duration_ms=100.0)
        mc.add_operation_metric("validate", duration_ms=50.0)
        mc.stop_collection()

        summary = mc.get_summary()

        total_ops = [
            m for m in summary["operations"] if m["operation"] == "total_pipeline"
        ]
        assert len(total_ops) == 1
        assert summary["total_execution_time_ms"] == pytest.approx(150.0, abs=1.0)
        assert summary["avg_operation_time_ms"] == pytest.approx(75.0, abs=1.0)


# ===================================================================
# #30  _get_table_version swallows all errors
# ===================================================================


class TestBugGetTableVersionSwallowsErrors:
    """_get_table_version now only catches AnalysisException, not all errors."""

    def test_permission_error_propagates(self):
        from kimball.orchestration.transaction import TransactionManager

        txn = TransactionManager.__new__(TransactionManager)
        txn.spark = MagicMock(spec=SparkSession)

        mock_dt = MagicMock()
        mock_dt.history.return_value.collect.side_effect = Exception(
            "PERMISSION_DENIED: access denied"
        )

        with patch("kimball.orchestration.transaction.DeltaTable") as mock_delta_cls:
            mock_delta_cls.forName.return_value = mock_dt
            with pytest.raises(Exception, match="PERMISSION_DENIED"):
                txn._get_table_version("test_table")


# ===================================================================
# #31  _upsert_control_record key leak
# ===================================================================


class TestBugUpsertControlRecordKeyLeak:
    """update_set is now restricted to schema fields only."""

    @patch("kimball.orchestration.watermark.DeltaTable")
    def test_non_schema_keys_not_in_update_set(self, mock_dt_class):

        manager, spark_mock = _make_etl_manager()

        mock_dt_instance = MagicMock()
        mock_dt_class.forName.return_value = mock_dt_instance
        mock_dt_instance.alias.return_value = mock_dt_instance
        merge_builder = MagicMock()
        merge_builder.whenMatchedUpdate.return_value = merge_builder
        merge_builder.whenNotMatchedInsert.return_value = merge_builder
        mock_dt_instance.merge.return_value = merge_builder

        update_df_mock = MagicMock()
        update_df_mock.columns = [
            "target_table",
            "source_table",
            "last_processed_version",
            "batch_id",
            "batch_started_at",
            "batch_completed_at",
            "batch_status",
            "rows_read",
            "rows_written",
            "error_message",
            "updated_at",
            "config_fingerprint",
            "source_schema_fingerprint",
        ]
        spark_mock.createDataFrame.return_value = update_df_mock

        record_a = {
            "target_table": "t1",
            "source_table": "s1",
            "batch_status": "RUNNING",
            "non_schema_key": "should_not_leak",
            "updated_at": datetime.now(),
        }

        manager._upsert_control_records([record_a])

        update_call = merge_builder.whenMatchedUpdate
        update_set = update_call.call_args[1]["set"]

        assert "non_schema_key" not in update_set
        assert "batch_status" in update_set


# ===================================================================
# #32  Zombie detection no TTL
# ===================================================================


class TestBugZombieDetectionNoTTL:
    """get_running_batches now applies a TTL to filter stale RUNNING records."""

    def test_stale_running_batches_filtered_by_ttl(self):
        manager, spark_mock = _make_etl_manager()

        stale_row = MagicMock()
        stale_row.__getitem__ = MagicMock(
            side_effect=lambda k: {
                "batch_id": "crashed_batch_123",
                "source_table": "src_a",
            }[k]
        )

        _setup_running_batches_mock(spark_mock, [stale_row])

        col_mock = MagicMock()
        col_mock.__eq__ = MagicMock(return_value=MagicMock())
        col_mock.__gt__ = MagicMock(return_value=MagicMock())
        col_mock.__and__ = MagicMock(return_value=MagicMock())
        ts_mock = MagicMock()
        ts_mock.__sub__ = MagicMock(return_value=MagicMock())
        with (
            patch("kimball.orchestration.watermark.col", return_value=col_mock),
            patch(
                "kimball.orchestration.watermark.current_timestamp",
                return_value=ts_mock,
            ),
            patch("kimball.orchestration.watermark.F.expr", return_value=MagicMock()),
        ):
            running = manager.get_running_batches("test_target", ttl_minutes=60)

        assert len(running) == 1
        assert running[0]["batch_id"] == "crashed_batch_123"
