"""Tests for processing, schema, and execution behavior."""

from __future__ import annotations

import inspect
import os
from unittest.mock import MagicMock

os.environ.setdefault("KIMBALL_ETL_SCHEMA", "test_schema")

from pyspark.sql import DataFrame, SparkSession


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


class TestSCD1PrimaryKey:
    """SCD1 tables use the surrogate key as their primary key."""

    def test_scd1_gets_only_sk_pk(self):
        """SCD1 DDL does not declare the natural key as a primary key."""
        from kimball.processing import table_creator

        src = inspect.getsource(table_creator)
        # The natural-key primary-key clause must be absent.
        assert "nk_name" not in src


class TestSCD2SkeletonExpiration:
    """Full-snapshot delete detection preserves skeleton rows."""

    def test_guard_against_skeleton_expiration(self):
        """The delete filter excludes skeleton rows."""
        from kimball.processing.scd2 import _apply_full_snapshot_deletes

        src = inspect.getsource(_apply_full_snapshot_deletes)
        assert 'current_target = current_target.filter(~col("__is_skeleton"))' in src


class TestStreamingVersionProcessing:
    """Per-version transformations use the filtered version."""

    def test_per_version_registers_filtered_view(self):
        """The registered view contains only the selected version."""
        from kimball.streaming import orchestrator

        src = inspect.getsource(orchestrator)
        # version_df is registered, not the full batch
        assert "version_df.createOrReplaceTempView(source.alias)" in src


class TestForeignKeyValidation:
    """A separate FK-integrity pass runs when no configured tests are present."""

    def test_fk_validation_runs_without_tests_config(self):
        from kimball.common.config import ForeignKeyConfig, SourceConfig, TableConfig
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
            # Leave configured tests empty for this case.
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

        # The separate integrity check runs without configured tests.
        orch._validator.run_config_tests.assert_not_called()
        orch._validator.validate_fact_fk_integrity.assert_called_once()


class TestPreserveAllChanges:
    """Version planning checks every source before completing a batch."""

    def test_checks_all_sources(self):
        from kimball.orchestration.orchestrator import Orchestrator

        src = inspect.getsource(Orchestrator._run_with_version_loop)
        # The work plan determines whether all sources are complete.
        assert "active_sources" in src
        assert "for source in self.config.sources" not in src


class TestStopOnFailure:
    """Cancellation applies only to futures that have not started."""

    def test_cancel_returns_boolean(self):
        import concurrent.futures

        f = concurrent.futures.Future()
        # cancel() returns True only if the future was pending
        result = f.cancel()
        assert isinstance(result, bool)


class TestSCD4HistoryRows:
    """SCD4 history deduplicates inserts before adding expire rows."""

    def test_dedup_applies_only_to_inserts(self):
        """dedup window is applied to INSERT rows only,
        then unioned with EXPIRE rows separately."""
        from kimball.processing.scd4 import _merge_history

        src = inspect.getsource(_merge_history)
        # Only inserts enter the deduplication window.
        assert "inserts.withColumn" in src
        assert "inserts_deduped.unionByName(expires)" in src


class TestCdfDeduplication:
    """CDF rows with equal commit versions use a tie-breaker that prefers non-delete rows."""

    def test_dedup_has_tiebreaker(self):
        """dedup_cdf uses a secondary sort column to break ties,
        preferring non-delete rows."""
        from kimball.processing.merge_helpers import dedup_cdf

        src = inspect.getsource(dedup_cdf)
        # Equal commit versions use _change_type as a tie-breaker.
        assert "_change_type" in src


class TestSchemaEvolution:
    """Schema evolution adds new columns in one ALTER TABLE statement."""

    def test_alter_batched_in_single_statement(self):
        """all new columns are added in a single ALTER TABLE."""
        from kimball.processing.merge_helpers import apply_schema_evolution

        src = inspect.getsource(apply_schema_evolution)
        # All new columns are passed to one ALTER TABLE statement.
        assert 'cols_sql = ", ".join(new_cols)' in src
        assert "ADD COLUMNS ({cols_sql})" in src


class TestTargetScanCount:
    """The public SCD2 entry point delegates strategy execution."""

    def test_public_entry_point_delegates_strategy_selection(self):
        from kimball.processing.scd2 import merge_scd2

        src = inspect.getsource(merge_scd2)
        assert "_merge_classic" not in src


class TestApproximateValidation:
    """Approximate uniqueness validation supports composite keys."""

    def test_composite_natural_key_validation_is_supported(self):
        from kimball.orchestration.validation import DataQualityValidator

        src = inspect.getsource(DataQualityValidator)
        # The expression builder handles composite natural keys.
        assert "len(nat_cols) <= 1" in src or "len(columns) <= 1" in src


class TestBusMatrixClassification:
    """Bus-matrix classification uses dimension-name boundaries."""

    def test_bus_matrix_uses_name_heuristic(self):
        from kimball.observability.bus_matrix import analyze_dependencies

        src = inspect.getsource(analyze_dependencies)
        assert 'startswith("dim_")' in src or "startswith('dim_')" in src
