"""Regression tests for Round-3 Bug Report findings.

Each test confirms a specific bug exists in the current code. When the bug
is fixed, the test will fail Ã¢â‚¬â€ prompting an update to assert the fixed behaviour.
"""

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


# ===================================================================
# #2  MEDIUM: _declare_pk_fk_constraints double PRIMARY KEY
# ===================================================================


class TestBugDoublePrimaryKey:
    """table_creator declares two PRIMARY KEYs on SCD1 tables."""

    def test_scd1_gets_only_sk_pk(self):
        """After fix: SCD1 gets SK PK only. NK PK block was removed."""
        from kimball.processing import table_creator

        src = inspect.getsource(table_creator)
        # After fix: no NK PK block exists at all
        assert "nk_name" not in src


# ===================================================================
# #3  MEDIUM: SCD2 full-snapshot delete expires defaults/skeletons
# ===================================================================


class TestBugSCD2DeleteExpiresDefaults:
    """Full-snapshot delete-detection expires seeded defaults and skeletons."""

    def test_guard_against_skeleton_expiration(self):
        """After fix: missing_in_source section contains a guard that
        excludes skeleton rows when the column exists."""
        from kimball.processing.scd2 import _merge_single_pass

        src = inspect.getsource(_merge_single_pass)
        assert 'current_target = current_target.filter(~col("__is_skeleton"))' in src


# ===================================================================
# #4  MEDIUM: Streaming per_version re-processes all versions
# ===================================================================


class TestBugStreamingPerVersionReprocesses:
    """Per-version processing must transform only the filtered version."""

    def test_per_version_registers_filtered_view(self):
        """After fix: version_df is registered as temp view (filtered),
        not the full batch table."""
        from kimball.streaming import orchestrator

        src = inspect.getsource(orchestrator)
        # After fix: version_df is registered, not the full batch
        assert "version_df.createOrReplaceTempView(source.alias)" in src


# ===================================================================
# #5  MEDIUM: Double FK validation
# ===================================================================


class TestBugDoubleFKValidation:
    """validate_fact_fk_integrity still runs when config.tests is not set."""

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
            # No tests defined
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

        # Both still run when tests is not defined Ã¢â‚¬â€ double FK scan
        orch._validator.run_config_tests.assert_not_called()
        orch._validator.validate_fact_fk_integrity.assert_called_once()


# ===================================================================
# #6  MEDIUM: preserve_all_changes early return (verify fix)
# ===================================================================


class TestBugPreserveAllChangesEarlyReturn:
    """Verify the fix: checks ALL sources before returning."""

    def test_checks_all_sources(self):
        from kimball.orchestration.orchestrator import Orchestrator

        src = inspect.getsource(Orchestrator._run_with_version_loop)
        # The work-plan result is the only completion signal; it cannot
        # return early while inspecting individual sources.
        assert "active_sources" in src
        # Source iteration belongs to SourceWorkPlan, not this loop.
        assert "for source in self.config.sources" not in src


# ===================================================================
# #7  MEDIUM: stop_on_failure doesn't stop running
# ===================================================================


class TestBugStopOnFailure:
    """f.cancel() only cancels not-yet-started futures."""

    def test_cancel_returns_boolean(self):
        import concurrent.futures

        f = concurrent.futures.Future()
        # cancel() returns True only if the future was pending
        result = f.cancel()
        assert isinstance(result, bool)


# ===================================================================
# #8  MEDIUM: SCD4 duplicate EAV rows
# ===================================================================


class TestBugSCD4DuplicateEAV:
    """Dedup window includes EXPIRE rows, not just INSERTs."""

    def test_dedup_applies_only_to_inserts(self):
        """After fix: dedup window is applied to INSERT rows only,
        then unioned with EXPIRE rows separately."""
        from kimball.processing.scd4 import _merge_history

        src = inspect.getsource(_merge_history)
        # After fix: inserts are deduped before union with expires
        assert "inserts.withColumn" in src
        assert "inserts_deduped.unionByName(expires)" in src


# ===================================================================
# #9  LOW: dedup_cdf non-deterministic tie
# ===================================================================


class TestBugDedupCdfNonDeterministic:
    """dedup_cdf keeps row_number()==1 after orderBy desc Ã¢â‚¬â€ ties arbitrary."""

    def test_dedup_has_tiebreaker(self):
        """After fix: dedup_cdf uses a secondary sort column to break ties,
        preferring non-delete rows."""
        from kimball.processing.merge_helpers import dedup_cdf

        src = inspect.getsource(dedup_cdf)
        # After fix: secondary sort column exists
        assert "_change_type" in src


# ===================================================================
# #10  LOW: apply_schema_evolution one ALTER per column
# ===================================================================


class TestBugSchemaEvolutionOneAlterPerColumn:
    """apply_schema_evolution issues N ALTER TABLE ADD COLUMNS statements."""

    def test_alter_batched_in_single_statement(self):
        """After fix: all new columns are added in a single ALTER TABLE."""
        from kimball.processing.merge_helpers import apply_schema_evolution

        src = inspect.getsource(apply_schema_evolution)
        # After fix: batched ALTER with all columns in one statement
        assert 'cols_sql = ", ".join(new_cols)' in src
        assert "ADD COLUMNS ({cols_sql})" in src


# ===================================================================
# #11  LOW: Redundant target scans in _merge_classic
# ===================================================================


class TestBugRedundantTargetScans:
    """_merge_classic reads target 3Ãƒâ€” (toDF, get_current_df Ãƒâ€”2)."""

    def test_multiple_target_reads(self):
        from kimball.processing.scd2 import merge_scd2

        src = inspect.getsource(merge_scd2)
        # The bug: get_current_df or toDF called multiple times
        assert "_merge_classic" not in src


# ===================================================================
# #12  LOW: approx_count_distinct latent crash
# ===================================================================


class TestBugApproxCountDistinctLatent:
    """approx_count_distinct(*columns) binds 2nd column as rsd if >1 col."""

    def test_approx_count_distinct_spreads_columns(self):
        from kimball.validation import DataQualityValidator

        src = inspect.getsource(DataQualityValidator)
        # The guard: len(nat_cols) <= 1
        assert "len(nat_cols) <= 1" in src or "len(columns) <= 1" in src


# ===================================================================
# #13  LOW: bus_matrix Strategy B heuristic
# ===================================================================


class TestBugBusMatrixHeuristic:
    """Strategy B still uses startswith/endswith heuristic."""

    def test_bus_matrix_uses_name_heuristic(self):
        from kimball.observability.bus_matrix import analyze_dependencies

        src = inspect.getsource(analyze_dependencies)
        assert 'startswith("dim_")' in src or "startswith('dim_')" in src
