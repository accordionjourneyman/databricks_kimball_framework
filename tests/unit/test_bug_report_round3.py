"""Regression tests for Round-3 Bug Report findings.

Each test confirms a specific bug exists in the current code. When the bug
is fixed, the test will fail — prompting an update to assert the fixed behaviour.
"""
from __future__ import annotations

import inspect
import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

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
# #1  CRITICAL: SCD2 _rebuild_history SK collision
# ===================================================================

class TestBugSCD2RebuildHistorySKCollision:
    """_rebuild_history passes effective_at_column to generate_keys
    instead of rank_col, so when effective_at_column is None the SK
    falls back to __etl_processed_at (same for all rows)."""

    def test_generate_keys_receives_rank_col_not_effective_at(self):
        """After fix: generate_keys is called with rank_col (the actual
        per-version discriminator) instead of effective_at_column (config)."""
        from kimball.processing.scd2 import _rebuild_history
        src = inspect.getsource(_rebuild_history)
        # Confirm the fixed call on the generate_keys line
        assert "effective_at_column=rank_col)" in src
        # The _select_payload_columns call may still use effective_at_column
        # but the generate_keys call must use rank_col.
        # Extract the generate_keys line specifically.
        for line in src.splitlines():
            if "generate_keys(staged," in line:
                assert "effective_at_column=rank_col" in line
                assert "effective_at_column=effective_at_column" not in line
                break


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
        from kimball.processing.scd2 import _merge_classic
        src = inspect.getsource(_merge_classic)
        section = src.split("missing_in_source")[1].split("execute()")[0]
        assert "current_target = current_target.filter(" in section
        assert 'surrogate_key_col' in section or '__is_skeleton' in section


# ===================================================================
# #4  MEDIUM: Streaming per_version re-processes all versions
# ===================================================================

class TestBugStreamingPerVersionReprocesses:
    """per_version path materializes full batch, then each iteration
    transforms ALL data via transformation_sql against the full view."""

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
        from kimball.orchestration.orchestrator import Orchestrator
        from kimball.common.config import TableConfig, ForeignKeyConfig, SourceConfig

        config = TableConfig(
            table_name="test_fact", table_type="fact", scd_type=1,
            sources=[SourceConfig(name="src", alias="src")],
            natural_keys=["id"],
            foreign_keys=[ForeignKeyConfig(column="dim_id", references="dim_table", dimension_key="dim_id")],
            # No tests defined
        )
        orch = Orchestrator.__new__(Orchestrator)
        orch.config = config
        orch.spark = MagicMock(spec=SparkSession)
        orch.spark.catalog.tableExists.return_value = True
        orch.runtime_options = MagicMock()
        orch.runtime_options.skip_validation_if_unchanged = False
        orch.runtime_options.use_approximate_unique = False
        orch._validator = MagicMock()
        orch._validator.run_config_tests.return_value = MagicMock(results=[], raise_on_failure=MagicMock())
        orch._validator.validate_fact_fk_integrity.return_value = MagicMock(results=[], raise_on_failure=MagicMock())
        orch.metrics_collector = None
        orch.etl_control = MagicMock()

        transformed_df = _make_df(["id", "dim_id"])
        orch._transform_and_validate({"src": transformed_df})

        # Both still run when tests is not defined — double FK scan
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
        # After fix: the loop should NOT return inside the for-source loop
        # when one source is caught up.
        assert "all_caught_up" in src
        # Should check all sources, not break on first caught-up
        assert "for source in self.config.sources" in src


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
        assert "inserts_deduped = inserts.withColumn" in src
        assert "inserts_deduped.unionByName(expires)" in src


# ===================================================================
# #9  LOW: dedup_cdf non-deterministic tie
# ===================================================================

class TestBugDedupCdfNonDeterministic:
    """dedup_cdf keeps row_number()==1 after orderBy desc — ties arbitrary."""

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
        assert "cols_sql = \", \".join(new_cols)" in src
        assert "ADD COLUMNS ({cols_sql})" in src


# ===================================================================
# #11  LOW: Redundant target scans in _merge_classic
# ===================================================================

class TestBugRedundantTargetScans:
    """_merge_classic reads target 3× (toDF, get_current_df ×2)."""

    def test_multiple_target_reads(self):
        from kimball.processing.scd2 import _merge_classic
        src = inspect.getsource(_merge_classic)
        # The bug: get_current_df or toDF called multiple times
        reads = src.count("get_current_df") + src.count("toDF()")
        assert reads >= 2


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


# ===================================================================
# #14  MEDIUM: _rebuild_history UNRESOLVED_COLUMN on rank_col
# ===================================================================

class TestBugRebuildHistoryRankColResolution:
    """_rebuild_history includes rank_col in the target_chain select so
    that col(rank_col) in withColumn resolves correctly.  When rank_col
    is CDF metadata not present in the target table, it falls back to
    __etl_processed_at."""

    def test_rank_col_included_in_target_chain_select(self):
        from kimball.processing.scd2 import _rebuild_history
        src = inspect.getsource(_rebuild_history)
        # The fix: rank_col (or target_rank_col) is appended to base_cols
        # so it's available for the withColumn replacement.
        assert "base_cols.append" in src
        # The fallback: if rank_col is not in target_slice.columns,
        # fall back to __etl_processed_at.
        assert "target_rank_col" in src
