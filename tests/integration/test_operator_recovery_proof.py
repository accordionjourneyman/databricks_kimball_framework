"""Runtime fault-injection proof for the Phase 1 operator workflow.

These tests deliberately leave the two crash states that ROADMAP 1.8d requires:
an orphan Delta commit with no control row and a fresh RUNNING control row whose
target commit has completed.  They use the real Spark/Delta adapters and run on
both local (Classic semantics) and Databricks Serverless.
"""

from __future__ import annotations

import uuid

import pytest
from pyspark.sql import SparkSession

from kimball.ops.errors import ErrorCategory, StructuredError
from kimball.ops.recover import recover_target
from kimball.ops.runtime_profile import detect_runtime_profile
from kimball.ops.spark_adapters import build_providers
from kimball.orchestration.watermark import ETLControlManager

pytestmark = pytest.mark.usefixtures("spark")


def _create_target(spark: SparkSession, table: str) -> int:
    spark.sql(f"CREATE TABLE {table} (id INT, value STRING) USING DELTA")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'baseline')")
    return int(spark.sql(f"DESCRIBE HISTORY {table}").first()["version"])


def _inject_commit(
    spark: SparkSession, table: str, batch_id: str, *, tag: bool
) -> None:
    conf = "spark.databricks.delta.commitInfo.userMetadata"
    if tag:
        spark.conf.set(conf, batch_id)
    try:
        spark.sql(f"INSERT INTO {table} VALUES (2, 'fault-injected')")
    finally:
        if tag:
            spark.conf.unset(conf)


def test_orphan_commit_operator_proof(spark: SparkSession, test_db: str) -> None:
    """Recover an attributable orphan or stop safely when tags are unavailable."""
    target = f"{test_db}.operator_orphan"
    batch_id = f"orphan-{uuid.uuid4().hex}"
    baseline = _create_target(spark, target)
    runtime = detect_runtime_profile(spark)
    providers = build_providers(spark, test_db)

    _inject_commit(
        spark,
        target,
        batch_id,
        tag=runtime.supports_commit_tagging,
    )

    if not runtime.supports_commit_tagging:
        # Serverless cannot attribute an unrecorded commit.  The operator tool
        # must stop with a structured, actionable error rather than report a
        # successful recovery of a batch it cannot identify.
        with pytest.raises(StructuredError) as exc_info:
            recover_target(
                target,
                providers,
                runtime,
                batch_id=batch_id,
                force=True,
            )
        assert exc_info.value.category is ErrorCategory.RECOVERY
        assert "no RUNNING batch or orphan commit" in str(exc_info.value)
        assert spark.table(target).count() == 2
        return

    result = recover_target(
        target,
        providers,
        runtime,
        batch_id=batch_id,
        force=True,
    )
    assert result.partial is False
    assert result.plans[0].restore_version == baseline
    assert spark.table(target).collect()[0].asDict() == {
        "id": 1,
        "value": "baseline",
    }


def test_fresh_zombie_operator_proof(spark: SparkSession, test_db: str) -> None:
    """A seconds-old RUNNING row is recovered without waiting for a TTL."""
    target = f"{test_db}.operator_fresh_zombie"
    source = f"{test_db}.operator_source"
    batch_id = f"fresh-{uuid.uuid4().hex}"
    baseline = _create_target(spark, target)
    runtime = detect_runtime_profile(spark)
    control = ETLControlManager(etl_schema=test_db, spark_session=spark)
    control.batch_start_all(target, [source], run_batch_id=batch_id)
    providers = build_providers(spark, test_db)

    _inject_commit(
        spark,
        target,
        batch_id,
        tag=runtime.supports_commit_tagging,
    )

    result = recover_target(
        target,
        providers,
        runtime,
        batch_id=batch_id,
        version=None if runtime.supports_commit_tagging else baseline,
    )

    assert result.partial is False
    assert result.plans[0].restore_version == baseline
    assert spark.table(target).count() == 1
    state = providers.control.get_target_state(target)
    # A first-run zombie has no prior watermark.  Rewinding it to ``None``
    # deletes the current-state row, so the durable invariant is that no
    # RUNNING row survives.  With a prior watermark the row is retained as a
    # recovered/failed record instead.
    assert all(batch.status != "RUNNING" for batch in state.batches)
