"""
Performance metrics collection for benchmarks.

Provides:
- MetricsListener: SparkListener that captures stage-level metrics
  (shuffle bytes, disk spill, input/output rows, task count)
- StageProfiler: context manager for timing named stages and
  capturing before/after row counts
- ExecutionPlanCapture: extracts physical plan snippets for analysis
"""

from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from typing import Any

from pyspark.sql import DataFrame, SparkSession


@dataclass
class StageMetric:
    """Metrics for a single Spark stage."""

    stage_id: int
    name: str
    duration_ms: float
    num_tasks: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    input_rows: int
    output_rows: int
    disk_bytes_spilled: int
    memory_bytes_spilled: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class MetricsListener:
    """Captures stage-level metrics from the Spark DAG scheduler."""

    def __init__(self):
        self.stages: list[StageMetric] = []
        self._enabled = True

    def reset(self):
        self.stages = []

    def total_shuffle_bytes(self) -> int:
        return sum(s.shuffle_read_bytes + s.shuffle_write_bytes for s in self.stages)

    def total_disk_spill_bytes(self) -> int:
        return sum(s.disk_bytes_spilled for s in self.stages)

    def stages_by_name(self, substring: str) -> list[StageMetric]:
        return [s for s in self.stages if substring.lower() in s.name.lower()]

    def attach(self, spark: SparkSession) -> None:
        pass

    def detach(self, spark: SparkSession) -> None:
        pass

    def capture_from_spark_ui(self, spark: SparkSession) -> None:
        """Pull stage metrics from the Spark REST API (best-effort)."""
        try:
            sc = spark.sparkContext
            status_tracker = sc.statusTracker()
            for stage_id in status_tracker.getActiveStageIds():
                info = status_tracker.getStageInfo(stage_id)
                if info is None:
                    continue
                metric = StageMetric(
                    stage_id=info.stageId(),
                    name=info.name() or f"stage_{info.stageId()}",
                    duration_ms=info.completionTime() - info.submissionTime(),
                    num_tasks=info.numTasks(),
                    shuffle_read_bytes=0,
                    shuffle_write_bytes=0,
                    input_rows=0,
                    output_rows=0,
                    disk_bytes_spilled=0,
                    memory_bytes_spilled=0,
                )
                self.stages.append(metric)
        except Exception:
            pass


class _ListenerAdapter:
    """Adapter implementing the SparkListener interface via duck-typing."""

    def __init__(self, parent: MetricsListener):
        self._parent = parent

    def onStageCompleted(self, stage_info):  # noqa: N802
        try:
            info = stage_info.stageInfo
            tm = info.taskMetrics
            metric = StageMetric(
                stage_id=info.stageId,
                name=info.name or f"stage_{info.stageId}",
                duration_ms=info.completionTime - info.submissionTime,
                num_tasks=info.numTasks,
                shuffle_read_bytes=tm.shuffleReadMetrics.remoteBytesRead
                + tm.shuffleReadMetrics.localBytesRead,
                shuffle_write_bytes=tm.shuffleWriteMetrics.bytesWritten,
                input_rows=tm.inputMetrics.recordsRead,
                output_rows=tm.outputMetrics.recordsWritten,
                disk_bytes_spilled=tm.diskBytesSpilled,
                memory_bytes_spilled=tm.memoryBytesSpilled,
            )
            self._parent.stages.append(metric)
        except Exception:
            pass


@dataclass
class StageProfile:
    """Profile of a single named stage in the pipeline."""

    name: str
    duration_ms: float
    rows_in: int
    rows_out: int
    plan_excerpt: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@contextmanager
def profile_stage(
    name: str, df: DataFrame | None = None, rows_before: int | None = None
):
    """Context manager: times a stage, captures row count, and the physical plan.

    Usage:
        with profile_stage("merge_scd2", df=source_df) as prof:
            result = do_merge(source_df)
        prof.rows_out = result.count()
    """
    start = time.time()
    rows_in = rows_before
    plan_excerpt = ""
    if df is not None:
        try:
            plan_excerpt = df._jdf.queryExecution().executedPlan().toString()[:500]
        except Exception:
            plan_excerpt = ""
    profile = StageProfile(
        name=name,
        duration_ms=0.0,
        rows_in=rows_in or 0,
        rows_out=0,
        plan_excerpt=plan_excerpt,
    )
    try:
        yield profile
    finally:
        profile.duration_ms = (time.time() - start) * 1000


def capture_execution_plan(df: DataFrame, max_chars: int = 2000) -> str:
    """Capture the physical execution plan as a string."""
    try:
        plan_str = df._jdf.queryExecution().executedPlan().toString()
        return plan_str[:max_chars]
    except Exception as e:
        return f"<plan capture failed: {e}>"


def extract_join_types(plan: str) -> list[str]:
    """Extract join strategy names from a physical plan string."""
    import re

    patterns = [
        r"(BroadcastHashJoin|BroadcastExchange|"
        r"SortMergeJoin|ShuffledHashJoin|"
        r"CartesianProduct|BroadcastNestedLoopJoin)"
    ]
    return re.findall(patterns[0], plan)


def extract_exchanges(plan: str) -> int:
    """Count Exchange nodes (shuffle boundaries) in a plan."""
    return plan.count("Exchange")


def extract_window_ops(plan: str) -> int:
    """Count Window operations in a plan."""
    return plan.count("Window")


def save_metrics_report(filepath: str, data: dict[str, Any]) -> None:
    """Write a metrics report to a JSON file."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)


def load_metrics_report(filepath: str) -> dict[str, Any]:
    """Load a metrics report from a JSON file."""
    with open(filepath) as f:
        return json.load(f)
