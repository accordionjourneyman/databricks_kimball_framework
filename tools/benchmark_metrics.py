"""Parse measured performance metrics from completed Spark event logs."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

GROUP_PREFIX = "kimball-benchmark:"


def _empty_summary() -> dict[str, int]:
    keys = (
        "jobs stages tasks executor_run_time_ms jvm_gc_time_ms "
        "input_bytes input_rows output_bytes output_rows "
        "shuffle_read_bytes shuffle_read_rows shuffle_write_bytes "
        "shuffle_write_rows memory_spill_bytes disk_spill_bytes"
    )
    return {key: 0 for key in keys.split()}


def _value(mapping: dict[str, Any], key: str) -> int:
    value = mapping.get(key, 0)
    return int(value) if isinstance(value, (int, float)) else 0


def _events(root: Path) -> Iterator[dict[str, Any]]:
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix == ".crc":
            continue
        try:
            lines = path.open(encoding="utf-8", errors="replace")
        except OSError:
            continue
        with lines:
            for line in lines:
                try:
                    event = json.loads(line)
                except (json.JSONDecodeError, TypeError):
                    continue
                if isinstance(event, dict):
                    yield event


def _stage_id(event: dict[str, Any]) -> int | None:
    value = event.get("Stage ID")
    if value is None:
        value = event.get("Stage Info", {}).get("Stage ID")
    return int(value) if isinstance(value, (int, float)) else None


def _add_task_metrics(summary: dict[str, int], metrics: dict[str, Any]) -> None:
    summary["executor_run_time_ms"] += _value(metrics, "Executor Run Time")
    summary["jvm_gc_time_ms"] += _value(metrics, "JVM GC Time")
    summary["memory_spill_bytes"] += _value(metrics, "Memory Bytes Spilled")
    summary["disk_spill_bytes"] += _value(metrics, "Disk Bytes Spilled")
    input_metrics = metrics.get("Input Metrics", {})
    output_metrics = metrics.get("Output Metrics", {})
    shuffle_read = metrics.get("Shuffle Read Metrics", {})
    shuffle_write = metrics.get("Shuffle Write Metrics", {})
    summary["input_bytes"] += _value(input_metrics, "Bytes Read")
    summary["input_rows"] += _value(input_metrics, "Records Read")
    summary["output_bytes"] += _value(output_metrics, "Bytes Written")
    summary["output_rows"] += _value(output_metrics, "Records Written")
    summary["shuffle_read_bytes"] += _value(shuffle_read, "Remote Bytes Read") + _value(
        shuffle_read, "Local Bytes Read"
    )
    summary["shuffle_read_rows"] += _value(shuffle_read, "Records Read")
    summary["shuffle_write_bytes"] += _value(shuffle_write, "Shuffle Bytes Written")
    summary["shuffle_write_rows"] += _value(shuffle_write, "Shuffle Records Written")

    summary["shuffle_read_rows"] += _value(shuffle_read, "Total Records Read")


def _record_event(
    summary: dict[str, int], event: dict[str, Any], stage_ids: set[int] | None
) -> None:
    event_name = event.get("Event")
    stage_id = _stage_id(event)
    included = stage_ids is None or stage_id in stage_ids
    if event_name == "SparkListenerJobStart":
        summary["jobs"] += 1
    elif event_name == "SparkListenerStageCompleted" and included:
        summary["stages"] += 1
    elif event_name == "SparkListenerTaskEnd" and included:
        summary["tasks"] += 1
        _add_task_metrics(summary, event.get("Task Metrics", {}))


def summarize_event_logs(event_log_dir: str | Path) -> dict[str, int]:
    """Return measured-group metrics, falling back to the whole session.

    Benchmarks label timed Spark jobs with ``kimball-benchmark:`` job groups.
    Setup and correctness jobs are excluded when at least one such group exists.
    """

    root = Path(event_log_dir)
    if not root.exists():
        return _empty_summary()
    events = list(_events(root))
    all_summary = _empty_summary()
    measured_summary = _empty_summary()
    measured_stage_ids: set[int] = set()
    measured_jobs: set[int] = set()

    for event in events:
        _record_event(all_summary, event, None)
        if event.get("Event") != "SparkListenerJobStart":
            continue
        properties = event.get("Properties", {}) or {}
        group_id = properties.get("spark.jobGroup.id", "")
        if not str(group_id).startswith(GROUP_PREFIX):
            continue
        job_id = event.get("Job ID")
        if isinstance(job_id, (int, float)):
            measured_jobs.add(int(job_id))
        measured_summary["jobs"] += 1
        measured_stage_ids.update(int(value) for value in event.get("Stage IDs", []))

    if not measured_summary["jobs"]:
        return all_summary
    for event in events:
        if event.get("Event") == "SparkListenerJobStart":
            continue
        _record_event(measured_summary, event, measured_stage_ids)
    measured_summary["jobs"] = len(measured_jobs) or measured_summary["jobs"]
    return measured_summary
