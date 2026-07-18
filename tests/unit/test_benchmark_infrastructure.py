from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.benchmark_metrics import summarize_event_logs
from tools.benchmark_runner import (
    BenchmarkCompatibilityError,
    _publish,
    build_bencher_command,
    build_manifest,
    compare_benchmark_files,
    validate_baseline_candidate,
    validate_comparable,
)


def test_manifest_contains_reproducibility_metadata(tmp_path: Path) -> None:
    manifest = build_manifest(
        suite="spark",
        scale="tiny",
        testbed_id="tiago-windows-docker",
        repo_root=tmp_path,
        environ={
            "KIMBALL_BENCHMARK_IMAGE": "kimball-tests:spark-4.0.1-delta-4.2.0",
        },
    )

    assert manifest["schema_version"] == 1
    assert manifest["suite"] == "spark"
    assert manifest["scale"] == "tiny"
    assert manifest["testbed_id"] == "tiago-windows-docker"
    assert manifest["runtime"]["docker_image"] == (
        "kimball-tests:spark-4.0.1-delta-4.2.0"
    )
    assert manifest["hardware"]["logical_cores"] >= 1
    assert "python" in manifest["runtime"]
    assert "commit_sha" in manifest["git"]
    assert "dirty" in manifest["git"]


def test_comparison_rejects_different_testbeds_or_runtimes() -> None:
    baseline = {
        "testbed_id": "local-a",
        "runtime": {"spark": "4.0.1", "delta": "4.2.0", "python": "3.11.9"},
    }

    with pytest.raises(BenchmarkCompatibilityError, match="testbed_id"):
        validate_comparable(
            baseline,
            {**baseline, "testbed_id": "github-hosted"},
        )


def test_baseline_promotion_requires_clean_protected_branch() -> None:
    baseline = {
        "testbed_id": "local-a",
        "runtime": {"spark": "4.0.1", "delta": "4.2.0", "python": "3.11.9"},
    }
    manifest = {
        "git": {
            "branch": "master",
            "commit_sha": "abc123",
            "dirty": False,
        }
    }
    validate_baseline_candidate(manifest)

    with pytest.raises(ValueError, match="clean"):
        validate_baseline_candidate(
            {**manifest, "git": {**manifest["git"], "dirty": True}}
        )
    with pytest.raises(ValueError, match="master"):
        validate_baseline_candidate(
            {**manifest, "git": {**manifest["git"], "branch": "feature/perf"}}
        )

    with pytest.raises(BenchmarkCompatibilityError, match="spark"):
        validate_comparable(
            baseline,
            {
                **baseline,
                "runtime": {
                    **baseline["runtime"],
                    "spark": "4.1.0",
                },
            },
        )


def test_comparison_reports_advisory_regression(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.json"
    current = tmp_path / "current.json"
    baseline.write_text(
        json.dumps(
            {"benchmarks": [{"name": "scd1_changed", "stats": {"median": 10.0}}]}
        ),
        encoding="utf-8",
    )
    current.write_text(
        json.dumps(
            {"benchmarks": [{"name": "scd1_changed", "stats": {"median": 11.6}}]}
        ),
        encoding="utf-8",
    )

    comparison = compare_benchmark_files(baseline, current, threshold=0.15)

    assert comparison[0]["change_percent"] == pytest.approx(16.0)
    assert comparison[0]["regression"] is True


def test_event_log_summary_collects_spark_task_metrics(tmp_path: Path) -> None:
    event_log = tmp_path / "local-1"
    events = [
        {"Event": "SparkListenerJobStart"},
        {"Event": "SparkListenerStageCompleted"},
        {
            "Event": "SparkListenerTaskEnd",
            "Task Metrics": {
                "Executor Run Time": 120,
                "JVM GC Time": 7,
                "Memory Bytes Spilled": 11,
                "Disk Bytes Spilled": 13,
                "Input Metrics": {"Bytes Read": 100, "Records Read": 10},
                "Output Metrics": {"Bytes Written": 200, "Records Written": 20},
                "Shuffle Read Metrics": {
                    "Remote Bytes Read": 30,
                    "Local Bytes Read": 40,
                    "Records Read": 3,
                },
                "Shuffle Write Metrics": {
                    "Shuffle Bytes Written": 50,
                    "Shuffle Records Written": 5,
                },
            },
        },
    ]
    event_log.write_text(
        "\n".join(json.dumps(event) for event in events), encoding="utf-8"
    )

    metrics = summarize_event_logs(tmp_path)

    assert metrics["jobs"] == 1
    assert metrics["stages"] == 1
    assert metrics["tasks"] == 1
    assert metrics["executor_run_time_ms"] == 120
    assert metrics["shuffle_read_bytes"] == 70
    assert metrics["shuffle_write_bytes"] == 50
    assert metrics["disk_spill_bytes"] == 13


def test_bencher_command_uses_pytest_adapter_without_embedding_token(
    tmp_path: Path,
) -> None:
    result = tmp_path / "pytest-benchmark.json"
    command = build_bencher_command(
        result,
        project="kimball-framework",
        branch="feature/perf",
        testbed="tiago-windows-docker",
    )

    assert command == [
        "bencher",
        "run",
        "--project",
        "kimball-framework",
        "--branch",
        "feature/perf",
        "--testbed",
        "tiago-windows-docker",
        "--adapter",
        "python_pytest",
        "--average",
        "median",
        "--file",
        str(result),
    ]


def test_publish_requires_current_bencher_api_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("BENCHER_API_KEY", raising=False)
    monkeypatch.setenv("BENCHER_API_TOKEN", "deprecated-token")
    args = type("Args", (), {"publish": True})()

    with pytest.raises(RuntimeError, match="BENCHER_API_KEY"):
        _publish(
            args,
            tmp_path / "pytest-benchmark.json",
            {"git": {"branch": "master"}},
        )


def test_event_log_parser_ignores_non_object_json(tmp_path: Path) -> None:
    event_log = tmp_path / "metadata"
    event_log.write_text("42\nnull\n", encoding="utf-8")

    metrics = summarize_event_logs(tmp_path)

    assert metrics["jobs"] == 0
    assert metrics["tasks"] == 0


def test_event_log_parser_prefers_measured_job_groups(tmp_path: Path) -> None:
    event_log = tmp_path / "events"
    events = [
        {
            "Event": "SparkListenerJobStart",
            "Stage IDs": [1],
            "Properties": {"spark.jobGroup.id": "kimball-benchmark:scd1_changed"},
        },
        {"Event": "SparkListenerJobStart", "Stage IDs": [2]},
        {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": 1,
            "Task Metrics": {"Executor Run Time": 10},
        },
        {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": 2,
            "Task Metrics": {"Executor Run Time": 99},
        },
    ]
    event_log.write_text(
        "\n".join(json.dumps(event) for event in events), encoding="utf-8"
    )

    metrics = summarize_event_logs(tmp_path)

    assert metrics["jobs"] == 1
    assert metrics["tasks"] == 1
    assert metrics["executor_run_time_ms"] == 10
