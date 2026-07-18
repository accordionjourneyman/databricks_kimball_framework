"""Reproducible benchmark runner for micro and Spark/Delta suites."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import platform
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

try:
    from benchmark_metrics import summarize_event_logs
except ModuleNotFoundError:
    from tools.benchmark_metrics import summarize_event_logs

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = 1
DEFAULT_THRESHOLD = 0.15
SUITES = {
    "micro": "tests/unit/test_performance.py",
    "spark": "tests/benchmarks",
}


class BenchmarkCompatibilityError(ValueError):
    """Raised when benchmark runs were produced on unlike testbeds."""


def _package_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "not-installed"


def _git(args: Sequence[str], repo_root: Path) -> str:
    args = ["-c", f"safe.directory={repo_root}", *args]
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
        return completed.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _memory_bytes() -> int | None:
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    return None


def build_manifest(
    *,
    suite: str,
    scale: str,
    testbed_id: str,
    repo_root: Path = REPO_ROOT,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Build the comparison contract for one benchmark run."""
    env = os.environ if environ is None else environ
    dirty_output = _git(["status", "--porcelain"], repo_root)
    return {
        "schema_version": SCHEMA_VERSION,
        "created_at": datetime.now(UTC).isoformat(),
        "suite": suite,
        "scale": scale,
        "testbed_id": testbed_id,
        "git": {
            "commit_sha": _git(["rev-parse", "HEAD"], repo_root),
            "branch": _git(["branch", "--show-current"], repo_root),
            "dirty": dirty_output not in {"", "unknown"},
        },
        "runtime": _runtime_metadata(env),
        "hardware": _hardware_metadata(),
    }


def _runtime_metadata(env: Mapping[str, str]) -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "java": env.get("JAVA_VERSION", "17"),
        "spark": _package_version("pyspark"),
        "delta": _package_version("delta-spark"),
        "docker_image": env.get("KIMBALL_BENCHMARK_IMAGE", "unknown"),
    }


def _hardware_metadata() -> dict[str, Any]:
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor() or "unknown",
        "logical_cores": os.cpu_count() or 1,
        "memory_bytes": _memory_bytes(),
    }


def validate_comparable(
    baseline: Mapping[str, Any], current: Mapping[str, Any]
) -> None:
    """Reject scientifically invalid cross-testbed/runtime comparisons."""
    if baseline.get("testbed_id") != current.get("testbed_id"):
        raise BenchmarkCompatibilityError("testbed_id differs")
    baseline_runtime = baseline.get("runtime", {})
    current_runtime = current.get("runtime", {})
    for field in ("python", "java", "spark", "delta", "docker_image"):
        if baseline_runtime.get(field) != current_runtime.get(field):
            raise BenchmarkCompatibilityError(f"runtime {field} differs")


def _medians(path: Path) -> dict[str, float]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        item["name"]: float(item["stats"]["median"])
        for item in payload.get("benchmarks", [])
        if item.get("stats", {}).get("median") is not None
    }


def compare_benchmark_files(
    baseline_path: Path,
    current_path: Path,
    *,
    threshold: float = DEFAULT_THRESHOLD,
) -> list[dict[str, Any]]:
    """Compare like-named medians and mark regressions above the threshold."""
    baseline = _medians(baseline_path)
    current = _medians(current_path)
    comparisons: list[dict[str, Any]] = []
    for name in sorted(baseline.keys() & current.keys()):
        old = baseline[name]
        new = current[name]
        change = ((new - old) / old) if old else 0.0
        comparisons.append(
            {
                "name": name,
                "baseline_median": old,
                "current_median": new,
                "change_percent": change * 100,
                "regression": change > threshold,
            }
        )
    return comparisons


def build_bencher_command(
    result_path: Path,
    *,
    project: str,
    branch: str,
    testbed: str,
) -> list[str]:
    """Return a token-free Bencher command."""
    return [
        "bencher",
        "run",
        "--project",
        project,
        "--branch",
        branch,
        "--testbed",
        testbed,
        "--adapter",
        "python_pytest",
        "--average",
        "median",
        "--file",
        str(result_path),
    ]


def _resolve_baseline(path: Path) -> tuple[Path, Path]:
    if path.is_dir():
        return path / "pytest-benchmark.json", path / "manifest.json"
    return path, path.with_name("manifest.json")


def _write_summary(
    path: Path,
    manifest: Mapping[str, Any],
    comparisons: list[dict[str, Any]],
    spark_metrics: Mapping[str, int],
) -> None:
    suite = manifest["suite"]
    scale = manifest["scale"]
    testbed_id = manifest["testbed_id"]
    commit_sha = manifest["git"]["commit_sha"]
    jobs = spark_metrics["jobs"]
    stages = spark_metrics["stages"]
    tasks = spark_metrics["tasks"]
    lines = [
        f"# Benchmark: {suite} / {scale}",
        "",
        f"- Testbed: {testbed_id}",
        f"- Commit: {commit_sha}",
        f"- Spark jobs/stages/tasks: {jobs}/{stages}/{tasks}",
        "",
    ]
    if comparisons:
        lines.extend(
            [
                "| Benchmark | Baseline | Current | Change | Advisory |",
                "|---|---:|---:|---:|:---:|",
            ]
        )
        for item in comparisons:
            status = "REGRESSION" if item["regression"] else "OK"
            name = item["name"]
            baseline_median = item["baseline_median"]
            current_median = item["current_median"]
            change_percent = item["change_percent"]
            lines.append(
                f"| {name} | {baseline_median:.6f}s | "
                f"{current_median:.6f}s | "
                f"{change_percent:+.1f}% | {status} |"
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=sorted(SUITES), default="spark")
    parser.add_argument("--scale", choices=("tiny", "small", "medium"), default="tiny")
    parser.add_argument("--output-dir", default="benchmark-results")
    parser.add_argument(
        "--testbed",
        default=os.environ.get("KIMBALL_BENCHMARK_TESTBED", "tiago-windows-docker"),
    )
    parser.add_argument("--compare", type=Path)
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    parser.add_argument("--fail-on-regression", action="store_true")
    parser.add_argument("--save-baseline")
    parser.add_argument("--publish", action="store_true")
    parser.add_argument(
        "--bencher-project",
        default=os.environ.get("BENCHER_PROJECT", "kimball-framework"),
    )
    return parser.parse_args(argv)


def _run_pytest(
    args: argparse.Namespace, run_dir: Path, event_dir: Path
) -> tuple[subprocess.CompletedProcess[Any], Path]:
    result_path = run_dir / "pytest-benchmark.json"
    command = [
        sys.executable,
        "-m",
        "pytest",
        SUITES[args.suite],
        "-q",
        "--benchmark-only",
        f"--benchmark-json={result_path}",
        "--benchmark-disable-gc",
    ]
    if args.suite == "spark":
        command.extend(["--scale", args.scale])
    env = os.environ.copy()
    env["KIMBALL_BENCHMARK_EVENTLOG_DIR"] = str(event_dir)
    completed = subprocess.run(command, cwd=REPO_ROOT, env=env, check=False)
    return completed, result_path


def _compare(
    args: argparse.Namespace,
    manifest: Mapping[str, Any],
    result_path: Path,
    run_dir: Path,
) -> list[dict[str, Any]]:
    if not args.compare:
        return []
    baseline_result, baseline_manifest = _resolve_baseline(args.compare.resolve())
    baseline_metadata = json.loads(baseline_manifest.read_text(encoding="utf-8"))
    validate_comparable(baseline_metadata, manifest)
    comparisons = compare_benchmark_files(
        baseline_result, result_path, threshold=args.threshold
    )
    if not comparisons:
        raise BenchmarkCompatibilityError(
            "baseline and current run share no benchmark names"
        )
    (run_dir / "comparison.json").write_text(
        json.dumps(comparisons, indent=2), encoding="utf-8"
    )
    return comparisons


def validate_baseline_candidate(manifest: Mapping[str, Any]) -> None:
    git = manifest.get("git", {})
    if git.get("dirty"):
        raise ValueError("Baseline promotion requires a clean worktree")
    if git.get("branch") not in {"main", "master"}:
        raise ValueError("Baseline promotion requires main or master")
    if git.get("commit_sha") in {None, "", "unknown"}:
        raise ValueError("Baseline promotion requires a known commit")


def _save_baseline(
    args: argparse.Namespace,
    result_path: Path,
    manifest_path: Path,
    manifest: Mapping[str, Any],
) -> None:
    if not args.save_baseline:
        return
    validate_baseline_candidate(manifest)
    baseline_dir = (
        REPO_ROOT
        / args.output_dir
        / "baselines"
        / args.testbed
        / f"{args.suite}-{args.scale}-{args.save_baseline}"
    )
    baseline_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(result_path, baseline_dir / result_path.name)
    shutil.copy2(manifest_path, baseline_dir / manifest_path.name)


def _publish(
    args: argparse.Namespace,
    result_path: Path,
    manifest: Mapping[str, Any],
) -> None:
    if not args.publish:
        return
    if not os.environ.get("BENCHER_API_KEY"):
        raise RuntimeError("BENCHER_API_KEY is required for --publish")
    if shutil.which("bencher") is None:
        raise RuntimeError("Bencher CLI is required for --publish")
    command = build_bencher_command(
        result_path,
        project=args.bencher_project,
        branch=manifest["git"]["branch"],
        testbed=args.testbed,
    )
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_dir = (REPO_ROOT / args.output_dir / "raw" / run_id).resolve()
    event_dir = run_dir / "spark-events"
    run_dir.mkdir(parents=True, exist_ok=False)
    event_dir.mkdir()
    manifest = build_manifest(
        suite=args.suite,
        scale=args.scale,
        testbed_id=args.testbed,
    )
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    completed, result_path = _run_pytest(args, run_dir, event_dir)
    spark_metrics = summarize_event_logs(event_dir)
    (run_dir / "spark-metrics.json").write_text(
        json.dumps(spark_metrics, indent=2), encoding="utf-8"
    )
    comparisons: list[dict[str, Any]] = []
    if completed.returncode == 0:
        comparisons = _compare(args, manifest, result_path, run_dir)
        _save_baseline(args, result_path, manifest_path, manifest)
        _publish(args, result_path, manifest)
    _write_summary(run_dir / "summary.md", manifest, comparisons, spark_metrics)
    print(f"Benchmark artifacts: {run_dir}")
    if completed.returncode != 0:
        return completed.returncode
    if args.fail_on_regression and any(item["regression"] for item in comparisons):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
