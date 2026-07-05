"""
Benchmark analyzer: reads JSON results, identifies bottlenecks, generates a report.

Usage:
    python tools/benchmark_analyzer.py tools/benchmarks/results/benchmark_*.json
    python tools/benchmark_analyzer.py --latest
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from collections import defaultdict
from typing import Any


def load_latest(results_dir: str) -> dict[str, Any]:
    """Load the most recent benchmark report."""
    files = sorted(glob.glob(os.path.join(results_dir, "benchmark_*.json")))
    if not files:
        print(f"No benchmark files found in {results_dir}")
        sys.exit(1)
    with open(files[-1]) as f:
        return json.load(f)


def load_files(paths: list[str]) -> list[dict[str, Any]]:
    """Load multiple benchmark reports and merge their results."""
    merged: list[dict[str, Any]] = []
    for p in paths:
        with open(p) as f:
            data = json.load(f)
        merged.extend(data.get("results", []))
    return merged


def load_all_individual(results_dir: str) -> list[dict[str, Any]]:
    """Load all individual benchmark result files (scenario_scale.json format)."""
    merged: list[dict[str, Any]] = []
    for pattern in ["*_tiny.json", "*_small.json", "*_medium.json"]:
        for filepath in glob.glob(os.path.join(results_dir, pattern)):
            with open(filepath) as f:
                data = json.load(f)
            data["first_run_total_shuffle_bytes"] = 0
            data["second_run_total_shuffle_bytes"] = 0
            data["first_run_disk_spill_bytes"] = 0
            data["second_run_disk_spill_bytes"] = 0
            data["first_run_exchanges"] = 0
            data["second_run_exchanges"] = 0
            data["first_run_windows"] = 0
            data["second_run_windows"] = 0
            data["first_run_join_types"] = []
            data["second_run_join_types"] = []
            data["first_run_stages"] = []
            data["second_run_stages"] = []
            data["rows_in_target"] = 0
            merged.append(data)
    return merged


def print_summary_table(results: list[dict[str, Any]]) -> None:
    """Print a summary table of all results."""
    print(
        f"\n{'Scenario':<30} {'Scale':<12} "
        f"{'1st(ms)':>8} {'2nd(ms)':>8} "
        f"{'1st(MB)':>8} {'2nd(MB)':>8} "
        f"{'1stExch':>7} {'2ndExch':>7}"
    )
    print("-" * 100)
    for r in results:
        print(
            f"{r['scenario']:<30} {r['scale']:<12} "
            f"{r['first_run_total_ms']:>8.0f} {r['second_run_total_ms']:>8.0f} "
            f"{r['first_run_total_shuffle_bytes'] / 1024 / 1024:>8.1f} "
            f"{r['second_run_total_shuffle_bytes'] / 1024 / 1024:>8.1f} "
            f"{r['first_run_exchanges']:>7} {r['second_run_exchanges']:>7}"
        )


def identify_bottlenecks(results: list[dict[str, Any]]) -> list[str]:
    """Analyze results and identify the top bottlenecks."""
    findings: list[str] = []

    if not results:
        return findings

    # 1. Shuffle-heavy scenarios (high shuffle / duration ratio)
    shuffle_ratios = []
    for r in results:
        dur = r["second_run_total_ms"] or 1
        shuf = r["second_run_total_shuffle_bytes"]
        shuffle_ratios.append((r["scenario"], r["scale"], shuf / dur))
    shuffle_ratios.sort(key=lambda x: -x[2])
    findings.append(
        f"Highest shuffle/duration ratio: {shuffle_ratios[0][0]} at {shuffle_ratios[0][1]} "
        f"({shuffle_ratios[0][2]:.0f} bytes/ms)"
    )

    # 2. Scenarios with disk spill
    spill = [r for r in results if r["second_run_disk_spill_bytes"] > 0]
    if spill:
        findings.append(
            f"Disk spill detected in {len(spill)} scenario(s): "
            + ", ".join(f"{r['scenario']}@{r['scale']}" for r in spill)
        )
    else:
        findings.append("No disk spill detected at any scale tier tested.")

    # 3. High exchange count (many shuffle boundaries)
    high_exch = [r for r in results if r["second_run_exchanges"] > 10]
    if high_exch:
        findings.append(
            "High shuffle boundary count (>10) in: "
            + ", ".join(
                f"{r['scenario']}@{r['scale']}({r['second_run_exchanges']})"
                for r in high_exch
            )
        )

    # 4. SCD2 specific: compare second run to first run
    scd2_runs = [
        r
        for r in results
        if r["scenario"]
        in ("scd2_change_detection", "scd2_full_cdc_delete", "scd2_effective_at")
    ]
    for r in scd2_runs:
        if r["first_run_total_ms"] > 0:
            ratio = r["second_run_total_ms"] / r["first_run_total_ms"]
            findings.append(
                f"SCD2 incremental vs initial ratio for {r['scenario']}@{r['scale']}: "
                f"{ratio:.2f}x (2nd={r['second_run_total_ms']:.0f}ms, 1st={r['first_run_total_ms']:.0f}ms)"
            )

    # 5. Window operation overhead
    window_runs = [r for r in results if r["second_run_windows"] > 0]
    if window_runs:
        findings.append(
            "Window operations (single-partition) in: "
            + ", ".join(
                f"{r['scenario']}@{r['scale']}({r['second_run_windows']})"
                for r in window_runs
            )
        )

    # 6. Join strategies used
    join_types_seen = set()
    for r in results:
        join_types_seen.update(r.get("second_run_join_types", []))
    if join_types_seen:
        findings.append(f"Join strategies observed: {sorted(join_types_seen)}")

    # 7. Scaling behavior (linear vs super-linear)
    by_scenario = defaultdict(dict)
    for r in results:
        by_scenario[r["scenario"]][r["scale"]] = r["second_run_total_ms"]
    for scenario, scale_data in by_scenario.items():
        if "tiny" in scale_data and "small" in scale_data and scale_data["tiny"] > 0:
            ratio = scale_data["small"] / scale_data["tiny"]
            scale_factor = 100
            findings.append(
                f"Scaling tiny->small ({scale_factor}x data) for {scenario}: "
                f"{ratio:.1f}x time  {'(sub-linear)' if ratio < scale_factor else '(super-linear)' if ratio > scale_factor else '(linear)'}"
            )
        if "small" in scale_data and "medium" in scale_data and scale_data["small"] > 0:
            ratio = scale_data["medium"] / scale_data["small"]
            scale_factor = 10
            findings.append(
                f"Scaling small->medium ({scale_factor}x data) for {scenario}: "
                f"{ratio:.1f}x time  {'(sub-linear)' if ratio < scale_factor else '(super-linear)' if ratio > scale_factor else '(linear)'}"
            )

    return findings


def print_top_stages_per_scenario(
    results: list[dict[str, Any]], top_n: int = 5
) -> None:
    """For each scenario, print the top N slowest stages in the second run."""
    print(f"\n{'=' * 80}")
    print("  TOP SLOWEST STAGES (second run, per scenario)")
    print(f"{'=' * 80}")
    for r in results:
        stages = r.get("second_run_stages", [])
        if not stages:
            continue
        sorted_stages = sorted(stages, key=lambda s: -s.get("duration_ms", 0))[:top_n]
        print(
            f"\n  {r['scenario']} @ {r['scale']} (total: {r['second_run_total_ms']:.0f}ms):"
        )
        for s in sorted_stages:
            print(
                f"    {s['duration_ms']:>8.0f}ms  "
                f"shuffle_read={s.get('shuffle_read_bytes', 0) / 1024 / 1024:>6.1f}MB  "
                f"shuffle_write={s.get('shuffle_write_bytes', 0) / 1024 / 1024:>6.1f}MB  "
                f"tasks={s.get('num_tasks', 0):>3}  "
                f"{s['name'][:50]}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze benchmark results")
    parser.add_argument(
        "files",
        nargs="*",
        help="Specific JSON files to analyze. If none given and --latest, use latest.",
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Use the latest benchmark file in tools/benchmarks/results/",
    )
    parser.add_argument(
        "--results-dir",
        default="tools/benchmarks/results",
        help="Directory to search for --latest",
    )
    args = parser.parse_args()

    if args.files:
        results = load_files(args.files)
    elif args.latest:
        data = load_latest(args.results_dir)
        results = data.get("results", [])
    else:
        results = load_all_individual(args.results_dir)

    if not results:
        print("No results to analyze")
        sys.exit(1)

    print_summary_table(results)
    print_top_stages_per_scenario(results)

    print(f"\n{'=' * 80}")
    print("  BOTTLENECK ANALYSIS")
    print(f"{'=' * 80}")
    findings = identify_bottlenecks(results)
    for i, f in enumerate(findings, 1):
        print(f"  {i}. {f}")

    print(f"\n{'=' * 80}")
    print("  TOP RECOMMENDATIONS (based on findings)")
    print(f"{'=' * 80}")
    for rec in generate_recommendations(findings, results):
        print(f"  * {rec}")


def generate_recommendations(findings: list[str], results: list[dict]) -> list[str]:
    """Generate actionable recommendations from findings."""
    recs = []
    for f in findings:
        if "Highest shuffle/duration ratio" in f:
            recs.append(
                "Reduce shuffle: broadcast small dimension tables, use bucketing for "
                "large joins, or pre-partition target by join key."
            )
        if "Disk spill detected" in f:
            recs.append(
                "Increase driver memory or reduce partition size. Consider "
                "spark.sql.shuffle.partitions tuning or Adaptive Query Execution."
            )
        if "High shuffle boundary count" in f:
            recs.append(
                "Each Exchange is a shuffle. Reduce by combining operations or "
                "enabling AQE (spark.sql.adaptive.enabled=true)."
            )
        if "Window operations" in f:
            recs.append(
                "row_number() OVER (ORDER BY 1) is a single-partition operation. "
                "Consider replacing with monotonically_increasing_id() or "
                "pre-fetching max SK via a scalar subquery."
            )
        if "super-linear" in f:
            recs.append(
                "Super-linear scaling suggests shuffle or memory bottleneck. "
                "Profile individual stages to find the root cause."
            )
    if not recs:
        recs.append("No critical bottlenecks detected at the scales tested.")
    return recs


if __name__ == "__main__":
    main()
