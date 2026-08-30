#!/usr/bin/env python3
"""
Standalone operator entry point for ``kimball inspect`` (ROADMAP 1.1).

This is a thin wrapper over :func:`kimball.ops.inspect.inspect_target`, so the
standalone and CLI (``kimball inspect``) surfaces share one harness and never
diverge on state reasoning. The harness (StateReconciler, WriterContract,
SourceHealth, RuntimeProfile) does the work; this script only resolves the
Spark session / target and renders the report.

Usage::

    # Deep diagnosis of one target (canonical 1.1 output)
    python tools/inspect_etl_control.py --target prod --table gold.fact_sales

    # Overview of every target in etl_control
    python tools/inspect_etl_control.py --target prod

    # Machine-readable JSON (valid: env messages go to stderr)
    python tools/inspect_etl_control.py --target prod --table gold.t --json

    # Filter displayed batches
    python tools/inspect_etl_control.py --target prod --table gold.t --running --older-than 30

Exit codes: 0 = healthy (consistent), 1 = attention required, 2 = could not run.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent

_ANSI = re.compile(r"\033\[[0-9;]*m")
_STATUS_COLORS: dict[str, str] = {
    "SUCCESS": "\033[32m",
    "FAILED": "\033[31m",
    "RUNNING": "\033[33m",
    "RECOVERED": "\033[36m",
}
_RESET = "\033[0m"


# ---------------------------------------------------------------------------
# Session / target resolution
# ---------------------------------------------------------------------------


def _load_env_file(env_file: str | None) -> None:
    if env_file is None:
        env_file = str(REPO_ROOT / ".env")
    if not os.path.exists(env_file):
        return
    try:
        from dotenv import load_dotenv

        load_dotenv(env_file, override=True)
        print(f"[inspect] loaded environment from {env_file}", file=sys.stderr)
    except ImportError:
        pass


def _get_spark() -> Any:
    from kimball.common.spark_session import get_spark

    try:
        return get_spark()
    except Exception as exc:  # noqa: BLE001
        print(f"error: could not create a Spark session: {exc}", file=sys.stderr)
        print(
            "  local: set JAVA_HOME and run with a local Spark+Delta env; "
            "remote: install databricks-connect and set DATABRICKS_HOST/TOKEN.",
            file=sys.stderr,
        )
        sys.exit(2)


def _resolve_etl_schema(args: argparse.Namespace) -> str:
    if args.target:
        from kimball.common.config import TargetLoader

        try:
            return str(TargetLoader(args.targets).load(args.target).etl_schema)
        except Exception as exc:  # noqa: BLE001
            print(
                f"error: could not load target '{args.target}': {exc}", file=sys.stderr
            )
            sys.exit(1)
    if args.etl_schema:
        return str(args.etl_schema)
    if env := os.environ.get("KIMBALL_ETL_SCHEMA"):
        return env
    print(
        "error: resolve the ETL schema via --target (kimball.targets.yml), "
        "--etl-schema, or KIMBALL_ETL_SCHEMA.",
        file=sys.stderr,
    )
    sys.exit(1)


def _distinct_targets(spark: Any, fq: str) -> list[str]:
    from kimball.common.utils import quote_table_name

    try:
        rows = spark.sql(
            f"SELECT DISTINCT target_table FROM {quote_table_name(fq)}"
        ).collect()
        return sorted(r[0] for r in rows if r[0])
    except Exception as exc:  # noqa: BLE001
        print(f"error: could not read {fq}: {exc}", file=sys.stderr)
        sys.exit(2)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _color(status: str, is_tty: bool) -> tuple[str, str]:
    return (_STATUS_COLORS.get(status, ""), _RESET) if is_tty else ("", "")


def _pad(cell: str, width: int) -> str:
    """Pad a possibly-ANSI cell to `width` based on visible length."""
    visible_len = len(_ANSI.sub("", cell))
    return cell + " " * max(0, width - visible_len)


def _fmt_dt(dt: Any) -> str:
    if dt is None:
        return "-"
    if hasattr(dt, "strftime"):
        return str(dt.strftime("%Y-%m-%d %H:%M"))
    return str(dt)[:16]


def _fmt_elapsed(b: dict[str, Any]) -> str:
    started = b.get("started_at")
    if started is None:
        return "-"
    start = started if started.tzinfo else started.replace(tzinfo=timezone.utc)
    completed = b.get("completed_at")
    end = completed or datetime.now(timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    secs = (end - start).total_seconds()
    if secs < 60:
        return f"{secs:.0f}s"
    return f"{secs / 60:.1f}m" if secs < 3600 else f"{secs / 3600:.1f}h"


def _filter_batches(
    batches: list[dict[str, Any]],
    *,
    running: bool,
    failed: bool,
    older_than: int | None,
) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    out: list[dict[str, Any]] = []
    for b in batches:
        status = b.get("status") or ""
        if running and status != "RUNNING":
            continue
        if failed and status != "FAILED":
            continue
        if running and older_than is not None:
            started = b.get("started_at")
            if started is None:
                continue
            s = started if started.tzinfo else started.replace(tzinfo=timezone.utc)
            if (now - s).total_seconds() < older_than * 60:
                continue
        out.append(b)
    return out


def _render_single(
    report: dict[str, Any], args: argparse.Namespace, is_tty: bool
) -> int:
    rec = report["reconciliation"]
    rt = report["runtime"]
    print(f"target: {report['target_table']}")
    print(
        f"runtime: {rt['flavor']} (commit tagging: {'on' if rt['supports_commit_tagging'] else 'off'})"
    )
    print(
        f"control_table: {'present' if report['control_table_exists'] else 'missing'}"
    )
    print(f"reconciliation: {rec['verdict']}")
    print(
        f"  watermark={rec['watermark_version']} "
        f"target_version={rec['target_version']} "
        f"zombie_batches={rec['zombie_batches']} "
        f"zombie_commits={rec['zombie_commits']}"
    )
    print(f"  evidence: {rec['evidence']}")
    print(f"  remediation: {rec['remediation']}")
    if rec["runbook_link"]:
        print(f"  see: {rec['runbook_link']}")
    wc = report["writer_contract"]
    print(
        f"writer_contract: {wc['verdict']} (suspicious_commits={wc['suspicious_commits']})"
    )
    print("source_health:")
    for s in report["source_health"]:
        print(f"  {s['source_table']}: {s['verdict']} - {s['detail']}")

    batches = _filter_batches(
        report["batches"],
        running=args.running,
        failed=args.failed,
        older_than=args.older_than if args.running else None,
    )
    if args.limit:
        batches = batches[: args.limit]
    print(f"\nbatches ({len(batches)} shown / {len(report['batches'])} total):")
    _print_batch_table(batches, is_tty)
    return 0 if rec["verdict"] == "consistent" else 1


def _print_batch_table(batches: list[dict[str, Any]], is_tty: bool) -> None:
    if not batches:
        print("  (none)")
        return
    headers = [
        "TARGET",
        "SOURCE",
        "STATUS",
        "VERSION",
        "BATCH",
        "STARTED",
        "ELAPSED",
        "ROWS",
        "ERROR",
    ]
    rows: list[list[str]] = []
    for b in batches:
        status = b.get("status") or ""
        color, reset = _color(status, is_tty)
        rows.append(
            [
                str(b.get("target_table", "")),
                str(b.get("source_table", "")),
                f"{color}{status}{reset}",
                str(
                    b.get("last_processed_version")
                    if b.get("last_processed_version") is not None
                    else "-"
                ),
                (b.get("batch_id") or "")[:16],
                _fmt_dt(b.get("started_at")),
                _fmt_elapsed(b),
                str(b.get("rows_written", 0)),
                (b.get("error_message") or "")[:30],
            ]
        )
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(_ANSI.sub("", cell)))
    print("  " + "  ".join(_pad(h, w) for h, w in zip(headers, widths, strict=True)))
    print("  " + "  ".join("-" * w for w in widths))
    for row in rows:
        print("  " + "  ".join(_pad(c, w) for c, w in zip(row, widths, strict=True)))


def _render_overview(reports: list[dict[str, Any]], args: argparse.Namespace) -> int:
    rows: list[dict[str, Any]] = []
    for rep in reports:
        rec = rep["reconciliation"]
        batches = rep["batches"]
        if args.running and all(b.get("status") != "RUNNING" for b in batches):
            continue
        if args.failed and all(b.get("status") != "FAILED" for b in batches):
            continue
        rows.append(
            {
                "target": rep["target_table"],
                "verdict": rec["verdict"],
                "watermark": rec["watermark_version"],
                "target_version": rec["target_version"],
                "zombies": rec["zombie_batches"],
                "writer": rep["writer_contract"]["verdict"],
            }
        )
    if args.limit:
        rows = rows[: args.limit]
    headers = ["TARGET", "VERDICT", "WATERMARK", "TARGET_V", "ZOMBIES", "WRITER"]
    widths = [len(h) for h in headers]
    cells = [
        [
            str(r["target"]),
            str(r["verdict"]),
            str(r["watermark"]),
            str(r["target_version"]),
            str(r["zombies"]),
            str(r["writer"]),
        ]
        for r in rows
    ]
    for c in cells:
        for i, v in enumerate(c):
            widths[i] = max(widths[i], len(v))
    if not cells:
        print("(no targets)")
    else:
        print(
            "  " + "  ".join(_pad(h, w) for h, w in zip(headers, widths, strict=True))
        )
        print("  " + "  ".join("-" * w for w in widths))
        for c in cells:
            print("  " + "  ".join(_pad(v, w) for v, w in zip(c, widths, strict=True)))
    print(f"\n({len(rows)} target(s))")
    return 0 if all(r["verdict"] == "consistent" for r in rows) else 1


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--table", default=None, help="Target table to inspect deeply.")
    parser.add_argument(
        "--running",
        action="store_true",
        help="Show only RUNNING batches (or targets with RUNNING batches).",
    )
    parser.add_argument(
        "--failed",
        action="store_true",
        help="Show only FAILED batches (or targets with FAILED batches).",
    )
    parser.add_argument(
        "--older-than",
        type=int,
        default=0,
        help="Min age in minutes for --running (default 0 = no age filter).",
    )
    parser.add_argument(
        "--target",
        default=None,
        choices=("dev", "test", "prod"),
        help="kimball.targets.yml target name.",
    )
    parser.add_argument(
        "--targets", default="kimball.targets.yml", help="Path to targets file."
    )
    parser.add_argument(
        "--etl-schema",
        default=None,
        help="Schema containing etl_control (overrides --target/env).",
    )
    parser.add_argument(
        "--limit", type=int, default=50, help="Cap rows/targets shown (default 50)."
    )
    parser.add_argument(
        "--history-limit",
        type=int,
        default=200,
        help="Delta history rows to scan per target.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON.")
    parser.add_argument("--env-file", default=None, help="Path to .env file.")
    args = parser.parse_args()

    _load_env_file(args.env_file)  # messages go to stderr only

    spark = _get_spark()
    etl_schema = _resolve_etl_schema(args)
    fq = f"{etl_schema}.etl_control"

    from kimball.ops.inspect import inspect_target
    from kimball.ops.runtime_profile import detect_runtime_profile
    from kimball.ops.spark_adapters import build_providers

    providers = build_providers(spark, etl_schema)
    runtime = detect_runtime_profile(spark)

    is_tty = sys.stdout.isatty()

    if args.table:
        report = inspect_target(
            args.table, providers, runtime, history_limit=args.history_limit
        )
        if args.running or args.failed:
            report["batches"] = _filter_batches(
                report["batches"],
                running=args.running,
                failed=args.failed,
                older_than=args.older_than if args.running else None,
            )
        if args.json:
            print(json.dumps(report, indent=2, sort_keys=True, default=str))
            return 0 if report["reconciliation"]["verdict"] == "consistent" else 1
        return _render_single(report, args, is_tty)

    # Overview mode: inspect every target in etl_control via the harness.
    if not spark.catalog.tableExists(fq):
        print(
            f"error: etl_control not found at {fq}. Has a pipeline run yet?",
            file=sys.stderr,
        )
        return 2
    targets = _distinct_targets(spark, fq)
    reports = [
        inspect_target(t, providers, runtime, history_limit=args.history_limit)
        for t in targets
    ]
    if args.running or args.failed:
        reports = [
            {
                **r,
                "batches": _filter_batches(
                    r["batches"],
                    running=args.running,
                    failed=args.failed,
                    older_than=args.older_than if args.running else None,
                ),
            }
            for r in reports
        ]
    if args.json:
        print(json.dumps(reports, indent=2, sort_keys=True, default=str))
        return (
            0
            if all(r["reconciliation"]["verdict"] == "consistent" for r in reports)
            else 1
        )
    return _render_overview(reports, args)


if __name__ == "__main__":
    sys.exit(main())
