#!/usr/bin/env python3
"""
Kimball Framework Test Runner with target selection.

Supports two execution targets:
  -t local       Run tests against a local Spark+Delta environment (Docker or native)
  -t databricks  Run tests against a remote Databricks cluster via Databricks Connect

Usage:
  python tools/run_tests.py -t local
  python tools/run_tests.py -t local -k test_scd2
  python tools/run_tests.py -t local --integration
  python tools/run_tests.py -t databricks
  python tools/run_tests.py -t databricks --integration

The local target starts a local SparkSession with Delta Lake extensions,
suitable for fast iteration and debugging without waiting for serverless
clusters to spin up.

The databricks target connects to a Databricks workspace using
Databricks Connect v2. Requires DATABRICKS_HOST and DATABRICKS_TOKEN
environment variables (or a .env file).
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kimball Framework test runner with target selection"
    )
    parser.add_argument(
        "-t",
        "--target",
        choices=["local", "databricks"],
        default="local",
        help="Execution target: 'local' (local Spark+Delta) or 'databricks' (remote Databricks Connect)",
    )
    parser.add_argument(
        "-k",
        "--keyword",
        default=None,
        help="pytest -k keyword expression to filter tests",
    )
    parser.add_argument(
        "--integration",
        action="store_true",
        help="Run integration tests (tests/integration/) in addition to unit tests",
    )
    parser.add_argument(
        "--unit-only",
        action="store_true",
        help="Run only unit tests (default)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose pytest output",
    )
    parser.add_argument(
        "--tb",
        "--traceback",
        default="short",
        help="Traceback style (short, long, line, native)",
    )
    parser.add_argument(
        "-x",
        "--exitfirst",
        action="store_true",
        help="Exit on first failure",
    )
    parser.add_argument(
        "--lf",
        "--last-failed",
        action="store_true",
        dest="last_failed",
        help="Re-run only the tests that failed last time",
    )
    parser.add_argument(
        "--databricks-cluster-id",
        default=None,
        help="Databricks cluster ID (for databricks target, overrides DATABRICKS_CLUSTER_ID)",
    )
    parser.add_argument(
        "--databricks-schema",
        default=None,
        help="Schema/catalog for test tables on Databricks (default: kimball_test)",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="After tests, run cleanup_databricks.py to remove stale test schemas",
    )
    parser.add_argument(
        "--cleanup-older-than",
        type=int,
        default=1,
        help="Only clean up schemas older than N days (default: 1, used with --cleanup)",
    )
    return parser.parse_args()


def build_pytest_args(args: argparse.Namespace) -> list[str]:
    """Build the pytest command line arguments."""
    cmd = ["python", "-m", "pytest"]

    # Determine which test paths to run
    test_paths = ["tests/unit/"]
    if args.integration:
        test_paths.append("tests/integration/")
    if args.unit_only and args.integration:
        # --unit-only takes precedence
        test_paths = ["tests/unit/"]

    cmd.extend(test_paths)

    if args.keyword:
        cmd.extend(["-k", args.keyword])

    if args.verbose:
        cmd.append("-v")

    cmd.extend(["--tb", args.tb])

    if args.exitfirst:
        cmd.append("-x")

    if args.last_failed:
        cmd.append("--lf")

    return cmd


def setup_local_environment() -> dict[str, str]:
    """Set up environment variables for local Spark+Delta testing."""
    env = os.environ.copy()
    # Signal to conftest.py that we want local Spark
    env["KIMBALL_TARGET"] = "local"
    # Use a simple schema name for local tests
    env.setdefault("KIMBALL_ETL_SCHEMA", "etl_control")
    # Ensure we don't accidentally connect to Databricks
    env.pop("DATABRICKS_HOST", None)
    env.pop("DATABRICKS_TOKEN", None)
    return env


def setup_databricks_environment(args: argparse.Namespace) -> dict[str, str]:
    """Set up environment variables for Databricks Connect testing."""
    env = os.environ.copy()
    env["KIMBALL_TARGET"] = "databricks"

    # Load .env file if present
    dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if os.path.exists(dotenv_path):
        try:
            from dotenv import load_dotenv

            load_dotenv(dotenv_path, override=False)
            env = os.environ.copy()
            env["KIMBALL_TARGET"] = "databricks"
        except ImportError:
            print(
                "WARNING: python-dotenv not installed. "
                "Set DATABRICKS_HOST and DATABRICKS_TOKEN manually."
            )

    # Validate required credentials
    if not env.get("DATABRICKS_HOST"):
        print("ERROR: DATABRICKS_HOST not set. Create a .env file or export it.")
        sys.exit(1)
    if not env.get("DATABRICKS_TOKEN"):
        print("ERROR: DATABRICKS_TOKEN not set. Create a .env file or export it.")
        sys.exit(1)

    # Set cluster ID if provided
    if args.databricks_cluster_id:
        env["DATABRICKS_CLUSTER_ID"] = args.databricks_cluster_id

    # Set test schema for Databricks
    env.setdefault("KIMBALL_ETL_SCHEMA", args.databricks_schema or "kimball_test")
    env.setdefault("KIMBALL_TEST_CATALOG", "hive_metastore")

    return env


def main() -> None:
    args = parse_args()

    if args.target == "local":
        print("=" * 60)
        print("Kimball Test Runner - Target: LOCAL (Spark+Delta)")
        print("=" * 60)
        env = setup_local_environment()
    else:
        print("=" * 60)
        print("Kimball Test Runner - Target: DATABRICKS (Databricks Connect)")
        print("=" * 60)
        env = setup_databricks_environment(args)

    cmd = build_pytest_args(args)
    print(f"Running: {' '.join(cmd)}")
    print(f"KIMBALL_TARGET={env.get('KIMBALL_TARGET')}")
    print(f"KIMBALL_ETL_SCHEMA={env.get('KIMBALL_ETL_SCHEMA')}")
    if args.target == "databricks":
        print(f"DATABRICKS_HOST={env.get('DATABRICKS_HOST')}")
        if env.get("DATABRICKS_CLUSTER_ID"):
            print(f"DATABRICKS_CLUSTER_ID={env['DATABRICKS_CLUSTER_ID']}")
    print("-" * 60)

    result = subprocess.run(cmd, env=env)

    # Optionally clean up stale test schemas on Databricks
    if args.cleanup and args.target == "databricks":
        print("\n" + "=" * 60)
        print("Running Databricks schema cleanup...")
        print("=" * 60)
        cleanup_cmd = [
            sys.executable,
            str(Path(__file__).parent / "cleanup_databricks.py"),
            "--older-than",
            str(args.cleanup_older_than),
            "--all",
            "--yes",
        ]
        if args.databricks_schema:
            cleanup_cmd.extend(["--catalog", args.databricks_schema])
        cleanup_result = subprocess.run(cleanup_cmd, env=env)
        if cleanup_result.returncode != 0:
            print("warning: cleanup reported errors (see above)")
    elif args.cleanup and args.target == "local":
        print("\nnote: --cleanup is only meaningful with --target databricks (local temp dirs are auto-cleaned)")

    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
