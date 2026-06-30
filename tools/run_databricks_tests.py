#!/usr/bin/env python3
"""
Run the integration / golden test suite on a real Databricks workspace.

This tool is useful when local Java/Spark is unavailable or when you want to test
against the same Delta Lake runtime used in production.

Workflow:
  1. Load Databricks credentials from .env or environment variables.
  2. Build a wheel from the local repository.
  3. Sync the test files to a workspace location.
  4. Submit a one-time Databricks job that installs the wheel and runs pytest.
  5. Stream job output and print the final pytest report.

Serverless support:
  - Databricks Free Edition provides serverless compute, not classic clusters.
  - This tool submits a Python wheel task to a serverless job cluster by default.
  - Crash-recovery tests that rely on commit tagging may be skipped on serverless
    because setting spark.databricks.delta.commitInfo.userMetadata is restricted.

Usage:
  export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
  export DATABRICKS_TOKEN=dapi...
  python tools/run_databricks_tests.py tests/integration

  # With a .env file in the repo root:
  python tools/run_databricks_tests.py tests/integration --env-file .env

  # Use a classic cluster instead of serverless:
  python tools/run_databricks_tests.py tests/integration --cluster-id 0123-456789-abcdef0
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_env_file(env_file: str | None) -> None:
    """Load environment variables from a .env file if python-dotenv is available."""
    if env_file is None:
        env_file = str(REPO_ROOT / ".env")
    if not os.path.exists(env_file):
        return
    try:
        from dotenv import load_dotenv

        load_dotenv(env_file, override=True)
        print(f"Loaded environment from {env_file}")
    except ImportError:
        print("warning: python-dotenv not installed; .env file ignored")


def _ensure_credentials() -> tuple[str, str]:
    """Verify that DATABRICKS_HOST and DATABRICKS_TOKEN are available."""
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        print(
            "error: DATABRICKS_HOST and DATABRICKS_TOKEN must be set.\n"
            "Set them in the environment or in a .env file at the repo root."
        )
        sys.exit(1)
    return host.rstrip("/"), token


def _build_wheel() -> Path:
    """Build the package wheel and return its path."""
    print("Building wheel...")
    dist_dir = REPO_ROOT / "dist"
    result = subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--outdir", str(dist_dir)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print("error: wheel build failed")
        print(result.stdout)
        print(result.stderr)
        sys.exit(1)

    wheels = sorted(
        dist_dir.glob("*.whl"), key=lambda p: p.stat().st_mtime, reverse=True
    )
    if not wheels:
        print("error: no wheel produced")
        sys.exit(1)
    wheel = wheels[0]
    print(f"Built wheel: {wheel}")
    return wheel


def _upload_wheel(wheel: Path, ws: Any) -> str:
    """Upload the wheel to a workspace files location and return the workspace path."""
    remote_dir = "/Workspace/Users/ci/kimball_framework"
    remote_path = f"{remote_dir}/{wheel.name}"

    print(f"Uploading wheel to {remote_path}...")
    import base64

    content = wheel.read_bytes()
    ws.workspace.mkdirs(remote_dir)
    ws.workspace.upload(
        remote_path,
        base64.b64encode(content).decode("utf-8"),
        overwrite=True,
        format="BASE64",
    )
    return remote_path


def _sync_tests(remote_tests_dir: str, ws: Any) -> None:
    """Upload integration/golden tests to workspace files."""
    import base64

    print(f"Syncing tests to {remote_tests_dir}...")
    local_tests = REPO_ROOT / "tests"
    for path in local_tests.rglob("*"):
        if path.is_file():
            relative = path.relative_to(local_tests)
            remote_path = f"{remote_tests_dir}/{relative.as_posix()}"
            ws.workspace.mkdirs(str(Path(remote_path).parent))
            ws.workspace.upload(
                remote_path,
                base64.b64encode(path.read_bytes()).decode("utf-8"),
                overwrite=True,
                format="BASE64",
            )
    print("Tests synced")


def _create_runner_script(ws: Any, remote_tests_dir: str) -> str:
    """Upload a small Python driver that runs pytest on the cluster."""
    import base64

    script = f"""import os
import sys

test_path = sys.argv[1] if len(sys.argv) > 1 else "{remote_tests_dir}"
catalog = sys.argv[2] if len(sys.argv) > 2 else "spark_catalog"
os.environ.setdefault("KIMBALL_TEST_CATALOG", catalog)

# On a Databricks cluster the local spark session is the real DBR session,
# so clear any stale remote-connect env vars.
os.environ.setdefault("DATABRICKS_HOST", "")
os.environ.setdefault("DATABRICKS_TOKEN", "")

import pytest
exit_code = pytest.main([test_path, "-v"])
sys.exit(exit_code)
"""
    remote_path = "/Workspace/Users/ci/kimball_framework/run_tests.py"
    ws.workspace.upload(
        remote_path,
        base64.b64encode(script.encode("utf-8")).decode("utf-8"),
        overwrite=True,
        format="BASE64",
    )
    print(f"Uploaded runner script to {remote_path}")
    return remote_path


def _run_job(
    ws: Any,
    cluster_id: str | None,
    wheel_path: str,
    runner_path: str,
    test_path: str,
    catalog: str,
) -> int:
    """Submit a job that installs the wheel and runs pytest. Return exit code."""
    from databricks.sdk.service import jobs

    print("Submitting integration test job...")

    if cluster_id:
        # Classic all-purpose cluster path
        run = ws.jobs.submit(
            run_name="kimball-framework-integration-tests",
            tasks=[
                jobs.SubmitTask(
                    task_key="run_tests",
                    existing_cluster_id=cluster_id,
                    spark_python_task=jobs.SparkPythonTask(
                        python_file=runner_path,
                        parameters=[test_path, catalog],
                    ),
                    libraries=[
                        jobs.Library(whl=wheel_path),
                        jobs.Library(pypi=jobs.PythonPyPiLibraryPackage("pytest")),
                    ],
                )
            ],
        )
    else:
        # Serverless / job compute path (preferred for free edition)
        run = ws.jobs.submit(
            run_name="kimball-framework-integration-tests",
            tasks=[
                jobs.SubmitTask(
                    task_key="run_tests",
                    spark_python_task=jobs.SparkPythonTask(
                        python_file=runner_path,
                        parameters=[test_path, catalog],
                    ),
                    libraries=[
                        jobs.Library(whl=wheel_path),
                        jobs.Library(pypi=jobs.PythonPyPiLibraryPackage("pytest")),
                    ],
                )
            ],
        )

    run_id = run.run_id
    print(f"Job run ID: {run_id}")

    # Poll for completion
    while True:
        run = ws.jobs.get_run(run_id)
        life_cycle_state = run.state.life_cycle_state.value
        print(f"  job state: {life_cycle_state}")
        if life_cycle_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        time.sleep(15)

    # Print output
    _print_run_output(ws, run_id)

    if run.state.result_state and run.state.result_state.value == "SUCCESS":
        return 0
    return 1


def _print_run_output(ws: Any, run_id: int) -> None:
    """Fetch and print job run logs."""
    try:
        output = ws.jobs.get_run_output(run_id)
        if output and output.logs:
            print("\n--- Job stdout ---")
            print(output.logs)
    except Exception as e:
        print(f"warning: could not retrieve job output: {e}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Kimball Framework integration/golden tests on Databricks."
    )
    parser.add_argument(
        "test_path",
        nargs="?",
        default="tests/integration",
        help="Local path to the test directory to run remotely (default: tests/integration)",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env file with DATABRICKS_HOST and DATABRICKS_TOKEN",
    )
    parser.add_argument(
        "--cluster-id",
        default=None,
        help="Optional existing classic all-purpose cluster ID. If omitted, uses serverless compute.",
    )
    parser.add_argument(
        "--build-only",
        action="store_true",
        help="Build the wheel and exit (useful for CI caching)",
    )
    args = parser.parse_args()

    _load_env_file(args.env_file)
    host, token = _ensure_credentials()

    if args.build_only:
        _build_wheel()
        return 0

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("error: databricks-sdk is not installed. Run: pip install '.[remote]'")
        return 1

    ws = WorkspaceClient(host=host, token=token)

    wheel = _build_wheel()
    wheel_path = _upload_wheel(wheel, ws)

    remote_tests_dir = "/Workspace/Users/ci/kimball_framework/tests"
    _sync_tests(remote_tests_dir, ws)
    runner_path = _create_runner_script(ws, remote_tests_dir)

    catalog = os.environ.get("KIMBALL_TEST_CATALOG", "spark_catalog")
    # The remote runner receives the workspace path to the tests, not the local path
    remote_test_path = f"{remote_tests_dir}/{Path(args.test_path).name}"
    return _run_job(
        ws,
        args.cluster_id,
        wheel_path,
        runner_path,
        remote_test_path,
        catalog,
    )


if __name__ == "__main__":
    sys.exit(main())
