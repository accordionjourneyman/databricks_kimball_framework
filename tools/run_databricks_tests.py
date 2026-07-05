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

  # Dry-run: validate the upload/sync logic without a Databricks workspace:
  python tools/run_databricks_tests.py tests/integration --dry-run
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


def _get_remote_base_dir(ws: Any) -> str:
    """Return a workspace directory owned by the current user.

    The /Workspace/Users root is protected; we must write under the
    authenticated user's own folder. Override with KIMBALL_WORKSPACE_DIR.
    """
    override = os.environ.get("KIMBALL_WORKSPACE_DIR")
    if override:
        return override.rstrip("/")

    try:
        user = ws.current_user.me()
        user_name = getattr(user, "user_name", None) or getattr(
            user, "display_name", None
        )
    except Exception:
        user_name = None

    if not user_name:
        print(
            "error: could not determine current Databricks user. "
            "Set KIMBALL_WORKSPACE_DIR to a writable workspace path, e.g. "
            "/Workspace/Users/your.email@example.com/kimball_framework_ci"
        )
        sys.exit(1)

    return f"/Workspace/Users/{user_name}/kimball_framework_ci"


def _upload_wheel(wheel: Path, ws: Any) -> str:
    """Upload the wheel to a workspace files location and return the workspace path."""
    from databricks.sdk.service.workspace import ImportFormat

    remote_dir = _get_remote_base_dir(ws)
    remote_path = f"{remote_dir}/{wheel.name}"

    print(f"Uploading wheel to {remote_path}...")
    ws.workspace.mkdirs(remote_dir)
    ws.workspace.upload(
        remote_path,
        wheel.read_bytes(),
        format=ImportFormat.AUTO,
        overwrite=True,
    )
    return remote_path


def _sync_tests(remote_tests_dir: str, ws: Any) -> None:
    """Upload integration/golden tests to workspace files."""
    from databricks.sdk.service.workspace import ImportFormat

    print(f"Syncing tests to {remote_tests_dir}...")
    local_tests = REPO_ROOT / "tests"
    for path in local_tests.rglob("*"):
        if not path.is_file():
            continue
        if "__pycache__" in path.parts or path.name.endswith(".pyc"):
            continue
        relative = path.relative_to(local_tests).as_posix()
        remote_path = f"{remote_tests_dir}/{relative}"
        parent_dir = "/".join(remote_path.split("/")[:-1])
        ws.workspace.mkdirs(parent_dir)
        ws.workspace.upload(
            remote_path,
            path.read_bytes(),
            format=ImportFormat.AUTO,
            overwrite=True,
        )
    print("Tests synced")


def _create_runner_script(ws: Any, remote_tests_dir: str) -> str:
    """Upload a small Python driver that runs pytest on the cluster."""
    from databricks.sdk.service.workspace import ImportFormat

    script = f"""import os
import sys

test_path = sys.argv[1] if len(sys.argv) > 1 else "{remote_tests_dir}"
catalog = sys.argv[2] if len(sys.argv) > 2 else "spark_catalog"
extra_args = sys.argv[3:] if len(sys.argv) > 3 else []
os.environ.setdefault("KIMBALL_TEST_CATALOG", catalog)

# On a Databricks cluster the local spark session is the real DBR session,
# so clear any stale remote-connect env vars.
os.environ.setdefault("DATABRICKS_HOST", "")
os.environ.setdefault("DATABRICKS_TOKEN", "")

# Workspace files are read-only; pytest must not write __pycache__.
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
sys.dont_write_bytecode = True

import pytest
exit_code = pytest.main([test_path, "-v", "-p", "no:cacheprovider"] + extra_args)
sys.exit(exit_code)
"""
    remote_path = f"{_get_remote_base_dir(ws)}/run_tests.py"
    ws.workspace.mkdirs(_get_remote_base_dir(ws))
    ws.workspace.upload(
        remote_path,
        script.encode("utf-8"),
        format=ImportFormat.AUTO,
        overwrite=True,
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
    extra_pytest_args: list[str] | None = None,
) -> int:
    """Run pytest on the target path. Return exit code."""
    from databricks.sdk.service import jobs
    from databricks.sdk.service.compute import Environment, Library, PythonPyPiLibrary

    extra_args = extra_pytest_args or []
    if cluster_id:
        return _run_job_classic(
            ws,
            cluster_id,
            wheel_path,
            runner_path,
            test_path,
            catalog,
            extra_args,
            jobs=jobs,
            Library=Library,
            PythonPyPiLibrary=PythonPyPiLibrary,
        )
    return _run_job_serverless(
        ws,
        wheel_path,
        runner_path,
        test_path,
        catalog,
        extra_args,
        jobs=jobs,
        Environment=Environment,
    )


def _run_job_classic(
    ws: Any,
    cluster_id: str,
    wheel_path: str,
    runner_path: str,
    test_path: str,
    catalog: str,
    extra_args: list[str],
    *,
    jobs: Any,
    Library: Any,
    PythonPyPiLibrary: Any,
) -> int:
    """Submit a one-time job on an existing classic cluster."""
    print("Submitting integration test job (classic cluster)...")

    task = jobs.SubmitTask(
        task_key="run_tests",
        existing_cluster_id=cluster_id,
        spark_python_task=jobs.SparkPythonTask(
            python_file=runner_path,
            parameters=[test_path, catalog] + extra_args,
        ),
        libraries=[
            Library(whl=wheel_path),
            Library(pypi=PythonPyPiLibrary(package="pytest")),
        ],
    )
    run = ws.jobs.submit(
        run_name="kimball-framework-integration-tests",
        tasks=[task],
    )
    return _poll_run(ws, run.run_id)


def _run_job_serverless(
    ws: Any,
    wheel_path: str,
    runner_path: str,
    test_path: str,
    catalog: str,
    extra_args: list[str],
    *,
    jobs: Any,
    Environment: Any,
) -> int:
    """Create or update a persistent serverless job and run it.

    Free Edition may reject one-time `runs/submit` in performance-optimized
    mode, but scheduled/persistent jobs support standard serverless mode.
    """
    from databricks.sdk.service.jobs import PerformanceTarget

    job_name = "kimball-framework-integration-tests"
    env_key = "kimball_test_env"

    task = jobs.Task(
        task_key="run_tests",
        spark_python_task=jobs.SparkPythonTask(
            python_file=runner_path,
            parameters=[test_path, catalog] + extra_args,
        ),
        environment_key=env_key,
    )

    # Find an existing job with the same name or create one.
    job_id = _find_job_by_name(ws, job_name)
    if job_id is None:
        print(f"Creating persistent serverless job '{job_name}'...")
        response = ws.jobs.create(
            name=job_name,
            tasks=[task],
            environments=[
                jobs.JobEnvironment(
                    environment_key=env_key,
                    spec=Environment(
                        environment_version="5",
                        dependencies=[
                            f"{wheel_path}",
                            "pytest",
                        ],
                    ),
                )
            ],
            performance_target=PerformanceTarget.STANDARD,
        )
        job_id = response.job_id
    else:
        print(f"Updating existing serverless job {job_id}...")
        ws.jobs.update(
            job_id,
            new_settings=jobs.JobSettings(
                name=job_name,
                tasks=[task],
                environments=[
                    jobs.JobEnvironment(
                        environment_key=env_key,
                        spec=Environment(
                            environment_version="5",
                            dependencies=[
                                f"{wheel_path}",
                                "pytest",
                            ],
                        ),
                    )
                ],
                performance_target=PerformanceTarget.STANDARD,
            ),
        )

    print(f"Triggering run for serverless job {job_id}...")
    run = ws.jobs.run_now(job_id, performance_target=PerformanceTarget.STANDARD)
    return _poll_run(ws, run.run_id)


def _poll_run(ws: Any, run_id: int) -> int:
    """Poll a run to completion and return its exit code."""
    print(f"Job run ID: {run_id}")

    run = None
    # Poll for completion
    while True:
        run = ws.jobs.get_run(run_id)
        life_cycle_state = run.state.life_cycle_state.value
        print(f"  job state: {life_cycle_state}")
        if life_cycle_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        time.sleep(15)

    # Print output and any failure diagnostics
    _print_run_output(ws, run_id)

    if run and run.state.result_state and run.state.result_state.value == "SUCCESS":
        return 0
    return 1


def _find_job_by_name(ws: Any, name: str) -> int | None:
    """Return the first job ID matching the given name, or None."""
    for job in ws.jobs.list():
        if job.settings and job.settings.name == name:
            return job.job_id
    return None


def _print_run_output(ws: Any, run_id: int) -> None:
    """Fetch and print job run logs and task-level diagnostics."""
    run = ws.jobs.get_run(run_id)
    print("\n--- Job run diagnostics ---")
    if getattr(run.state, "state_message", None):
        print(f"Run state message: {run.state.state_message}")
    for task in getattr(run, "tasks", []) or []:
        task_state = task.state
        print(
            f"Task '{task.task_key}' (attempt {getattr(task, 'attempt_number', 0)}): "
            f"{task_state.life_cycle_state.value} / "
            f"{task_state.result_state.value if task_state.result_state else 'N/A'}"
        )
        if getattr(task_state, "state_message", None):
            print(f"  message: {task_state.state_message}")
        try:
            output = ws.jobs.get_run_output(task.run_id)
            if output and output.logs:
                print("\n--- Task stdout ---")
                print(output.logs)
        except Exception as e:
            print(f"  warning: could not retrieve task output: {e}")


class _DryRunClient:
    """Fake WorkspaceClient that prints upload/sync actions instead of calling Databricks.

    Used for local smoke-testing the runner without credentials.
    """

    def __init__(self) -> None:
        self.workspace = _DryRunWorkspace()

    @property
    def current_user(self) -> _DryRunCurrentUser:
        return _DryRunCurrentUser()

    @property
    def jobs(self) -> _DryRunJobs:
        return _DryRunJobs()


class _DryRunWorkspace:
    """Fake workspace namespace for dry-run client."""

    def mkdirs(self, path: str) -> None:
        print(f"  [dry-run] mkdirs: {path}")

    def upload(self, path: str, content: bytes, *, overwrite: bool = False) -> None:
        size = len(content) if content else 0
        print(f"  [dry-run] upload: {path} ({size} bytes, overwrite={overwrite})")


class _DryRunCurrentUser:
    """Fake current_user namespace for dry-run client."""

    def me(self) -> Any:
        class _User:
            user_name = "dry-run-user"
            display_name = "dry-run-user"

        return _User()


class _DryRunJobs:
    """Fake jobs namespace for dry-run client."""

    def submit(self, **kwargs: Any) -> Any:
        class _Run:
            run_id = 12345

        return _Run()

    def get_run(self, run_id: int) -> Any:
        class _Run:
            class state:
                class life_cycle_state:
                    value = "TERMINATED"

                class result_state:
                    value = "SUCCESS"

        return _Run()

    def get_run_output(self, run_id: int) -> Any:
        class _Output:
            logs = "[dry-run] pytest output would appear here."

        return _Output()


def _run_dry_run(test_path: str) -> int:
    """Run the upload/sync/job-submission flow with a fake client."""
    print("Running in dry-run mode. No Databricks workspace will be contacted.\n")
    wheel = _build_wheel()
    ws = _DryRunClient()

    wheel_path = _upload_wheel(wheel, ws)
    remote_base_dir = _get_remote_base_dir(ws)
    remote_tests_dir = f"{remote_base_dir}/tests"
    _sync_tests(remote_tests_dir, ws)
    runner_path = _create_runner_script(ws, remote_tests_dir)

    catalog = os.environ.get("KIMBALL_TEST_CATALOG", "spark_catalog")
    remote_test_path = f"{remote_tests_dir}/{Path(test_path).name}"
    return _run_job(ws, None, wheel_path, runner_path, remote_test_path, catalog)


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
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate upload/sync/job logic without contacting Databricks",
    )
    parser.add_argument(
        "pytest_args",
        nargs=argparse.REMAINDER,
        help="Additional pytest arguments forwarded to the cluster (e.g. --scale small)",
    )
    args = parser.parse_args()

    if args.dry_run:
        return _run_dry_run(args.test_path)

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

    remote_base_dir = _get_remote_base_dir(ws)
    remote_tests_dir = f"{remote_base_dir}/tests"
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
        extra_pytest_args=args.pytest_args,
    )


if __name__ == "__main__":
    sys.exit(main())
