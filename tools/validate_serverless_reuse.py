#!/usr/bin/env python3
"""Validate serverless compute reuse across parallel tasks in the same job.

Creates a 3-task serverless job:
  - warmup: SELECT 1 (pays the ~5 min startup)
  - select_2 + select_3: parallel, depend on warmup

If serverless compute is reused, select_2 and select_3 complete in seconds
(< 20s each) rather than minutes.

Usage:
    export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
    export DATABRICKS_TOKEN=dapi...
    python tools/validate_serverless_reuse.py

    # With .env file:
    python tools/validate_serverless_reuse.py --env-file .env

    # Clean up resources after validation:
    python tools/validate_serverless_reuse.py --cleanup
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone

_TEST_SCRIPT = r"""from pyspark.sql import SparkSession
import sys, time, json

spark = SparkSession.builder.getOrCreate()
task = sys.argv[1] if len(sys.argv) > 1 else "unknown"

start = time.time()
if task == "warmup":
    spark.sql("SELECT 1 as value").show()
elif task == "select_2":
    spark.sql("SELECT 2 as value").show()
elif task == "select_3":
    spark.sql("SELECT 3 as value").show()

elapsed = time.time() - start
print(json.dumps({"task": task, "elapsed_sec": round(elapsed, 3)}))
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=None, help="Path to .env file")
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove the temp job and workspace file",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Leave resources in place for debugging",
    )
    args = parser.parse_args(argv)

    if args.env_file:
        try:
            from dotenv import load_dotenv
        except ImportError:
            print("python-dotenv not installed; reading .env manually")
            for line in open(args.env_file):
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
        else:
            load_dotenv(args.env_file)

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs
    from databricks.sdk.service.compute import Environment
    from databricks.sdk.service.workspace import ImportFormat

    w = WorkspaceClient()
    me = w.current_user.me()
    username = me.user_name
    base_dir = f"/Users/{username}/validate_serverless_reuse"
    script_path = f"{base_dir}/test_task.py"

    # --cleanup: remove resources
    if args.cleanup:
        _cleanup(w, script_path, base_dir)
        return 0

    # 1. Upload the test script to workspace
    # Remove any previous test artifacts (may exist from failed runs)
    try:
        w.workspace.delete(path=base_dir, recursive=True)
        print(f"Removed previous artifacts at {base_dir}")
    except Exception:
        pass

    try:
        w.workspace.mkdirs(base_dir)
    except Exception:
        pass
    print(f"Uploading test script to {script_path}...")
    w.workspace.upload(
        path=script_path,
        content=_TEST_SCRIPT.encode("utf-8"),
        overwrite=True,
        format=ImportFormat.RAW,
    )

    # 2. Create the serverless job
    job_name = (
        f"kimball-serverless-reuse-test-"
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )
    print(f"Creating serverless job '{job_name}'...")

    env_key = "kimball_test_env"

    tasks = [
        jobs.Task(
            task_key="warmup",
            environment_key=env_key,
            spark_python_task=jobs.SparkPythonTask(
                python_file=script_path,
                parameters=["warmup"],
            ),
            timeout_seconds=600,
        ),
        jobs.Task(
            task_key="select_2",
            depends_on=[jobs.TaskDependency(task_key="warmup")],
            environment_key=env_key,
            spark_python_task=jobs.SparkPythonTask(
                python_file=script_path,
                parameters=["select_2"],
            ),
            timeout_seconds=120,
        ),
        jobs.Task(
            task_key="select_3",
            depends_on=[jobs.TaskDependency(task_key="warmup")],
            environment_key=env_key,
            spark_python_task=jobs.SparkPythonTask(
                python_file=script_path,
                parameters=["select_3"],
            ),
            timeout_seconds=120,
        ),
    ]

    created = w.jobs.create(
        name=job_name,
        tasks=tasks,
        max_concurrent_runs=1,
        environments=[
            jobs.JobEnvironment(
                environment_key=env_key,
                spec=Environment(
                    environment_version="5",
                    dependencies=[],
                ),
            )
        ],
    )
    job_id = created.job_id
    print(f"Created job {job_id}: {w.config.host}#job/{job_id}")

    # 3. Run the job
    print("Triggering run with standard performance mode...")
    run = w.jobs.run_now(
        job_id,
        performance_target=jobs.PerformanceTarget.STANDARD,
    )
    run_id = run.run_id
    run_url = f"{w.config.host}#job/{job_id}/run/{run_id}"
    print(f"Run ID: {run_id}")
    print(f"Monitor: {run_url}")

    # 4. Poll until completed
    print("\nPolling... (warmup takes ~5 min, others should be fast)")
    run_data = None
    while True:
        run_data = w.jobs.get_run(run_id)
        state = run_data.state.life_cycle_state.value
        print(f"  {datetime.now(timezone.utc).strftime('%H:%M:%S')}  state={state}")
        if state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        time.sleep(15)

    # 5. Print results
    result_state = (
        run_data.state.result_state.value if run_data.state.result_state else "N/A"
    )
    print(f"\n=== Result: {result_state} ===")
    print(f"  Run URL: {run_url}")

    timing: dict[str, float] = {}
    for task in run_data.tasks or []:
        task_state = task.state
        tsk = task.task_key
        ts = task_state.life_cycle_state.value
        rs = task_state.result_state.value if task_state.result_state else "N/A"
        print(f"  Task '{tsk}': {ts} / {rs}")

        # Calculate duration from start/end time
        if task.start_time and task.end_time:
            dur_s = (task.end_time - task.start_time) / 1000.0
            timing[tsk] = dur_s
            print(f"    duration: {dur_s:.1f}s")

        # Print stdout output
        try:
            output = w.jobs.get_run_output(task.run_id)
            if output and output.logs:
                for line in output.logs.strip().splitlines():
                    try:
                        parsed = json.loads(line)
                        if "elapsed_sec" in parsed:
                            timing[f"{tsk}_sql"] = parsed["elapsed_sec"]
                    except json.JSONDecodeError:
                        pass
                    print(f"    stdout: {line}")
            if output and output.error:
                print(f"    stderr: {output.error[:500]}")
        except Exception as e:
            print(f"    (could not fetch output: {e})")

    # 6. Validate hypothesis
    print("\n=== Hypothesis check ===")
    if "warmup" in timing and "select_2" in timing and "select_3" in timing:
        w_dur = timing["warmup"]
        s2_dur = timing["select_2"]
        s3_dur = timing["select_3"]
        print(f"  warmup:     {w_dur:.1f}s (if ~300s, that's the cold start)")
        print(f"  select_2:   {s2_dur:.1f}s (should be < 20s if session reused)")
        print(f"  select_3:   {s3_dur:.1f}s (should be < 20s if session reused)")

        if s2_dur < 20 and s3_dur < 20 and w_dur > 60:
            print(
                "\n  YES HYPOTHESIS CONFIRMED: serverless compute is reused across tasks."
            )
            print("     select_2 and select_3 ran fast because they inherited the")
            print("     warm compute session from warmup.")
        elif s2_dur > 60 or s3_dur > 60:
            print("\n  NO HYPOTHESIS REFUTED: tasks each pay their own startup cost.")
            print(
                "     select_2 and/or select_3 took >60s, suggesting no session reuse."
            )
        else:
            print(
                "\n  WARNING  INCONCLUSIVE: warmup was fast too, or timing data unclear."
            )
    else:
        print("  WARNING  Could not collect timing data for all tasks.")

    # 7. Clean up unless --no-cleanup
    if not args.no_cleanup:
        print("\nCleaning up...")
        _cleanup(w, script_path, base_dir, job_id=job_id)
    else:
        print("\nResources left in place. To clean up later:")
        print("  python tools/validate_serverless_reuse.py --cleanup")
        print(f"  OR manually delete job {job_id} via the UI.")

    return 0 if run_data and result_state == "SUCCESS" else 1


def _cleanup(
    w,
    script_path: str,
    base_dir: str,
    job_id: int | None = None,
) -> None:
    """Remove the temp job and workspace file."""
    if job_id:
        try:
            w.jobs.delete(job_id)
            print(f"  Deleted job {job_id}")
        except Exception as e:
            print(f"  Warning: could not delete job {job_id}: {e}")

    try:
        w.workspace.delete(path=base_dir, recursive=True)
        print(f"  Deleted workspace path {base_dir}")
    except Exception as e:
        print(f"  Warning: could not delete workspace path: {e}")


if __name__ == "__main__":
    raise SystemExit(main())
