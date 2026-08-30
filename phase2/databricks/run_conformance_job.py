"""Deploy and run the Phase 2.6 native Databricks dbt job."""

from __future__ import annotations

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


REPO_ROOT = Path(__file__).parents[2]
REFERENCE = REPO_ROOT / "phase2" / "reference"
CATALOG = os.environ.get("PHASE2_DATABRICKS_CATALOG", "workspace")
DBT_SCHEMA = os.environ.get("PHASE2_DBT_SCHEMA", "phase2_dbt_conformance")
FRAMEWORK_SCHEMA = os.environ.get(
    "PHASE2_FRAMEWORK_SCHEMA", "phase2_framework_conformance"
)


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key, value.strip().strip("\"'"))


def api(
    method: str,
    endpoint: str,
    payload: dict[str, object] | None = None,
    *,
    raw: bytes | None = None,
) -> dict[str, object]:
    url = os.environ["DATABRICKS_HOST"].rstrip("/") + endpoint
    headers = {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}
    data = raw
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"{method} {endpoint} failed ({exc.code}): {detail}") from exc


load_env(REPO_ROOT / ".env")
for required in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID"):
    if required not in os.environ:
        raise SystemExit(f"missing {required}")
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]

identity = api("GET", "/api/2.0/preview/scim/v2/Me")
username = str(identity.get("userName") or identity.get("displayName"))
if not username or username == "None":
    raise SystemExit("could not resolve current Databricks user")

workspace_root = f"/Users/{username}/phase2-conformance"
project_path = f"{workspace_root}/reference"
api("POST", "/api/2.0/workspace/mkdirs", {"path": project_path})

excluded_parts = {"target", "logs", "evidence", "__pycache__", "framework"}
files = [
    path
    for path in REFERENCE.rglob("*")
    if path.is_file()
    and all(
        part not in excluded_parts
        for part in path.relative_to(REFERENCE).parts
    )
    and path.name
    not in {"profiles.yml", ".user.yml", "reconcile.py", "README.md"}
]
files.append(REPO_ROOT / "phase2" / "databricks" / "framework_conformance.py")
for path in files:
    if path.name == "framework_conformance.py":
        remote = f"{workspace_root}/{path.name}"
    else:
        remote = f"{project_path}/{path.relative_to(REFERENCE).as_posix()}"
    parent = remote.rsplit("/", 1)[0]
    api("POST", "/api/2.0/workspace/mkdirs", {"path": parent})
    api(
        "POST",
        "/api/2.0/workspace/import",
        {
            "path": remote,
            "content": base64.b64encode(path.read_bytes()).decode(),
            "format": "RAW",
            "overwrite": True,
        },
    )

job = {
    "name": "[phase2] dbt-framework-conformance",
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": "dbt_reference",
            "environment_key": "phase2_env",
            "dbt_task": {
                "project_directory": project_path,
                "source": "WORKSPACE",
                "warehouse_id": WAREHOUSE_ID,
                "catalog": CATALOG,
                "schema": DBT_SCHEMA,
                "commands": [
                    "dbt seed --full-refresh",
                    "dbt run --full-refresh --vars '{max_event_seq: 3}'",
                    "dbt snapshot --vars '{max_event_seq: 3}'",
                    "dbt test --vars '{max_event_seq: 3}'",
                    "dbt run --vars '{max_event_seq: 14}'",
                    "dbt snapshot --vars '{max_event_seq: 14}'",
                    "dbt test --vars '{max_event_seq: 14}'",
                ],
            },
            "timeout_seconds": 1800,
        },
        {
            "task_key": "framework_reconciliation",
            "depends_on": [{"task_key": "dbt_reference"}],
            "environment_key": "phase2_env",
            "spark_python_task": {
                "python_file": f"{workspace_root}/framework_conformance.py",
                "parameters": [CATALOG, DBT_SCHEMA, FRAMEWORK_SCHEMA],
            },
            "timeout_seconds": 1800,
        },
    ],
    "environments": [
        {
            "environment_key": "phase2_env",
            "spec": {
                "environment_version": "4",
                "dependencies": [
                    "dbt-core==1.11.7",
                    "dbt-databricks==1.12.2",
                ],
            },
        }
    ],
}

created = api("POST", "/api/2.2/jobs/create", job)
job_id = created["job_id"]
started = api("POST", "/api/2.2/jobs/run-now", {"job_id": job_id})
run_id = started["run_id"]
print(
    "PHASE2_DATABRICKS_JOB="
    + json.dumps(
        {
            "job_id": job_id,
            "run_id": run_id,
            "warehouse_id": WAREHOUSE_ID,
            "catalog": CATALOG,
            "dbt_schema": DBT_SCHEMA,
            "framework_schema": FRAMEWORK_SCHEMA,
            "workspace_root": workspace_root,
        },
        sort_keys=True,
    ),
    flush=True,
)

while True:
    run = api("GET", f"/api/2.2/jobs/runs/get?run_id={run_id}")
    state = run.get("state", {})
    lifecycle = state.get("life_cycle_state")
    result = state.get("result_state")
    print(f"run {run_id}: lifecycle={lifecycle} result={result}", flush=True)
    if lifecycle in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
        evidence = REPO_ROOT / "phase2" / "reference" / "evidence"
        evidence.mkdir(exist_ok=True)
        (evidence / "databricks-run.json").write_text(
            json.dumps(run, indent=2, sort_keys=True, default=str) + "\n"
        )
        if result != "SUCCESS":
            raise SystemExit(f"Databricks conformance failed: {result}")
        break
    time.sleep(15)
