"""Fetch task output for the retained Phase 2.6 Databricks run."""

from __future__ import annotations

import json
import os
import urllib.request
from pathlib import Path


repo_root = Path(__file__).parents[2]
for raw_line in (repo_root / ".env").read_text().splitlines():
    line = raw_line.strip()
    if line and not line.startswith("#") and "=" in line:
        key, value = line.split("=", 1)
        os.environ.setdefault(key, value.strip().strip("\"'"))

evidence_dir = repo_root / "phase2" / "reference" / "evidence"
run = json.loads((evidence_dir / "databricks-run.json").read_text())
host = os.environ["DATABRICKS_HOST"].rstrip("/")
token = os.environ["DATABRICKS_TOKEN"]
for task in run["tasks"]:
    task_key = task["task_key"]
    task_run_id = task["run_id"]
    request = urllib.request.Request(
        f"{host}/api/2.2/jobs/runs/get-output?run_id={task_run_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        output = json.load(response)
    (evidence_dir / f"databricks-{task_key}-output.json").write_text(
        json.dumps(output, indent=2, sort_keys=True, default=str) + "\n"
    )
    logs = output.get("logs", "")
    markers = [
        line
        for line in logs.splitlines()
        if "PHASE2_" in line or "Completed successfully" in line
    ]
    print(
        f"{task_key}: result={task['state'].get('result_state')} "
        f"markers={json.dumps(markers)}"
    )
