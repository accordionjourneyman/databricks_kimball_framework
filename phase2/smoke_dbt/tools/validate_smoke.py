"""Validate the native dbt artifacts emitted by the Phase 2.0 smoke run."""

from __future__ import annotations

import json
from pathlib import Path


target = Path("target")
manifest = json.loads((target / "manifest.json").read_text())
results = json.loads((target / "run_results.json").read_text())

model_id = "model.kimball_phase2_smoke.smoke_incremental"
assert model_id in manifest["nodes"]
assert manifest["nodes"][model_id]["config"]["materialized"] == "incremental"
assert results["results"], "dbt emitted no test results"
assert all(item["status"] == "pass" for item in results["results"])
print(
    "PHASE2_DBT_ARTIFACT_PROOF="
    + json.dumps(
        {
            "dbt_version": manifest["metadata"]["dbt_version"],
            "model": model_id,
            "test_count": len(results["results"]),
        },
        sort_keys=True,
    )
)
