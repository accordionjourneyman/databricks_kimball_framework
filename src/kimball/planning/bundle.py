from __future__ import annotations

import re
from typing import Any

from kimball.planning.compiler import CompiledProject


def _task_key(table_name: str) -> str:
    key = re.sub(r"[^A-Za-z0-9_]", "_", table_name)
    key = re.sub(r"_+", "_", key).strip("_")
    if not key:
        raise ValueError(f"Cannot derive a Databricks task key from '{table_name}'")
    return key[:100]


def build_bundle_job(
    project: CompiledProject,
    *,
    job_name: str = "kimball_compiled_job",
    target_name: str,
    target_parameter: str = "{{job.parameters.target}}",
    targets_file_parameter: str = "{{job.parameters.targets_file}}",
) -> dict[str, Any]:
    """Render one Databricks wheel task per pipeline using compiled DAG edges."""

    task_keys = {name: _task_key(name) for name in project.nodes}
    if len(set(task_keys.values())) != len(task_keys):
        raise ValueError("Sanitized Databricks task keys are not unique")

    tasks: list[dict[str, Any]] = []
    for table_name in sorted(project.nodes):
        node = project.nodes[table_name]
        task: dict[str, Any] = {
            "task_key": task_keys[table_name],
            "python_wheel_task": {
                "package_name": "kimball_framework",
                "entry_point": "kimball",
                "parameters": [
                    "run",
                    "--config",
                    node.config_path,
                    "--target",
                    target_parameter,
                    "--targets",
                    targets_file_parameter,
                ],
            },
            "libraries": [{"whl": "../dist/*.whl"}],
        }
        if node.dependencies:
            task["depends_on"] = [
                {"task_key": task_keys[dependency]} for dependency in node.dependencies
            ]
        tasks.append(task)

    return {
        "resources": {
            "jobs": {
                job_name: {
                    "name": job_name,
                    # Compensating recovery is only safe under this product
                    # contract: one active writer graph for each target set.
                    "max_concurrent_runs": 1,
                    "parameters": [
                        {"name": "target", "default": target_name},
                        {"name": "targets_file", "default": "kimball.targets.yml"},
                    ],
                    "tasks": tasks,
                }
            }
        }
    }
