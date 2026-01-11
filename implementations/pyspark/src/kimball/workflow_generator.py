"""
Workflow Generator - Generate Databricks Asset Bundle YAML from configs.

Provides:
- Scan config directory for table definitions
- Build dependency graph (dimensions → facts)
- Emit databricks.yml with proper depends_on relationships

Usage:
    from kimball.workflow_generator import WorkflowGenerator

    generator = WorkflowGenerator()
    yaml_content = generator.generate_dab_yaml("configs/")
    generator.save_dab_yaml("configs/", "databricks.yml")
"""

from __future__ import annotations

import glob
import os
from dataclasses import dataclass, field
from typing import Any

import yaml

from kimball.config import ConfigLoader


@dataclass
class TaskDefinition:
    """Represents a task in a Databricks workflow."""

    task_key: str
    table_name: str
    table_type: str
    config_path: str
    dependencies: list[str] = field(default_factory=list)


class WorkflowGenerator:
    """Generate Databricks Asset Bundle YAML from Kimball configs."""

    def __init__(self, config_loader: ConfigLoader | None = None):
        """Initialize generator.

        Args:
            config_loader: ConfigLoader instance. If None, creates a new one.
        """
        self.loader = config_loader or ConfigLoader()

    def scan_configs(
        self, config_dir: str, fail_fast: bool = False
    ) -> tuple[list[TaskDefinition], list[tuple[str, str]]]:
        """
        Scan directory for table configs and build task definitions.

        Args:
            config_dir: Directory containing YAML config files.
            fail_fast: If True, raise on first error. If False, collect all errors.

        Returns:
            Tuple of (list of TaskDefinition objects, list of (file, error) tuples).

        Raises:
            ValueError: If fail_fast=True and any config fails to parse.
        """
        tasks: list[TaskDefinition] = []
        errors: list[tuple[str, str]] = []

        yaml_files = glob.glob(os.path.join(config_dir, "*.yml")) + glob.glob(
            os.path.join(config_dir, "*.yaml")
        )

        if not yaml_files:
            print(f"Warning: No YAML files found in {config_dir}")
            return tasks, errors

        for file_path in yaml_files:
            try:
                config = self.loader.load_config(file_path)

                # Create task key from table name (sanitized)
                task_key = self._sanitize_task_key(config.table_name)

                tasks.append(
                    TaskDefinition(
                        task_key=task_key,
                        table_name=config.table_name,
                        table_type=config.table_type,
                        config_path=file_path,
                        dependencies=[],  # Will be populated later
                    )
                )
            except Exception as e:
                error_msg = str(e)
                errors.append((file_path, error_msg))
                if fail_fast:
                    raise ValueError(
                        f"Failed to parse config {file_path}: {error_msg}"
                    ) from e

        # Report errors clearly
        if errors:
            print(f"\n⚠️  {len(errors)} config(s) failed to parse:")
            for file_path, error_msg in errors:
                print(f"  ❌ {os.path.basename(file_path)}: {error_msg[:100]}")
            print()

        return tasks, errors

    def _sanitize_task_key(self, name: str) -> str:
        """Convert table name to valid task key."""
        return name.replace(".", "_").replace("-", "_")

    def build_dependency_graph(
        self, tasks: list[TaskDefinition]
    ) -> list[TaskDefinition]:
        """
        Build dependency graph: Facts depend on all Dimensions.

        Kimball principle: Load dimensions first, then facts.

        Args:
            tasks: List of task definitions.

        Returns:
            Tasks with dependencies populated.
        """
        # Get all dimension task keys
        dimension_keys = [t.task_key for t in tasks if t.table_type == "dimension"]

        # Facts depend on all dimensions
        for task in tasks:
            if task.table_type == "fact":
                task.dependencies = dimension_keys.copy()

        return tasks

    def generate_dab_yaml(
        self,
        config_dir: str,
        job_name: str = "Nightly Kimball ETL",
        cluster_key: str = "shared_cluster",
        package_name: str = "kimball_framework",
    ) -> dict[str, Any]:
        """
        Generate Databricks Asset Bundle YAML structure.

        Args:
            config_dir: Directory containing YAML configs.
            job_name: Name for the Databricks job.
            cluster_key: Job cluster reference.
            package_name: Python wheel package name.

        Returns:
            Dict structure ready for YAML serialization.
        """
        # Scan and build dependencies
        tasks, errors = self.scan_configs(config_dir)
        if errors and not tasks:
            raise ValueError(
                f"No valid configs found. {len(errors)} file(s) failed to parse."
            )
        tasks = self.build_dependency_graph(tasks)

        # Separate dimensions and facts for layered execution
        dimensions = [t for t in tasks if t.table_type == "dimension"]
        facts = [t for t in tasks if t.table_type == "fact"]

        # Build task list
        dab_tasks = []

        # Layer 1: All dimensions (can run in parallel)
        if dimensions:
            dab_tasks.append(
                {
                    "task_key": "load_dimensions",
                    "job_cluster_key": cluster_key,
                    "python_wheel_task": {
                        "package_name": package_name,
                        "entry_point": "run_dimensions",
                        "parameters": [config_dir],
                    },
                }
            )

        # Layer 2: All facts (depend on dimensions)
        if facts:
            fact_task = {
                "task_key": "load_facts",
                "job_cluster_key": cluster_key,
                "python_wheel_task": {
                    "package_name": package_name,
                    "entry_point": "run_facts",
                    "parameters": [config_dir],
                },
            }
            if dimensions:
                fact_task["depends_on"] = [{"task_key": "load_dimensions"}]
            dab_tasks.append(fact_task)

        # Build full DAB structure
        dab = {
            "resources": {
                "jobs": {
                    "nightly_etl": {
                        "name": job_name,
                        "job_clusters": [
                            {
                                "job_cluster_key": cluster_key,
                                "new_cluster": {
                                    "spark_version": "14.3.x-scala2.12",
                                    "node_type_id": "Standard_DS3_v2",
                                    "num_workers": 2,
                                    "spark_conf": {
                                        "spark.scheduler.mode": "FAIR",
                                    },
                                },
                            }
                        ],
                        "tasks": dab_tasks,
                    }
                }
            }
        }

        return dab

    def generate_individual_tasks(
        self,
        config_dir: str,
        cluster_key: str = "shared_cluster",
    ) -> list[dict[str, Any]]:
        """
        Generate individual task definitions (one per table).

        Args:
            config_dir: Directory containing YAML configs.
            cluster_key: Job cluster reference.

        Returns:
            List of task definitions.
        """
        tasks, _errors = self.scan_configs(config_dir)
        tasks = self.build_dependency_graph(tasks)

        dab_tasks = []
        for task in tasks:
            task_def = {
                "task_key": task.task_key,
                "job_cluster_key": cluster_key,
                "notebook_task": {
                    "notebook_path": "/Repos/kimball/notebooks/run_table",
                    "base_parameters": {
                        "config_path": task.config_path,
                    },
                },
            }

            if task.dependencies:
                task_def["depends_on"] = [
                    {"task_key": dep} for dep in task.dependencies
                ]

            dab_tasks.append(task_def)

        return dab_tasks

    def save_dab_yaml(
        self,
        config_dir: str,
        output_path: str,
        **kwargs,
    ) -> None:
        """
        Generate and save DAB YAML to file.

        Args:
            config_dir: Directory containing YAML configs.
            output_path: Path to write databricks.yml.
            **kwargs: Additional arguments for generate_dab_yaml.
        """
        dab = self.generate_dab_yaml(config_dir, **kwargs)

        with open(output_path, "w") as f:
            yaml.dump(dab, f, default_flow_style=False, sort_keys=False)

        print(f"Generated DAB YAML: {output_path}")

    def print_dependency_graph(self, config_dir: str) -> None:
        """
        Print the dependency graph for visualization.

        Args:
            config_dir: Directory containing YAML configs.
        """
        tasks, _errors = self.scan_configs(config_dir)
        tasks = self.build_dependency_graph(tasks)

        print("\n=== Kimball Pipeline Dependency Graph ===\n")
        print("Layer 1: Dimensions (parallel)")
        for task in tasks:
            if task.table_type == "dimension":
                print(f"  📦 {task.task_key}")

        print("\nLayer 2: Facts (depend on dimensions)")
        for task in tasks:
            if task.table_type == "fact":
                deps = " → ".join(task.dependencies[:3])
                if len(task.dependencies) > 3:
                    deps += f" + {len(task.dependencies) - 3} more"
                print(f"  📊 {task.task_key}")
                if deps:
                    print(f"      └── depends on: {deps}")
