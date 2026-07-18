from __future__ import annotations

from kimball.common.config import ForeignKeyConfig, SourceConfig, TableConfig
from kimball.planning.bundle import build_bundle_job
from kimball.planning.compiler import ProjectCompiler
from kimball.planning.manifest import build_manifest, diff_manifests, manifest_json


def _dimension(name: str, **kwargs) -> TableConfig:
    return TableConfig(
        table_name=name,
        table_type="dimension",
        surrogate_key="dimension_sk",
        natural_keys=["id"],
        sources=[SourceConfig(name=f"silver.{name.split('.')[-1]}", alias="src")],
        **kwargs,
    )


def _fact(*, sql: str = "SELECT * FROM src", description: str | None = None):
    return TableConfig(
        table_name="gold.fact_sales",
        table_type="fact",
        merge_keys=["sale_id"],
        depends_on=["gold.dim_customer"],
        sources=[SourceConfig(name="silver.sales", alias="src")],
        transformation_sql=sql,
        foreign_keys=[
            ForeignKeyConfig(column="customer_sk", references="gold.dim_customer")
        ],
        table_description=description,
    )


def _project(*, sql: str = "SELECT * FROM src", description: str | None = None):
    return ProjectCompiler(profile="production").compile(
        [
            ("configs/dim.yml", _dimension("gold.dim_customer")),
            ("configs/fact.yml", _fact(sql=sql, description=description)),
        ]
    )


def test_manifest_is_deterministic_and_contains_execution_contract():
    manifest = build_manifest(_project(), framework_version="9.9.9")

    assert manifest["schema_version"] == "1.0"
    assert manifest["framework_version"] == "9.9.9"
    assert [node["table_name"] for node in manifest["pipelines"]] == [
        "gold.dim_customer",
        "gold.fact_sales",
    ]
    assert manifest["pipelines"][1]["dependencies"] == ["gold.dim_customer"]
    assert manifest_json(manifest) == manifest_json(manifest)
    assert manifest_json(manifest).endswith("\n")


def test_plan_classifies_description_only_change_as_metadata_only():
    previous = build_manifest(_project(description="Old"), framework_version="1")
    current = build_manifest(_project(description="New"), framework_version="1")

    plan = diff_manifests(previous, current)

    assert [(change.table_name, change.classification) for change in plan.changes] == [
        ("gold.fact_sales", "metadata_only")
    ]
    assert plan.affected_tables == ("gold.fact_sales",)


def test_plan_marks_sql_change_for_backfill_and_includes_downstream():
    base = _project()
    aggregate = TableConfig(
        table_name="gold.fact_sales_daily",
        table_type="fact",
        merge_keys=["day"],
        depends_on=["gold.fact_sales"],
        sources=[SourceConfig(name="gold.fact_sales", alias="sales")],
    )
    previous_project = ProjectCompiler(profile="production").compile(
        [(node.config_path, node.config) for node in base.nodes.values()]
        + [("configs/aggregate.yml", aggregate)]
    )
    changed_base = _project(sql="SELECT sale_id FROM src")
    current_project = ProjectCompiler(profile="production").compile(
        [(node.config_path, node.config) for node in changed_base.nodes.values()]
        + [("configs/aggregate.yml", aggregate)]
    )

    plan = diff_manifests(
        build_manifest(previous_project, framework_version="1"),
        build_manifest(current_project, framework_version="1"),
    )

    assert plan.changes[0].classification == "requires_backfill"
    assert plan.affected_tables == ("gold.fact_sales", "gold.fact_sales_daily")


def test_plan_marks_removed_pipeline_as_breaking():
    previous = build_manifest(_project(), framework_version="1")
    current_project = ProjectCompiler(profile="production").compile(
        [("configs/dim.yml", _dimension("gold.dim_customer"))]
    )

    plan = diff_manifests(
        previous, build_manifest(current_project, framework_version="1")
    )

    assert plan.changes[0].classification == "breaking"
    assert plan.changes[0].kind == "removed"


def test_bundle_job_uses_compiled_dependencies_and_one_task_per_pipeline():
    bundle = build_bundle_job(_project(), job_name="kimball_gold", target_name="prod")
    job = bundle["resources"]["jobs"]["kimball_gold"]
    tasks = job["tasks"]

    assert [task["task_key"] for task in tasks] == [
        "gold_dim_customer",
        "gold_fact_sales",
    ]
    assert "depends_on" not in tasks[0]
    assert tasks[1]["depends_on"] == [{"task_key": "gold_dim_customer"}]
    assert tasks[1]["python_wheel_task"]["entry_point"] == "kimball"
    assert tasks[1]["python_wheel_task"]["parameters"][:2] == [
        "run",
        "--config",
    ]
    assert "--target" in tasks[1]["python_wheel_task"]["parameters"]
    assert job["parameters"][0] == {"name": "target", "default": "prod"}
    assert job["max_concurrent_runs"] == 1
