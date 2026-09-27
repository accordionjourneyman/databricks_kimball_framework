from __future__ import annotations

import random

import pytest

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
        sources=[
            SourceConfig(
                name=f"silver.{name.split('.')[-1]}",
                alias="src",
                primary_keys=["id"],
            )
        ],
        table_description=kwargs.pop("table_description", f"{name} fixture."),
        **kwargs,
    )


def _fact(*, sql: str = "SELECT * FROM src", description: str | None = None):
    return TableConfig(
        table_name="gold.fact_sales",
        table_type="fact",
        merge_keys=["sale_id"],
        depends_on=["gold.dim_customer"],
        sources=[
            SourceConfig(
                name="silver.sales",
                alias="src",
                primary_keys=["sale_id"],
            )
        ],
        transformation_sql=sql,
        foreign_keys=[
            ForeignKeyConfig(column="customer_sk", references="gold.dim_customer")
        ],
        table_description=description or "Sales fact fixture.",
        column_descriptions={"customer_sk": "Customer surrogate key."},
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

    assert manifest["schema_version"] == "1.1"
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


def test_inferred_dependency_change_is_breaking_with_unchanged_consumer_digest():
    consumer = TableConfig(
        table_name="gold.consumer",
        table_type="dimension",
        surrogate_key="consumer_sk",
        natural_keys=["id"],
        sources=[
            SourceConfig(name="gold.producer", alias="producer", primary_keys=["id"])
        ],
        table_description="Consumer fixture.",
    )
    producer = _dimension("gold.producer")
    before = ProjectCompiler(profile="dev").compile([("consumer.yml", consumer)])
    after = ProjectCompiler(profile="dev").compile(
        [("consumer.yml", consumer), ("producer.yml", producer)]
    )
    before_manifest = build_manifest(before, framework_version="1")
    after_manifest = build_manifest(after, framework_version="1")
    before_consumer = next(
        node
        for node in before_manifest["pipelines"]
        if node["table_name"] == "gold.consumer"
    )
    after_consumer = next(
        node
        for node in after_manifest["pipelines"]
        if node["table_name"] == "gold.consumer"
    )

    assert before_consumer["semantic_digest"] == after_consumer["semantic_digest"]
    change = next(
        change
        for change in diff_manifests(before_manifest, after_manifest).changes
        if change.table_name == "gold.consumer"
    )
    assert change.classification == "breaking"
    assert change.changed_fields == ("dependencies",)


def test_write_only_manifest_change_is_breaking_with_equal_semantic_digest():
    node = {
        "table_name": "gold.consumer",
        "semantic_digest": "same-semantic-config",
        "metadata_digest": "same-metadata",
        "semantic_config": {},
        "dependencies": [],
        "writes": ["gold.consumer"],
    }
    previous = {"pipelines": [{**node, "writes": ["gold.consumer"]}]}
    current = {
        "pipelines": [{**node, "writes": ["gold.consumer", "gold.consumer_history"]}]
    }

    change = diff_manifests(previous, current).changes[0]

    assert change.classification == "breaking"
    assert change.changed_fields == ("writes",)


def test_plan_marks_sql_change_for_backfill_and_includes_downstream():
    base = _project()
    aggregate = TableConfig(
        table_name="gold.fact_sales_daily",
        table_type="fact",
        merge_keys=["day"],
        depends_on=["gold.fact_sales"],
        sources=[
            SourceConfig(name="gold.fact_sales", alias="sales", primary_keys=["day"])
        ],
        table_description="Daily sales aggregate fixture.",
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


def _manifest_graph(dependencies: dict[str, tuple[str, ...]], changed=()):
    changed = set(changed)
    return {
        "pipelines": [
            {
                "table_name": name,
                "semantic_digest": "changed" if name in changed else "same",
                "metadata_digest": "metadata",
                "semantic_config": {
                    "transformation_sql": "changed" if name in changed else "same"
                },
                "dependencies": list(deps),
                "writes": [name],
            }
            for name, deps in dependencies.items()
        ]
    }


@pytest.mark.parametrize(
    ("before", "after", "expected_changes", "expected_affected", "changed"),
    [
        (
            {"A": (), "B": ("A",), "C": ("B",), "D": ()},
            {"A": (), "B": ("A",), "C": ("B",), "D": ()},
            ("A",),
            ("A", "B", "C"),
            {"A"},
        ),
        (
            {"A": (), "B": ("A",), "C": ("B",)},
            {"A": (), "B": (), "C": ("B",)},
            ("B",),
            ("B", "C"),
            set(),
        ),
        (
            {"A": (), "B": ("A",), "C": ("B",)},
            {"A": (), "X": (), "B": ("A", "X"), "C": ("B",)},
            ("B", "X"),
            ("B", "C", "X"),
            set(),
        ),
        (
            {"A": (), "B": ("A",), "C": ("B",)},
            {"A": (), "C": ("B",)},
            ("B",),
            ("B", "C"),
            set(),
        ),
    ],
    ids=("branching-change", "dependency-removal", "added-upstream", "removed-node"),
)
def test_manifest_impact_uses_complete_transitive_dependents(
    before, after, expected_changes, expected_affected, changed
):
    plan = diff_manifests(_manifest_graph(before, changed), _manifest_graph(after))

    assert tuple(change.table_name for change in plan.changes) == expected_changes
    assert plan.affected_tables == expected_affected


def test_manifest_breadth_first_impact_matches_fixpoint_reference():
    rng = random.Random(20260928)
    for _ in range(30):
        order = [f"table_{index:02}" for index in range(rng.randint(3, 24))]
        rng.shuffle(order)
        dependencies = {
            name: tuple(candidate for candidate in order[:index] if rng.random() < 0.2)
            for index, name in enumerate(order)
        }
        changed = {rng.choice(order)}
        previous = _manifest_graph(dependencies, changed)
        current = _manifest_graph(dependencies)

        affected = set(changed)
        combined = {
            **{node["table_name"]: node for node in previous["pipelines"]},
            **{node["table_name"]: node for node in current["pipelines"]},
        }
        changed_set = True
        while changed_set:
            changed_set = False
            for table_name, node in combined.items():
                if table_name not in affected and affected.intersection(
                    node.get("dependencies", [])
                ):
                    affected.add(table_name)
                    changed_set = True

        plan = diff_manifests(previous, current)
        assert plan.affected_tables == tuple(sorted(affected))
