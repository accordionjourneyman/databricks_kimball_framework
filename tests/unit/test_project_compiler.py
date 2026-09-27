from __future__ import annotations

import random

import pytest
from pydantic import ValidationError

from kimball.common.config import (
    ForeignKeyConfig,
    ObservabilityConfig,
    SourceConfig,
    StreamingSourceConfig,
    TableConfig,
)
from kimball.planning.compiler import (
    CompiledPipeline,
    ProjectCompiler,
    ProjectValidationError,
)


def _dimension(name: str, *, sources: list[SourceConfig] | None = None, **kwargs):
    return TableConfig(
        table_name=name,
        table_type="dimension",
        surrogate_key=f"{name.split('.')[-1]}_sk",
        natural_keys=["id"],
        sources=sources
        or [
            SourceConfig(
                name=f"silver.{name}",
                alias="src",
                primary_keys=["id"],
            )
        ],
        table_description=kwargs.pop("table_description", f"{name} dimension fixture."),
        **kwargs,
    )


def _fact(name: str, **kwargs):
    return TableConfig(
        table_name=name,
        table_type="fact",
        merge_keys=["id"],
        sources=kwargs.pop(
            "sources",
            [
                SourceConfig(
                    name="silver.orders",
                    alias="src",
                    primary_keys=["order_id", "id"],
                )
            ],
        ),
        table_description=kwargs.pop("table_description", f"{name} fact fixture."),
        **kwargs,
    )


@pytest.mark.parametrize(
    ("model", "payload"),
    [
        (StreamingSourceConfig, {"enabled": True, "triger": "available_now"}),
        (ObservabilityConfig, {"enabledd": True}),
        (ForeignKeyConfig, {"column": "date_sk", "referencess": "gold.dim_date"}),
    ],
)
def test_nested_config_models_reject_unknown_fields(model, payload):
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        model.model_validate(payload)


def test_compiler_builds_deterministic_dependency_levels():
    date = _dimension("gold.dim_date")
    customer = _dimension("gold.dim_customer")
    sales = _fact(
        "gold.fact_sales",
        depends_on=["gold.dim_customer", "gold.dim_date"],
        foreign_keys=[
            ForeignKeyConfig(column="customer_sk", references="gold.dim_customer"),
            ForeignKeyConfig(column="order_date_sk", references="gold.dim_date"),
        ],
        column_descriptions={"customer_sk": "customer", "order_date_sk": "date"},
    )

    project = ProjectCompiler(profile="production").compile(
        [("sales.yml", sales), ("customer.yml", customer), ("date.yml", date)]
    )

    assert project.levels == (
        ("gold.dim_customer", "gold.dim_date"),
        ("gold.fact_sales",),
    )
    assert project.nodes["gold.fact_sales"].dependencies == (
        "gold.dim_customer",
        "gold.dim_date",
    )


def test_production_requires_inferred_dependency_to_be_explicit():
    dim = _dimension("gold.dim_customer")
    fact = _fact(
        "gold.fact_sales",
        foreign_keys=[
            ForeignKeyConfig(column="customer_sk", references="gold.dim_customer")
        ],
    )

    with pytest.raises(ProjectValidationError, match="UNDECLARED_DEPENDENCY"):
        ProjectCompiler(profile="production").compile(
            [("dim.yml", dim), ("fact.yml", fact)]
        )


def test_scd4_history_source_infers_writer_and_requires_declared_production_edge():
    producer = _dimension(
        "gold.dim_customer",
        scd_type=4,
        history_table="gold.dim_customer_history",
        column_descriptions={
            "id": "Customer business key.",
            "dim_customer_sk": "Customer surrogate key.",
        },
    )
    history_source = SourceConfig(
        name="gold.dim_customer_history",
        alias="history",
        primary_keys=["id"],
    )
    consumer = _dimension(
        "gold.customer_history_report",
        sources=[history_source],
        column_descriptions={
            "id": "Customer business key.",
            "customer_history_report_sk": "Report surrogate key.",
        },
    )

    development = ProjectCompiler(profile="dev").compile(
        [("producer.yml", producer), ("consumer.yml", consumer)]
    )

    assert development.nodes["gold.customer_history_report"].inferred_dependencies == (
        "gold.dim_customer",
    )
    assert development.nodes["gold.customer_history_report"].dependencies == (
        "gold.dim_customer",
    )
    assert development.levels == (
        ("gold.dim_customer",),
        ("gold.customer_history_report",),
    )
    with pytest.raises(ProjectValidationError, match="UNDECLARED_DEPENDENCY"):
        ProjectCompiler(profile="production").compile(
            [("producer.yml", producer), ("consumer.yml", consumer)]
        )

    declared_consumer = _dimension(
        "gold.customer_history_report",
        sources=[history_source],
        depends_on=["gold.dim_customer"],
        column_descriptions={
            "id": "Customer business key.",
            "customer_history_report_sk": "Report surrogate key.",
        },
    )
    production = ProjectCompiler(profile="production").compile(
        [("producer.yml", producer), ("consumer.yml", declared_consumer)]
    )
    assert production.levels == development.levels


def test_development_warns_and_uses_inferred_dependency():
    dim = _dimension("gold.dim_customer")
    fact = _fact(
        "gold.fact_sales",
        sources=[SourceConfig(name="gold.dim_customer", alias="customer")],
    )

    project = ProjectCompiler(profile="dev").compile(
        [("dim.yml", dim), ("fact.yml", fact)]
    )

    assert project.levels == (("gold.dim_customer",), ("gold.fact_sales",))
    assert sorted(issue.code for issue in project.warnings) == [
        "INCREMENTAL_LOAD_FRAGILE",
        "UNDECLARED_DEPENDENCY",
    ]


def test_compiler_rejects_missing_explicit_upstream():
    fact = _fact("gold.fact_sales", depends_on=["gold.dim_missing"])

    with pytest.raises(ProjectValidationError, match="MISSING_UPSTREAM"):
        ProjectCompiler().compile([("fact.yml", fact)])


def test_compiler_reports_cycle_path():
    a = _dimension(
        "gold.dim_a",
        sources=[SourceConfig(name="gold.dim_b", alias="b")],
        depends_on=["gold.dim_b"],
    )
    b = _dimension(
        "gold.dim_b",
        sources=[SourceConfig(name="gold.dim_a", alias="a")],
        depends_on=["gold.dim_a"],
    )

    with pytest.raises(
        ProjectValidationError,
        match=r"DEPENDENCY_CYCLE.*gold\.dim_a -> gold\.dim_b -> gold\.dim_a",
    ):
        ProjectCompiler(profile="production").compile([("a.yml", a), ("b.yml", b)])


def test_compiler_rejects_duplicate_target_writer():
    first = _dimension("gold.dim_customer")
    second = _dimension("gold.dim_customer")

    with pytest.raises(ProjectValidationError, match="TARGET_WRITER_CONFLICT"):
        ProjectCompiler().compile([("first.yml", first), ("second.yml", second)])


def test_compiler_rejects_auxiliary_writer_conflict():
    first = _fact(
        "gold.fact_a",
        junk_dimensions=[
            {
                "dimension_table": "gold.dim_flags",
                "surrogate_key": "flags_sk",
                "source_columns": ["is_gift"],
            }
        ],
    )
    second = _fact(
        "gold.fact_b",
        junk_dimensions=[
            {
                "dimension_table": "gold.dim_flags",
                "surrogate_key": "flags_sk",
                "source_columns": ["is_priority"],
            }
        ],
    )

    with pytest.raises(ProjectValidationError, match="TARGET_WRITER_CONFLICT"):
        ProjectCompiler().compile([("a.yml", first), ("b.yml", second)])


def _compiled_node(name: str, dependencies: tuple[str, ...]) -> CompiledPipeline:
    return CompiledPipeline(
        table_name=name,
        table_type="dimension",
        config_path="",
        explicit_dependencies=(),
        inferred_dependencies=(),
        dependencies=dependencies,
        writes=(name,),
        config=None,  # These static graph helpers do not inspect config.
    )


def test_cycle_finder_handles_deep_valid_dag_without_recursion() -> None:
    names = [f"n{index:05}" for index in range(1500)]
    nodes = {
        name: _compiled_node(
            name, () if index == len(names) - 1 else (names[index + 1],)
        )
        for index, name in enumerate(names)
    }

    assert ProjectCompiler._find_cycle(nodes) is None
    assert ProjectCompiler._topological_levels(nodes) == tuple(
        (name,) for name in reversed(names)
    )


def test_topological_levels_reject_unresolved_dependency() -> None:
    nodes = {"table": _compiled_node("table", ("missing",))}

    with pytest.raises(RuntimeError, match="Cannot order a cyclic dependency graph"):
        ProjectCompiler._topological_levels(nodes)


def test_topological_levels_deduplicate_dependencies_and_sort_frontiers() -> None:
    nodes = {
        "root-b": _compiled_node("root-b", ()),
        "root-a": _compiled_node("root-a", ()),
        "join": _compiled_node("join", ("root-b", "root-a", "root-a")),
    }

    assert ProjectCompiler._topological_levels(nodes) == (
        ("root-a", "root-b"),
        ("join",),
    )


def test_kahn_levels_match_repeated_scan_reference_on_seeded_dags():
    rng = random.Random(20260927)

    def reference_levels(nodes):
        remaining = set(nodes)
        completed = set()
        levels = []
        while remaining:
            ready = tuple(
                sorted(
                    name
                    for name in remaining
                    if set(nodes[name].dependencies) <= completed
                )
            )
            if not ready:
                raise RuntimeError("Cannot order a cyclic dependency graph")
            levels.append(ready)
            completed.update(ready)
            remaining.difference_update(ready)
        return tuple(levels)

    for _ in range(30):
        order = [f"node_{index:02}" for index in range(rng.randint(2, 24))]
        rng.shuffle(order)
        dependencies = {
            name: tuple(candidate for candidate in order[:index] if rng.random() < 0.18)
            for index, name in enumerate(order)
        }
        nodes = {
            name: CompiledPipeline(
                table_name=name,
                table_type="dimension",
                config_path=f"{name}.yml",
                explicit_dependencies=(),
                inferred_dependencies=(),
                dependencies=deps,
                writes=(name,),
                config=_dimension(name),
            )
            for name, deps in dependencies.items()
        }

        assert ProjectCompiler._topological_levels(nodes) == reference_levels(nodes)
