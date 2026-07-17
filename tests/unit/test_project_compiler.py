from __future__ import annotations

import pytest
from pydantic import ValidationError

from kimball.common.config import (
    ForeignKeyConfig,
    ObservabilityConfig,
    SourceConfig,
    StreamingSourceConfig,
    TableConfig,
)
from kimball.planning.compiler import ProjectCompiler, ProjectValidationError


def _dimension(name: str, *, sources: list[SourceConfig] | None = None, **kwargs):
    return TableConfig(
        table_name=name,
        table_type="dimension",
        surrogate_key=f"{name.split('.')[-1]}_sk",
        natural_keys=["id"],
        sources=sources or [SourceConfig(name=f"silver.{name}", alias="src")],
        **kwargs,
    )


def _fact(name: str, **kwargs):
    return TableConfig(
        table_name=name,
        table_type="fact",
        merge_keys=["id"],
        sources=kwargs.pop(
            "sources", [SourceConfig(name="silver.orders", alias="src")]
        ),
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
    assert [issue.code for issue in project.warnings] == ["UNDECLARED_DEPENDENCY"]


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
