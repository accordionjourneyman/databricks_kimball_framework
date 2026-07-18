"""Checked-in examples are executable configuration specifications."""

from __future__ import annotations

import ast
from pathlib import Path

import yaml

from kimball.common.config import ConfigLoader, TableConfig, TargetLoader
from kimball.planning import ProjectCompiler

ROOT = Path(__file__).parents[2]


def _assigned_strings(path: Path, names: set[str]) -> dict[str, str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    values: dict[str, str] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name) or target.id not in names:
            continue
        value = node.value
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            values[target.id] = value.value
        elif (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Attribute)
            and value.func.attr == "replace"
            and isinstance(value.func.value, ast.Constant)
        ):
            original = value.func.value.value
            old = ast.literal_eval(value.args[0])
            values[target.id] = original.replace(old, "/Volumes/test/checkpoints")
    return values


def test_all_checked_in_yaml_examples_load_strictly() -> None:
    target = TargetLoader(ROOT / "kimball.targets.yml").load("dev")
    loader = ConfigLoader(template_context=target.template_context())
    paths = sorted((ROOT / "examples" / "configs").glob("*.yml"))

    loaded = [loader.load_config(str(path)) for path in paths]

    assert len(loaded) == len(paths) >= 9
    assert any(source.contract_ref for config in loaded for source in config.sources)


def test_main_notebook_configs_form_a_valid_production_dag() -> None:
    documents = _assigned_strings(
        ROOT / "examples" / "Kimball_Demo.py",
        {
            "dim_customer_yaml",
            "dim_product_yaml",
            "fact_sales_yaml",
            "advanced_fact_yaml",
        },
    )
    configs = {
        name: TableConfig.model_validate(yaml.safe_load(document))
        for name, document in documents.items()
    }

    project = ProjectCompiler(profile="production").compile(
        [
            (name, config)
            for name, config in configs.items()
            if name != "advanced_fact_yaml"
        ]
    )

    assert project.levels == (
        ("demo_gold.dim_customer", "demo_gold.dim_product"),
        ("demo_gold.fact_sales",),
    )
    advanced = configs["advanced_fact_yaml"]
    assert [fk.role for fk in advanced.foreign_keys or []] == [
        "order_date",
        "ship_date",
    ]
    assert advanced.junk_dimensions[0].surrogate_key == "order_flags_sk"
    assert advanced.pii.columns[0].strategy == "tokenize"


def test_streaming_notebook_embedded_configs_load_strictly() -> None:
    documents = _assigned_strings(
        ROOT / "examples" / "Kimball_Streaming_Demo.py",
        {"dim_customer_streaming_yaml", "dim_product_yaml"},
    )

    configs = [
        TableConfig.model_validate(yaml.safe_load(document))
        for document in documents.values()
    ]

    customer = next(config for config in configs if config.scd_type == 2)
    assert customer.sources[0].contract.temporal.event_time_column == "updated_at"
    assert customer.observability.temporal_state_table == "etl_contract_temporal_state"
