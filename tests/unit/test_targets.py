from __future__ import annotations

import os

import pytest

from kimball.common.config import ConfigLoader, TargetLoader


def _targets(tmp_path):
    path = tmp_path / "kimball.targets.yml"
    path.write_text(
        """version: 1
targets:
  dev:
    catalog: workspace
    silver_schema: dev_silver
    gold_schema: dev_gold
    etl_schema: dev_ops
    checkpoint_root: /Volumes/workspace/dev_ops/checkpoints
""",
        encoding="utf-8",
    )
    return path


def test_target_loader_resolves_explicit_target(tmp_path):
    target = TargetLoader(_targets(tmp_path)).load("dev")

    assert target.name == "dev"
    assert target.etl_schema == "dev_ops"


def test_target_loader_reports_unknown_target(tmp_path):
    with pytest.raises(ValueError, match="Unknown target 'prod'.*dev"):
        TargetLoader(_targets(tmp_path)).load("prod")


def test_target_context_renders_portable_pipeline_yaml(tmp_path):
    config_path = tmp_path / "dim_customer.yml"
    config_path.write_text(
        """table_name: {{ target.gold_schema }}.dim_customer
table_type: dimension
surrogate_key: customer_sk
natural_keys: [customer_id]
sources:
  - name: {{ target.silver_schema }}.customers
    alias: customer
""",
        encoding="utf-8",
    )

    target = TargetLoader(_targets(tmp_path)).load("dev")
    config = ConfigLoader(template_context=target.template_context()).load_config(
        str(config_path)
    )

    assert config.table_name == "dev_gold.dim_customer"
    assert config.sources[0].name == "dev_silver.customers"


def test_loader_normalizes_windows_environment_key_case(tmp_path, monkeypatch):
    config_path = tmp_path / "legacy.yml"
    config_path.write_text(
        """table_name: {{ env }}_gold.dim_customer
table_type: dimension
surrogate_key: customer_sk
natural_keys: [customer_id]
sources:
  - name: {{ env }}_silver.customers
    alias: customer
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(os, "environ", {"ENV": "dev"})

    config = ConfigLoader().load_config(str(config_path))

    assert config.table_name == "dev_gold.dim_customer"


def test_loader_reports_missing_template_variable_without_traceback(tmp_path):
    config_path = tmp_path / "missing.yml"
    config_path.write_text("table_name: {{ target.gold_schema }}.x", encoding="utf-8")

    with pytest.raises(ValueError, match="missing.yml.*target"):
        ConfigLoader().load_config(str(config_path))
