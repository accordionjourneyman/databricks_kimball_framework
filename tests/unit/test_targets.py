from __future__ import annotations

import os
import warnings
from unittest.mock import MagicMock, patch

import pytest

from kimball.common.config import (
    ConfigLoader,
    TargetLoader,
    resolve_template_context,
)


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


def test_explicit_template_context_is_strict_and_target_values_win(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("APP_SCHEMA", "implicit_gold")
    target = TargetLoader(_targets(tmp_path)).load("dev")
    context = resolve_template_context(
        target,
        {
            "APP_SCHEMA": "explicit_gold",
            "target": {"gold_schema": "untrusted_override"},
        },
    )
    config_path = tmp_path / "explicit_context.yml"
    config_path.write_text(
        """table_name: {{ APP_SCHEMA }}.dim_customer
table_type: dimension
surrogate_key: customer_sk
natural_keys: [customer_id]
sources:
  - name: {{ target.silver_schema }}.customers
    alias: customer
""",
        encoding="utf-8",
    )

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        config = ConfigLoader(
            template_context=context,
            allow_implicit_environment=False,
        ).load_config(str(config_path))

    assert config.table_name == "explicit_gold.dim_customer"
    assert config.sources[0].name == "dev_silver.customers"
    assert not captured


@pytest.mark.parametrize(
    ("module_name", "class_name"),
    [
        ("kimball.orchestration.orchestrator", "Orchestrator"),
        ("kimball.streaming.orchestrator", "StreamingOrchestrator"),
    ],
)
def test_from_config_apis_forward_explicit_template_context(module_name, class_name):
    import importlib

    module = importlib.import_module(module_name)
    api = getattr(module, class_name)
    from kimball.common.config import TargetConfig

    target_config = TargetConfig(
        name="dev",
        catalog="workspace",
        silver_schema="dev_silver",
        gold_schema="dev_gold",
        etl_schema="dev_ops",
    )
    caller_context = {
        "application_schema": "app_gold",
        "target": {"gold_schema": "untrusted_override"},
    }
    expected_context = resolve_template_context(target_config, caller_context)
    loader = MagicMock()
    runtime = MagicMock()

    with (
        patch.object(module, "ConfigLoader", return_value=loader) as loader_class,
        patch.object(module.PipelineRuntime, "for_config", return_value=runtime),
    ):
        api.from_config(
            "pipeline.yml",
            target=target_config,
            template_context=caller_context,
            allow_implicit_environment=False,
        )

    loader_class.assert_called_once_with(
        template_context=expected_context,
        allow_implicit_environment=False,
    )
    loader.load_config.assert_called_once_with("pipeline.yml")


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


def test_loader_validates_unselected_target_entries(tmp_path):
    path = tmp_path / "kimball.targets.yml"
    path.write_text(
        """version: 1
targets:
  dev:
    catalog: workspace
    silver_schema: dev_silver
    gold_schema: dev_gold
    etl_schema: dev_ops
  prod:
    catalog: workspace
    silver_schema: prod_silver
    gold_schema: prod_gold
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"targets\.prod\.etl_schema"):
        TargetLoader(path).load("dev")


def test_target_fields_reject_whitespace_only_values(tmp_path):
    path = tmp_path / "kimball.targets.yml"
    path.write_text(
        """version: 1
targets:
  dev:
    catalog: '   '
    silver_schema: dev_silver
    gold_schema: dev_gold
    etl_schema: dev_ops
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"targets\.dev\.catalog"):
        TargetLoader(path).load("dev")


def test_implicit_environment_templates_warn_and_can_be_disabled(tmp_path, monkeypatch):
    import pytest

    from kimball.common.errors import ConfigurationValidationError

    monkeypatch.setenv("LEGACY_SCHEMA", "gold")
    config_path = tmp_path / "legacy_env.yml"
    config_path.write_text(
        """
table_name: {{ LEGACY_SCHEMA }}.dim_customer
table_type: dimension
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
sources:
  - name: silver.customers
""",
        encoding="utf-8",
    )

    with pytest.warns(UserWarning, match="implicit process environment"):
        ConfigLoader().load_config(str(config_path))
    with pytest.raises(
        ConfigurationValidationError, match="implicit process-environment"
    ):
        ConfigLoader(allow_implicit_environment=False).load_config(str(config_path))


def test_template_secret_values_are_rejected_without_echoing_them(
    tmp_path, monkeypatch
):
    import pytest

    from kimball.common.errors import ConfigurationValidationError

    monkeypatch.setenv("DB_PASSWORD", "never-print-this")
    config_path = tmp_path / "secret_template.yml"
    config_path.write_text(
        """
table_name: gold.{{ DB_PASSWORD }}
table_type: dimension
keys:
  surrogate_key: customer_sk
  natural_keys: [customer_id]
sources:
  - name: silver.customers
""",
        encoding="utf-8",
    )

    with pytest.raises(
        ConfigurationValidationError, match="secret environment"
    ) as exc_info:
        ConfigLoader().load_config(str(config_path))
    assert "never-print-this" not in str(exc_info.value)
