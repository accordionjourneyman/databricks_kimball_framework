from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from pydantic import ValidationError

from kimball.common.config import ConfigLoader, TableConfig, TargetLoader
from tools.generate_schema import generate_pipeline_schema


@pytest.fixture(scope="module")
def schema():
    return generate_pipeline_schema()


def _valid_legacy_dimension() -> dict:
    return {
        "table_name": "gold.dim_customer",
        "table_type": "dimension",
        "keys": {"surrogate_key": "customer_sk", "natural_keys": ["customer_id"]},
        "sources": [{"name": "silver.customers"}],
    }


def test_legacy_keys_and_derived_alias_are_valid_in_both_contracts(schema):
    payload = _valid_legacy_dimension()
    independent_schema = Draft202012Validator(schema)

    independent_schema.validate(payload)
    config = TableConfig.model_validate(payload)

    assert config.surrogate_key == "customer_sk"
    assert config.natural_keys == ["customer_id"]
    assert config.sources[0].alias == "customers"


def test_before_validators_do_not_mutate_caller_owned_mappings():
    table_payload = _valid_legacy_dimension()
    source_payload = {"name": "silver.customers"}
    table_before = json.loads(json.dumps(table_payload))
    source_before = dict(source_payload)

    TableConfig.model_validate(table_payload)

    from kimball.common.config import SourceConfig

    SourceConfig.model_validate(source_payload)
    assert table_payload == table_before
    assert source_payload == source_before


@pytest.mark.parametrize(
    ("mutate", "field_path"),
    [
        (lambda p: p["keys"].update({"natural_key": "id"}), "keys"),
        (lambda p: p["sources"][0].update({"alias": "   "}), "alias"),
        (
            lambda p: p["sources"][0].update({"cdc_strategy": "timestamp"}),
            "cdc_strategy",
        ),
    ],
)
def test_invalid_inputs_fail_independently_in_schema_and_model(
    schema, mutate, field_path
):
    payload = _valid_legacy_dimension()
    mutate(payload)

    with pytest.raises(JsonSchemaValidationError):
        Draft202012Validator(schema).validate(payload)
    with pytest.raises(ValidationError, match=field_path):
        TableConfig.model_validate(payload)


def test_duration_constraints_are_shared_by_model_and_schema(schema):
    payload = _valid_legacy_dimension()
    payload["sources"][0]["contract"] = {
        "id": "customer-contract",
        "version": "1.0.0",
        "schema": {},
        "freshness": {"max_age": "soon"},
    }

    with pytest.raises(JsonSchemaValidationError):
        Draft202012Validator(schema).validate(payload)
    with pytest.raises(ValidationError, match="max_age"):
        TableConfig.model_validate(payload)


def test_every_documented_example_config_validates_against_both_boundaries():
    repository = Path(__file__).resolve().parents[2]
    target = TargetLoader(repository / "kimball.targets.yml").load("dev")
    loader = ConfigLoader(env_vars={}, template_context=target.template_context())
    independent_schema = Draft202012Validator(generate_pipeline_schema())

    paths = sorted((repository / "examples/configs").glob("*.yml"))
    assert paths
    from jinja2 import Environment, StrictUndefined

    environment = Environment(undefined=StrictUndefined)
    for path in paths:
        source_text = path.read_text(encoding="utf-8")
        rendered_text = environment.from_string(source_text).render(
            target.template_context()
        )
        rendered_mapping = yaml.safe_load(rendered_text)
        independent_schema.validate(rendered_mapping)
        loaded = loader.load_config(str(path))
        assert loaded.table_name == rendered_mapping["table_name"]


def test_checked_in_schema_matches_fresh_generation(schema):
    repository = Path(__file__).resolve().parents[2]
    checked_in = json.loads(
        (repository / "schemas/pipeline-config.schema.json").read_text(encoding="utf-8")
    )
    assert checked_in == schema
