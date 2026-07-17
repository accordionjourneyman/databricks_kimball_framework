from __future__ import annotations

from copy import deepcopy

import pytest
from jsonschema import ValidationError as JsonSchemaValidationError
from pydantic import ValidationError

from kimball.common.config import ConfigLoader, SourceConfig, TableConfig
from kimball.contracts.compatibility import check_compatibility
from kimball.contracts.odcs import (
    ODCSContractLoader,
    adapt_odcs_to_source_contract,
    validate_contract_reference,
)


def _contract(version: str = "1.0.0") -> dict:
    return {
        "apiVersion": "v3.1.0",
        "kind": "DataContract",
        "id": "customer-contract",
        "name": "customers",
        "version": version,
        "status": "active",
        "schema": [
            {
                "name": "customers",
                "logicalType": "object",
                "properties": [
                    {
                        "name": "customer_id",
                        "logicalType": "integer",
                        "physicalType": "BIGINT",
                        "required": True,
                        "primaryKey": True,
                        "primaryKeyPosition": 1,
                        "quality": [
                            {
                                "id": "customer_id_null",
                                "metric": "nullValues",
                                "mustBe": 0,
                            }
                        ],
                    },
                    {
                        "name": "email",
                        "logicalType": "string",
                        "physicalType": "STRING",
                        "required": False,
                    },
                ],
                "quality": [
                    {
                        "id": "customer_pk_unique",
                        "metric": "duplicateValues",
                        "mustBe": 0,
                        "arguments": {"properties": ["customer_id"]},
                    }
                ],
            }
        ],
    }


def test_odcs_loader_validates_pinned_schema_and_adapts_runtime_contract():
    contract = ODCSContractLoader().load_mapping(_contract())
    runtime = adapt_odcs_to_source_contract(contract, object_name="customers")

    assert contract.api_version == "v3.1.0"
    assert runtime.id == "customer-contract"
    assert runtime.version == "1.0.0"
    assert runtime.schema_["customer_id"].type == "BIGINT"
    assert runtime.schema_["email"].nullable is True
    assert runtime.cdc.primary_key == ["customer_id"]
    assert {
        (rule.rule, tuple(rule.columns or []), rule.column) for rule in runtime.quality
    } == {
        ("not_null", (), "customer_id"),
        ("unique", ("customer_id",), None),
    }


def test_odcs_loader_rejects_unknown_root_field():
    document = _contract()
    document["versoin"] = "typo"

    with pytest.raises(JsonSchemaValidationError, match="Additional properties"):
        ODCSContractLoader().load_mapping(document)


def test_contract_reference_must_pin_id_and_version():
    contract = ODCSContractLoader().load_mapping(_contract())

    validate_contract_reference("contracts/customer-contract/1.0.0.odcs.yaml", contract)
    with pytest.raises(ValueError, match="must end with"):
        validate_contract_reference(
            "contracts/customer-contract/latest.odcs.yaml", contract
        )


def test_source_contract_ref_and_inline_contract_are_mutually_exclusive():
    with pytest.raises(ValidationError, match="mutually exclusive"):
        SourceConfig(
            name="silver.customers",
            alias="customers",
            contract_ref="contracts/customers/1.0.0.odcs.yaml",
            contract={
                "id": "customers",
                "version": "1.0.0",
                "schema": {"customer_id": {"type": "BIGINT"}},
            },
        )


def test_nullable_column_addition_is_backward_compatible():
    previous = ODCSContractLoader().load_mapping(_contract())
    current_doc = deepcopy(_contract("1.1.0"))
    current_doc["schema"][0]["properties"].append(
        {"name": "segment", "logicalType": "string", "required": False}
    )
    current = ODCSContractLoader().load_mapping(current_doc)

    report = check_compatibility(previous, current)

    assert report.allowed is True
    assert report.breaking_changes == ()
    assert report.changes[0].code == "COLUMN_ADDED_NULLABLE"


def test_required_column_addition_requires_major_version():
    previous = ODCSContractLoader().load_mapping(_contract())
    current_doc = deepcopy(_contract("1.1.0"))
    current_doc["schema"][0]["properties"].append(
        {"name": "country", "logicalType": "string", "required": True}
    )
    current = ODCSContractLoader().load_mapping(current_doc)

    report = check_compatibility(previous, current)

    assert report.allowed is False
    assert report.breaking_changes[0].code == "REQUIRED_COLUMN_ADDED"


def test_major_version_allows_publish_but_preserves_breaking_report():
    previous = ODCSContractLoader().load_mapping(_contract())
    current_doc = deepcopy(_contract("2.0.0"))
    current_doc["schema"][0]["properties"] = [current_doc["schema"][0]["properties"][0]]
    current = ODCSContractLoader().load_mapping(current_doc)

    report = check_compatibility(previous, current)

    assert report.allowed is True
    assert report.requires_consumer_migration is True
    assert report.breaking_changes[0].code == "COLUMN_REMOVED"


def test_contract_version_must_increase_when_document_changes():
    previous = ODCSContractLoader().load_mapping(_contract())
    current_doc = deepcopy(_contract())
    current_doc["schema"][0]["properties"][1]["description"] = "New docs"
    current = ODCSContractLoader().load_mapping(current_doc)

    report = check_compatibility(previous, current)

    assert report.allowed is False
    assert report.version_errors == ("Changed contract must increment version",)


def test_config_loader_resolves_contract_ref_for_runtime_checks():
    contract = ODCSContractLoader().load_mapping(_contract())
    config = TableConfig(
        table_name="gold.dim_customer",
        table_type="dimension",
        surrogate_key="customer_sk",
        natural_keys=["customer_id"],
        sources=[
            SourceConfig(
                name="silver.customers",
                alias="customers",
                contract_ref="../contracts/customer-contract/1.0.0.odcs.yaml",
            )
        ],
    )

    with pytest.MonkeyPatch.context() as monkeypatch:

        def load_file(_self, path):
            return contract

        monkeypatch.setattr(ODCSContractLoader, "load_file", load_file)
        resolved = ConfigLoader().resolve_contract_refs(config, "configs/customer.yml")

    assert resolved.sources[0].contract.id == "customer-contract"
    assert resolved.sources[0].contract.schema_["email"].nullable is True
