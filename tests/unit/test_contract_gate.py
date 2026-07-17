from __future__ import annotations

from copy import deepcopy

from kimball.contracts.gate import evaluate_contract_changes
from kimball.contracts.odcs import ODCSContractLoader
from tests.unit.test_odcs_contracts import _contract


def _load(document):
    return ODCSContractLoader().load_mapping(document)


def test_gate_accepts_versioned_nullable_addition():
    old = _load(_contract("1.0.0"))
    new_doc = deepcopy(_contract("1.1.0"))
    new_doc["schema"][0]["properties"].append(
        {"name": "segment", "logicalType": "string", "required": False}
    )
    new = _load(new_doc)

    result = evaluate_contract_changes([old], [old, new])

    assert result.allowed is True
    assert result.errors == ()
    assert result.reports[0].allowed is True


def test_gate_rejects_breaking_minor_version():
    old = _load(_contract("1.0.0"))
    new_doc = deepcopy(_contract("1.1.0"))
    new_doc["schema"][0]["properties"].pop()
    new = _load(new_doc)

    result = evaluate_contract_changes([old], [old, new])

    assert result.allowed is False
    assert "not compatible" in result.errors[0]


def test_gate_rejects_contract_removal():
    old = _load(_contract("1.0.0"))

    result = evaluate_contract_changes([old], [])

    assert result.allowed is False
    assert result.errors == ("Contract 'customer-contract' was removed",)


def test_gate_rejects_conflicting_duplicate_version():
    first = _load(_contract("1.0.0"))
    changed_doc = deepcopy(_contract("1.0.0"))
    changed_doc["description"] = {"purpose": "different"}
    conflicting = _load(changed_doc)

    result = evaluate_contract_changes([], [first, conflicting])

    assert result.allowed is False
    assert "conflicting definitions" in result.errors[0]
