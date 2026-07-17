from __future__ import annotations

import pytest

from kimball.common.config import SourceContractConfig
from kimball.orchestration.services.contracts import QualityValidationPlan


def _contract(**overrides):
    values = {
        "id": "supplier.orders",
        "version": "1.0.0",
        "schema": {"order_id": {"type": "bigint"}},
        "quality": [
            {"name": "id_present", "rule": "not_null", "column": "order_id"},
            {
                "name": "status_valid",
                "rule": "accepted_values",
                "column": "status",
                "values": ["open", "closed"],
            },
            {
                "name": "amount_null_rate",
                "rule": "null_rate",
                "column": "amount",
                "max_ratio": 0.01,
            },
            {
                "name": "order_unique",
                "rule": "unique",
                "columns": ["order_id"],
            },
            {
                "name": "line_unique",
                "rule": "unique",
                "columns": ["order_id", "line_id"],
            },
        ],
    }
    values.update(overrides)
    return SourceContractConfig.model_validate(values)


def test_quality_plan_combines_compatible_scalar_rules() -> None:
    plan = QualityValidationPlan.compile(_contract().quality)

    assert [rule.name for rule in plan.scalar_rules] == [
        "id_present",
        "status_valid",
        "amount_null_rate",
    ]
    assert [tuple(rule.columns or [rule.column]) for rule in plan.unique_rules] == [
        ("order_id",),
        ("order_id", "line_id"),
    ]
    assert plan.minimum_actions == 3


def test_contract_validation_budget_rejects_too_many_required_actions() -> None:
    contract = _contract(validation={"max_actions": 2})

    with pytest.raises(ValueError, match="requires at least 3 Spark actions"):
        QualityValidationPlan.compile(contract.quality, contract.validation)


def test_sampled_validation_requires_a_fraction() -> None:
    with pytest.raises(ValueError, match="sample_fraction"):
        _contract(validation={"mode": "sampled"})


def test_validation_policy_has_bounded_failure_samples() -> None:
    with pytest.raises(ValueError, match="less than or equal to 100"):
        _contract(validation={"max_failure_samples": 101})


@pytest.mark.parametrize(
    ("rule", "message"),
    [
        ({"rule": "not_null"}, "not_null requires column"),
        ({"rule": "null_rate", "column": "amount"}, "null_rate requires max_ratio"),
        (
            {"rule": "accepted_values", "column": "status"},
            "accepted_values requires values",
        ),
        ({"rule": "expression"}, "expression requires expression"),
        ({"rule": "unique"}, "unique requires column or columns"),
        (
            {"rule": "unique", "column": "id", "columns": ["id"]},
            "unique accepts either column or columns",
        ),
    ],
)
def test_quality_rule_shape_fails_during_configuration(rule, message) -> None:
    with pytest.raises(ValueError, match=message):
        _contract(quality=[rule])
