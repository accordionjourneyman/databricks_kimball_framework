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
    } | overrides
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
        ({"rule": "not_null"}, r"(?s)quality.0.not_null.column.*Field required"),
        (
            {"rule": "null_rate", "column": "amount"},
            r"(?s)quality.0.null_rate.max_ratio.*Field required",
        ),
        (
            {"rule": "accepted_values", "column": "status"},
            r"(?s)quality.0.accepted_values.values.*Field required",
        ),
        (
            {"rule": "expression"},
            r"(?s)quality.0.expression.expression.*Field required",
        ),
        ({"rule": "unique"}, "unique accepts either column or columns"),
        (
            {"rule": "unique", "column": "id", "columns": ["id"]},
            "unique accepts either column or columns",
        ),
    ],
)
def test_quality_rule_shape_fails_during_configuration(rule, message) -> None:
    with pytest.raises(ValueError, match=message):
        _contract(quality=[rule])


def test_approximate_unique_rules_keep_one_action_per_rule() -> None:
    contract = _contract(validation={"mode": "approximate"})

    plan = QualityValidationPlan.compile(contract.quality, contract.validation)

    assert plan.minimum_actions == 3
    limited = _contract(validation={"mode": "approximate", "max_actions": 2})
    with pytest.raises(ValueError, match="requires at least 3 Spark actions"):
        QualityValidationPlan.compile(limited.quality, limited.validation)
