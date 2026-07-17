"""TDD coverage for declarative fact-pattern contracts."""

import pytest

from kimball.common.config import TableConfig


def _fact(**overrides):
    data = {
        "table_name": "gold.fact_test",
        "table_type": "fact",
        "merge_keys": ["business_id"],
        "sources": [{"name": "silver.source", "alias": "s"}],
        **overrides,
    }
    return TableConfig(**data)


def test_legacy_fact_remains_compatible_without_contract():
    assert _fact().fact_pattern is None


def test_pattern_contract_requires_grain():
    with pytest.raises(ValueError, match="declared grain"):
        _fact(fact_pattern="transaction")


def test_periodic_snapshot_requires_snapshot_period():
    with pytest.raises(ValueError, match="snapshot_period"):
        _fact(grain="one product per day", fact_pattern="periodic_snapshot")


def test_semi_additive_measure_requires_non_additive_dimension():
    with pytest.raises(ValueError, match="semi_additive"):
        _fact(
            grain="one product per day",
            fact_pattern="periodic_snapshot",
            snapshot_period="day",
            measures=[
                {
                    "name": "on_hand_qty",
                    "aggregation": "sum",
                    "additivity": "semi_additive",
                }
            ],
        )


def test_accumulating_snapshot_requires_ordered_milestones():
    with pytest.raises(ValueError, match="at least two milestones"):
        _fact(grain="one order", fact_pattern="accumulating_snapshot")


def test_role_playing_fk_requires_role():
    with pytest.raises(ValueError, match="require role"):
        _fact(
            grain="one order",
            foreign_keys=[{"column": "order_date_sk", "role_playing": True}],
        )


def test_role_playing_fk_requires_a_physical_dimension_reference():
    with pytest.raises(ValueError, match="references"):
        _fact(
            grain="one order",
            foreign_keys=[
                {
                    "column": "order_date_sk",
                    "role_playing": True,
                    "role": "order_date",
                }
            ],
        )


def test_fact_declarations_reject_duplicate_semantic_columns():
    with pytest.raises(ValueError, match="measure names"):
        _fact(
            grain="one order",
            measures=[
                {"name": "amount", "aggregation": "sum", "additivity": "additive"},
                {"name": "amount", "aggregation": "avg", "additivity": "non_additive"},
            ],
        )


def test_valid_periodic_snapshot_contract():
    config = _fact(
        grain="one product per day",
        fact_pattern="periodic_snapshot",
        snapshot_period="day",
        measures=[
            {
                "name": "on_hand_qty",
                "aggregation": "sum",
                "additivity": "semi_additive",
                "non_additive_dimensions": ["snapshot_date"],
            }
        ],
    )
    assert config.measures[0].non_additive_dimensions == ["snapshot_date"]
