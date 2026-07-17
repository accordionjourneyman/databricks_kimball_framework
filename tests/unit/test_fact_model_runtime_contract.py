from __future__ import annotations

import pytest

from kimball.common.config import TableConfig
from kimball.orchestration.services.model_contracts import validate_fact_output_columns


def _fact(**overrides) -> TableConfig:
    values = {
        "table_name": "gold.fact_orders",
        "table_type": "fact",
        "merge_keys": ["order_id"],
        "grain": "one row per order",
        "fact_pattern": "accumulating_snapshot",
        "sources": [{"name": "silver.orders", "alias": "orders"}],
        "foreign_keys": [
            {
                "column": "order_date_sk",
                "references": "gold.dim_date",
                "dimension_key": "date_sk",
                "role_playing": True,
                "role": "order_date",
            }
        ],
        "degenerate_dimensions": ["order_number"],
        "measures": [
            {"name": "amount", "aggregation": "sum", "additivity": "additive"}
        ],
        "milestones": [
            {"name": "ordered", "column": "ordered_at", "order": 1},
            {"name": "shipped", "column": "shipped_at", "order": 2},
        ],
    }
    values.update(overrides)
    return TableConfig.model_validate(values)


def test_fact_runtime_contract_accepts_all_declared_columns() -> None:
    validate_fact_output_columns(
        _fact(),
        [
            "order_id",
            "order_date_sk",
            "order_number",
            "amount",
            "ordered_at",
            "shipped_at",
        ],
    )


@pytest.mark.parametrize(
    ("removed", "declaration"),
    [
        ("amount", "measure"),
        ("ordered_at", "milestone"),
        ("order_number", "degenerate dimension"),
        ("order_date_sk", "foreign key"),
    ],
)
def test_fact_runtime_contract_rejects_missing_declared_columns(
    removed: str, declaration: str
) -> None:
    columns = [
        "order_id",
        "order_date_sk",
        "order_number",
        "amount",
        "ordered_at",
        "shipped_at",
    ]
    columns.remove(removed)

    with pytest.raises(ValueError, match=declaration):
        validate_fact_output_columns(_fact(), columns)
