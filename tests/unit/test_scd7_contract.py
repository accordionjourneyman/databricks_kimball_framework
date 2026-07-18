import pytest
from pydantic import ValidationError

from kimball.common.config import TableConfig


def _source() -> list[dict[str, object]]:
    return [
        {
            "name": "silver.customers",
            "alias": "c",
            "cdc_strategy": "full",
        }
    ]


def test_scd7_requires_durable_key_and_effective_at() -> None:
    with pytest.raises(ValidationError, match="durable_key"):
        TableConfig(
            table_name="gold.dim_customer",
            table_type="dimension",
            scd_type=7,
            surrogate_key="customer_sk",
            natural_keys=["customer_id"],
            effective_at="updated_at",
            sources=_source(),
        )


def test_scd7_dimension_contract_is_accepted() -> None:
    config = TableConfig(
        table_name="gold.dim_customer",
        table_type="dimension",
        scd_type=7,
        surrogate_key="customer_sk",
        durable_key="customer_dk",
        natural_keys=["customer_id"],
        effective_at="updated_at",
        sources=_source(),
    )

    assert config.durable_key == "customer_dk"
    assert config.null_policy.mode == "kimball"


def test_type7_fact_lookup_requires_dual_keys_and_event_time() -> None:
    with pytest.raises(ValidationError, match="event_time"):
        TableConfig(
            table_name="gold.fact_sales",
            table_type="fact",
            merge_keys=["order_id"],
            sources=_source(),
            foreign_keys=[
                {
                    "column": "customer_sk",
                    "references": "gold.dim_customer",
                    "dimension_key": "customer_sk",
                    "relationship": "type7",
                    "durable_column": "customer_dk",
                    "durable_dimension_key": "customer_dk",
                    "lookup": {"source_columns": ["customer_id"]},
                }
            ],
        )


@pytest.mark.parametrize(
    "legacy_field,value",
    [
        ("identity_bridge", {"table": "xref", "join_on": "id", "target_column": "id"}),
        ("early_arriving_facts", []),
        ("early_arriving_dimensions", []),
    ],
)
def test_removed_legacy_contracts_fail_closed(legacy_field: str, value: object) -> None:
    payload = {
        "table_name": "gold.fact_sales",
        "table_type": "fact",
        "merge_keys": ["order_id"],
        "sources": _source(),
        legacy_field: value,
    }
    with pytest.raises(ValidationError, match=legacy_field):
        TableConfig(**payload)


def test_foreign_key_default_value_is_no_longer_accepted() -> None:
    with pytest.raises(ValidationError, match="default_value"):
        TableConfig(
            table_name="gold.fact_sales",
            table_type="fact",
            merge_keys=["order_id"],
            sources=_source(),
            foreign_keys=[{"column": "customer_sk", "default_value": -1}],
        )


def test_broker_column_mapping_lengths_must_match() -> None:
    with pytest.raises(ValidationError, match="dimension_columns"):
        TableConfig(
            table_name="gold.fact_sales",
            table_type="fact",
            merge_keys=["order_id"],
            sources=_source(),
            foreign_keys=[
                {
                    "column": "customer_sk",
                    "references": "gold.dim_customer",
                    "dimension_key": "customer_sk",
                    "lookup": {
                        "source_columns": ["supplier_id", "supplier_tenant"],
                        "dimension_columns": ["customer_id"],
                    },
                }
            ],
        )
