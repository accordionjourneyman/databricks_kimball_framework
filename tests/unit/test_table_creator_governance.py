"""Regression tests for configured table governance."""

from unittest.mock import MagicMock, patch

from kimball.common.runtime_policy import RuntimePolicy
from kimball.processing.table_creator import TableCreator


def test_configured_row_filter_is_applied():
    creator = TableCreator()
    creator._apply_row_filter = MagicMock()

    creator._apply_governance(
        "catalog.schema.table",
        {
            "row_filter": {
                "function_name": "region_filter",
                "function_body": "region_param = 'US'",
                "column": "country",
            }
        },
    )

    creator._apply_row_filter.assert_called_once()


@patch("kimball.processing.table_creator.get_spark")
@patch("kimball.processing.table_creator.get_runtime_policy")
@patch("kimball.governance.abac.ABACManager")
def test_configured_abac_policy_is_created(
    mock_manager, mock_runtime_policy, mock_get_spark
):
    mock_runtime_policy.return_value = RuntimePolicy(is_databricks=True)
    manager = mock_manager.return_value

    TableCreator()._apply_governance(
        "catalog.schema.table",
        {
            "abac_policies": [
                {
                    "policy_name": "hide_eu",
                    "policy_type": "row_filter",
                    "udf_name": "non_eu_region",
                    "udf_body": "region_param <> 'eu'",
                    "target_groups": ["analysts"],
                    "match_tag": "region",
                }
            ]
        },
    )

    mock_manager.assert_called_once_with(
        mock_get_spark(), "catalog", "schema", table_name="catalog.schema.table"
    )
    manager.create_policy.assert_called_once()


@patch("kimball.processing.table_creator.get_spark")
def test_constraints_use_a_single_ddl_executor(mock_get_spark):
    TableCreator().apply_delta_constraints(
        "catalog.schema.fact_sales",
        {
            "table_type": "fact",
            "surrogate_key": "sale_sk",
            "foreign_keys": [
                {"column": "customer_sk", "durable_column": "customer_dk"}
            ],
            "constraints": [
                {"name": "amount_nonnegative", "expression": "amount >= 0"}
            ],
            "declare_constraints": False,
        },
    )

    statements = [
        call.args[0] for call in mock_get_spark.return_value.sql.call_args_list
    ]
    assert statements == [
        "ALTER TABLE `catalog`.`schema`.`fact_sales` ALTER COLUMN `sale_sk` SET NOT NULL",
        "ALTER TABLE `catalog`.`schema`.`fact_sales` ADD CONSTRAINT `fk_customer_sk_not_null` CHECK (`customer_sk` IS NOT NULL)",
        "ALTER TABLE `catalog`.`schema`.`fact_sales` ADD CONSTRAINT `fk_customer_dk_not_null` CHECK (`customer_dk` IS NOT NULL)",
        "ALTER TABLE `catalog`.`schema`.`fact_sales` ADD CONSTRAINT `amount_nonnegative` CHECK (amount >= 0)",
    ]
