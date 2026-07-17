from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from kimball.contracts.odcs import ODCSContractLoader
from kimball.contracts.registry import DeltaContractRegistry
from tests.unit.test_odcs_contracts import _contract


def test_registry_creates_version_consumer_and_manifest_tables():
    spark = MagicMock()
    registry = DeltaContractRegistry(spark, "ops")

    registry.ensure_tables()

    statements = "\n".join(call.args[0] for call in spark.sql.call_args_list)
    assert "etl_contract_versions" in statements
    assert "etl_contract_consumers" in statements
    assert "etl_pipeline_manifests" in statements


def test_publish_contract_is_append_only_and_records_digest():
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = []
    registry = DeltaContractRegistry(spark, "ops")
    contract = ODCSContractLoader().load_mapping(_contract())

    created = registry.publish_contract(
        contract,
        source_path="contracts/customer-contract/1.0.0.odcs.yaml",
        published_by="ci",
    )

    assert created is True
    insert = spark.sql.call_args_list[-1].args[0]
    assert contract.digest in insert
    assert "customer-contract" in insert
    assert "current_timestamp()" in insert


def test_publish_contract_rejects_mutating_existing_version():
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = [{"spec_digest": "different"}]
    registry = DeltaContractRegistry(spark, "ops")
    contract = ODCSContractLoader().load_mapping(_contract())

    with pytest.raises(ValueError, match="immutable"):
        registry.publish_contract(
            contract, source_path="contract.yml", published_by="ci"
        )


def test_registry_records_exact_consumer_pin():
    spark = MagicMock()
    registry = DeltaContractRegistry(spark, "ops")

    registry.register_consumer(
        pipeline_table="gold.dim_customer",
        source_table="silver.customers",
        contract_id="customer-contract",
        contract_version="1.0.0",
        config_digest="abc123",
    )

    statement = spark.sql.call_args_list[-1].args[0]
    assert "MERGE INTO `ops`.`etl_contract_consumers`" in statement
    assert "1.0.0" in statement
