"""Unit tests for the contracts service (ContractValidator).

Covers schema validation (missing column, type change, nullability drift,
additive-column policy) against a real local Spark session.
"""

from __future__ import annotations

import os
import shutil
import uuid

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
)

from kimball.common.config import SourceConfig, SourceContractConfig
from kimball.orchestration.services.contracts import ContractValidator
from kimball.orchestration.validation import TestSeverity


def _is_remote_only() -> bool:
    try:
        from pyspark.rdd import is_remote_only

        return bool(is_remote_only())
    except ImportError:
        return False


def _has_java() -> bool:
    return shutil.which("java") is not None or bool(os.environ.get("JAVA_HOME"))


@pytest.fixture(scope="module")
def spark():
    if _is_remote_only():
        pytest.skip("Databricks Connect cannot create a local Spark session")
    if not _has_java():
        pytest.skip("Java is not available -- skipping Delta behavior tests")
    builder = SparkSession.builder.appName("KimballContractsBehavior")
    builder = builder.master("local[2]")
    builder = builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    )
    builder = builder.config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    builder = builder.config(
        "spark.sql.warehouse.dir", "spark-warehouse-contracts-behavior-tests"
    )
    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:
        pass
    session = builder.getOrCreate()
    from kimball.common.spark_session import set_active_spark

    set_active_spark(session)
    yield session


def _contract_with_schema(**overrides) -> SourceContractConfig:
    payload = {
        "id": "orders-contract",
        "version": "1.0.0",
        "schema": {
            "order_id": {"type": "int", "nullable": True},
            "customer_id": {"type": "int", "nullable": True},
            "amount": {"type": "double", "nullable": True},
        },
    }
    payload.update(overrides)
    return SourceContractConfig.model_validate(payload)


def _source_with_contract(
    spark, contract: SourceContractConfig, extra_column: str | None = None
) -> SourceConfig:
    table = f"ct_orders_{uuid.uuid4().hex[:6]}"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    fields = [
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
    ]
    rows: list[tuple] = [(1, 10, 5.0), (2, 20, None)]
    if extra_column:
        fields.append(StructField(extra_column, IntegerType(), True))
        rows = [(*row, 99 - i) for i, row in enumerate(rows)]
    df = spark.createDataFrame(rows, schema=StructType(fields))
    df.write.format("delta").mode("error").saveAsTable(table)
    return SourceConfig(name=table, alias="orders", contract=contract)


class TestValidateSourceSchema:
    def test_matching_schema_passes(self, spark: SparkSession):
        validator = ContractValidator(spark)
        source = _source_with_contract(spark, _contract_with_schema())
        findings = validator.validate_source(source)
        assert all(f.passed for f in findings), findings

    def test_not_null_column_passes_when_table_preserves_it(self, spark: SparkSession):
        table = f"ct_nn_{uuid.uuid4().hex[:6]}"
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        spark.sql(
            f"CREATE TABLE {table} "
            "(order_id INT NOT NULL, customer_id INT, amount DOUBLE) USING DELTA"
        )
        contract = _contract_with_schema(
            schema={
                "order_id": {"type": "int", "nullable": False},
                "customer_id": {"type": "int", "nullable": True},
                "amount": {"type": "double", "nullable": True},
            }
        )
        source = SourceConfig(name=table, alias="orders", contract=contract)
        findings = ContractValidator(spark).validate_source(source)
        assert all(f.passed for f in findings), findings

    def test_missing_column_fails(self, spark: SparkSession):
        from kimball.common.config import ContractColumnConfig

        contract = _contract_with_schema()
        contract.schema_["extra_required"] = ContractColumnConfig(type="string")
        source = _source_with_contract(spark, contract)
        findings = ContractValidator(spark).validate_source(source)
        assert any(
            f.check_name == "column:extra_required" and not f.passed for f in findings
        )

    def test_type_change_fails(self, spark: SparkSession):
        contract = _contract_with_schema(
            schema={
                "order_id": {"type": "int", "nullable": False},
                "customer_id": {"type": "int", "nullable": True},
                "amount": {"type": "string", "nullable": True},
            }
        )
        source = _source_with_contract(spark, contract)
        findings = ContractValidator(spark).validate_source(source)
        assert any(f.check_name == "type:amount" and not f.passed for f in findings)

    def test_nullability_drift_fails(self, spark: SparkSession):
        # The source column is nullable (Delta default via saveAsTable) while
        # the contract demands NOT NULL -> drift must be flagged.
        contract = _contract_with_schema(
            schema={
                "order_id": {"type": "int", "nullable": False},
                "customer_id": {"type": "int", "nullable": True},
                "amount": {"type": "double", "nullable": True},
            }
        )
        source = _source_with_contract(spark, contract)
        findings = ContractValidator(spark).validate_source(source)
        assert any(
            f.check_name == "nullable:order_id" and not f.passed for f in findings
        )

    def test_strict_rejects_additive_columns(self, spark: SparkSession):
        contract = _contract_with_schema(compatibility="strict")
        source = _source_with_contract(spark, contract, extra_column="bonus_col")
        findings = ContractValidator(spark).validate_source(source)
        assert any(
            f.check_name == "additive_columns" and f.severity == TestSeverity.ERROR
            for f in findings
        )

    def test_nullable_additions_warn_on_additive_columns(self, spark: SparkSession):
        contract = _contract_with_schema(compatibility="nullable_additions")
        source = _source_with_contract(spark, contract, extra_column="bonus_col")
        findings = ContractValidator(spark).validate_source(source)
        additive = [f for f in findings if f.check_name == "additive_columns"]
        assert additive and additive[0].severity == TestSeverity.WARN

    def test_missing_source_table_fails(self, spark: SparkSession):
        contract = _contract_with_schema()
        source = SourceConfig(name="never_exists_ct", alias="orders", contract=contract)
        findings = ContractValidator(spark).validate_source(source)
        assert len(findings) == 1
        assert findings[0].check_name == "source_exists"

    def test_no_contract_returns_empty(self, spark: SparkSession):
        source = SourceConfig(name="whatever", alias="w")
        assert ContractValidator(spark).validate_source(source) == []
