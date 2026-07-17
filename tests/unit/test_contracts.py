from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from kimball.common.config import SourceConfig, TableConfig
from kimball.observability.data_quality import (
    DataQualityEventSink,
    DataQualityEventWriter,
)
from kimball.orchestration.services.contracts import (
    ContractFinding,
    ContractValidationError,
    ContractValidator,
)
from kimball.orchestration.validation import TestSeverity as Severity


def _source(contract: dict) -> SourceConfig:
    return SourceConfig(
        name="silver.customer",
        alias="customer",
        cdc_strategy="cdf",
        primary_keys=["customer_id"],
        contract=contract,
    )


def test_contract_accepts_short_duration_syntax() -> None:
    source = _source(
        {
            "id": "customer",
            "version": "1.0.0",
            "schema": {"customer_id": {"type": "bigint", "nullable": False}},
            "freshness": {"max_age": "2h"},
            "temporal": {"event_time_column": "updated_at", "allowed_lateness": "24h"},
        }
    )
    assert source.contract.freshness.max_age == "2h"
    assert source.contract.temporal.allowed_lateness == "24h"


def test_contract_rejects_mismatched_cdc_keys() -> None:
    with pytest.raises(ValueError, match="must match contract.cdc.primary_key"):
        TableConfig(
            table_name="dim_customer",
            table_type="dimension",
            surrogate_key="customer_sk",
            natural_keys=["customer_id"],
            sources=[
                {
                    "name": "silver.customer",
                    "alias": "customer",
                    "primary_keys": ["customer_id"],
                    "contract": {
                        "id": "customer",
                        "version": "1.0.0",
                        "schema": {"customer_id": {"type": "bigint"}},
                        "cdc": {"primary_key": ["other_id"]},
                    },
                }
            ],
        )


def test_source_rejects_unsupported_timestamp_cdc_at_configuration_time() -> None:
    with pytest.raises(ValueError, match="not implemented"):
        SourceConfig(name="silver.customer", alias="customer", cdc_strategy="timestamp")


def test_table_config_rejects_unknown_settings() -> None:
    with pytest.raises(ValueError, match="schema_evoluton"):
        TableConfig(
            table_name="dim_customer",
            table_type="dimension",
            surrogate_key="customer_sk",
            natural_keys=["customer_id"],
            sources=[],
            schema_evoluton=True,
        )


def test_contract_validator_reports_missing_and_additive_columns() -> None:
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    spark.table.return_value.schema = StructType(
        [
            StructField("customer_id", LongType(), True),
            StructField("new_nullable_field", StringType(), True),
        ]
    )
    source = _source(
        {
            "id": "customer",
            "version": "1.0.0",
            "schema": {
                "customer_id": {"type": "bigint", "nullable": False},
                "updated_at": {"type": "timestamp", "nullable": False},
            },
        }
    )

    findings = ContractValidator(spark).validate_source(source)

    assert any(
        f.check_name == "nullable:customer_id" and not f.passed for f in findings
    )
    assert any(f.check_name == "column:updated_at" and not f.passed for f in findings)
    additive = next(f for f in findings if f.check_name == "additive_columns")
    assert additive.severity == Severity.WARN
    assert additive.passed


def test_contract_validator_blocks_only_error_findings() -> None:
    warning = ContractFinding(
        "contract_schema", "addition", Severity.WARN, False, "new field"
    )
    ContractValidator.raise_for_errors([warning])

    error = ContractFinding(
        "contract_schema", "missing", Severity.ERROR, False, "missing field"
    )
    with pytest.raises(ContractValidationError, match="missing"):
        ContractValidator.raise_for_errors([warning, error])


def test_event_id_is_stable_for_same_contract_finding() -> None:
    values = {
        "pipeline_table": "gold.dim_customer",
        "source_table": "silver.customer",
        "contract_id": "customer",
        "contract_version": "1.0.0",
        "source_version": 17,
        "category": "contract_schema",
        "check_name": "column:customer_id",
        "details": "customer_id is valid",
    }
    assert DataQualityEventWriter.event_id(values) == DataQualityEventWriter.event_id(
        values.copy()
    )


def test_event_sink_warn_mode_survives_writer_initialization_failure(caplog) -> None:
    class BrokenWriter:
        def __init__(self, *_args, **_kwargs):
            raise RuntimeError("secret storage detail")

    sink = DataQualityEventSink(
        object(),
        "ops",
        "events",
        failure_mode="warn",
        writer_type=BrokenWriter,
    )

    assert sink.write(anything="ignored") is None
    assert "RuntimeError" in caplog.text
    assert "secret storage detail" not in caplog.text


def test_event_sink_warn_mode_survives_append_failure(caplog) -> None:
    class BrokenWriter:
        def __init__(self, *_args, **_kwargs):
            pass

        def write(self, **_kwargs):
            raise RuntimeError("sensitive row detail")

    sink = DataQualityEventSink(
        object(),
        "ops",
        "events",
        failure_mode="warn",
        writer_type=BrokenWriter,
    )

    assert sink.write(anything="ignored") is None
    assert "RuntimeError" in caplog.text
    assert "sensitive row detail" not in caplog.text


def test_event_sink_error_mode_fails_closed() -> None:
    class BrokenWriter:
        def __init__(self, *_args, **_kwargs):
            raise RuntimeError("storage unavailable")

    with pytest.raises(RuntimeError, match="storage unavailable"):
        DataQualityEventSink(
            object(),
            "ops",
            "events",
            failure_mode="error",
            writer_type=BrokenWriter,
        )
