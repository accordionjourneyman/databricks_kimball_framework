"""Executable upstream data-contract checks.

The checks in this module are intentionally independent of a target table.  They
can therefore be used both by an ETL run and by the read-only contract monitor.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from kimball.common.config import ContractQualityRule, SourceConfig
from kimball.orchestration.validation import TestSeverity


@dataclass
class ContractFinding:
    category: str
    check_name: str
    severity: TestSeverity
    passed: bool
    details: str
    observed_value: str | None = None
    expected_value: str | None = None
    failed_rows: int | None = None
    total_rows: int | None = None
    samples: list[dict[str, Any]] = field(default_factory=list)


class ContractValidationError(ValueError):
    """Raised when one or more ERROR contract checks fail."""


def _spark_type_name(field: Any) -> str:
    return field.dataType.simpleString().lower()


class ContractValidator:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def validate_source(self, source: SourceConfig) -> list[ContractFinding]:
        """Validate the live source shape and CDF requirements, without reading rows."""
        contract = source.contract
        if contract is None:
            return []
        findings: list[ContractFinding] = []
        if not self.spark.catalog.tableExists(source.name):
            return [
                ContractFinding(
                    "contract_schema", "source_exists", TestSeverity.ERROR, False,
                    f"Contracted source table does not exist: {source.name}",
                )
            ]
        schema = self.spark.table(source.name).schema
        fields = {f.name: f for f in schema.fields}
        for name, expected in contract.schema_.items():
            field = fields.get(name)
            if field is None:
                findings.append(ContractFinding(
                    "contract_schema", f"column:{name}", TestSeverity.ERROR, False,
                    f"Required contract column '{name}' is missing", expected_value=expected.type,
                ))
                continue
            actual_type = _spark_type_name(field)
            if actual_type != expected.type.lower():
                findings.append(ContractFinding(
                    "contract_schema", f"type:{name}", TestSeverity.ERROR, False,
                    f"Column '{name}' type changed", observed_value=actual_type,
                    expected_value=expected.type,
                ))
            elif expected.nullable is False and field.nullable:
                findings.append(ContractFinding(
                    "contract_schema", f"nullable:{name}", TestSeverity.ERROR, False,
                    f"Column '{name}' became nullable", observed_value="nullable",
                    expected_value="not null",
                ))
            else:
                findings.append(ContractFinding(
                    "contract_schema", f"column:{name}", TestSeverity.ERROR, True,
                    f"Column '{name}' matches contract",
                ))
        undeclared = sorted(set(fields) - set(contract.schema_))
        if undeclared:
            severity = TestSeverity.WARN if contract.compatibility == "nullable_additions" else TestSeverity.ERROR
            nullable = all(fields[name].nullable for name in undeclared)
            findings.append(ContractFinding(
                "contract_schema", "additive_columns", severity,
                contract.compatibility == "nullable_additions" and nullable,
                f"Source has undeclared columns: {', '.join(undeclared)}",
                observed_value=json.dumps(undeclared), expected_value="nullable additions only",
            ))
        if contract.cdc and contract.cdc.required and source.cdc_strategy != "cdf":
            findings.append(ContractFinding(
                "contract_cdc", "cdf_required", TestSeverity.ERROR, False,
                "Contract requires CDF but source is not configured for cdf",
            ))
        if contract.cdc and contract.cdc.primary_key:
            source_keys = source.primary_keys or []
            findings.append(ContractFinding(
                "contract_cdc", "primary_key", TestSeverity.ERROR,
                source_keys == contract.cdc.primary_key,
                "Source primary keys match contract" if source_keys == contract.cdc.primary_key else "Source primary keys differ from contract",
                observed_value=json.dumps(source_keys), expected_value=json.dumps(contract.cdc.primary_key),
            ))
        if contract.temporal and contract.temporal.event_time_column not in fields:
            findings.append(ContractFinding(
                "contract_schema", "event_time_column", TestSeverity.ERROR, False,
                f"Temporal event-time column '{contract.temporal.event_time_column}' is missing",
            ))
        return findings

    def validate_data(self, df: DataFrame, source: SourceConfig) -> list[ContractFinding]:
        """Run explicitly declared supplier quality rules against an input batch."""
        contract = source.contract
        if contract is None:
            return []
        findings: list[ContractFinding] = []
        for rule in contract.quality:
            findings.append(self._run_rule(df, rule))
        return findings

    def validate_temporal(self, df: DataFrame, source: SourceConfig) -> list[ContractFinding]:
        """Inspect raw CDF rows for late business time and commit-order inversions."""
        temporal = source.contract.temporal if source.contract else None
        if temporal is None:
            return []
        if temporal.event_time_column not in df.columns:
            return [ContractFinding("late_event", "event_time_available", TestSeverity.ERROR, False,
                                    f"Missing event-time column '{temporal.event_time_column}'")]
        match = re.match(r"^(\d+)\s*(s|m|h|d|seconds?|minutes?|hours?|days?)$", temporal.allowed_lateness, re.I)
        amount, unit = (int(match.group(1)), match.group(2).lower()) if match else (0, "h")
        seconds = amount * ({"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit[0], 3600))
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=seconds)
        event_time = F.col(temporal.event_time_column).cast("timestamp")
        late = df.filter(event_time < F.lit(cutoff))
        late_count = late.count()
        findings = [ContractFinding(
            "late_event", "allowed_lateness", TestSeverity(temporal.late_event_severity), late_count == 0,
            "No late events" if late_count == 0 else f"Found {late_count} events older than allowed lateness",
            failed_rows=late_count, samples=[r.asDict() for r in late.limit(5).collect()],
        )]
        keys = (source.contract.cdc.primary_key if source.contract and source.contract.cdc else None) or source.primary_keys
        if keys and "_commit_version" in df.columns:
            w = Window.partitionBy(*keys).orderBy(F.col("_commit_version"))
            inversions = df.withColumn("_previous_event_time", F.lag(event_time).over(w)).filter(
                event_time < F.col("_previous_event_time")
            )
            count = inversions.count()
            findings.append(ContractFinding(
                "out_of_order_event", "cdf_commit_order", TestSeverity(temporal.out_of_order_severity), count == 0,
                "CDF event times are ordered" if count == 0 else f"Found {count} out-of-order CDF events",
                failed_rows=count, samples=[r.asDict() for r in inversions.drop("_previous_event_time").limit(5).collect()],
            ))
        elif "_commit_version" not in df.columns:
            findings.append(ContractFinding(
                "out_of_order_event", "cdf_capability", TestSeverity.WARN, False,
                "Cannot establish event ordering without CDF commit metadata",
            ))
        return findings

    def _run_rule(self, df: DataFrame, rule: ContractQualityRule) -> ContractFinding:
        name = rule.name or f"contract_{rule.rule}_{rule.column or '_'.join(rule.columns or [])}"
        severity = TestSeverity(rule.severity)
        if rule.rule == "not_null":
            bad = df.filter(F.col(rule.column).isNull())
        elif rule.rule == "unique":
            columns = rule.columns or [rule.column]
            bad = df.groupBy(*columns).count().filter("count > 1")
        elif rule.rule == "null_rate":
            total = df.count()
            nulls = df.filter(F.col(rule.column).isNull()).count()
            ratio = nulls / total if total else 0.0
            return ContractFinding(
                "source_quality", name, severity, ratio <= (rule.max_ratio or 0),
                f"Null ratio for '{rule.column}' is {ratio:.6f}", observed_value=str(ratio),
                expected_value=str(rule.max_ratio), failed_rows=nulls, total_rows=total,
            )
        elif rule.rule == "accepted_values":
            bad = df.filter(~F.col(rule.column).isin(rule.values or []))
        else:
            bad = df.filter(f"NOT ({rule.expression})")
        count = bad.count()
        return ContractFinding(
            "source_quality", name, severity, count == 0,
            "Contract quality rule passed" if count == 0 else f"Contract quality rule found {count} invalid rows",
            failed_rows=count, samples=[r.asDict() for r in bad.limit(5).collect()],
        )

    @staticmethod
    def raise_for_errors(findings: list[ContractFinding]) -> None:
        failures = [f for f in findings if not f.passed and f.severity == TestSeverity.ERROR]
        if failures:
            raise ContractValidationError("; ".join(f"{f.check_name}: {f.details}" for f in failures))
