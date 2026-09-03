"""Executable upstream data-contract checks.

The checks in this module are intentionally independent of a target table.  They
can therefore be used both by an ETL run and by the read-only contract monitor.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from kimball.common.config import (
    ContractQualityRule,
    ContractValidationPolicy,
    SourceConfig,
    SourceContractConfig,
)
from kimball.observability.temporal_state import TemporalStateStore
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


@dataclass(frozen=True)
class QualityValidationPlan:
    """Physical action plan for a contract quality suite."""

    scalar_rules: tuple[ContractQualityRule, ...]
    unique_rules: tuple[ContractQualityRule, ...]
    minimum_actions: int
    policy: ContractValidationPolicy

    @classmethod
    def compile(
        cls,
        rules: list[ContractQualityRule],
        policy: ContractValidationPolicy | None = None,
    ) -> QualityValidationPlan:
        execution = policy or ContractValidationPolicy()
        scalar = tuple(rule for rule in rules if rule.rule != "unique")
        unique = tuple(rule for rule in rules if rule.rule == "unique")
        actions = (1 if scalar else 0) + len(unique)
        if execution.max_actions is not None and actions > execution.max_actions:
            raise ValueError(
                f"Contract quality plan requires at least {actions} Spark actions, "
                f"exceeding validation.max_actions={execution.max_actions}"
            )
        return cls(scalar, unique, actions, execution)


def _spark_type_name(field: Any) -> str:
    result: str = field.dataType.simpleString().lower()
    return result


class ContractValidator:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.last_metrics: dict[str, Any] = {}

    def validate_source(self, source: SourceConfig) -> list[ContractFinding]:
        """Validate the live source shape and CDF requirements, without reading rows.

        Phases (ADR-004 grade-A pass): existence, per-column schema
        comparison, additive-column policy, CDF requirements, primary-key
        agreement, and the temporal event-time column.
        """
        contract = source.contract
        if contract is None:
            return []
        if not self.spark.catalog.tableExists(source.name):
            return [
                ContractFinding(
                    "contract_schema",
                    "source_exists",
                    TestSeverity.ERROR,
                    False,
                    f"Contracted source table does not exist: {source.name}",
                )
            ]
        schema = self.spark.table(source.name).schema
        fields = {f.name: f for f in schema.fields}
        findings: list[ContractFinding] = []
        for name, expected in contract.schema_.items():
            findings.append(self._compare_column(source.name, fields, name, expected))
        if additive := self._additive_columns_finding(contract, fields):
            findings.append(additive)
        findings.extend(self._cdf_requirement_findings(source, contract, fields))
        return findings

    def _compare_column(
        self, source_name: str, fields: dict, name: str, expected
    ) -> ContractFinding:
        """Compare one declared column: presence, type, nullability."""
        field = fields.get(name)
        if field is None:
            return ContractFinding(
                "contract_schema",
                f"column:{name}",
                TestSeverity.ERROR,
                False,
                f"Required contract column '{name}' is missing",
                expected_value=expected.type,
            )
        actual_type = _spark_type_name(field)
        if actual_type != expected.type.lower():
            return ContractFinding(
                "contract_schema",
                f"type:{name}",
                TestSeverity.ERROR,
                False,
                f"Column '{name}' type changed",
                observed_value=actual_type,
                expected_value=expected.type,
            )
        if expected.nullable is False and field.nullable:
            return ContractFinding(
                "contract_schema",
                f"nullable:{name}",
                TestSeverity.ERROR,
                False,
                f"Column '{name}' became nullable",
                observed_value="nullable",
                expected_value="not null",
            )
        return ContractFinding(
            "contract_schema",
            f"column:{name}",
            TestSeverity.ERROR,
            True,
            f"Column '{name}' matches contract",
        )

    @staticmethod
    def _additive_columns_finding(contract, fields: dict) -> ContractFinding | None:
        """Flag undeclared columns under the contract's compatibility policy."""
        if not (undeclared := sorted(set(fields) - set(contract.schema_))):
            return None
        severity = (
            TestSeverity.WARN
            if contract.compatibility == "nullable_additions"
            else TestSeverity.ERROR
        )
        nullable = all(fields[name].nullable for name in undeclared)
        return ContractFinding(
            "contract_schema",
            "additive_columns",
            severity,
            contract.compatibility == "nullable_additions" and nullable,
            f"Source has undeclared columns: {', '.join(undeclared)}",
            observed_value=json.dumps(undeclared),
            expected_value="nullable additions only",
        )

    def _cdf_requirement_findings(
        self, source: SourceConfig, contract, fields: dict
    ) -> list[ContractFinding]:
        """CDF-required, primary-key agreement, and event-time presence."""
        findings: list[ContractFinding] = []
        if contract.cdc and contract.cdc.required and source.cdc_strategy != "cdf":
            findings.append(
                ContractFinding(
                    "contract_cdc",
                    "cdf_required",
                    TestSeverity.ERROR,
                    False,
                    "Contract requires CDF but source is not configured for cdf",
                )
            )
        if contract.cdc and contract.cdc.primary_key:
            source_keys = source.primary_keys or []
            findings.append(
                ContractFinding(
                    "contract_cdc",
                    "primary_key",
                    TestSeverity.ERROR,
                    source_keys == contract.cdc.primary_key,
                    "Source primary keys match contract"
                    if source_keys == contract.cdc.primary_key
                    else "Source primary keys differ from contract",
                    observed_value=json.dumps(source_keys),
                    expected_value=json.dumps(contract.cdc.primary_key),
                )
            )
        if contract.temporal and contract.temporal.event_time_column not in fields:
            findings.append(
                ContractFinding(
                    "contract_schema",
                    "event_time_column",
                    TestSeverity.ERROR,
                    False,
                    f"Temporal event-time column '{contract.temporal.event_time_column}' is missing",
                )
            )
        return findings

    def validate_data(
        self, df: DataFrame, source: SourceConfig
    ) -> list[ContractFinding]:
        """Run explicitly declared supplier quality rules against an input batch."""
        contract = source.contract
        if contract is None:
            return []
        started_at = time.perf_counter()
        plan = QualityValidationPlan.compile(contract.quality, contract.validation)
        validation_df = df
        if plan.policy.mode == "sampled":
            if plan.policy.sample_fraction is None:
                raise ValueError("sampled validation requires sample_fraction")
            validation_df = validation_df.sample(
                False, plan.policy.sample_fraction, plan.policy.sample_seed
            )
        if plan.policy.max_sample_rows is not None:
            validation_df = validation_df.limit(plan.policy.max_sample_rows)
        findings = self._run_scalar_rules(
            validation_df, plan.scalar_rules, plan.policy.max_failure_samples
        )
        findings.extend(
            self._run_unique_rule(
                validation_df,
                rule,
                plan.policy.max_failure_samples,
                approximate=plan.policy.mode == "approximate",
            )
            for rule in plan.unique_rules
        )
        scalar_sample_actions = sum(
            bool(finding.samples) for finding in findings[: len(plan.scalar_rules)]
        )
        self.last_metrics = {
            "stage": "data_contract",
            "contract_id": contract.id,
            "contract_version": contract.version,
            "validation_mode": plan.policy.mode,
            "spark_actions": plan.minimum_actions + scalar_sample_actions,
            "duration_ms": (time.perf_counter() - started_at) * 1000,
        }
        return findings

    def _run_scalar_rules(
        self,
        df: DataFrame,
        rules: tuple[ContractQualityRule, ...],
        sample_limit: int,
    ) -> list[ContractFinding]:
        if not rules:
            return []
        expressions = [F.count(F.lit(1)).alias("__total_rows")]
        conditions: list[Any] = []
        for index, rule in enumerate(rules):
            condition = self._invalid_condition(rule)
            conditions.append(condition)
            expressions.append(
                F.sum(F.when(condition, 1).otherwise(0)).cast("long").alias(f"r{index}")
            )
        metrics = df.agg(*expressions).first()
        if metrics is None:
            raise RuntimeError("Contract aggregation did not return metrics")
        total = int(metrics["__total_rows"] or 0)
        findings: list[ContractFinding] = []
        for index, (rule, condition) in enumerate(zip(rules, conditions, strict=True)):
            failed = int(metrics[f"r{index}"] or 0)
            ratio = failed / total if total else 0.0
            passed = (
                ratio <= (rule.max_ratio or 0)
                if rule.rule == "null_rate"
                else failed == 0
            )
            samples = (
                [
                    row.asDict()
                    for row in df.filter(condition).limit(sample_limit).collect()
                ]
                if failed and sample_limit
                else []
            )
            name = rule.name or f"contract_{rule.rule}_{rule.column}"
            findings.append(
                ContractFinding(
                    "source_quality",
                    name,
                    TestSeverity(rule.severity),
                    passed,
                    f"Null ratio for '{rule.column}' is {ratio:.6f}"
                    if rule.rule == "null_rate"
                    else (
                        "Contract quality rule passed"
                        if passed
                        else f"Contract quality rule found {failed} invalid rows"
                    ),
                    observed_value=str(ratio) if rule.rule == "null_rate" else None,
                    expected_value=str(rule.max_ratio)
                    if rule.rule == "null_rate"
                    else None,
                    failed_rows=failed,
                    total_rows=total,
                    samples=samples,
                )
            )
        return findings

    @staticmethod
    def _invalid_condition(rule: ContractQualityRule) -> Any:
        column = rule.column
        if rule.rule in {"not_null", "null_rate"}:
            if column is None:
                raise ValueError(f"{rule.rule} requires column")
            return F.col(column).isNull()
        if rule.rule == "accepted_values":
            if column is None:
                raise ValueError("accepted_values requires column")
            return ~F.col(column).isin(rule.values or [])
        if rule.rule == "expression":
            return F.expr(f"NOT ({rule.expression})")
        raise ValueError(f"Rule '{rule.rule}' is not a scalar rule")

    def _run_unique_rule(
        self,
        df: DataFrame,
        rule: ContractQualityRule,
        sample_limit: int,
        *,
        approximate: bool,
    ) -> ContractFinding:
        raw = rule.columns or ([rule.column] if rule.column is not None else [])
        columns = [column for column in raw if column is not None]
        if not columns:
            raise ValueError("unique contract rules require column or columns")
        name = rule.name or f"contract_unique_{'_'.join(columns)}"
        if approximate:
            row = df.agg(
                F.count(F.lit(1)).alias("total"),
                F.approx_count_distinct(F.struct(*columns)).alias("distinct"),
            ).first()
            if row is None:
                raise RuntimeError("Approximate uniqueness did not return metrics")
            failed = max(int(row["total"] or 0) - int(row["distinct"] or 0), 0)
            samples: list[dict[str, Any]] = []
        else:
            duplicates = df.groupBy(*columns).count().filter(F.col("count") > 1)
            sample_expr = F.slice(
                F.collect_list(F.to_json(F.struct(*[F.col(c) for c in columns]))),
                1,
                sample_limit,
            )
            row = duplicates.agg(
                F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("failed"),
                sample_expr.alias("samples"),
            ).first()
            if row is None:
                raise RuntimeError("Uniqueness aggregation did not return metrics")
            failed = int(row["failed"] or 0)
            samples = [json.loads(value) for value in (row["samples"] or [])]
        return ContractFinding(
            "source_quality",
            name,
            TestSeverity(rule.severity),
            failed == 0,
            "Contract quality rule passed"
            if failed == 0
            else f"Contract quality rule found {failed} duplicate rows",
            failed_rows=failed,
            samples=samples,
        )

    def validate_temporal(
        self,
        df: DataFrame,
        source: SourceConfig,
        *,
        prior_state: DataFrame | None = None,
    ) -> list[ContractFinding]:
        """Inspect event-time disorder within this batch and across prior runs."""
        contract = source.contract
        if contract is None or contract.temporal is None:
            self.last_metrics = {}
            return []
        temporal = contract.temporal
        started_at = time.perf_counter()
        if temporal.event_time_column not in df.columns:
            self.last_metrics = {
                "stage": "temporal_contract",
                "contract_id": contract.id,
                "contract_version": contract.version,
                "validation_mode": contract.validation.mode,
                "spark_actions": 0,
                "duration_ms": (time.perf_counter() - started_at) * 1000,
            }
            return [
                ContractFinding(
                    "late_event",
                    "event_time_available",
                    TestSeverity.ERROR,
                    False,
                    f"Missing event-time column '{temporal.event_time_column}'",
                )
            ]

        cutoff = self._lateness_cutoff(temporal.allowed_lateness)
        evaluated = self._prepare_temporal_evaluated(
            df, source, contract, temporal.event_time_column, prior_state, cutoff
        )
        (
            late_count,
            batch_disorder,
            cross_batch_disorder,
            late_samples,
            batch_samples,
            cross_samples,
        ) = self._measure_temporal_disorder(
            evaluated, contract.validation.max_failure_samples
        )

        findings = [
            ContractFinding(
                "late_event",
                "allowed_lateness",
                TestSeverity(temporal.late_event_severity),
                late_count == 0,
                "No late events"
                if late_count == 0
                else f"Found {late_count} events older than allowed lateness",
                failed_rows=late_count,
                samples=late_samples,
            )
        ]
        findings.extend(
            self._ordering_findings(
                evaluated, contract, temporal, df, batch_disorder, batch_samples
            )
        )
        findings.append(
            ContractFinding(
                "out_of_order_event",
                "cross_batch_event_order",
                TestSeverity(temporal.out_of_order_severity),
                cross_batch_disorder == 0,
                "Event times do not precede the persisted high-water mark"
                if cross_batch_disorder == 0
                else (
                    f"Found {cross_batch_disorder} events older than the persisted "
                    "business-key event-time maximum"
                ),
                failed_rows=cross_batch_disorder,
                samples=cross_samples,
            )
        )
        sample_actions = sum(bool(finding.samples) for finding in findings)
        self.last_metrics = {
            "stage": "temporal_contract",
            "contract_id": contract.id,
            "contract_version": contract.version,
            "validation_mode": contract.validation.mode,
            "spark_actions": 1 + sample_actions,
            "duration_ms": (time.perf_counter() - started_at) * 1000,
        }
        return findings

    @staticmethod
    def _lateness_cutoff(allowed_lateness: str) -> datetime:
        """Parse a human lateness string ('5m', '2 hours') into a UTC cutoff."""
        match = re.match(
            r"^(\d+)\s*(s|m|h|d|seconds?|minutes?|hours?|days?)$",
            allowed_lateness,
            re.I,
        )
        amount, unit = (int(match[1]), match[2].lower()) if match else (0, "h")
        seconds = amount * ({"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit[0], 3600))
        return datetime.now(timezone.utc) - timedelta(seconds=seconds)

    @staticmethod
    def _prepare_temporal_evaluated(
        df: DataFrame,
        source: SourceConfig,
        contract: SourceContractConfig,
        event_time_column: str,
        prior_state: DataFrame | None,
        cutoff: datetime,
    ) -> DataFrame:
        """Per-business-key observations with prior-state and lag context."""
        keys = (
            (contract.cdc.primary_key if contract.cdc else None)
            or source.primary_keys
            or []
        )
        evaluated = TemporalStateStore.observations(df, event_time_column, keys)
        if prior_state is not None:
            previous = prior_state.select(
                "business_key_hash",
                F.col("max_event_time").alias("__prior_max_event_time"),
            )
            evaluated = evaluated.join(previous, "business_key_hash", "left")
        else:
            evaluated = evaluated.withColumn(
                "__prior_max_event_time", F.lit(None).cast("timestamp")
            )
        if keys and "_commit_version" in df.columns:
            window = Window.partitionBy("business_key_hash").orderBy("commit_version")
            evaluated = evaluated.withColumn(
                "__previous_event_time", F.lag("event_time").over(window)
            )
        else:
            evaluated = evaluated.withColumn(
                "__previous_event_time", F.lit(None).cast("timestamp")
            )
        return (
            evaluated.withColumn("__late", F.col("event_time") < F.lit(cutoff))
            .withColumn(
                "__batch_disorder",
                F.col("event_time") < F.col("__previous_event_time"),
            )
            .withColumn(
                "__cross_batch_disorder",
                F.col("event_time") < F.col("__prior_max_event_time"),
            )
        )

    @staticmethod
    def _measure_temporal_disorder(
        evaluated: DataFrame, sample_limit: int
    ) -> tuple[int, int, int, list[dict], list[dict], list[dict]]:
        """Aggregate the three disorder flags; collect samples for non-zeros."""
        metrics = evaluated.agg(
            F.sum(F.when(F.col("__late"), 1).otherwise(0)).alias("late"),
            F.sum(F.when(F.col("__batch_disorder"), 1).otherwise(0)).alias(
                "batch_disorder"
            ),
            F.sum(F.when(F.col("__cross_batch_disorder"), 1).otherwise(0)).alias(
                "cross_batch_disorder"
            ),
        ).first()
        if metrics is None:
            raise RuntimeError("Temporal aggregation did not return metrics")
        late_count = int(metrics["late"] or 0)
        batch_disorder = int(metrics["batch_disorder"] or 0)
        cross_batch_disorder = int(metrics["cross_batch_disorder"] or 0)

        def _samples(flag: str, count: int) -> list[dict]:
            if not count or not sample_limit:
                return []
            return [
                row.asDict()
                for row in evaluated.filter(flag).limit(sample_limit).collect()
            ]

        return (
            late_count,
            batch_disorder,
            cross_batch_disorder,
            _samples("__late", late_count),
            _samples("__batch_disorder", batch_disorder),
            _samples("__cross_batch_disorder", cross_batch_disorder),
        )

    @staticmethod
    def _ordering_findings(
        evaluated: DataFrame,
        contract: SourceContractConfig,
        temporal,
        df: DataFrame,
        batch_disorder: int,
        batch_samples: list[dict],
    ) -> list[ContractFinding]:
        """Within-batch ordering findings, or the CDF-capability warning.

        Commit-order semantics require CDF metadata: with ``_commit_version``
        present the lag window was built, so batch disorder is reportable;
        without it the finding downgrades to a capability warning.
        """
        del evaluated, contract
        if "_commit_version" in df.columns:
            return [
                ContractFinding(
                    "out_of_order_event",
                    "cdf_commit_order",
                    TestSeverity(temporal.out_of_order_severity),
                    batch_disorder == 0,
                    "CDF event times are ordered"
                    if batch_disorder == 0
                    else f"Found {batch_disorder} out-of-order CDF events",
                    failed_rows=batch_disorder,
                    samples=batch_samples,
                )
            ]
        return [
            ContractFinding(
                "out_of_order_event",
                "cdf_capability",
                TestSeverity.WARN,
                False,
                "Cannot establish event ordering without CDF commit metadata",
            )
        ]

    @staticmethod
    def raise_for_errors(findings: list[ContractFinding]) -> None:
        if failures := [
            f for f in findings if not f.passed and f.severity == TestSeverity.ERROR
        ]:
            raise ContractValidationError(
                "; ".join(f"{f.check_name}: {f.details}" for f in failures)
            )
