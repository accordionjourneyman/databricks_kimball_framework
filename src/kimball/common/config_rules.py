"""Kimball invariant predicates for TableConfig.

Each named rule reports every violation it finds. This lets one config load
surface independent fixes together while preserving the historical rule order
and first-error helper for callers that still need a single message.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kimball.common.config import TableConfig


@dataclass(frozen=True)
class ConfigRuleViolation:
    rule: str
    message: str


class ConfigRuleValidationError(ValueError):
    """All violated TableConfig rules from one model validation."""

    def __init__(self, issues: tuple[ConfigRuleViolation, ...]):
        self.issues = issues
        super().__init__(
            "Table config violates rules:\n"
            + "\n".join(f"- {issue.message}" for issue in issues)
        )


def _dimension_keys(config: TableConfig) -> tuple[str, ...]:
    issues: list[str] = []
    if config.table_type == "dimension":
        if not config.surrogate_key:
            issues.append("Dimensions require keys.surrogate_key")
        if not config.natural_keys:
            issues.append("Dimensions require keys.natural_keys")
    return tuple(issues)


def _fact_shape(config: TableConfig) -> tuple[str, ...]:
    issues: list[str] = []
    if config.table_type == "fact":
        if not config.merge_keys:
            issues.append("fact tables require merge_keys")
        if config.fact_pattern and not config.grain:
            issues.append("facts require a declared grain")
    return tuple(issues)


def _description_quality(config: TableConfig) -> tuple[str, ...]:
    issues: list[str] = []
    if config.table_description is not None and not config.table_description.strip():
        issues.append("table_description must not be empty")
    if any(
        not name or not description.strip()
        for name, description in config.column_descriptions.items()
    ):
        issues.append(
            "column_descriptions requires non-empty column names and descriptions"
        )
    return tuple(issues)


def _fact_metadata_placement(config: TableConfig) -> tuple[str, ...]:
    issues: list[str] = []
    if config.table_type != "fact" and (
        config.fact_pattern
        or config.snapshot_period
        or config.measures
        or config.milestones
        or config.degenerate_dimensions
        or config.junk_dimensions
    ):
        issues.append("fact pattern metadata is only valid for fact tables")
    if config.fact_pattern == "periodic_snapshot" and not config.snapshot_period:
        issues.append("periodic_snapshot facts require snapshot_period")
    return tuple(issues)


def _accumulating_snapshot(config: TableConfig) -> tuple[str, ...]:
    if config.fact_pattern != "accumulating_snapshot":
        return ()
    issues: list[str] = []
    orders = [milestone.order for milestone in config.milestones]
    if len(orders) < 2:
        issues.append("accumulating_snapshot facts require at least two milestones")
    if len(orders) != len(set(orders)):
        issues.append("accumulating_snapshot milestones must have unique order values")
    return tuple(issues)


def _foreign_key_roles(config: TableConfig) -> tuple[str, ...]:
    foreign_keys = config.foreign_keys or []
    issues: list[str] = []
    for foreign_key in foreign_keys:
        if foreign_key.role_playing and not foreign_key.role:
            issues.append("role_playing foreign keys require role")
        if foreign_key.role_playing and not foreign_key.references:
            issues.append(
                "role_playing foreign keys require references to a physical dimension"
            )
    roles = [
        foreign_key.role for foreign_key in foreign_keys if foreign_key.role_playing
    ]
    if len(roles) != len(set(roles)):
        issues.append("role_playing foreign key roles must be unique")
    return tuple(issues)


def _output_column_uniqueness(config: TableConfig) -> tuple[str, ...]:
    relationship_columns = [
        column
        for foreign_key in config.foreign_keys or []
        for column in (foreign_key.column, foreign_key.durable_column)
        if column
    ]
    if len(relationship_columns) != len(set(relationship_columns)):
        return ("foreign-key output columns must be unique",)
    return ()


def _measure_and_milestone_names(config: TableConfig) -> tuple[str, ...]:
    issues: list[str] = []
    measure_names = [measure.name for measure in config.measures]
    if len(measure_names) != len(set(measure_names)):
        issues.append("fact measure names must be unique")
    milestone_names = [milestone.name for milestone in config.milestones]
    if len(milestone_names) != len(set(milestone_names)):
        issues.append("fact milestone names must be unique")
    milestone_columns = [milestone.column for milestone in config.milestones]
    if len(milestone_columns) != len(set(milestone_columns)):
        issues.append("fact milestone columns must be unique")
    return tuple(issues)


def _junk_degenerate_partition(config: TableConfig) -> tuple[str, ...]:
    junk_keys = [junk.surrogate_key for junk in config.junk_dimensions]
    issues: list[str] = []
    if len(junk_keys) != len(set(junk_keys)):
        issues.append("junk dimension surrogate_key values must be unique")
    if set(config.degenerate_dimensions).intersection(junk_keys):
        issues.append("a column cannot be both a degenerate and junk dimension key")
    return tuple(issues)


def _append_only_rules(config: TableConfig) -> tuple[str, ...]:
    issues: list[str] = []
    if config.append_only and config.table_type != "fact":
        issues.append("append_only is only valid for fact tables")
    if (
        any(source.cdc_strategy == "append" for source in config.sources)
        and not config.append_only
    ):
        issues.append(
            "cdc_strategy='append' requires append_only=true for the target table"
        )
    return tuple(issues)


def _scd_type_rules(config: TableConfig) -> tuple[str, ...]:
    issues: list[str] = []
    if config.scd_type in (2, 7) and not config.effective_at:
        issues.append(
            f"SCD Type {config.scd_type} requires 'effective_at' for idempotent history tracking. "
            "Specify the business-time column (e.g. 'updated_at') in the YAML config."
        )
    if config.scd_type == 7 and not config.durable_key:
        issues.append("SCD Type 7 requires keys.durable_key")
    if config.scd_type != 7 and config.durable_key:
        issues.append("keys.durable_key is only valid for SCD Type 7")
    if config.scd_type == 4 and not config.history_table:
        issues.append("SCD Type 4 requires 'history_table' to be specified.")
    if config.scd_type == 6 and not config.current_value_columns:
        issues.append("SCD Type 6 requires 'current_value_columns' to be specified.")
    return tuple(issues)


def _modeling_exceptions(config: TableConfig) -> tuple[str, ...]:
    exception_keys = [
        (exception.code, column)
        for exception in config.modeling_exceptions
        for column in exception.columns
    ]
    if len(exception_keys) != len(set(exception_keys)):
        return ("modeling_exceptions must not repeat the same (code, column) pair",)
    return ()


def _contract_cdc_consistency(config: TableConfig) -> tuple[str, ...]:
    issues: list[str] = []
    for source in config.sources:
        if not source.contract or not source.contract.cdc:
            continue
        contract_keys = source.contract.cdc.primary_key
        if (
            contract_keys
            and source.primary_keys
            and contract_keys != source.primary_keys
        ):
            issues.append(
                f"Source '{source.name}' primary_keys must match contract.cdc.primary_key"
            )
        if source.contract.cdc.required and source.cdc_strategy != "cdf":
            issues.append(
                f"Source '{source.name}' contract requires CDF but "
                f"cdc_strategy is '{source.cdc_strategy}'"
            )
    return tuple(issues)


Rule = Callable[["TableConfig"], tuple[str, ...]]

# Execution order preserved from the original inline implementation.
TABLE_CONFIG_RULES: tuple[tuple[str, Rule], ...] = (
    ("dimension_keys", _dimension_keys),
    ("fact_shape", _fact_shape),
    ("description_quality", _description_quality),
    ("fact_metadata_placement", _fact_metadata_placement),
    ("accumulating_snapshot", _accumulating_snapshot),
    ("foreign_key_roles", _foreign_key_roles),
    ("output_column_uniqueness", _output_column_uniqueness),
    ("measure_and_milestone_names", _measure_and_milestone_names),
    ("junk_degenerate_partition", _junk_degenerate_partition),
    ("append_only_rules", _append_only_rules),
    ("scd_type_rules", _scd_type_rules),
    ("modeling_exceptions", _modeling_exceptions),
    ("contract_cdc_consistency", _contract_cdc_consistency),
)


def collect_config_violations(config: TableConfig) -> tuple[ConfigRuleViolation, ...]:
    """Return all violated invariants in stable rule order."""
    return tuple(
        ConfigRuleViolation(rule_name, message)
        for rule_name, rule in TABLE_CONFIG_RULES
        for message in rule(config)
    )


def first_config_violation(config: TableConfig) -> str | None:
    """Return the first violation for compatibility with single-error callers."""
    violations = collect_config_violations(config)
    return violations[0].message if violations else None
