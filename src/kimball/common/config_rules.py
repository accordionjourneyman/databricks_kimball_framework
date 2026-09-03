"""Kimball invariant predicates for TableConfig (ADR-004 step 5).

Pure functions: each rule takes the config's relevant fields and returns
an error message when the invariant is violated, or ``None`` when it
holds. ``TableConfig.validate_kimball_rules`` runs them in order and
raises the first non-``None`` message — the fail-closed ValueError
surface and every message string are unchanged from the previous inline
implementation.

Keeping the rules as named functions gives each invariant one home, a
single reason to change, and makes the rule set enumerable for the
model-integrity documentation.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kimball.common.config import TableConfig


def _dimension_keys(config: TableConfig) -> str | None:
    if config.table_type == "dimension":
        if not config.surrogate_key:
            return "Dimensions require keys.surrogate_key"
        if not config.natural_keys:
            return "Dimensions require keys.natural_keys"
    return None


def _fact_shape(config: TableConfig) -> str | None:
    if config.table_type == "fact":
        if not config.merge_keys:
            return "fact tables require merge_keys"
        if config.fact_pattern and not config.grain:
            return "facts require a declared grain"
    return None


def _description_quality(config: TableConfig) -> str | None:
    if config.table_description is not None and not config.table_description.strip():
        return "table_description must not be empty"
    if any(
        not name or not description.strip()
        for name, description in config.column_descriptions.items()
    ):
        return "column_descriptions requires non-empty column names and descriptions"
    return None


def _fact_metadata_placement(config: TableConfig) -> str | None:
    if config.table_type != "fact" and (
        config.fact_pattern
        or config.snapshot_period
        or config.measures
        or config.milestones
        or config.degenerate_dimensions
        or config.junk_dimensions
    ):
        return "fact pattern metadata is only valid for fact tables"
    if config.fact_pattern == "periodic_snapshot" and not config.snapshot_period:
        return "periodic_snapshot facts require snapshot_period"
    return None


def _accumulating_snapshot(config: TableConfig) -> str | None:
    if config.fact_pattern != "accumulating_snapshot":
        return None
    if len(config.milestones) < 2:
        return "accumulating_snapshot facts require at least two milestones"
    orders = [m.order for m in config.milestones]
    if len(orders) != len(set(orders)):
        return "accumulating_snapshot milestones must have unique order values"
    return None


def _foreign_key_roles(config: TableConfig) -> str | None:
    for fk in config.foreign_keys or []:
        if fk.role_playing and not fk.role:
            return "role_playing foreign keys require role"
        if fk.role_playing and not fk.references:
            return (
                "role_playing foreign keys require references to a physical dimension"
            )
    roles = [fk.role for fk in config.foreign_keys or [] if fk.role_playing]
    if len(roles) != len(set(roles)):
        return "role_playing foreign key roles must be unique"
    return None


def _output_column_uniqueness(config: TableConfig) -> str | None:
    relationship_columns = [
        column
        for fk in config.foreign_keys or []
        for column in (fk.column, fk.durable_column)
        if column
    ]
    if len(relationship_columns) != len(set(relationship_columns)):
        return "foreign-key output columns must be unique"
    return None


def _measure_and_milestone_names(config: TableConfig) -> str | None:
    measure_names = [measure.name for measure in config.measures]
    if len(measure_names) != len(set(measure_names)):
        return "fact measure names must be unique"
    milestone_names = [milestone.name for milestone in config.milestones]
    milestone_columns = [milestone.column for milestone in config.milestones]
    if len(milestone_names) != len(set(milestone_names)):
        return "fact milestone names must be unique"
    if len(milestone_columns) != len(set(milestone_columns)):
        return "fact milestone columns must be unique"
    return None


def _junk_degenerate_partition(config: TableConfig) -> str | None:
    junk_keys = [junk.surrogate_key for junk in config.junk_dimensions]
    if len(junk_keys) != len(set(junk_keys)):
        return "junk dimension surrogate_key values must be unique"
    if set(config.degenerate_dimensions).intersection(junk_keys):
        return "a column cannot be both a degenerate and junk dimension key"
    return None


def _append_only_rules(config: TableConfig) -> str | None:
    if config.append_only and config.table_type != "fact":
        return "append_only is only valid for fact tables"
    if (
        any(s.cdc_strategy == "append" for s in config.sources)
        and not config.append_only
    ):
        return "cdc_strategy='append' requires append_only=true for the target table"
    return None


def _scd_type_rules(config: TableConfig) -> str | None:
    if config.scd_type in (2, 7) and not config.effective_at:
        return (
            f"SCD Type {config.scd_type} requires 'effective_at' for idempotent history tracking. "
            "Specify the business-time column (e.g. 'updated_at') in the YAML config."
        )
    if config.scd_type == 7 and not config.durable_key:
        return "SCD Type 7 requires keys.durable_key"
    if config.scd_type != 7 and config.durable_key:
        return "keys.durable_key is only valid for SCD Type 7"
    if config.scd_type == 4 and not config.history_table:
        return "SCD Type 4 requires 'history_table' to be specified."
    if config.scd_type == 6 and not config.current_value_columns:
        return "SCD Type 6 requires 'current_value_columns' to be specified."
    return None


def _modeling_exceptions(config: TableConfig) -> str | None:
    exception_keys = [
        (exception.code, column)
        for exception in config.modeling_exceptions
        for column in exception.columns
    ]
    if len(exception_keys) != len(set(exception_keys)):
        return "modeling_exceptions must not repeat the same (code, column) pair"
    return None


def _contract_cdc_consistency(config: TableConfig) -> str | None:
    for source in config.sources:
        if source.contract and source.contract.cdc:
            contract_keys = source.contract.cdc.primary_key
            if (
                contract_keys
                and source.primary_keys
                and contract_keys != source.primary_keys
            ):
                return f"Source '{source.name}' primary_keys must match contract.cdc.primary_key"
            if source.contract.cdc.required and source.cdc_strategy != "cdf":
                return (
                    f"Source '{source.name}' contract requires CDF but "
                    f"cdc_strategy is '{source.cdc_strategy}'"
                )
    return None


# Execution order preserved from the original inline implementation.
TABLE_CONFIG_RULES: tuple[tuple[str, Callable[[TableConfig], str | None]], ...] = (
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


def first_config_violation(config: TableConfig) -> str | None:
    """Return the first violated Kimball invariant's message, or ``None``.

    The caller raises ``ValueError(message)`` — fail closed, first error
    wins, identical to the previous inline behavior.
    """
    for _name, rule in TABLE_CONFIG_RULES:
        if message := rule(config):
            return message
    return None
