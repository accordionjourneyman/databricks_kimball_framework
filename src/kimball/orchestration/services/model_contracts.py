"""Runtime enforcement for declarative Kimball fact metadata."""

from __future__ import annotations

from kimball.common.config import TableConfig


def validate_fact_output_columns(config: TableConfig, columns: list[str]) -> None:
    """Fail before merge when declared fact semantics are absent from SQL output."""
    if config.table_type != "fact":
        return
    available = set(columns)
    declarations = {
        "merge key": set(config.merge_keys or []),
        "foreign key": {fk.column for fk in config.foreign_keys or []},
        "measure": {measure.name for measure in config.measures},
        "milestone": {milestone.column for milestone in config.milestones},
        "degenerate dimension": set(config.degenerate_dimensions),
        "junk dimension key": {junk.surrogate_key for junk in config.junk_dimensions},
    }
    errors = [
        f"{kind}: {sorted(required - available)}"
        for kind, required in declarations.items()
        if required - available
    ]
    if errors:
        raise ValueError(
            f"Fact model declarations for {config.table_name} are missing from the "
            f"transformation output ({'; '.join(errors)})"
        )
