from __future__ import annotations

from dataclasses import dataclass

from kimball.contracts.odcs import ODCSContract, ODCSObject, ODCSProperty


@dataclass(frozen=True)
class ContractChange:
    code: str
    path: str
    message: str
    breaking: bool = False


@dataclass(frozen=True)
class ContractCompatibilityReport:
    allowed: bool
    changes: tuple[ContractChange, ...]
    breaking_changes: tuple[ContractChange, ...]
    version_errors: tuple[str, ...]
    requires_consumer_migration: bool


def _semver(version: str) -> tuple[int, int, int]:
    return tuple(int(part) for part in version.split("."))  # type: ignore[return-value]


def _compare_property(
    object_name: str, previous: ODCSProperty, current: ODCSProperty
) -> list[ContractChange]:
    path = f"schema.{object_name}.{previous.name}"
    changes: list[ContractChange] = []
    if previous.logical_type != current.logical_type:
        widening = (
            previous.logical_type == "integer" and current.logical_type == "number"
        )
        changes.append(
            ContractChange(
                "COLUMN_TYPE_CHANGED",
                path,
                f"logical type changed from {previous.logical_type} to {current.logical_type}",
                not widening,
            )
        )
    if previous.physical_type != current.physical_type:
        changes.append(
            ContractChange(
                "PHYSICAL_TYPE_CHANGED",
                path,
                f"physical type changed from {previous.physical_type} to {current.physical_type}",
                True,
            )
        )
    if not previous.required and current.required:
        changes.append(
            ContractChange(
                "COLUMN_BECAME_REQUIRED",
                path,
                "nullable column became required",
                True,
            )
        )
    elif previous.required and not current.required:
        changes.append(
            ContractChange(
                "COLUMN_BECAME_NULLABLE",
                path,
                "required column became nullable",
            )
        )
    if previous.primary_key != current.primary_key:
        changes.append(
            ContractChange(
                "PRIMARY_KEY_CHANGED", path, "primary-key membership changed", True
            )
        )
    return changes


def _compare_object(previous: ODCSObject, current: ODCSObject) -> list[ContractChange]:
    changes: list[ContractChange] = []
    for name in sorted(set(previous.properties) | set(current.properties)):
        before = previous.properties.get(name)
        after = current.properties.get(name)
        path = f"schema.{previous.name}.{name}"
        if before is None and after is not None:
            changes.append(
                ContractChange(
                    "REQUIRED_COLUMN_ADDED"
                    if after.required
                    else "COLUMN_ADDED_NULLABLE",
                    path,
                    "column added",
                    after.required,
                )
            )
        elif after is None:
            changes.append(
                ContractChange("COLUMN_REMOVED", path, "column removed", True)
            )
        elif before is not None:
            changes.extend(_compare_property(previous.name, before, after))
    if previous.primary_key != current.primary_key:
        changes.append(
            ContractChange(
                "PRIMARY_KEY_ORDER_CHANGED",
                f"schema.{previous.name}",
                f"primary key changed from {previous.primary_key} to {current.primary_key}",
                True,
            )
        )
    return changes


def check_compatibility(
    previous: ODCSContract, current: ODCSContract
) -> ContractCompatibilityReport:
    changes: list[ContractChange] = []
    version_errors: list[str] = []
    if previous.id != current.id:
        version_errors.append("Contract id cannot change")

    for name in sorted(set(previous.objects) | set(current.objects)):
        before = previous.objects.get(name)
        after = current.objects.get(name)
        if before is None:
            changes.append(
                ContractChange("OBJECT_ADDED", f"schema.{name}", "schema object added")
            )
        elif after is None:
            changes.append(
                ContractChange(
                    "OBJECT_REMOVED", f"schema.{name}", "schema object removed", True
                )
            )
        else:
            changes.extend(_compare_object(before, after))

    changed = previous.digest != current.digest
    previous_version = _semver(previous.version)
    current_version = _semver(current.version)
    if changed and current_version <= previous_version:
        version_errors.append("Changed contract must increment version")

    breaking = tuple(change for change in changes if change.breaking)
    major_incremented = current_version[0] > previous_version[0]
    allowed = not version_errors and (not breaking or major_incremented)
    return ContractCompatibilityReport(
        allowed=allowed,
        changes=tuple(changes),
        breaking_changes=breaking,
        version_errors=tuple(version_errors),
        requires_consumer_migration=bool(breaking),
    )
