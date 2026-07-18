from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import Any, Literal

from kimball.planning.compiler import CompiledProject

ChangeKind = Literal["added", "removed", "modified"]
Classification = Literal[
    "metadata_only",
    "non_breaking",
    "requires_validation",
    "requires_backfill",
    "breaking",
]

_METADATA_FIELDS = {"table_description", "column_descriptions"}
_BACKFILL_FIELDS = {
    "transformation_sql",
    "natural_keys",
    "merge_keys",
    "surrogate_key",
    "durable_key",
    "scd_type",
    "effective_at",
    "track_history_columns",
    "fact_pattern",
    "snapshot_period",
    "milestones",
    "null_policy",
    "foreign_keys",
    "junk_dimensions",
    "degenerate_dimensions",
}
_BREAKING_FIELDS = {
    "table_name",
    "table_type",
    "history_table",
    "append_only",
    "durable_key",
    "null_policy",
}


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _framework_version() -> str:
    try:
        return version("kimball_framework")
    except PackageNotFoundError:
        return "0+unknown"


def build_manifest(
    project: CompiledProject, *, framework_version: str | None = None
) -> dict[str, Any]:
    """Build a deterministic, secret-free description of a compiled project."""

    pipelines: list[dict[str, Any]] = []
    for table_name in sorted(project.nodes):
        node = project.nodes[table_name]
        config = node.config.model_dump(by_alias=True, mode="json")
        metadata = {field: config.pop(field, None) for field in _METADATA_FIELDS}
        pipelines.append(
            {
                "table_name": node.table_name,
                "table_type": node.table_type,
                "config_path": node.config_path.replace("\\", "/"),
                "dependencies": list(node.dependencies),
                "explicit_dependencies": list(node.explicit_dependencies),
                "inferred_dependencies": list(node.inferred_dependencies),
                "writes": list(node.writes),
                "semantic_config": config,
                "metadata": metadata,
                "semantic_digest": _digest(config),
                "metadata_digest": _digest(metadata),
            }
        )

    body = {
        "schema_version": "1.0",
        "framework_version": framework_version or _framework_version(),
        "levels": [list(level) for level in project.levels],
        "pipelines": pipelines,
    }
    body["project_digest"] = _digest(body)
    return body


def manifest_json(manifest: dict[str, Any]) -> str:
    return json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


@dataclass(frozen=True)
class PlanChange:
    table_name: str
    kind: ChangeKind
    classification: Classification
    changed_fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class ProjectPlan:
    changes: tuple[PlanChange, ...]
    affected_tables: tuple[str, ...]

    @property
    def has_breaking_changes(self) -> bool:
        return any(change.classification == "breaking" for change in self.changes)


def _classify_modified(
    previous: dict[str, Any], current: dict[str, Any]
) -> tuple[Classification, tuple[str, ...]]:
    if previous.get("semantic_digest") == current.get("semantic_digest"):
        return "metadata_only", tuple(sorted(_METADATA_FIELDS))

    previous_config = previous.get("semantic_config", {})
    current_config = current.get("semantic_config", {})
    fields = tuple(
        sorted(
            key
            for key in set(previous_config) | set(current_config)
            if previous_config.get(key) != current_config.get(key)
        )
    )
    if (
        previous.get("dependencies") != current.get("dependencies")
        or previous.get("writes") != current.get("writes")
        or _BREAKING_FIELDS.intersection(fields)
    ):
        return "breaking", fields
    if _BACKFILL_FIELDS.intersection(fields):
        return "requires_backfill", fields
    return "requires_validation", fields


def diff_manifests(previous: dict[str, Any], current: dict[str, Any]) -> ProjectPlan:
    """Classify project changes and compute their transitive downstream impact."""

    previous_nodes = {
        node["table_name"]: node for node in previous.get("pipelines", [])
    }
    current_nodes = {node["table_name"]: node for node in current.get("pipelines", [])}
    changes: list[PlanChange] = []

    for table_name in sorted(set(previous_nodes) | set(current_nodes)):
        before = previous_nodes.get(table_name)
        after = current_nodes.get(table_name)
        if before is None:
            changes.append(PlanChange(table_name, "added", "requires_validation"))
        elif after is None:
            changes.append(PlanChange(table_name, "removed", "breaking"))
        elif (
            before.get("semantic_digest") != after.get("semantic_digest")
            or before.get("metadata_digest") != after.get("metadata_digest")
            or before.get("dependencies") != after.get("dependencies")
            or before.get("writes") != after.get("writes")
        ):
            classification, fields = _classify_modified(before, after)
            changes.append(PlanChange(table_name, "modified", classification, fields))

    affected = {change.table_name for change in changes}
    combined_nodes = {**previous_nodes, **current_nodes}
    changed = True
    while changed:
        changed = False
        for table_name, node in combined_nodes.items():
            if table_name not in affected and affected.intersection(
                node.get("dependencies", [])
            ):
                affected.add(table_name)
                changed = True

    return ProjectPlan(tuple(changes), tuple(sorted(affected)))
