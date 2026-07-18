from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Literal

from kimball.common.config import TableConfig

Profile = Literal["dev", "test", "production"]
Severity = Literal["warning", "error"]


@dataclass(frozen=True)
class ProjectIssue:
    code: str
    severity: Severity
    message: str
    pipeline: str | None = None

    def __str__(self) -> str:
        location = f" ({self.pipeline})" if self.pipeline else ""
        return f"{self.code}{location}: {self.message}"


class ProjectValidationError(ValueError):
    """Raised when a project cannot be compiled into a safe execution DAG."""

    def __init__(self, issues: Sequence[ProjectIssue]):
        self.issues = tuple(issues)
        super().__init__("Project validation failed:\n" + "\n".join(map(str, issues)))


@dataclass(frozen=True)
class CompiledPipeline:
    table_name: str
    table_type: str
    config_path: str
    explicit_dependencies: tuple[str, ...]
    inferred_dependencies: tuple[str, ...]
    dependencies: tuple[str, ...]
    writes: tuple[str, ...]
    config: TableConfig


@dataclass(frozen=True)
class CompiledProject:
    nodes: dict[str, CompiledPipeline]
    levels: tuple[tuple[str, ...], ...]
    issues: tuple[ProjectIssue, ...] = ()

    @property
    def warnings(self) -> tuple[ProjectIssue, ...]:
        return tuple(issue for issue in self.issues if issue.severity == "warning")


class ProjectCompiler:
    """Compile table configurations into a validated deterministic DAG.

    Explicit ``depends_on`` declarations are the production contract. References
    inferred from sources and foreign keys are still included so development
    plans are accurate, but production rejects an omitted declaration.
    """

    def __init__(self, profile: Profile = "dev"):
        if profile not in ("dev", "test", "production"):
            raise ValueError(f"Unknown compilation profile: {profile}")
        self.profile = profile

    def compile(self, entries: Sequence[tuple[str, TableConfig]]) -> CompiledProject:
        issues: list[ProjectIssue] = []
        grouped: dict[str, list[tuple[str, TableConfig]]] = defaultdict(list)
        for path, config in entries:
            grouped[config.table_name].append((str(path), config))

        for table_name, writers in sorted(grouped.items()):
            if len(writers) > 1:
                paths = ", ".join(sorted(path for path, _ in writers))
                issues.append(
                    ProjectIssue(
                        "TARGET_WRITER_CONFLICT",
                        "error",
                        f"target '{table_name}' has multiple writers: {paths}",
                        table_name,
                    )
                )

        known_targets = set(grouped)
        nodes: dict[str, CompiledPipeline] = {}
        write_owners: dict[str, list[str]] = defaultdict(list)

        for path, config in entries:
            if config.table_name in nodes:
                continue
            explicit = set(config.depends_on)
            missing = explicit - known_targets
            for upstream in sorted(missing):
                issues.append(
                    ProjectIssue(
                        "MISSING_UPSTREAM",
                        "error",
                        f"declared upstream '{upstream}' is not part of the project",
                        config.table_name,
                    )
                )

            inferred = self._infer_dependencies(config, known_targets)
            undeclared = inferred - explicit
            for upstream in sorted(undeclared):
                severity: Severity = (
                    "error" if self.profile == "production" else "warning"
                )
                issues.append(
                    ProjectIssue(
                        "UNDECLARED_DEPENDENCY",
                        severity,
                        f"inferred upstream '{upstream}' must be added to depends_on",
                        config.table_name,
                    )
                )

            writes = self._writes(config)
            for target in writes:
                write_owners[target].append(config.table_name)

            nodes[config.table_name] = CompiledPipeline(
                table_name=config.table_name,
                table_type=config.table_type,
                config_path=str(path),
                explicit_dependencies=tuple(sorted(explicit)),
                inferred_dependencies=tuple(sorted(inferred)),
                dependencies=tuple(sorted((explicit | inferred) & known_targets)),
                writes=tuple(sorted(writes)),
                config=config,
            )

        for target, owners in sorted(write_owners.items()):
            unique_owners = sorted(set(owners))
            if len(unique_owners) > 1:
                issues.append(
                    ProjectIssue(
                        "TARGET_WRITER_CONFLICT",
                        "error",
                        f"target '{target}' is written by: {', '.join(unique_owners)}",
                        target,
                    )
                )

        cycle = self._find_cycle(nodes)
        if cycle:
            issues.append(
                ProjectIssue(
                    "DEPENDENCY_CYCLE",
                    "error",
                    " -> ".join(cycle),
                    cycle[0],
                )
            )

        errors = [issue for issue in issues if issue.severity == "error"]
        if errors:
            raise ProjectValidationError(errors)

        return CompiledProject(
            nodes=nodes,
            levels=self._topological_levels(nodes),
            issues=tuple(issues),
        )

    @staticmethod
    def _infer_dependencies(config: TableConfig, known_targets: set[str]) -> set[str]:
        candidates = {source.name for source in config.sources}
        candidates.update(
            fk.references for fk in (config.foreign_keys or []) if fk.references
        )
        candidates.update(
            fk.lookup.identity_map
            for fk in (config.foreign_keys or [])
            if fk.lookup and fk.lookup.identity_map
        )
        candidates.discard(config.table_name)
        return candidates & known_targets

    @staticmethod
    def _writes(config: TableConfig) -> set[str]:
        writes = {config.table_name}
        if config.history_table:
            writes.add(config.history_table)
        writes.update(junk.dimension_table for junk in config.junk_dimensions)
        return writes

    @staticmethod
    def _find_cycle(nodes: dict[str, CompiledPipeline]) -> tuple[str, ...] | None:
        visited: set[str] = set()
        active: list[str] = []
        active_set: set[str] = set()

        def visit(name: str) -> tuple[str, ...] | None:
            if name in active_set:
                start = active.index(name)
                return tuple(active[start:] + [name])
            if name in visited:
                return None
            active.append(name)
            active_set.add(name)
            for dependency in nodes[name].dependencies:
                if dependency in nodes:
                    cycle = visit(dependency)
                    if cycle:
                        return cycle
            active.pop()
            active_set.remove(name)
            visited.add(name)
            return None

        for name in sorted(nodes):
            cycle = visit(name)
            if cycle:
                return cycle
        return None

    @staticmethod
    def _topological_levels(
        nodes: dict[str, CompiledPipeline],
    ) -> tuple[tuple[str, ...], ...]:
        remaining = set(nodes)
        completed: set[str] = set()
        levels: list[tuple[str, ...]] = []
        while remaining:
            ready = tuple(
                sorted(
                    name
                    for name in remaining
                    if set(nodes[name].dependencies) <= completed
                )
            )
            if not ready:
                # Cycles are reported before this function is called.
                raise RuntimeError("Cannot order a cyclic dependency graph")
            levels.append(ready)
            completed.update(ready)
            remaining.difference_update(ready)
        return tuple(levels)


def compile_project(
    entries: Iterable[tuple[str, TableConfig]], profile: Profile = "dev"
) -> CompiledProject:
    """Convenience wrapper for callers that hold an iterable of configs."""

    return ProjectCompiler(profile=profile).compile(list(entries))
