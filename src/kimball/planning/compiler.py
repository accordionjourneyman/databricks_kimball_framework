from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from kimball.common.config import (
    ModelIntegrityPolicy,
    TableConfig,
)
from kimball.planning.model_integrity import (
    Fixability,
    FixSuggestion,
    check_project,
)

Profile = Literal["dev", "test", "production"]
Severity = Literal["warning", "error"]


@dataclass(frozen=True)
class ProjectIssue:
    code: str
    severity: Severity
    message: str
    pipeline: str | None = None
    fixability: Fixability | None = None
    fix: FixSuggestion | None = None

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
    model_integrity_summary: str | None = None

    @property
    def warnings(self) -> tuple[ProjectIssue, ...]:
        return tuple(issue for issue in self.issues if issue.severity == "warning")


class ProjectCompiler:
    """Compile table configurations into a validated deterministic DAG.

    Explicit ``depends_on`` declarations are the production contract. References
    inferred from sources and foreign keys are still included so development
    plans are accurate, but production rejects an omitted declaration.
    """

    def __init__(
        self,
        profile: Profile = "dev",
        *,
        strict: bool = False,
        rule_policy: ModelIntegrityPolicy | None = None,
    ):
        if profile not in ("dev", "test", "production"):
            raise ValueError(f"Unknown compilation profile: {profile}")
        self.profile = profile
        self.strict = strict
        self.rule_policy = rule_policy

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
            issues.extend(
                ProjectIssue(
                    "MISSING_UPSTREAM",
                    "error",
                    f"declared upstream '{upstream}' is not part of the project",
                    config.table_name,
                )
                for upstream in sorted(missing)
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

        if cycle := self._find_cycle(nodes):
            issues.append(
                ProjectIssue(
                    "DEPENDENCY_CYCLE",
                    "error",
                    " -> ".join(cycle),
                    cycle[0],
                )
            )

        issues.extend(self._model_integrity_issues(nodes))
        errors = [issue for issue in issues if issue.severity == "error"]
        if errors:
            raise ProjectValidationError(errors)

        return CompiledProject(
            nodes=nodes,
            levels=self._topological_levels(nodes),
            issues=tuple(issues),
            model_integrity_summary=_summaries(issues),
        )

    def _model_integrity_issues(
        self, nodes: dict[str, CompiledPipeline]
    ) -> list[ProjectIssue]:
        """Run ADR-003 cross-table checks; resolve severity and suppression.

        Resolution order (ADR-003 §Decision 8; each layer only narrows):
          1. policy.enabled == false  -> skip, emit RULE_DISABLED.
          2. policy.severity          -> override the finding baseline.
          3. profile                  -> test/production error-promote.
          4. --strict                 -> force error in every profile.
          5. modeling_exceptions      -> suppress, emit EXCEPTION_APPROVED.
        """
        configs = {name: pipeline.config for name, pipeline in nodes.items()}
        policy = self.rule_policy
        rule_params: dict[str, dict] = {}
        if policy:
            for rule in policy.rules:
                rule_params[rule.code] = dict(rule.params)
        findings = check_project(configs, rule_params=rule_params)
        issues: list[ProjectIssue] = []
        for finding in findings:
            rule_policy = policy.policy_for(finding.code) if policy else None
            if rule_policy is not None and rule_policy.enabled is False:
                issues.append(self._rule_disabled_issue(finding))
                continue
            waived_by = _waiving_table(configs, finding.code, finding.column)
            if waived_by is not None:
                issues.extend(
                    self._exception_approved_issues(configs, finding, waived_by)
                )
                continue
            issues.append(
                ProjectIssue(
                    finding.code,
                    self._resolved_severity(finding, rule_policy),
                    finding.message,
                    finding.pipeline,
                    fixability=finding.fixability,
                    fix=finding.fix,
                )
            )
        return issues

    def _rule_disabled_issue(self, finding) -> ProjectIssue:
        """A disabled rule is not invisible (ADR-003 §Decision 8): notice."""
        return ProjectIssue(
            "RULE_DISABLED",
            "warning",
            f"rule '{finding.code}' is disabled by target policy; "
            f"suppressed finding on {finding.pipeline}"
            + (f", column '{finding.column}'" if finding.column else ""),
            finding.pipeline,
        )

    def _exception_approved_issues(
        self, configs: dict[str, TableConfig], finding, waived_by: str
    ) -> list[ProjectIssue]:
        """Suppressed-by-exception findings emit a notice in error contexts."""
        error_profiles = ("test", "production")
        if not (finding.severity == "error" or self.profile in error_profiles):
            return []
        ref = next(
            (
                exception.decision_ref
                for exception in configs[waived_by].modeling_exceptions
                if exception.code == finding.code
                and (finding.column is None or finding.column in exception.columns)
            ),
            None,
        )
        suffix = f" via decision_ref={ref}" if ref else ""
        return [
            ProjectIssue(
                "EXCEPTION_APPROVED",
                "warning",
                f"suppressed {finding.code} for column '{finding.column}'{suffix}",
                waived_by,
            )
        ]

    def _resolved_severity(self, finding, rule_policy) -> Severity:
        """ADR-003 §Decision 8 order: policy severity, then profile, strict wins."""
        error_profiles = ("test", "production")
        if self.strict:
            return "error"
        if rule_policy is not None and rule_policy.severity is not None:
            severity: Severity = rule_policy.severity
            return severity
        if self.profile in error_profiles:
            return "error"
        return "error" if finding.severity == "error" else "warning"

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


def _column_in_exception(config: TableConfig, code: str, column: str | None) -> bool:
    """True when a modeling exception covers (code, column).

    Table-level findings carry ``column=None``; any entry with the matching
    code waives them (at least one column entry is required by the model).
    """
    for exception in config.modeling_exceptions:
        if exception.code != code:
            continue
        if column is None or column in exception.columns:
            return True
    return False


def _waiving_table(
    configs: dict[str, TableConfig], code: str, column: str | None
) -> str | None:
    """The table whose modeling_exceptions waive this finding, if any.

    The anchored table (``finding.pipeline``) is checked first, then any other
    project table: a cross-table finding can be waived by the ledger entry of
    whichever table the modeler annotated (ADR-003 §Decision 4).
    """
    if column is None:
        return None
    waived = [
        name
        for name, config in configs.items()
        if _column_in_exception(config, code, column)
    ]
    return waived[0] if waived else None


def _summaries(issues: Sequence[ProjectIssue]) -> str:
    """Sourcery-style overview line for the model-integrity findings (ADR-003 §6)."""
    integrity = [issue for issue in issues if issue.code in _INTEGRITY_CODES]
    approved = [issue for issue in issues if issue.code == "EXCEPTION_APPROVED"]
    disabled = [issue for issue in issues if issue.code == "RULE_DISABLED"]
    if not integrity and not approved and not disabled:
        return "model-integrity: no issues"
    errors = [issue for issue in integrity if issue.severity == "error"]
    warnings = [issue for issue in integrity if issue.severity == "warning"]
    parts = [f"{len(errors)} error", f"{len(warnings)} warning"]
    fixable = [
        issue
        for issue in integrity
        if issue.fixability in ("auto_fixable", "suggest_fix")
    ]
    if fixable:
        parts.append(f"{len(fixable)} fixable")
    if approved:
        parts.append(f"{len(approved)} exception-approved")
    if disabled:
        parts.append(f"{len(disabled)} rule-disabled")
    counted = len(integrity) + len(disabled)
    return f"model-integrity: {counted} issues — {', '.join(parts)}"


_INTEGRITY_CODES = frozenset(
    {
        "COLUMN_SEMANTICS_CONFLICT",
        "FACT_DIMENSION_ATTRIBUTE",
        "GRAIN_KEY_MISMATCH",
        "MEASURE_ADDITIVITY_MISSING",
        "INCREMENTAL_LOAD_FRAGILE",
        "MISSING_REFERENCE_TARGET",
        "ORPHAN_REFERENCE",
        "MISSING_DESCRIPTION",
    }
)
