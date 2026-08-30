"""Cross-table model-integrity checks over a normalized project graph.

Governed by ``docs/adr/ADR-003-model-integrity-validator.md``. Everything in
this module is spark-free: stdlib plus ``kimball.common.config`` imports only.

Design (dbt-manifest shaped, kept typed): ``compile()`` in the caller turns
YAML configs into a ``ProjectGraph`` **once** — nodes, typed FK edges, and
column indexes. Each integrity rule is then a small query over that graph
instead of re-deriving indexes from raw configs. Findings carry a per-rule
fixability classification; suppression belongs to the compiler (ADR-003
§Decision 4). Nothing here mutates configuration.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from difflib import get_close_matches
from typing import Literal

from kimball.common.config import MODEL_INTEGRITY_CODES, TableConfig

IssueSeverity = Literal["error", "warning"]
Fixability = Literal["auto_fixable", "suggest_fix", "decision_required"]
CheckFn = Callable[..., list["ModelIssue"]]


@dataclass(frozen=True)
class RuleSpec:
    """Registry entry for one integrity rule (ADR-003 §Decision 8)."""

    code: str
    description: str
    default_severity: IssueSeverity
    params: frozenset[str] = frozenset()


_RULE_INCREMENTAL_PARAMS = frozenset({"require_primary_keys"})

RULE_SPECS: dict[str, RuleSpec] = {
    "COLUMN_SEMANTICS_CONFLICT": RuleSpec(
        code="COLUMN_SEMANTICS_CONFLICT",
        description=(
            "Column name reused across tables with diverging descriptions "
            "or conflicting declared contract types."
        ),
        default_severity="warning",
    ),
    "FACT_DIMENSION_ATTRIBUTE": RuleSpec(
        code="FACT_DIMENSION_ATTRIBUTE",
        description=(
            "A fact declares a column that is a descriptive attribute of a "
            "referenced dimension."
        ),
        default_severity="warning",
    ),
    "GRAIN_KEY_MISMATCH": RuleSpec(
        code="GRAIN_KEY_MISMATCH",
        description=(
            "A fact FK resolves via a key the referenced dimension does not "
            "declare as a grain key (surrogate, durable for type7, natural)."
        ),
        default_severity="warning",
    ),
    "MEASURE_ADDITIVITY_MISSING": RuleSpec(
        code="MEASURE_ADDITIVITY_MISSING",
        description=(
            "A semi-additive measure lists non-additive dimensions the fact "
            "does not reference (and that are not grain columns)."
        ),
        default_severity="warning",
    ),
    "INCREMENTAL_LOAD_FRAGILE": RuleSpec(
        code="INCREMENTAL_LOAD_FRAGILE",
        description=(
            "A CDF source without primary_keys has no dedup basis, or a "
            "hard-delete strategy can strand referenced downstream tables."
        ),
        default_severity="warning",
        params=_RULE_INCREMENTAL_PARAMS,
    ),
    "MISSING_REFERENCE_TARGET": RuleSpec(
        code="MISSING_REFERENCE_TARGET",
        description=("A FK references a table outside the project or a non-dimension."),
        default_severity="error",
    ),
    "ORPHAN_REFERENCE": RuleSpec(
        code="ORPHAN_REFERENCE",
        description=(
            "A FK dimension_key is not a key the referenced dimension declares."
        ),
        default_severity="error",
    ),
    "MISSING_DESCRIPTION": RuleSpec(
        code="MISSING_DESCRIPTION",
        description=(
            "A table or declared column ships without YAML-owned "
            "documentation, so the catalog entry (or the semantics checks) "
            "have no signal."
        ),
        default_severity="warning",
    ),
}


@dataclass(frozen=True)
class FixSuggestion:
    """A proposed metadata change. Advisory: never applied by the compiler."""

    field: str
    old: str | tuple[str, ...] | None = None
    new: str | tuple[str, ...] | None = None
    candidates: tuple[str, ...] = ()


@dataclass(frozen=True)
class ModelIssue:
    """A cross-table finding; converts into a compiler ``ProjectIssue``."""

    code: str
    severity: IssueSeverity
    message: str
    pipeline: str
    column: str | None = None
    fixability: Fixability = "decision_required"
    fix: FixSuggestion | None = None


@dataclass(frozen=True)
class ForeignKeyEdge:
    """A typed fact->dimension edge with the resolution metadata attached."""

    pipeline: str
    column: str
    references: str
    dimension_key: str | None = None
    relationship: str = "standard"
    durable_column: str | None = None
    identity_map: str | None = None


@dataclass(frozen=True)
class ProjectGraph:
    """Normalized, spark-free view of the whole project (the model manifest).

    Built once per compilation; every integrity rule queries this instead of
    re-deriving indexes from raw ``TableConfig`` objects.
    """

    nodes: dict[str, TableConfig]
    fk_edges: tuple[ForeignKeyEdge, ...] = ()
    downstream: dict[str, set[str]] = field(default_factory=dict)
    descriptions: dict[str, dict[str, str]] = field(default_factory=dict)
    column_types: dict[str, dict[str, str]] = field(default_factory=dict)
    column_tables: dict[str, set[str]] = field(default_factory=dict)

    @property
    def dimensions(self) -> dict[str, TableConfig]:
        return {
            name: config
            for name, config in self.nodes.items()
            if config.table_type == "dimension"
        }

    def fk_edges_of(self, table: str) -> tuple[ForeignKeyEdge, ...]:
        return tuple(edge for edge in self.fk_edges if edge.pipeline == table)

    def waives(self, table: str, code: str, column: str | None) -> bool:
        """True when ``table``'s modeling_exceptions cover (code, column).

        Table-level findings carry ``column=None``; any entry with the
        matching code waives them (the model requires >=1 column entry).
        """
        config = self.nodes.get(table)
        if config is None:
            return False
        for exception in config.modeling_exceptions:
            if exception.code != code:
                continue
            if column is None or column in exception.columns:
                return True
        return False

    def any_waives(self, code: str, column: str | None) -> str | None:
        """Name of the first project table waiving (code, column), if any.

        Cross-table findings may be waived by whichever table the modeler
        annotated (ADR-003 §Decision 4).
        """
        if column is None:
            return None
        for name in sorted(self.nodes):
            if self.waives(name, code, column):
                return name
        return None

    def declared_types(self, table: str) -> dict[str, str]:
        """Column -> declared contract type across the table's sources."""
        types: dict[str, str] = {}
        for source in self.nodes[table].sources:
            contract = source.contract
            if contract is None:
                continue
            for column, column_config in contract.schema_.items():
                types.setdefault(column, column_config.type)
        return types


_RESERVED_PREFIXES = ("__",)
_KEY_SUFFIXES = ("_sk", "_dk")
_GENERIC_ID_COLUMNS = frozenset({"id", "key", "version"})


def _is_reserved_column(column: str) -> bool:
    """System, surrogate/durable-key, and generic-id columns carry no semantics."""
    lowered = column.lower()
    return (
        lowered.startswith(_RESERVED_PREFIXES)
        or lowered.endswith(_KEY_SUFFIXES)
        or lowered in _GENERIC_ID_COLUMNS
    )


def _build_graph(nodes: dict[str, TableConfig]) -> ProjectGraph:
    edges: list[ForeignKeyEdge] = []
    downstream: dict[str, set[str]] = {}
    descriptions: dict[str, dict[str, str]] = {}
    column_types: dict[str, dict[str, str]] = {}
    column_tables: dict[str, set[str]] = {}

    for name, config in sorted(nodes.items()):
        for fk in config.foreign_keys or []:
            if not fk.references:
                continue
            edges.append(
                ForeignKeyEdge(
                    pipeline=name,
                    column=fk.column,
                    references=fk.references,
                    dimension_key=fk.dimension_key,
                    relationship=fk.relationship,
                    durable_column=fk.durable_column,
                    identity_map=(fk.lookup.identity_map if fk.lookup else None),
                )
            )
            if fk.references != name:
                downstream.setdefault(fk.references, set()).add(name)
        for upstream in config.depends_on:
            downstream.setdefault(upstream, set()).add(name)

        for column, text in config.column_descriptions.items():
            if not _is_reserved_column(column):
                descriptions.setdefault(column, {})[name] = text
        for source in config.sources:
            contract = source.contract
            if contract is None:
                continue
            for column, column_config in contract.schema_.items():
                column_types.setdefault(column, {})[name] = column_config.type
                column_tables.setdefault(column, set()).add(name)

    return ProjectGraph(
        nodes=dict(nodes),
        fk_edges=tuple(edges),
        downstream=downstream,
        descriptions=descriptions,
        column_types=column_types,
        column_tables=column_tables,
    )


def _check_column_semantics(graph: ProjectGraph, **_params) -> list[ModelIssue]:
    """Flag column names reused with diverging descriptions or declared types.

    Description drift is a warning (wording may legitimately evolve); a
    contract type clash is an error. Findings are emitted per column once,
    anchored on the first table declaring the column, to avoid N-way
    duplicates. Exception resolution is the compiler's job (ADR-003 §Decision
    4): the engine reports, the gate suppresses.
    """
    issues: list[ModelIssue] = []
    for column in sorted(set(graph.descriptions) | set(graph.column_types)):
        desc_map = graph.descriptions.get(column, {})
        type_map = graph.column_types.get(column, {})

        if len(set(desc_map.values())) > 1:
            for name in sorted(desc_map):
                others = {
                    other: text
                    for other, text in desc_map.items()
                    if other != name and text != desc_map[name]
                }
                if others:
                    issues.append(
                        ModelIssue(
                            code="COLUMN_SEMANTICS_CONFLICT",
                            severity="warning",
                            message=(
                                f"column '{column}' is described differently "
                                f"across tables: {name} says "
                                f"'{desc_map[name]}'; {sorted(others)} say "
                                f"{sorted(set(others.values()))}"
                            ),
                            pipeline=name,
                            column=column,
                            fixability="decision_required",
                        )
                    )
                    break

        if len(set(type_map.values())) > 1:
            for name in sorted(type_map):
                others = {
                    other: other_type
                    for other, other_type in type_map.items()
                    if other != name and other_type != type_map[name]
                }
                if others:
                    issues.append(
                        ModelIssue(
                            code="COLUMN_SEMANTICS_CONFLICT",
                            severity="error",
                            message=(
                                f"column '{column}' has conflicting declared "
                                f"types: {name} declares '{type_map[name]}'; "
                                f"{sorted(others)} declare "
                                f"{sorted(set(others.values()))}"
                            ),
                            pipeline=name,
                            column=column,
                            fixability="decision_required",
                        )
                    )
    return issues


def _check_fact_dimension_attribute(graph: ProjectGraph, **_params) -> list[ModelIssue]:
    """Flag descriptive dimension attributes denormalized into fact columns.

    Conservative by design (ADR-003): fires only when the fact explicitly
    declares the column in ``column_descriptions`` and the referenced
    dimension declares it descriptively. Keys, FK outputs, degenerate
    dimensions, and reserved columns are always allowed.
    """
    issues: list[ModelIssue] = []
    for name, config in sorted(graph.nodes.items()):
        if config.table_type != "fact":
            continue
        fk_outputs = {
            column
            for edge in graph.fk_edges_of(name)
            for column in (edge.column, edge.durable_column)
            if column
        }
        targets = {edge.references for edge in graph.fk_edges_of(name)}
        for target in sorted(targets):
            dim = graph.nodes.get(target)
            if dim is None or dim.table_type != "dimension":
                continue
            dim_keys = {dim.surrogate_key, dim.durable_key, *(dim.natural_keys or [])}
            for column in sorted(dim.column_descriptions):
                if column not in config.column_descriptions:
                    continue
                if (
                    _is_reserved_column(column)
                    or column in dim_keys
                    or column in fk_outputs
                    or column in config.degenerate_dimensions
                ):
                    continue
                issues.append(
                    ModelIssue(
                        code="FACT_DIMENSION_ATTRIBUTE",
                        severity="warning",
                        message=(
                            f"fact {name} declares '{column}', a descriptive "
                            f"attribute of referenced dimension {target}"
                        ),
                        pipeline=name,
                        column=column,
                        fixability="decision_required",
                    )
                )
    return issues


def _check_grain_and_measures(graph: ProjectGraph, **_params) -> list[ModelIssue]:
    """FKs resolve to the referenced dimension's declared key; measures add up."""
    issues: list[ModelIssue] = []
    for name, config in sorted(graph.nodes.items()):
        if config.table_type != "fact":
            continue
        for edge in graph.fk_edges_of(name):
            dim = graph.nodes.get(edge.references)
            if dim is None or dim.table_type != "dimension" or not edge.dimension_key:
                continue
            # A type7 relationship may resolve via either the durable key
            # (current-state joins) or the surrogate key (point-in-time
            # joins); both are declared keys of the dimension. Standard
            # relationships must use the surrogate or a natural key.
            allowed = {dim.surrogate_key, *(dim.natural_keys or [])}
            if edge.relationship == "type7" and dim.durable_key:
                allowed.add(dim.durable_key)
            if edge.dimension_key in allowed:
                continue
            expected = (
                dim.durable_key
                if edge.relationship == "type7" and dim.durable_key
                else dim.surrogate_key
            )
            issues.append(
                ModelIssue(
                    code="GRAIN_KEY_MISMATCH",
                    severity="warning",
                    message=(
                        f"fact {name} FK '{edge.column}' resolves via "
                        f"'{edge.dimension_key}' but dimension {edge.references} "
                        f"declares {sorted(k for k in allowed if k)} as its keys"
                    ),
                    pipeline=name,
                    column=edge.column,
                    fixability="auto_fixable",
                    fix=FixSuggestion(
                        field=f"foreign_keys[{edge.column}].dimension_key",
                        old=edge.dimension_key,
                        new=expected,
                    )
                    if expected
                    else None,
                )
            )

        declared_keys = {
            edge.dimension_key for edge in graph.fk_edges_of(name) if edge.dimension_key
        }
        referenced_dims = {edge.references for edge in graph.fk_edges_of(name)}
        for measure in config.measures:
            if measure.additivity != "semi_additive":
                continue
            # A semi-additive measure is also legitimately keyed by the
            # fact's own grain: merge keys and degenerate dimensions are the
            # "date/level" the value cannot be summed across.
            unknown = sorted(
                dimension
                for dimension in measure.non_additive_dimensions
                if dimension not in declared_keys
                and dimension not in referenced_dims
                and dimension not in grain_keys_of(config)
            )
            if not unknown:
                continue
            issues.append(
                ModelIssue(
                    code="MEASURE_ADDITIVITY_MISSING",
                    severity="warning",
                    message=(
                        f"semi-additive measure '{measure.name}' on {name} lists "
                        f"non-additive dimensions {unknown} that the fact does "
                        f"not reference"
                    ),
                    pipeline=name,
                    column=measure.name,
                    fixability="suggest_fix",
                    fix=FixSuggestion(
                        field=(f"measures[{measure.name}].non_additive_dimensions"),
                        old=tuple(measure.non_additive_dimensions),
                        new=tuple(
                            dimension
                            for dimension in measure.non_additive_dimensions
                            if dimension not in unknown
                        ),
                        candidates=tuple(
                            sorted(
                                declared_keys | referenced_dims | grain_keys_of(config)
                            )
                        ),
                    ),
                )
            )
    return issues


def grain_keys_of(config: TableConfig) -> set[str]:
    """Grain-level columns of a fact: merge keys plus degenerate dimensions."""
    return {*(config.merge_keys or []), *config.degenerate_dimensions}


def _check_incremental_safety(
    graph: ProjectGraph, *, require_primary_keys: bool = True
) -> list[ModelIssue]:
    """Incremental sources must be resumable; hard deletes must not strand.

    Params (ADR-003 §Decision 8):
      require_primary_keys — key-less CDF sources are flagged (default
      ``True``); set ``False`` for teams that dedupe downstream instead.
    """
    issues: list[ModelIssue] = []
    for name, config in sorted(graph.nodes.items()):
        for source in config.sources:
            if not require_primary_keys or source.cdc_strategy != "cdf":
                continue
            if source.primary_keys:
                continue
            schema_columns = (
                tuple(sorted(source.contract.schema_))
                if source.contract is not None
                else ()
            )
            issues.append(
                ModelIssue(
                    code="INCREMENTAL_LOAD_FRAGILE",
                    severity="warning",
                    message=(
                        f"source '{source.name}' of {name} uses "
                        f"cdc_strategy='cdf' without primary_keys; incremental "
                        f"resumption has no dedup basis. Pick dedup keys, or "
                        f"switch to a full-load strategy (a full reload via "
                        f"`kimball recover --full-reload` remains available)."
                    ),
                    pipeline=name,
                    column=source.name,
                    fixability="suggest_fix",
                    fix=FixSuggestion(
                        field=f"sources[{source.name}].primary_keys",
                        candidates=schema_columns,
                    ),
                )
            )

        if config.delete_strategy == "hard" and graph.downstream.get(name):
            issues.append(
                ModelIssue(
                    code="INCREMENTAL_LOAD_FRAGILE",
                    severity="warning",
                    message=(
                        f"{name} uses delete_strategy='hard' but is referenced "
                        f"by {sorted(graph.downstream[name])}; hard deletes can "
                        f"strand downstream keys. Use soft delete, or record an "
                        f"exception."
                    ),
                    pipeline=name,
                    column=None,
                    fixability="decision_required",
                )
            )
    return issues


def _suggest_reference_target(
    references: str, graph: ProjectGraph
) -> tuple[str | None, tuple[str, ...]]:
    """Return the unique case-insensitive match, or the close-match set."""
    case_folded = {name.lower(): name for name in graph.dimensions}
    exact_folded = case_folded.get(references.lower())
    if exact_folded and exact_folded != references:
        return exact_folded, (exact_folded,)
    close = tuple(
        sorted(get_close_matches(references, sorted(case_folded), n=3, cutoff=0.85))
    )
    if len(close) == 1:
        return case_folded[close[0]], close
    return None, close


def _check_reference_completeness(graph: ProjectGraph, **_params) -> list[ModelIssue]:
    """Every FK must resolve to a dimension in the project via a declared key."""
    issues: list[ModelIssue] = []
    for name, config in sorted(graph.nodes.items()):
        if config.table_type != "fact":
            continue
        for edge in graph.fk_edges_of(name):
            dim = graph.nodes.get(edge.references)
            if dim is None:
                canonical, candidates = _suggest_reference_target(
                    edge.references, graph
                )
                if canonical is not None:
                    issues.append(
                        ModelIssue(
                            code="MISSING_REFERENCE_TARGET",
                            severity="error",
                            message=(
                                f"fact {name} FK '{edge.column}' references "
                                f"'{edge.references}' which is not in the "
                                f"project; a dimension named '{canonical}' "
                                f"exists (case/typo difference?)"
                            ),
                            pipeline=name,
                            column=edge.column,
                            fixability="auto_fixable",
                            fix=FixSuggestion(
                                field=f"foreign_keys[{edge.column}].references",
                                old=edge.references,
                                new=canonical,
                            ),
                        )
                    )
                else:
                    issues.append(
                        ModelIssue(
                            code="MISSING_REFERENCE_TARGET",
                            severity="error",
                            message=(
                                f"fact {name} FK '{edge.column}' references "
                                f"'{edge.references}' which is not part of the "
                                f"project"
                            ),
                            pipeline=name,
                            column=edge.column,
                            fixability="suggest_fix",
                            fix=FixSuggestion(
                                field=f"foreign_keys[{edge.column}].references",
                                old=edge.references,
                                candidates=candidates,
                            )
                            if candidates
                            else None,
                        )
                    )
                continue
            if dim.table_type != "dimension":
                issues.append(
                    ModelIssue(
                        code="MISSING_REFERENCE_TARGET",
                        severity="error",
                        message=(
                            f"fact {name} FK '{edge.column}' references "
                            f"'{edge.references}' which is a fact, not a "
                            f"dimension"
                        ),
                        pipeline=name,
                        column=edge.column,
                        fixability="decision_required",
                    )
                )
                continue

            if not edge.dimension_key:
                continue
            dim_keys = {
                dim.surrogate_key,
                dim.durable_key,
                *(dim.natural_keys or []),
            } - {None}
            if edge.dimension_key in dim_keys:
                continue
            issues.append(
                ModelIssue(
                    code="ORPHAN_REFERENCE",
                    severity="error",
                    message=(
                        f"fact {name} FK '{edge.column}' uses dimension_key "
                        f"'{edge.dimension_key}' which {edge.references} does "
                        f"not declare (declared: {sorted(k for k in dim_keys if k)})"
                    ),
                    pipeline=name,
                    column=edge.column,
                    fixability="auto_fixable",
                    fix=FixSuggestion(
                        field=f"foreign_keys[{edge.column}].dimension_key",
                        old=edge.dimension_key,
                        new=dim.surrogate_key,
                    )
                    if dim.surrogate_key
                    else None,
                )
            )

        for edge in graph.fk_edges_of(name):
            if not edge.identity_map:
                continue
            source_names = {source.name for source in config.sources}
            if (
                edge.identity_map not in graph.nodes
                and edge.identity_map not in source_names
            ):
                issues.append(
                    ModelIssue(
                        code="MISSING_REFERENCE_TARGET",
                        severity="error",
                        message=(
                            f"fact {name} FK '{edge.column}' lookup.identity_map "
                            f"'{edge.identity_map}' is not declared in the "
                            f"project or its sources"
                        ),
                        pipeline=name,
                        column=edge.column,
                        fixability="decision_required",
                    )
                )
    return issues


def _check_missing_description(graph: ProjectGraph, **_params) -> list[ModelIssue]:
    """Flag tables and output columns that lack YAML-owned documentation.

    The DescriptionManager can only publish what YAML declares, so a missing
    entry means an undocumented catalog asset (ADR-003 Motivation). Documented
    columns come from ``column_descriptions``; FK output columns are the
    model's declared outputs in addition to contract columns.
    """
    issues: list[ModelIssue] = []
    for name, config in sorted(graph.nodes.items()):
        if config.table_description is None:
            issues.append(
                ModelIssue(
                    code="MISSING_DESCRIPTION",
                    severity="warning",
                    message=(
                        f"{name} has no table_description; it will be "
                        f"undocumented in the catalog. Add one, or record an "
                        f"exception."
                    ),
                    pipeline=name,
                    column=None,
                    fixability="decision_required",
                )
            )
        declared_columns: set[str] = set(config.column_descriptions)
        declared_columns.update(graph.declared_types(name))
        declared_columns.update(
            column
            for edge in graph.fk_edges_of(name)
            for column in (edge.column, edge.durable_column)
            if column
        )
        declared_columns -= _is_reserved_or_key(name, config, graph)
        for column in sorted(declared_columns - set(config.column_descriptions)):
            if _is_reserved_column(column):
                continue
            issues.append(
                ModelIssue(
                    code="MISSING_DESCRIPTION",
                    severity="warning",
                    message=(
                        f"{name} column '{column}' has no description; add it "
                        f"to column_descriptions so the catalog is complete"
                    ),
                    pipeline=name,
                    column=column,
                    fixability="suggest_fix",
                    fix=FixSuggestion(
                        field=f"column_descriptions[{column}]",
                    ),
                )
            )
    return issues


def _is_reserved_or_key(
    table: str, config: TableConfig, graph: ProjectGraph
) -> set[str]:
    """Columns excused from description coverage: keys, FK outputs, system cols."""
    excused: set[str] = set()
    for column in config.column_descriptions:
        excused.add(column)
    excused.update(config.natural_keys or [])
    if config.surrogate_key:
        excused.add(config.surrogate_key)
    if config.durable_key:
        excused.add(config.durable_key)
    excused.update(
        column
        for edge in graph.fk_edges_of(table)
        for column in (edge.column, edge.durable_column)
        if column
    )
    return excused


# Dispatch table: each integrity code reports under the function that emits
# its findings. A function may serve several codes (reference completeness
# emits MISSING_REFERENCE_TARGET + ORPHAN_REFERENCE; grain/measures emits
# GRAIN_KEY_MISMATCH + MEASURE_ADDITIVITY_MISSING).
_CHECKS: dict[str, CheckFn] = {
    "COLUMN_SEMANTICS_CONFLICT": _check_column_semantics,
    "FACT_DIMENSION_ATTRIBUTE": _check_fact_dimension_attribute,
    "GRAIN_KEY_MISMATCH": _check_grain_and_measures,
    "MEASURE_ADDITIVITY_MISSING": _check_grain_and_measures,
    "INCREMENTAL_LOAD_FRAGILE": _check_incremental_safety,
    "MISSING_REFERENCE_TARGET": _check_reference_completeness,
    "ORPHAN_REFERENCE": _check_reference_completeness,
    "MISSING_DESCRIPTION": _check_missing_description,
}


def build_graph(nodes: dict[str, TableConfig]) -> ProjectGraph:
    """Normalize raw configs into the queryable project graph (public for tests)."""
    return _build_graph(nodes)


def check_project(
    nodes: dict[str, TableConfig],
    rule_params: dict[str, dict] | None = None,
) -> list[ModelIssue]:
    """Run every cross-table check over the compiled project's nodes.

    ``rule_params`` carries validated per-rule policy params (ADR-003 §Decision
    8); each check receives the params declared by its RuleSpec.

    Returns raw findings. Severity resolution (policy, profile, --strict,
    exception suppression, EXCEPTION_APPROVED emission) is the compiler's
    responsibility per ADR-003 §Decision 5: the engine stays a pure function of
    declared metadata.
    """
    if not nodes:
        return []
    graph = _build_graph(nodes)
    rule_params = rule_params or {}
    findings: list[ModelIssue] = []
    for check_fn in dict.fromkeys(_CHECKS.values()):
        # One run per check function, merging params from every rule the
        # function serves - prevents duplicate findings when two codes share
        # a function.
        params: dict = {}
        for code, serving in _CHECKS.items():
            if serving is check_fn:
                params.update(rule_params.get(code, {}))
        findings.extend(check_fn(graph, **params))
    return findings


__all__ = [
    "MODEL_INTEGRITY_CODES",
    "ForeignKeyEdge",
    "FixSuggestion",
    "Fixability",
    "IssueSeverity",
    "ModelIssue",
    "ProjectGraph",
    "RuleSpec",
    "RULE_SPECS",
    "build_graph",
    "check_project",
]
