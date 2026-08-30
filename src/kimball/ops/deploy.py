"""``kimball deploy`` command logic (ROADMAP 1.5) - the promotion gate.

Change classification is already provided by :func:`kimball.planning.manifest.
diff_manifests` (metadata_only / non_breaking / requires_validation /
requires_backfill / breaking). This module adds the **pre-flight** that the
refined roadmap mandates and the deploy decision:

* no target in the project may be in an inconsistent state (zombie, watermark
  drift, orphan commits) or under a suspected single-writer violation -
  deploying onto either is how you create a second writer;
* every source must exist, and CDF must be enabled where ``cdc_strategy: cdf``;
* config secret references (``env://`` / ``databricks://``) must be resolvable
  in the target environment;
* breaking changes block unless ``--allow-breaking``.

``kimball deploy`` is a read-only gate; actual manifest/bundle promotion is
done via the existing ``kimball manifest publish`` once the gate passes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from kimball.common.secrets import SecretResolutionError, SecretResolver
from kimball.ops.providers import OpsProviders
from kimball.ops.runtime_profile import RuntimeProfile
from kimball.ops.source_health import SourceHealthVerdict, assess_source_health
from kimball.ops.state_reconciler import ReconciliationVerdict, StateReconciler
from kimball.ops.writer_contract import WriterVerdict, check_writer_contract
from kimball.planning.manifest import ProjectPlan, diff_manifests

_INCONSISTENT_VERDICTS = frozenset(
    [
        ReconciliationVerdict.CONTROL_TABLE_MISSING,
        ReconciliationVerdict.TARGET_MISSING,
        ReconciliationVerdict.ZOMBIE_WITH_COMMITTED_DATA,
        ReconciliationVerdict.ZOMBIE_NO_COMMIT,
        ReconciliationVerdict.WATERMARK_AHEAD_OF_TARGET,
        ReconciliationVerdict.TARGET_AHEAD_OF_WATERMARK,
    ]
)


@dataclass(frozen=True)
class DeployBlocker:
    subject: str  # target or source table
    kind: str  # inconsistent_state | concurrent_writer | source_missing | cdf_disabled | secret_unresolved
    detail: str


@dataclass(frozen=True)
class PreflightReport:
    blockers: tuple[DeployBlocker, ...] = ()
    warnings: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.blockers


@dataclass
class DeployResult:
    plan: ProjectPlan
    preflight: PreflightReport
    blocked: bool
    reason: str | None
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "blocked": self.blocked,
            "reason": self.reason,
            "warnings": list(self.warnings),
            "changes": [
                {
                    "table": c.table_name,
                    "kind": c.kind,
                    "classification": c.classification,
                    "changed_fields": list(c.changed_fields),
                }
                for c in self.plan.changes
            ],
            "affected_tables": list(self.plan.affected_tables),
            "has_breaking_changes": self.plan.has_breaking_changes,
            "preflight": {
                "blockers": [
                    {"subject": b.subject, "kind": b.kind, "detail": b.detail}
                    for b in self.preflight.blockers
                ],
                "warnings": list(self.preflight.warnings),
            },
        }


def _check_secrets(
    secret_refs: tuple[str, ...],
    secret_resolver: SecretResolver | None,
) -> tuple[list[DeployBlocker], list[str]]:
    """Verify config secret references are resolvable in this environment.

    ``env://`` refs are checked against the environment (block if unset).
    ``databricks://`` refs require dbutils; if unavailable in this session they
    are warned (cannot verify) rather than blocked, since the actual run may
    execute with dbutils present.
    """
    blockers: list[DeployBlocker] = []
    warnings: list[str] = []
    if secret_resolver is None or not secret_refs:
        return blockers, warnings
    for ref in secret_refs:
        if (
            ref.startswith("databricks://")
            and not secret_resolver.can_resolve_databricks
        ):
            warnings.append(f"secret {ref}: cannot verify (no dbutils in this session)")
            continue
        try:
            secret_resolver.resolve(ref)
        except SecretResolutionError as exc:
            blockers.append(DeployBlocker(ref, "secret_unresolved", str(exc)))
    return blockers, warnings


def preflight(
    providers: OpsProviders,
    runtime: RuntimeProfile,
    targets: list[str],
    sources: list[tuple[str, bool]],
    *,
    secret_refs: tuple[str, ...] = (),
    secret_resolver: SecretResolver | None = None,
) -> PreflightReport:
    """Read-only pre-flight across every target and source in the project."""
    blockers: list[DeployBlocker] = []
    warnings: list[str] = []
    if runtime.is_serverless:
        warnings.append(
            "single-writer guard inactive on Serverless (commit tagging off; "
            "writer_contract returns UNKNOWN)"
        )
    reconciler = StateReconciler(providers.control, providers.history, runtime)

    for target in targets:
        rec = reconciler.reconcile(target)
        if rec.verdict in _INCONSISTENT_VERDICTS:
            blockers.append(
                DeployBlocker(
                    target,
                    "inconsistent_state",
                    f"{rec.verdict.value}: {rec.evidence}",
                )
            )
            continue  # writer check is unreliable on an inconsistent target
        control_state = providers.control.get_target_state(target)
        delta_state = providers.history.get_target_delta_state(target)
        known_ids = tuple(sorted({b.batch_id for b in control_state.batches}))
        writer = check_writer_contract(
            delta_state, known_ids, runtime.supports_commit_tagging
        )
        if writer.verdict is WriterVerdict.SUSPECTED_VIOLATION:
            blockers.append(
                DeployBlocker(
                    target,
                    "concurrent_writer",
                    f"{len(writer.suspicious_commits)} commit(s) with unknown batch_ids",
                )
            )

    # A shared source (e.g. a conformed dimension read by several facts) is
    # checked once; if any consumer requires CDF, treat the source as requiring.
    deduped: dict[str, bool] = {}
    for source_table, requires_cdf in sources:
        deduped[source_table] = deduped.get(source_table, False) or requires_cdf
    for source_table, requires_cdf in deduped.items():
        report = providers.sources.get_source_health(source_table, None)
        assess = assess_source_health(report)
        if assess.verdict is SourceHealthVerdict.MISSING:
            blockers.append(
                DeployBlocker(source_table, "source_missing", assess.detail)
            )
        elif requires_cdf and assess.verdict is SourceHealthVerdict.CDF_DISABLED:
            blockers.append(DeployBlocker(source_table, "cdf_disabled", assess.detail))
        elif assess.verdict is SourceHealthVerdict.UNKNOWN:
            warnings.append(f"source {source_table}: CDF metadata unavailable")

    s_blockers, s_warnings = _check_secrets(secret_refs, secret_resolver)
    blockers.extend(s_blockers)
    warnings.extend(s_warnings)
    return PreflightReport(tuple(blockers), tuple(warnings))


def deploy(
    previous_manifest: dict[str, Any],
    current_manifest: dict[str, Any],
    providers: OpsProviders,
    runtime: RuntimeProfile,
    targets: list[str],
    sources: list[tuple[str, bool]],
    *,
    allow_breaking: bool = False,
    secret_refs: tuple[str, ...] = (),
    secret_resolver: SecretResolver | None = None,
) -> DeployResult:
    plan = diff_manifests(previous_manifest, current_manifest)
    pre = preflight(
        providers,
        runtime,
        targets,
        sources,
        secret_refs=secret_refs,
        secret_resolver=secret_resolver,
    )

    # Non-blocking advisory: changes that require a backfill / full refresh.
    change_warnings = [
        f"{c.table_name}: requires backfill/full-refresh "
        f"({', '.join(c.changed_fields) or 'semantic change'})"
        for c in plan.changes
        if c.classification == "requires_backfill"
    ]

    breaking = [c for c in plan.changes if c.classification == "breaking"]
    if breaking and not allow_breaking:
        return DeployResult(
            plan,
            pre,
            blocked=True,
            reason="breaking changes present; re-run with --allow-breaking to override",
            warnings=change_warnings,
        )
    if not pre.ok:
        return DeployResult(
            plan,
            pre,
            blocked=True,
            reason=f"pre-flight failed: {len(pre.blockers)} blocker(s)",
            warnings=change_warnings,
        )
    return DeployResult(plan, pre, blocked=False, reason=None, warnings=change_warnings)
