"""``kimball explain`` command logic (ROADMAP 1.3).

Explains a failure or dangerous state from one of three entry points:

* ``--config``        - a compile-time / configuration failure (the common case:
  no ``etl_control`` row is written, because the run never reached the engine
  and the H4 optimisation skips synchronous RUNNING writes by default).
* ``--table``         - diagnose a target's current state via the harness.
* ``--batch-id``      - attribute a recorded failure to a specific batch and
  compare its recorded config / source-schema fingerprints to the current ones
  (drift detection) to answer "the world changed under you".

The diagnosis is categorised via :class:`StructuredError` and mapped to a
RUNBOOK procedure and a recommended recovery command.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from kimball.ops.errors import ErrorCategory, categorize, runbook_link_for
from kimball.ops.providers import OpsProviders, TargetControlState
from kimball.ops.runtime_profile import RuntimeProfile
from kimball.ops.source_health import SourceHealthVerdict, assess_source_health
from kimball.ops.state_reconciler import ReconciliationVerdict, StateReconciler
from kimball.ops.writer_contract import WriterVerdict, check_writer_contract

RUNBOOK = "docs/RUNBOOK.md"


@dataclass
class SourceDiagnosis:
    source_table: str
    verdict: SourceHealthVerdict
    detail: str
    schema_drift: bool


@dataclass
class ExplainReport:
    target_table: str
    entry_point: str  # "config" | "table"
    category: str  # ErrorCategory value, or "OK"
    reconciliation_verdict: str | None
    writer_verdict: str | None
    evidence: str
    remediation: str
    runbook_link: str | None
    recommended_recovery: str | None
    config_drift: bool = False
    sources: list[SourceDiagnosis] = field(default_factory=list)
    batch_error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_table": self.target_table,
            "entry_point": self.entry_point,
            "category": self.category,
            "reconciliation_verdict": self.reconciliation_verdict,
            "writer_verdict": self.writer_verdict,
            "evidence": self.evidence,
            "remediation": self.remediation,
            "runbook_link": self.runbook_link,
            "recommended_recovery": self.recommended_recovery,
            "config_drift": self.config_drift,
            "sources": [
                {
                    "source_table": s.source_table,
                    "verdict": s.verdict.value,
                    "detail": s.detail,
                    "schema_drift": s.schema_drift,
                }
                for s in self.sources
            ],
            "batch_error_message": self.batch_error_message,
        }


def explain_config_error(
    exc: BaseException, config_path: str | None = None
) -> ExplainReport:
    """Explain a compile-time / configuration failure (no etl_control row)."""
    category = categorize(exc)
    message = str(exc) or exc.__class__.__name__
    return ExplainReport(
        target_table="(config)",
        entry_point="config",
        category=category.value,
        reconciliation_verdict=None,
        writer_verdict=None,
        evidence=f"compile/config failure: {message}",
        remediation=_default_remediation(category),
        runbook_link=runbook_link_for(category),
        recommended_recovery=(
            f"fix the config{f' ({config_path})' if config_path else ''} "
            "and run `kimball validate --config <path> --target <env>`"
        ),
    )


def explain(
    target_table: str,
    providers: OpsProviders,
    runtime: RuntimeProfile,
    *,
    batch_id: str | None = None,
    current_config_fingerprint: str | None = None,
    history_limit: int = 200,
) -> ExplainReport:
    """Diagnose a target's current state and recommend a recovery action."""
    reconciler = StateReconciler(providers.control, providers.history, runtime)
    reconciliation = reconciler.reconcile(target_table)
    control_state = providers.control.get_target_state(target_table)
    delta_state = providers.history.get_target_delta_state(
        target_table, history_limit=history_limit
    )
    known_ids = tuple(sorted({b.batch_id for b in control_state.batches}))
    writer = check_writer_contract(
        delta_state, known_ids, runtime.supports_commit_tagging
    )

    # Recorded fingerprints come from the last SUCCESS batch (written at
    # batch_complete); FAILED/RUNNING rows carry none.
    # Inline _last_success_fingerprint: most recent SUCCESS batch's config_fingerprint.
    rec_candidates = sorted(
        [
            b
            for b in control_state.batches
            if b.status == "SUCCESS" and b.config_fingerprint is not None
        ],
        key=lambda b: b.completed_at or b.started_at or datetime.min,
        reverse=True,
    )
    recorded_config_fp = (
        rec_candidates[0].config_fingerprint if rec_candidates else None
    )
    config_drift = bool(
        current_config_fingerprint
        and recorded_config_fp
        and current_config_fingerprint != recorded_config_fp
    )

    sources: list[SourceDiagnosis] = []
    seen: set[str] = set()
    for source_table, recorded_src_fp in _last_success_source_fingerprints(
        control_state
    ):
        if source_table in seen:
            continue
        seen.add(source_table)
        report = providers.sources.get_source_health(
            source_table,
            max(
                (
                    b.last_processed_version
                    for b in control_state.batches
                    if b.source_table == source_table
                    and b.last_processed_version is not None
                ),
                default=None,
            ),
            recorded_src_fp,
        )
        assess = assess_source_health(report)
        sources.append(
            SourceDiagnosis(
                source_table,
                assess.verdict,
                assess.detail,
                schema_drift=assess.verdict is SourceHealthVerdict.SCHEMA_DRIFT,
            )
        )

    batch_error_message: str | None = None
    if batch_id is not None:
        for batch in control_state.batches:
            if batch.batch_id == batch_id:
                batch_error_message = batch.error_message
                break

    category, remediation, runbook, recovery = _diagnose(
        reconciliation.verdict,
        writer.verdict,
        sources,
        batch_error_message,
        config_drift,
        [c.batch_id for c in reconciliation.orphan_commits if c.batch_id],
    )

    return ExplainReport(
        target_table=target_table,
        entry_point="table",
        category=category.value,
        reconciliation_verdict=reconciliation.verdict.value,
        writer_verdict=writer.verdict.value,
        evidence=reconciliation.evidence,
        remediation=remediation,
        runbook_link=runbook,
        recommended_recovery=recovery,
        config_drift=config_drift,
        sources=sources,
        batch_error_message=batch_error_message,
    )


def _diagnose(
    rec_verdict: ReconciliationVerdict,
    writer_verdict: WriterVerdict,
    sources: list[SourceDiagnosis],
    batch_error_message: str | None,
    config_drift: bool,
    orphan_commit_ids: list[str],
) -> tuple[ErrorCategory, str, str | None, str | None]:
    # 1. Config drift dominates: the YAML the operator is looking at is not
    #    what ran.
    if config_drift:
        return (
            ErrorCategory.CONFIG,
            "The config has changed since the recorded run. Re-validate and re-run.",
            f"{RUNBOOK}#config",
            "kimball validate --config <path> --target <env>",
        )
    # 2. Source schema drift.
    if any(s.schema_drift for s in sources):
        src = next(s.source_table for s in sources if s.schema_drift)
        return (
            ErrorCategory.SCHEMA_DRIFT,
            f"Source {src} schema changed since the last successful run.",
            f"{RUNBOOK}#schema-drift",
            "Update the config to match the source, or full-reload.",
        )
    # 3. CDF gap (source vacuumed past watermark).
    if any(s.verdict is SourceHealthVerdict.CDF_GAP for s in sources):
        src = next(
            s.source_table for s in sources if s.verdict is SourceHealthVerdict.CDF_GAP
        )
        return (
            ErrorCategory.CDF_GAP,
            f"Source {src} was VACUUMed past the watermark; incremental resume will skip data.",
            f"{RUNBOOK}#cdf-gap",
            "kimball recover --table <target> --full-reload",
        )
    # 4. Source missing.
    if any(s.verdict is SourceHealthVerdict.MISSING for s in sources):
        src = next(
            s.source_table for s in sources if s.verdict is SourceHealthVerdict.MISSING
        )
        return (
            ErrorCategory.SOURCE_UNAVAILABLE,
            f"Source {src} is missing or unreachable from this target's catalog.",
            f"{RUNBOOK}#source-unavailable",
            f"Confirm source {src} exists in this environment.",
        )
    # 5. Reconciliation verdicts (static table).
    _VERDICT_MAP: dict[ReconciliationVerdict, tuple[ErrorCategory, str, str, str]] = {
        ReconciliationVerdict.ZOMBIE_WITH_COMMITTED_DATA: (
            ErrorCategory.RECOVERY,
            "A crashed batch left committed data on the target.",
            f"{RUNBOOK}#zombie-with-committed-data",
            "kimball recover --table <target>",
        ),
        ReconciliationVerdict.ZOMBIE_NO_COMMIT: (
            ErrorCategory.RECOVERY,
            "A RUNNING batch has no committed data (or tagging is off).",
            f"{RUNBOOK}#zombie-no-commit",
            "kimball recover --table <target>",
        ),
        ReconciliationVerdict.WATERMARK_AHEAD_OF_TARGET: (
            ErrorCategory.RECOVERY,
            "The watermark is ahead of the target (post-rollback drift).",
            f"{RUNBOOK}#watermark-ahead-of-target",
            "kimball recover --table <target> --rewind-watermark",
        ),
        ReconciliationVerdict.TARGET_AHEAD_OF_WATERMARK: (
            ErrorCategory.CONCURRENT_WRITER,
            "The target has commits the control table does not know about.",
            f"{RUNBOOK}#target-ahead-of-watermark",
            "kimball inspect --table <target>",
        ),
        ReconciliationVerdict.CONTROL_TABLE_MISSING: (
            ErrorCategory.SOURCE_UNAVAILABLE,
            "etl_control table not found for this environment.",
            f"{RUNBOOK}#control-table-missing",
            "Run `kimball run` once to create the control schema.",
        ),
        ReconciliationVerdict.TARGET_MISSING: (
            ErrorCategory.SOURCE_UNAVAILABLE,
            "Target table does not exist.",
            f"{RUNBOOK}#target-missing",
            "Run the pipeline to create the target.",
        ),
    }
    if rec_verdict in _VERDICT_MAP:
        if (
            rec_verdict is ReconciliationVerdict.TARGET_AHEAD_OF_WATERMARK
            and orphan_commit_ids
        ):
            return (
                ErrorCategory.CONCURRENT_WRITER,
                f"The target has {len(orphan_commit_ids)} orphan commit(s) the "
                "control table does not know about.",
                f"{RUNBOOK}#target-ahead-of-watermark",
                f"kimball recover --table <target> --batch-id {orphan_commit_ids[0]} --force",
            )
        return _VERDICT_MAP[rec_verdict]
    # 6. Writer-contract violation.
    if writer_verdict is WriterVerdict.SUSPECTED_VIOLATION:
        return (
            ErrorCategory.CONCURRENT_WRITER,
            "Commits tagged with unknown batch_ids - single-writer violation suspected.",
            f"{RUNBOOK}#concurrent-writer",
            "kimball recover --table <target> --force",
        )
    # 7. Fall back to the batch's recorded error message.
    if batch_error_message:
        cat = _infer_category_from_text(batch_error_message)
        return (
            cat,
            f"Recorded batch error: {batch_error_message}",
            runbook_link_for(cat),
            "kimball inspect --table <target>",
        )
    # 8. Healthy.
    return (
        ErrorCategory.UNKNOWN,
        "No obvious failure; the target appears consistent.",
        None,
        None,
    )


def _infer_category_from_text(message: str) -> ErrorCategory:
    m = message.lower()
    if "vacuum" in m or "change data feed" in m or "cdf" in m:
        return ErrorCategory.CDF_GAP
    if "concurrent" in m or "concurrentappend" in m or "concurrentmodification" in m:
        return ErrorCategory.CONCURRENT_WRITER
    if "schema drift" in m or "schema.differs" in m or "schema changed" in m:
        return ErrorCategory.SCHEMA_DRIFT
    if "not found" in m or "does not exist" in m or "no such table" in m:
        return ErrorCategory.SOURCE_UNAVAILABLE
    if "config" in m or "yaml" in m or "validation" in m:
        return ErrorCategory.CONFIG
    if "timeout" in m or "memory" in m or "oom" in m or "executor" in m:
        return ErrorCategory.RESOURCE
    return ErrorCategory.UNKNOWN


def _default_remediation(category: ErrorCategory) -> str:
    table = {
        ErrorCategory.CONFIG: "Fix the pipeline YAML and re-run `kimball validate`.",
        ErrorCategory.SCHEMA_DRIFT: "Align the config with the source schema, or full-reload.",
        ErrorCategory.SOURCE_UNAVAILABLE: "Confirm the source exists and is reachable.",
        ErrorCategory.CDF_GAP: "Full-reload the target; the source was vacuumed past the watermark.",
        ErrorCategory.CONCURRENT_WRITER: "Enforce one writer per target and re-run.",
        ErrorCategory.RESOURCE: "Retry with backoff; check cluster health.",
        ErrorCategory.RECOVERY: "Run `kimball recover --table <target>`.",
        ErrorCategory.DATA_QUALITY: "Inspect the findings table for the failing rows.",
        ErrorCategory.UNKNOWN: "Inspect `kimball inspect --table <target>` for details.",
    }
    return table[category]


def _last_success_source_fingerprints(
    control_state: TargetControlState,
):
    """Yield (source_table, recorded_schema_fingerprint) per source, from the
    last SUCCESS batch for each source (sorted by started_at descending)."""
    seen: set[str] = set()
    for batch in sorted(
        (b for b in control_state.batches if b.status == "SUCCESS"),
        key=lambda b: b.started_at or b.completed_at or datetime.min,
        reverse=True,
    ):
        if batch.source_table in seen:
            continue
        seen.add(batch.source_table)
        yield batch.source_table, batch.source_schema_fingerprint
