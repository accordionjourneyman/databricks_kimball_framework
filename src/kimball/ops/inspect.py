"""``kimball inspect`` command logic (ROADMAP 1.1).

Composes the harness into a single JSON-serialisable report. Works off
``etl_control`` + target Delta history only - no YAML config required, so an
operator can inspect a target whose config file is not at hand.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Any

from kimball.ops.providers import OpsProviders
from kimball.ops.runtime_profile import RuntimeProfile
from kimball.ops.source_health import assess_source_health
from kimball.ops.state_reconciler import StateReconciler
from kimball.ops.writer_contract import check_writer_contract


def inspect_target(
    target_table: str,
    providers: OpsProviders,
    runtime: RuntimeProfile,
    history_limit: int = 200,
) -> dict[str, Any]:
    """Return the full inspection report for one target."""
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

    source_health = []
    seen_sources: set[str] = set()
    for batch in sorted(
        control_state.batches,
        key=lambda b: b.started_at or b.completed_at or datetime.min,
        reverse=True,
    ):
        if batch.source_table in seen_sources:
            continue
        seen_sources.add(batch.source_table)
        report = providers.sources.get_source_health(
            batch.source_table,
            batch.last_processed_version,
            batch.source_schema_fingerprint,
        )
        source_health.append(_assessment_dict(assess_source_health(report)))

    return {
        "target_table": target_table,
        "runtime": {
            "flavor": runtime.flavor.value,
            "supports_commit_tagging": runtime.supports_commit_tagging,
        },
        "control_table_exists": control_state.control_table_exists,
        "reconciliation": _reconciliation_dict(reconciliation),
        "writer_contract": {
            "verdict": writer.verdict.value,
            "suspicious_commits": len(writer.suspicious_commits),
        },
        "source_health": source_health,
        "batches": [asdict(b) for b in control_state.batches],
    }


def _reconciliation_dict(rep: Any) -> dict[str, Any]:
    return {
        "verdict": rep.verdict.value,
        "watermark_version": rep.watermark_version,
        "target_version": rep.target_version,
        "zombie_batches": len(rep.zombie_batches),
        "zombie_commits": len(rep.zombie_commits),
        "orphan_commits": len(rep.orphan_commits),
        "orphan_batch_ids": [c.batch_id for c in rep.orphan_commits if c.batch_id],
        "evidence": rep.evidence,
        "remediation": rep.remediation,
        "runbook_link": rep.runbook_link,
    }


def _assessment_dict(assess: Any) -> dict[str, Any]:
    return {
        "source_table": assess.source_table,
        "verdict": assess.verdict.value,
        "detail": assess.detail,
        "watermark_version": assess.report.watermark_version,
        "cdf_enabled": assess.report.cdf_enabled,
        "earliest_cdf_version": assess.report.earliest_cdf_version,
        "schema_drift": (
            assess.report.recorded_schema_fingerprint
            != assess.report.current_schema_fingerprint
            if assess.report.recorded_schema_fingerprint
            and assess.report.current_schema_fingerprint
            else False
        ),
    }
