"""Source health checker (ROADMAP 1.B).

Detects the most common real-world CDF failure: the source was VACUUMed past
the watermark, so the next incremental run cannot resume from watermark+1.
Also surfaces disabled CDF and recorded-vs-current schema drift.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from kimball.ops.providers import SourceHealthReport


class SourceHealthVerdict(str, Enum):
    HEALTHY = "healthy"
    CDF_DISABLED = "cdf_disabled"
    CDF_GAP = "cdf_gap"  # watermark beyond retained history (vacuumed)
    MISSING = "missing"
    SCHEMA_DRIFT = "schema_drift"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class SourceHealthAssessment:
    source_table: str
    verdict: SourceHealthVerdict
    report: SourceHealthReport
    detail: str


def assess_source_health(report: SourceHealthReport) -> SourceHealthAssessment:
    table = report.source_table
    if not report.exists:
        return SourceHealthAssessment(
            table, SourceHealthVerdict.MISSING, report, "source table not found"
        )
    if report.cdf_enabled is False:
        return SourceHealthAssessment(
            table,
            SourceHealthVerdict.CDF_DISABLED,
            report,
            "CDF not enabled; incremental resume impossible without full reload",
        )
    if (
        report.cdf_enabled
        and report.watermark_version is not None
        and report.earliest_cdf_version is not None
    ):
        needed = report.watermark_version + 1
        if report.earliest_cdf_version > needed:
            return SourceHealthAssessment(
                table,
                SourceHealthVerdict.CDF_GAP,
                report,
                f"source vacuumed past watermark: need version {needed}, "
                f"earliest available is {report.earliest_cdf_version}",
            )
    if (
        report.recorded_schema_fingerprint
        and report.current_schema_fingerprint
        and report.recorded_schema_fingerprint != report.current_schema_fingerprint
    ):
        return SourceHealthAssessment(
            table,
            SourceHealthVerdict.SCHEMA_DRIFT,
            report,
            "source schema changed since the last successful run",
        )
    if report.cdf_enabled is None or report.earliest_cdf_version is None:
        return SourceHealthAssessment(
            table, SourceHealthVerdict.UNKNOWN, report, "CDF metadata unavailable"
        )
    return SourceHealthAssessment(table, SourceHealthVerdict.HEALTHY, report, "ok")
