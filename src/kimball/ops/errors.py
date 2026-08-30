"""Structured error taxonomy for operational tooling (ROADMAP 1.E).

User-facing failures carry a category, a one-line remediation and a runbook
link so ``kimball explain`` and ``kimball inspect`` present actionable
diagnosis instead of bare tracebacks. Legacy exceptions without these fields
are categorised by type via :func:`categorize`.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import Enum
from typing import Any

from kimball.common.errors import DataQualityError, KimballError

RUNBOOK = "docs/RUNBOOK.md"


class ErrorCategory(str, Enum):
    CONFIG = "CONFIG"
    SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
    CDF_GAP = "CDF_GAP"
    CONCURRENT_WRITER = "CONCURRENT_WRITER"
    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    DATA_QUALITY = "DATA_QUALITY"
    RESOURCE = "RESOURCE"
    RECOVERY = "RECOVERY"
    UNKNOWN = "UNKNOWN"


_DEFAULT_REMEDIATION: dict[ErrorCategory, str] = {
    ErrorCategory.CONFIG: "Check the pipeline YAML against docs/CONFIGURATION.md and re-run `kimball validate`.",
    ErrorCategory.SOURCE_UNAVAILABLE: "Confirm the source table exists and is reachable from this target's catalog/schema.",
    ErrorCategory.CDF_GAP: "The source was VACUUMed past the watermark. Run `kimball recover --table <target> --full-reload`.",
    ErrorCategory.CONCURRENT_WRITER: "Another writer committed to the target. Enforce one writer per target and re-run.",
    ErrorCategory.SCHEMA_DRIFT: "The source schema changed since the last successful run. Update the config or full-reload.",
    ErrorCategory.DATA_QUALITY: "A data-quality rule failed; inspect the findings table for the offending rows.",
    ErrorCategory.RESOURCE: "Transient cluster/resource issue; retry with backoff.",
    ErrorCategory.RECOVERY: "Recovery could not complete cleanly; run `kimball inspect --table <target>` and follow RUNBOOK.",
    ErrorCategory.UNKNOWN: "Inspect `kimball explain --table <target>` for details.",
}

_RUNBOOK_ANCHOR: dict[ErrorCategory, str] = {
    ErrorCategory.CONFIG: "config",
    ErrorCategory.SOURCE_UNAVAILABLE: "source-unavailable",
    ErrorCategory.CDF_GAP: "cdf-gap",
    ErrorCategory.CONCURRENT_WRITER: "concurrent-writer",
    ErrorCategory.SCHEMA_DRIFT: "schema-drift",
    ErrorCategory.DATA_QUALITY: "data-quality",
    ErrorCategory.RESOURCE: "resource",
    ErrorCategory.RECOVERY: "recovery",
    ErrorCategory.UNKNOWN: "unknown",
}


class StructuredError(KimballError):
    """A KimballError carrying an actionable category + remediation."""

    retriable: bool = False

    def __init__(
        self,
        message: str,
        *,
        category: ErrorCategory = ErrorCategory.UNKNOWN,
        remediation: str | None = None,
        runbook_link: str | None = None,
        details: Mapping[str, Any] | None = None,
        retriable: bool | None = None,
    ) -> None:
        super().__init__(message, details)
        self.category = category
        self.remediation = remediation or _DEFAULT_REMEDIATION.get(category)
        self.runbook_link = runbook_link or runbook_link_for(category)
        if retriable is not None:
            self.retriable = retriable


def runbook_link_for(category: ErrorCategory) -> str:
    return f"{RUNBOOK}#{_RUNBOOK_ANCHOR[category]}"


def categorize(exc: BaseException) -> ErrorCategory:
    """Map supported framework and common exceptions to an error category."""
    if isinstance(exc, StructuredError):
        return exc.category
    if isinstance(exc, DataQualityError):
        return ErrorCategory.DATA_QUALITY
    if isinstance(exc, FileNotFoundError):
        return ErrorCategory.SOURCE_UNAVAILABLE
    if isinstance(exc, OSError):
        return ErrorCategory.RESOURCE
    if isinstance(exc, ValueError):
        return ErrorCategory.CONFIG
    if type(exc).__name__ == "ValidationError" and "jsonschema" in (
        type(exc).__module__ or ""
    ):
        return ErrorCategory.CONFIG
    return ErrorCategory.UNKNOWN


def format_error(exc: BaseException) -> str:
    """Render a ``Try:/Fix:`` style message for user-facing display (1.7)."""
    category = categorize(exc)
    message = str(exc) or exc.__class__.__name__
    if isinstance(exc, StructuredError):
        remediation = exc.remediation
        link = exc.runbook_link
    else:
        remediation = _DEFAULT_REMEDIATION.get(category)
        link = runbook_link_for(category)
    lines = [f"{category.value}: {message}"]
    if remediation:
        lines.append(f"  Fix: {remediation}")
    if link:
        lines.append(f"  See: {link}")
    return "\n".join(lines)
