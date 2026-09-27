"""Framework error primitives.

Use ``StructuredError`` from ``kimball.ops.errors`` for user-facing operational
failures.  The generic retry classes remain for orchestration retry policy.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


class KimballError(Exception):
    """Base exception for framework failures."""

    retriable = False

    def __init__(self, message: str, details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.details: Mapping[str, Any] = details or {}


class RetriableError(KimballError):
    """A failure the orchestrator may retry."""

    retriable = True


class NonRetriableError(KimballError):
    """A failure that cannot succeed by retrying unchanged input."""


class DataQualityError(NonRetriableError):
    """A data-quality gate failed."""


@dataclass(frozen=True)
class ConfigIssue:
    """One sanitized, path-aware configuration problem."""

    path: str
    field_path: str | None
    message: str
    code: str = "invalid_config"

    def __str__(self) -> str:
        location = self.path
        if self.field_path:
            location = f"{location}:{self.field_path}"
        return f"{location}: {self.message}"


class ConfigurationValidationError(ValueError):
    """One or more configuration issues from one or more files."""

    def __init__(self, issues: tuple[ConfigIssue, ...] | list[ConfigIssue]):
        self.issues = tuple(issues)
        super().__init__(
            "Configuration validation failed:\n" + "\n".join(map(str, self.issues))
        )


def format_validation_location(location: tuple[Any, ...] | list[Any]) -> str | None:
    """Format Pydantic's field location without including invalid input values."""
    rendered = ""
    for part in location:
        if isinstance(part, int):
            rendered += f"[{part}]"
        else:
            rendered += ("." if rendered else "") + str(part)
    return rendered or None


def config_issues_from_validation_error(path: str, error: Any) -> list[ConfigIssue]:
    """Convert Pydantic errors into safe file/field issues.

    Pydantic's default string form includes input values. This conversion uses
    only the field location and message so secret values are not echoed.
    """
    issues: list[ConfigIssue] = []
    for item in error.errors(include_input=False, include_url=False):
        nested_error = item.get("ctx", {}).get("error")
        rule_issues = getattr(nested_error, "issues", None)
        if rule_issues:
            for rule_issue in rule_issues:
                issues.append(
                    ConfigIssue(
                        path,
                        f"rules.{rule_issue.rule}",
                        rule_issue.message,
                        "rule_violation",
                    )
                )
            continue
        issues.append(
            ConfigIssue(
                path,
                format_validation_location(item.get("loc", ())),
                item.get("msg", "invalid value"),
                item.get("type", "invalid_config"),
            )
        )
    return issues
