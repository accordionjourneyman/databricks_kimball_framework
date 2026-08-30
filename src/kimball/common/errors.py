"""Framework error primitives.

Use ``StructuredError`` from ``kimball.ops.errors`` for user-facing operational
failures.  The generic retry classes remain for orchestration retry policy.
"""

from __future__ import annotations

from collections.abc import Mapping
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
