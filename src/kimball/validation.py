"""Backward-compatible re-export — validation logic now lives in
``kimball.orchestration.validation``.  This module is kept so that
existing ``from kimball.validation import ...`` and
``patch("kimball.validation.F")`` calls continue to work.
"""
from kimball.orchestration.validation import (  # noqa: F401
    DataQualityValidator,
    TestResult,
    TestSeverity,
    ValidationReport,
)