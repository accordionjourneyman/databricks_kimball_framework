"""Tests for the structured error taxonomy and user-facing rendering."""

from __future__ import annotations

import pytest

from kimball.ops.errors import ErrorCategory, StructuredError, categorize, format_error


def test_structured_error_carries_category_and_remediation():
    err = StructuredError(
        "source vacuumed past watermark",
        category=ErrorCategory.CDF_GAP,
        remediation="full reload",
    )
    assert err.category is ErrorCategory.CDF_GAP
    assert err.remediation == "full reload"
    assert err.runbook_link and "cdf-gap" in err.runbook_link


def test_categorize_value_error_is_config():
    assert categorize(ValueError("bad yaml")) is ErrorCategory.CONFIG


def test_categorize_structured_concurrent_modification():
    err = StructuredError("conflict", category=ErrorCategory.CONCURRENT_WRITER)
    assert categorize(err) is ErrorCategory.CONCURRENT_WRITER


def test_categorize_unknown_exception():
    assert categorize(RuntimeError("boom")) is ErrorCategory.UNKNOWN


def test_categorize_prefers_structured_category():
    err = StructuredError("x", category=ErrorCategory.RECOVERY)
    assert categorize(err) is ErrorCategory.RECOVERY


def test_format_error_includes_fix_and_see_for_structured():
    err = StructuredError(
        "watermark ahead", category=ErrorCategory.CDF_GAP, remediation="rewind"
    )
    rendered = format_error(err)
    assert "CDF_GAP" in rendered
    assert "Fix: rewind" in rendered
    assert "See:" in rendered


def test_format_error_value_error_gets_default_remediation():
    rendered = format_error(ValueError("missing field"))
    assert "CONFIG" in rendered
    assert "Fix:" in rendered


def test_structured_error_is_kimball_error_and_raisable():
    with pytest.raises(StructuredError) as excinfo:
        raise StructuredError("no", category=ErrorCategory.RECOVERY)
    assert excinfo.value.category is ErrorCategory.RECOVERY


def test_categorize_filenotfound_is_source_unavailable():
    assert categorize(FileNotFoundError("x")) is ErrorCategory.SOURCE_UNAVAILABLE


def test_categorize_oserror_is_resource():
    assert categorize(OSError("io")) is ErrorCategory.RESOURCE


def test_categorize_jsonschema_validation_error_is_config():
    from jsonschema import ValidationError

    assert categorize(ValidationError("bad")) is ErrorCategory.CONFIG


def test_format_error_value_error_renders_fix():
    rendered = format_error(ValueError("missing field"))
    assert "CONFIG" in rendered
    assert "Fix:" in rendered
    assert "See:" in rendered
