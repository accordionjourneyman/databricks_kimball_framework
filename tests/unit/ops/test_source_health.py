"""Tests for the source health checker (1.B)."""

from __future__ import annotations

from kimball.ops.providers import SourceHealthReport
from kimball.ops.source_health import SourceHealthVerdict, assess_source_health


def _report(**kw):
    base = (
        dict(
            source_table="silver.s",
            exists=True,
            cdf_enabled=True,
            earliest_cdf_version=0,
            watermark_version=None,
            recorded_schema_fingerprint=None,
            current_schema_fingerprint=None,
        )
        | kw
    )
    return SourceHealthReport(**base)


def test_healthy():
    assert assess_source_health(_report()).verdict is SourceHealthVerdict.HEALTHY


def test_missing():
    assert (
        assess_source_health(_report(exists=False)).verdict
        is SourceHealthVerdict.MISSING
    )


def test_cdf_disabled():
    assert (
        assess_source_health(_report(cdf_enabled=False)).verdict
        is SourceHealthVerdict.CDF_DISABLED
    )


def test_cdf_gap_when_vacuumed_past_watermark():
    r = _report(watermark_version=10, earliest_cdf_version=12)
    a = assess_source_health(r)
    assert a.verdict is SourceHealthVerdict.CDF_GAP
    assert "12" in a.detail


def test_no_gap_when_watermark_within_retention():
    r = _report(watermark_version=10, earliest_cdf_version=5)
    assert assess_source_health(r).verdict is SourceHealthVerdict.HEALTHY


def test_schema_drift():
    r = _report(recorded_schema_fingerprint="abc", current_schema_fingerprint="def")
    assert assess_source_health(r).verdict is SourceHealthVerdict.SCHEMA_DRIFT
