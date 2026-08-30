"""Tests for kimball explain (1.3)."""

from __future__ import annotations

from kimball.ops.explain import explain, explain_config_error
from kimball.ops.providers import SourceHealthReport
from kimball.ops.runtime_profile import RuntimeFlavor, RuntimeProfile
from tests.unit.ops.fakes import (
    FakeControl,
    FakeHistory,
    FakeSources,
    batch,
    commit,
    providers,
)

CLASSIC = RuntimeProfile(RuntimeFlavor.CLASSIC, True)


def _sources_healthy():
    return FakeSources(
        {"silver.s": SourceHealthReport("silver.s", True, True, 0, None, None, None)}
    )


def test_explain_config_error_categorizes_configuration_error():
    report = explain_config_error(ValueError("missing surrogate_key"))
    assert report.entry_point == "config"
    assert report.category == "CONFIG"
    assert (
        report.recommended_recovery
        and "kimball validate" in report.recommended_recovery
    )
    assert report.runbook_link and "config" in report.runbook_link


def test_explain_consistent_target_is_unknown_healthy():
    batches = (batch("b1", "silver.s", "SUCCESS", 5),)
    hist = FakeHistory(True, 3, (commit(3, "b1"),))
    report = explain(
        "gold.t",
        providers(FakeControl(True, batches), hist, _sources_healthy()),
        CLASSIC,
    )
    assert report.reconciliation_verdict == "consistent"
    assert report.category == "UNKNOWN"
    assert report.recommended_recovery is None


def test_explain_zombie_recommends_recover():
    batches = (batch("z1", "silver.s", "RUNNING", 5),)
    hist = FakeHistory(True, 6, (commit(6, "z1"),))
    report = explain(
        "gold.t",
        providers(FakeControl(True, batches), hist, _sources_healthy()),
        CLASSIC,
    )
    assert report.category == "RECOVERY"
    assert report.recommended_recovery == "kimball recover --table <target>"


def test_explain_watermark_ahead_recommends_rewind():
    batches = (batch("b1", "silver.s", "SUCCESS", 10),)
    hist = FakeHistory(True, 8, (commit(8, None, operation="RESTORE"),))
    report = explain(
        "gold.t",
        providers(FakeControl(True, batches), hist, _sources_healthy()),
        CLASSIC,
    )
    assert report.category == "RECOVERY"
    assert (
        report.recommended_recovery
        == "kimball recover --table <target> --rewind-watermark"
    )


def test_explain_cdf_gap_recommends_full_reload():
    batches = (batch("b1", "silver.s", "SUCCESS", 10),)
    hist = FakeHistory(True, 5, (commit(5, "b1"),))
    src = FakeSources(
        {"silver.s": SourceHealthReport("silver.s", True, True, 12, 10, None, None)}
    )
    report = explain(
        "gold.t", providers(FakeControl(True, batches), hist, src), CLASSIC
    )
    assert report.category == "CDF_GAP"
    assert (
        report.recommended_recovery == "kimball recover --table <target> --full-reload"
    )


def test_explain_config_drift_dominates():
    batches = (batch("b1", "silver.s", "SUCCESS", 5, config_fingerprint="old"),)
    hist = FakeHistory(True, 3, (commit(3, "b1"),))
    report = explain(
        "gold.t",
        providers(FakeControl(True, batches), hist, _sources_healthy()),
        CLASSIC,
        current_config_fingerprint="new",
    )
    assert report.config_drift is True
    assert report.category == "CONFIG"
    assert (
        report.recommended_recovery
        and "kimball validate" in report.recommended_recovery
    )


def test_explain_schema_drift():
    batches = (batch("b1", "silver.s", "SUCCESS", 5, source_schema_fingerprint="abc"),)
    hist = FakeHistory(True, 3, (commit(3, "b1"),))
    src = FakeSources(
        {"silver.s": SourceHealthReport("silver.s", True, True, 0, 5, None, "def")}
    )
    report = explain(
        "gold.t", providers(FakeControl(True, batches), hist, src), CLASSIC
    )
    assert report.category == "SCHEMA_DRIFT"
    assert report.sources[0].schema_drift is True


def test_explain_batch_id_surfaces_error_message_and_infers_category():
    batches = (
        batch(
            "z1",
            "silver.s",
            "FAILED",
            5,
            error_message="ConcurrentModificationException: conflict",
        ),
    )
    hist = FakeHistory(True, 5, ())
    report = explain(
        "gold.t",
        providers(FakeControl(True, batches), hist, _sources_healthy()),
        CLASSIC,
        batch_id="z1",
    )
    assert (
        report.batch_error_message
        and "ConcurrentModification" in report.batch_error_message
    )
    # No zombie, no restore, no drift -> falls back to text inference -> CONCURRENT_WRITER
    assert report.category == "CONCURRENT_WRITER"


def test_explain_writer_violation_recommends_force():
    batches = (
        batch("z1", "silver.s", "RUNNING", 5),
        batch("b1", "silver.s", "SUCCESS", 3),
    )
    hist = FakeHistory(True, 6, (commit(6, "z1"), commit(5, "external")))
    report = explain(
        "gold.t",
        providers(FakeControl(True, batches), hist, _sources_healthy()),
        CLASSIC,
    )
    # zombie-with-committed-data takes precedence over writer violation
    assert report.category == "RECOVERY"


def test_explain_target_missing():
    report = explain(
        "gold.t",
        providers(
            FakeControl(exists=True, batches=()),
            FakeHistory(exists=False),
            FakeSources(),
        ),
        CLASSIC,
    )
    assert report.reconciliation_verdict == "target_missing"
    assert report.category == "SOURCE_UNAVAILABLE"


def test_explain_target_ahead_orphan_recommends_recover_batch_id():
    batches = (batch("b1", "silver.s", "SUCCESS", 5),)
    hist = FakeHistory(True, 6, (commit(6, "ghost"), commit(5, "b1")))
    report = explain(
        "gold.t",
        providers(FakeControl(True, batches), hist, _sources_healthy()),
        CLASSIC,
    )
    assert report.category == "CONCURRENT_WRITER"
    assert report.recommended_recovery
    assert (
        "recover --table <target> --batch-id ghost --force"
        in report.recommended_recovery
    )
