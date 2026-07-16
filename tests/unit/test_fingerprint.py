from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from kimball.orchestration.services.fingerprint import FingerprintService


@pytest.fixture
def ctx():
    mock = MagicMock()
    mock.runtime_options.skip_validation_if_unchanged = True
    mock.config.table_name = "test_table"
    mock.config.sources = [MagicMock(name="src1"), MagicMock(name="src2")]
    return mock


class TestShouldSkipValidation:
    def test_returns_false_when_skip_disabled(self, ctx):
        ctx.runtime_options.skip_validation_if_unchanged = False
        service = FingerprintService()
        result = service.should_skip_validation(ctx)
        assert result is False

    def test_returns_true_when_all_unchanged(self, ctx):
        config_loader = MagicMock()
        config_loader.compute_fingerprint.return_value = "fp1"
        ctx.etl_control.get_config_fingerprint.return_value = "fp1"
        ctx.etl_control.get_source_schema_fingerprint.return_value = "sfp1"
        service = FingerprintService(config_loader)
        with patch("kimball.orchestration.services.fingerprint.compute_source_schema_fingerprint", return_value="sfp1"):
            result = service.should_skip_validation(ctx)
        assert result is True

    def test_returns_false_when_config_changed(self, ctx):
        config_loader = MagicMock()
        config_loader.compute_fingerprint.return_value = "fp_new"
        ctx.etl_control.get_config_fingerprint.return_value = "fp_old"
        service = FingerprintService(config_loader)
        result = service.should_skip_validation(ctx)
        assert result is False

    def test_returns_false_when_source_schema_changed(self, ctx):
        config_loader = MagicMock()
        config_loader.compute_fingerprint.return_value = "fp1"
        ctx.etl_control.get_config_fingerprint.return_value = "fp1"
        ctx.etl_control.get_source_schema_fingerprint.return_value = "sfp_old"
        service = FingerprintService(config_loader)
        with patch("kimball.orchestration.services.fingerprint.compute_source_schema_fingerprint", return_value="sfp_new"):
            result = service.should_skip_validation(ctx)
        assert result is False


class TestSaveFingerprints:
    def test_saves_fingerprints_for_all_sources(self, ctx):
        config_loader = MagicMock()
        config_loader.compute_fingerprint.return_value = "fp1"
        service = FingerprintService(config_loader)
        with patch("kimball.orchestration.services.fingerprint.compute_source_schema_fingerprint", return_value="sfp1"):
            service.save_fingerprints(ctx)
        assert ctx.etl_control.update_fingerprints.call_count == 2
