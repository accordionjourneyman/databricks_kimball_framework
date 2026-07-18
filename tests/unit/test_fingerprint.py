from __future__ import annotations

from unittest.mock import MagicMock, patch

from kimball.orchestration.services.fingerprint import FingerprintService


class TestSaveFingerprints:
    def _ctx(self):
        mock = MagicMock()
        mock.config.table_name = "test_table"
        mock.config.sources = [MagicMock(name="src1"), MagicMock(name="src2")]
        return mock

    def test_saves_fingerprints_for_all_sources(self):
        ctx = self._ctx()
        config_loader = MagicMock()
        config_loader.compute_fingerprint.return_value = "fp1"
        service = FingerprintService(config_loader)
        with patch(
            "kimball.orchestration.services.fingerprint.compute_source_schema_fingerprint",
            return_value="sfp1",
        ):
            service.save_fingerprints(ctx)
        assert ctx.etl_control.update_fingerprints.call_count == 2
