from __future__ import annotations

import logging

from kimball.common.config import ConfigLoader
from kimball.orchestration.services.context import PipelineContext
from kimball.orchestration.watermark import compute_source_schema_fingerprint

logger = logging.getLogger(__name__)


class FingerprintService:
    def __init__(self, config_loader: ConfigLoader | None = None):
        self.config_loader = config_loader or ConfigLoader()

    def save_fingerprints(self, ctx: PipelineContext) -> None:
        config_fp = self.config_loader.compute_fingerprint(ctx.config)
        for source in ctx.config.sources:
            source_fp = compute_source_schema_fingerprint(ctx.spark, source.name)
            ctx.etl_control.update_fingerprints(
                ctx.config.table_name,
                source.name,
                config_fingerprint=config_fp,
                source_schema_fingerprint=source_fp,
            )
