from __future__ import annotations

import logging

from kimball.common.config import ConfigLoader
from kimball.orchestration.services.context import PipelineContext
from kimball.orchestration.watermark import compute_source_schema_fingerprint

logger = logging.getLogger(__name__)


class FingerprintService:
    def __init__(self, config_loader: ConfigLoader | None = None):
        self.config_loader = config_loader or ConfigLoader()

    def should_skip_validation(self, ctx: PipelineContext) -> bool:
        if not ctx.runtime_options.skip_validation_if_unchanged:
            return False
        current_config_fp = self.config_loader.compute_fingerprint(ctx.config)
        source_names = [s.name for s in ctx.config.sources]
        all_unchanged = True
        for source_name in source_names:
            prev_config_fp = ctx.etl_control.get_config_fingerprint(
                ctx.config.table_name, source_name
            )
            prev_source_fp = ctx.etl_control.get_source_schema_fingerprint(
                ctx.config.table_name, source_name
            )
            if prev_config_fp != current_config_fp:
                all_unchanged = False
                break
            current_source_fp = compute_source_schema_fingerprint(
                ctx.spark, source_name
            )
            if prev_source_fp != current_source_fp:
                all_unchanged = False
                break
        return all_unchanged

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