from __future__ import annotations

import logging
import time
from typing import Any

from pyspark.sql import DataFrame

from kimball.orchestration.services.context import PipelineContext

logger = logging.getLogger(__name__)


class SourceLoader:
    def load(self, ctx: PipelineContext) -> tuple[dict[str, Any], dict[str, DataFrame]]:
        source_versions: dict[str, Any] = {}
        active_dfs: dict[str, DataFrame] = {}
        stage_start = time.time()

        for source in ctx.config.sources:
            if source.format == "delta" and source.cdc_strategy in ("cdf", "append"):
                latest_v = ctx.loader.get_latest_version(source.name)
            else:
                latest_v = 0
            source_versions[source.name] = latest_v

            if source.cdc_strategy == "full":
                df = ctx.loader.load_full_snapshot(
                    source.name, format=source.format, options=source.options
                )
            elif source.cdc_strategy == "cdf":
                wm = ctx.etl_control.get_watermark(ctx.config.table_name, source.name)
                if wm is None:
                    logger.info(
                        f"No watermark for {source.name}. "
                        f"Performing Initial Load via CDF from Version 0."
                    )
                    if ctx.config.preserve_all_changes and ctx.config.scd_type == 2:
                        logger.info(
                            f"Preserve All Changes: Processing version {source.starting_version} only"
                        )
                        df = ctx.loader.load_cdf(
                            source.name,
                            starting_version=source.starting_version,
                            deduplicate_keys=source.primary_keys,
                            ending_version=source.starting_version,
                        )
                        source_versions[source.name] = source.starting_version
                    else:
                        df = ctx.loader.load_cdf(
                            source.name,
                            starting_version=source.starting_version,
                            deduplicate_keys=source.primary_keys,
                            ending_version=latest_v,
                        )
                        source_versions[source.name] = latest_v
                else:
                    if wm >= latest_v:
                        logger.info(
                            f"Source {source.name} already at version {latest_v}. Skipping."
                        )
                        ctx.etl_control.batch_complete(
                            ctx.config.table_name,
                            source.name,
                            new_version=latest_v,
                            rows_read=0,
                            rows_written=0,
                        )
                        continue

                    if ctx.config.preserve_all_changes and ctx.config.scd_type == 2:
                        logger.info(
                            f"Preserve All Changes: Processing version {wm + 1} only"
                        )
                        df = ctx.loader.load_cdf(
                            source.name,
                            wm + 1,
                            deduplicate_keys=source.primary_keys,
                            ending_version=wm + 1,
                        )
                        source_versions[source.name] = wm + 1
                    else:
                        df = ctx.loader.load_cdf(
                            source.name,
                            wm + 1,
                            deduplicate_keys=source.primary_keys,
                            ending_version=latest_v,
                        )
            elif source.cdc_strategy == "append":
                wm = ctx.etl_control.get_watermark(ctx.config.table_name, source.name)
                if wm is None:
                    logger.info(
                        f"No watermark for {source.name}. "
                        f"Performing initial append load via CDF from version {source.starting_version}."
                    )
                    df = ctx.loader.load_cdf(
                        source.name,
                        starting_version=source.starting_version,
                        deduplicate_keys=None,
                        ending_version=latest_v,
                    )
                    source_versions[source.name] = latest_v
                else:
                    if wm >= latest_v:
                        logger.info(
                            f"Source {source.name} already at version {latest_v}. Skipping."
                        )
                        ctx.etl_control.batch_complete(
                            ctx.config.table_name,
                            source.name,
                            new_version=latest_v,
                            rows_read=0,
                            rows_written=0,
                        )
                        continue
                    df = ctx.loader.load_cdf(
                        source.name,
                        wm + 1,
                        deduplicate_keys=None,
                        ending_version=latest_v,
                    )
                    source_versions[source.name] = latest_v
                cdf_metadata_cols = [
                    c for c in ("_change_type", "_commit_version", "_commit_timestamp")
                    if c in df.columns
                ]
                if cdf_metadata_cols:
                    df = df.drop(*cdf_metadata_cols)
            elif source.cdc_strategy == "timestamp":
                raise NotImplementedError(
                    f"cdc_strategy='timestamp' is not yet implemented for source '{source.name}'. "
                    "Use 'cdf' (recommended) or 'full' instead."
                )
            else:
                raise ValueError(f"Unknown CDC strategy: {source.cdc_strategy}")

            df.createOrReplaceTempView(source.alias)
            active_dfs[source.name] = df

        logger.info(
            f"Loaded {len(active_dfs)} source(s) in {time.time() - stage_start:.2f}s"
        )
        return source_versions, active_dfs