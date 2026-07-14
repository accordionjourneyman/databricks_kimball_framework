from __future__ import annotations

import logging

from pyspark.sql import DataFrame

from kimball.orchestration.services.context import PipelineContext
from kimball.processing.skeleton_generator import SkeletonGenerator

logger = logging.getLogger(__name__)


class SkeletonManager:
    def __init__(self, skeleton_generator: SkeletonGenerator | None = None):
        self.skeleton_generator = skeleton_generator or SkeletonGenerator()

    def generate_skeletons(self, ctx: PipelineContext, active_dfs: dict[str, DataFrame]):
        if not ctx.config.early_arriving_facts:
            return
        logger.info("Checking for Early Arriving Facts...")
        col_to_df: dict[str, DataFrame] = {}
        for df in active_dfs.values():
            for col_name in df.columns:
                if col_name not in col_to_df:
                    col_to_df[col_name] = df
        for eaf in ctx.config.early_arriving_facts:
            fact_source_df = col_to_df.get(eaf["fact_join_key"])
            if fact_source_df is not None:
                self.skeleton_generator.generate_skeletons(
                    fact_df=fact_source_df,
                    dim_table_name=eaf["dimension_table"],
                    fact_join_key=eaf["fact_join_key"],
                    dim_join_key=eaf["dimension_join_key"],
                    surrogate_key_col=eaf.get("surrogate_key_col", "surrogate_key"),
                    batch_id=ctx.batch_id,
                    effective_at_column=ctx.config.effective_at,
                )
            else:
                logger.info(
                    f"Warning: Could not find source with column {eaf['fact_join_key']} for skeleton generation."
                )

    def apply_identity_bridge(self, ctx: PipelineContext, df: DataFrame) -> DataFrame:
        bridge = ctx.config.identity_bridge
        if bridge is None:
            return df
        logger.info(
            f"Applying identity bridge: {bridge.table} on {bridge.join_on} -> {bridge.target_column}"
        )
        df.createOrReplaceTempView("_identity_bridge_src")
        resolved = ctx.spark.sql(
            f"SELECT "
            f"COALESCE(map.`{bridge.target_column}`, src.`{bridge.join_on}`) AS `{bridge.join_on}`, "
            f"{', '.join(f'src.`{c}`' for c in df.columns if c != bridge.join_on)} "
            f"FROM _identity_bridge_src src "
            f"LEFT JOIN {bridge.table} map ON src.`{bridge.join_on}` = map.`{bridge.join_on}`"
        )
        return resolved