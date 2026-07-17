from __future__ import annotations

import logging
from types import SimpleNamespace

from pyspark.sql import DataFrame

from kimball.common.errors import DataQualityError
from kimball.observability.data_quality import (
    DataQualityEventSink,
    DataQualityEventWriter,
)
from kimball.orchestration.services.context import PipelineContext
from kimball.orchestration.services.contracts import ContractFinding
from kimball.orchestration.validation import TestSeverity
from kimball.processing.skeleton_generator import (
    SkeletonGenerationResult,
    SkeletonGenerator,
)

logger = logging.getLogger(__name__)


class SkeletonManager:
    def __init__(self, skeleton_generator: SkeletonGenerator | None = None):
        self.skeleton_generator = skeleton_generator or SkeletonGenerator()

    def generate_skeletons(
        self, ctx: PipelineContext, active_dfs: dict[str, DataFrame]
    ):
        configured = [
            {**item, "action": "skeleton"}
            for item in (ctx.config.early_arriving_facts or [])
        ]
        # ``early_arriving_dimensions`` is the fact-side contract vocabulary.
        # Translate it to the established generator shape for compatibility.
        configured += [
            {
                "fact_join_key": item.fact_key,
                "dimension_table": item.dimension_table,
                "dimension_join_key": item.dimension_key,
                "surrogate_key_col": item.surrogate_key,
                "action": item.action,
            }
            for item in (ctx.config.early_arriving_dimensions or [])
        ]
        if not configured:
            return
        logger.info("Checking for Early Arriving Facts...")
        sources_by_name = {source.name: source for source in ctx.config.sources}
        col_to_df: dict[str, tuple[object | None, DataFrame]] = {}
        for source_name, df in active_dfs.items():
            for col_name in df.columns:
                if col_name not in col_to_df:
                    col_to_df[col_name] = (sources_by_name.get(source_name), df)
        for eaf in configured:
            source_and_df = col_to_df.get(eaf["fact_join_key"])
            if source_and_df is not None:
                source, fact_source_df = source_and_df
                result = self.skeleton_generator.generate_skeletons(
                    fact_df=fact_source_df,
                    dim_table_name=eaf["dimension_table"],
                    fact_join_key=eaf["fact_join_key"],
                    dim_join_key=eaf["dimension_join_key"],
                    surrogate_key_col=eaf.get("surrogate_key_col", "surrogate_key"),
                    batch_id=ctx.batch_id,
                    effective_at_column=ctx.config.effective_at,
                    create=eaf.get("action", "skeleton") == "skeleton",
                )
                self._record_result(
                    ctx,
                    source
                    or SimpleNamespace(name="unknown", alias="unknown", contract=None),
                    result,
                    action=eaf.get("action", "skeleton"),
                )
            else:
                logger.info(
                    f"Warning: Could not find source with column {eaf['fact_join_key']} for skeleton generation."
                )

    @staticmethod
    def _record_result(
        ctx: PipelineContext,
        source: object,
        result: SkeletonGenerationResult,
        *,
        action: str,
    ) -> None:
        if not result.missing_rows:
            return
        blocked = action == "error"
        finding = ContractFinding(
            "early_arriving_fact",
            f"missing_dimension_keys:{result.dimension_table}",
            TestSeverity.ERROR if blocked else TestSeverity.WARN,
            False,
            f"Found {result.missing_rows} fact key(s) missing from "
            f"{result.dimension_table}; skeleton action is {action}",
            failed_rows=result.missing_rows,
            samples=result.samples,
        )
        schema = getattr(ctx.etl_control, "schema", None) or "default"
        observability = ctx.config.observability
        writer = DataQualityEventSink(
            ctx.spark,
            schema,
            observability.event_table if observability else "etl_data_quality_events",
            failure_mode=observability.write_failure if observability else "warn",
            writer_type=DataQualityEventWriter,
        )
        writer.write(
            pipeline_table=ctx.config.table_name,
            source=source,
            finding=finding,
            run_id=ctx.batch_id,
            source_version=ctx.source_versions.get(getattr(source, "name", "")),
            action="blocked" if blocked else "skeleton_created",
        )
        if blocked:
            raise DataQualityError(
                f"Found early-arriving fact keys without {result.dimension_table}",
                details={"sample_failures": result.samples},
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
