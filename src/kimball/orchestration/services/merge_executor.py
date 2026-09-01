from __future__ import annotations

import logging
import os

from pyspark.errors import PySparkException as PYSPARK_EXCEPTION_BASE
from pyspark.sql import DataFrame
from pyspark.sql.functions import count as spark_count

from kimball.common.utils import quote_table_name
from kimball.observability.resilience import _feature_enabled
from kimball.orchestration.services.context import PipelineContext
from kimball.processing import merger as _merger
from kimball.processing.defaults import sql_literal
from kimball.processing.dimension_nulls import replacement_for_type
from kimball.processing.table_creator import TableCreator

logger = logging.getLogger(__name__)

SYSTEM_COLUMNS = {
    "__is_current",
    "__valid_from",
    "__valid_to",
    "__etl_processed_at",
    "__is_deleted",
    "__etl_batch_id",
    "__is_skeleton",
    "hashdiff",
    "__merge_action",
}
CDF_COLUMNS = {"_change_type", "_commit_version", "_commit_timestamp"}


class MergeExecutor:
    def __init__(self, table_creator: TableCreator | None = None):
        self.table_creator = table_creator or TableCreator()

    def ensure_target_table(
        self, ctx: PipelineContext, transformed_df: DataFrame
    ) -> bool:
        if ctx.table_exists(ctx.config.table_name):
            return False
        logger.info(f"Creating table {ctx.config.table_name}...")
        self._create_target_table(ctx, transformed_df)
        if ctx.config.scd_type == 4 and ctx.config.history_table:
            self.table_creator.create_history_table(ctx.config.history_table)
        # Invalidate cache so subsequent checks see the new table.
        ctx.invalidate_table(ctx.config.table_name)
        return True

    def _create_target_table(
        self, ctx: PipelineContext, transformed_df: DataFrame
    ) -> None:
        schema_df = self.table_creator.add_system_columns(
            transformed_df.limit(0),
            ctx.config.scd_type,
            ctx.config.surrogate_key,
            durable_key=ctx.config.durable_key,
            current_value_columns=ctx.config.current_value_columns,
        )
        cluster_cols = ctx.config.cluster_by
        if (
            not cluster_cols
            and ctx.config.table_type == "dimension"
            and _feature_enabled("auto_cluster")
        ):
            cluster_cols = ctx.config.natural_keys or []
            if cluster_cols:
                logger.info(f"Auto-clustering on natural keys: {cluster_cols}")
        self.table_creator.create_table_with_clustering(
            table_name=ctx.config.table_name,
            schema_df=schema_df,
            config={**ctx.config.model_dump(), "cluster_by": cluster_cols or []},
            surrogate_key_col=ctx.config.surrogate_key,
        )

    def seed_defaults(self, ctx: PipelineContext, table_created: bool) -> None:
        if not table_created or ctx.config.table_type != "dimension":
            return
        target_schema = ctx.get_target_schema(ctx.config.table_name)
        if ctx.config.scd_type in (2, 7):
            _merger.ensure_scd2_defaults(
                ctx.config.table_name,
                target_schema,
                ctx.config.surrogate_key or "surrogate_key",
                ctx.config.default_rows,
                durable_key=ctx.config.durable_key,
            )
        elif ctx.config.scd_type == 1 and ctx.config.surrogate_key:
            _merger.ensure_scd1_defaults(
                ctx.config.table_name,
                target_schema,
                ctx.config.surrogate_key,
                ctx.config.default_rows,
            )

    def prepare_source_df(
        self, ctx: PipelineContext, transformed_df: DataFrame
    ) -> DataFrame:
        if getattr(ctx.config, "enable_lineage_truncation", False):
            logger.info("Creating DataFrame checkpoint for merge operation...")
            try:
                if ctx.spark.sparkContext.getCheckpointDir():
                    checkpointed_df = transformed_df.checkpoint()
                else:
                    checkpointed_df = transformed_df.localCheckpoint()
            except PYSPARK_EXCEPTION_BASE as e:
                logger.info(f"Checkpoint failed, using local: {e}")
                checkpointed_df = transformed_df.localCheckpoint()
        else:
            checkpointed_df = transformed_df

        if ctx.table_exists(ctx.config.table_name):
            target_schema = ctx.get_target_schema(ctx.config.table_name)
            target_columns = {f.name for f in target_schema.fields}
            return self._evolve_schema_if_needed(ctx, checkpointed_df, target_columns)
        return checkpointed_df

    def _evolve_schema_if_needed(
        self, ctx: PipelineContext, df: DataFrame, target_columns: set[str]
    ) -> DataFrame:
        """Add opted-in source columns that are absent from the target schema."""
        if not ctx.config.schema_evolution:
            return df

        if new_columns := [
            column
            for column in df.columns
            if column not in target_columns
            and column not in SYSTEM_COLUMNS
            and column not in CDF_COLUMNS
        ]:
            self._evolve_target_schema(ctx, new_columns)
        return df

    def _evolve_target_schema(
        self, ctx: PipelineContext, new_columns: list[str]
    ) -> None:
        try:
            target_df = ctx.spark.table(ctx.config.table_name)
            target_fields = {f.name: f.dataType for f in target_df.schema.fields}
            for col_name in new_columns:
                if col_name in target_fields:
                    continue
                try:
                    src_type = (
                        ctx.spark.table(f"{ctx.config.sources[0].name}")
                        .schema[col_name]
                        .dataType
                    )
                    ctx.spark.sql(
                        f"ALTER TABLE {quote_table_name(ctx.config.table_name)} "
                        f"ADD COLUMNS ({col_name} {src_type.simpleString()})"
                    )
                    if ctx.config.table_type == "dimension":
                        replacement = ctx.config.null_policy.attribute_substitutes.get(
                            col_name, replacement_for_type(src_type)
                        )
                        quoted_table = quote_table_name(ctx.config.table_name)
                        ctx.spark.sql(
                            f"UPDATE {quoted_table} "
                            f"SET `{col_name}` = {sql_literal(replacement)} "
                            f"WHERE `{col_name}` IS NULL"
                        )
                        ctx.spark.sql(
                            f"ALTER TABLE {quoted_table} "
                            f"ALTER COLUMN `{col_name}` SET NOT NULL"
                        )
                    logger.info(
                        f"Added column {col_name} ({src_type.simpleString()}) to target"
                    )
                except PYSPARK_EXCEPTION_BASE as e:
                    logger.warning(f"Could not add column {col_name}: {e}")
        except PYSPARK_EXCEPTION_BASE as e:
            logger.warning(f"Schema evolution failed: {e}")

    def validate_grain(
        self, ctx: PipelineContext, source_df: DataFrame, join_keys: list[str]
    ) -> None:
        if not join_keys:
            return
        grain_key = tuple(join_keys)
        if grain_key in ctx.validated_grains:
            logger.info(
                "Reusing exact grain validation for %s on %s",
                ctx.config.table_name,
                join_keys,
            )
            return
        grain_mode = getattr(ctx.config, "grain_validation", "error")
        if grain_mode == "skip":
            logger.info(
                f"Grain validation skipped for {ctx.config.table_name} (grain_validation=skip)"
            )
            return
        append_only = getattr(ctx.config, "append_only", False)
        if append_only is True:
            logger.info(
                f"Grain validation skipped for {ctx.config.table_name} (append_only=true; "
                "duplicates expected in the source batch)"
            )
            return

        use_approximate = getattr(
            ctx.runtime_options, "approx_grain_check", False
        ) or getattr(ctx.runtime_options, "use_approximate_unique", False)

        if use_approximate:
            from pyspark.sql.functions import approx_count_distinct, struct

            logger.info(
                f"Using approximate grain validation for {ctx.config.table_name}"
            )
            if metrics := source_df.agg(
                spark_count("*").alias("total"),
                approx_count_distinct(struct(*join_keys)).alias("distinct"),
            ).first():
                total = metrics["total"]
                distinct = metrics["distinct"]
                if total > distinct * 1.05:
                    msg = (
                        f"Approximate grain violation in {ctx.config.table_name}: "
                        f"Estimated {distinct} unique keys vs {total} total rows for grain {join_keys}. "
                        "Fix upstream deduplication before loading."
                    )
                    if grain_mode == "warn":
                        logger.warning(msg)
                    else:
                        raise ValueError(msg)
            ctx.validated_grains.add(grain_key)
            return

        grain_violations = (
            source_df.groupBy(*join_keys)
            .agg(spark_count("*").alias("__grain_count"))
            .filter("__grain_count > 1")
        )
        if sample_violations := grain_violations.limit(5).collect():
            violation_keys = [
                {k: row[k] for k in join_keys} for row in sample_violations
            ]
            msg = (
                f"Grain violation in {ctx.config.table_name}: "
                f"Duplicate keys found for grain {join_keys}. "
                f"Sample violations: {violation_keys}. "
                "Fix upstream deduplication before loading."
            )
            if grain_mode == "warn":
                logger.warning(msg)
            else:
                raise ValueError(msg)
        ctx.validated_grains.add(grain_key)

    def execute_merge(
        self, ctx: PipelineContext, source_df: DataFrame, join_keys: list[str]
    ) -> None:
        _merger.merge(
            target_table_name=ctx.config.table_name,
            source_df=source_df,
            join_keys=join_keys,
            delete_strategy=ctx.config.delete_strategy,
            batch_id=ctx.batch_id,
            scd_type=ctx.config.scd_type,
            track_history_columns=ctx.config.track_history_columns,
            surrogate_key_col=ctx.config.surrogate_key,
            schema_evolution=ctx.config.schema_evolution,
            effective_at_column=ctx.config.effective_at,
            durable_key_col=ctx.config.durable_key,
            history_table=ctx.config.history_table,
            current_value_columns=ctx.config.current_value_columns,
            append_only=ctx.config.append_only,
            full_snapshot_reconciliation=(
                ctx.work_plan.full_snapshot_reconciliation
                if ctx.work_plan is not None
                else True
            ),
        )

        from kimball.orchestration.services.descriptions import DescriptionManager

        try:
            DescriptionManager().sync(
                ctx.spark,
                ctx.config.table_name,
                ctx.config.table_description,
                ctx.config.column_descriptions,
            )
        except Exception as exc:
            logger.warning(
                "Description metadata sync failed for %s: %s",
                ctx.config.table_name,
                exc,
            )
        if ctx.config.optimize_after_merge:
            if os.environ.get("KIMBALL_ENABLE_INLINE_OPTIMIZE") == "1":
                _merger.optimize_table(
                    ctx.config.table_name, ctx.config.cluster_by or []
                )
            else:
                logger.info(
                    "Skipping inline OPTIMIZE. "
                    "Set KIMBALL_ENABLE_INLINE_OPTIMIZE=1 to enable."
                )
        if getattr(ctx.config, "vacuum_after_merge", False) is True:
            if os.environ.get("KIMBALL_ENABLE_VACUUM") == "1":
                _merger.vacuum_table(
                    ctx.config.table_name, ctx.config.vacuum_retention_hours
                )
            else:
                logger.info(
                    "Skipping inline VACUUM. Set KIMBALL_ENABLE_VACUUM=1 to enable."
                )

    def get_merge_metrics(self, ctx: PipelineContext) -> dict[str, int]:
        metrics = _merger.get_last_merge_metrics(
            ctx.config.table_name,
            batch_id=ctx.batch_id,
            spark=ctx.spark,
        )
        return {
            "rows_read": int(metrics.get("numSourceRows", 0)),
            "rows_written": int(metrics.get("numTargetRowsInserted", 0))
            + int(metrics.get("numTargetRowsUpdated", 0)),
        }

    def generate_skeletons(
        self, ctx: PipelineContext, source_df: DataFrame, join_keys: list[str]
    ) -> None:
        if ctx.config.table_type != "dimension":
            return
        if not ctx.table_exists(ctx.config.table_name):
            return
        target_schema = ctx.get_target_schema(ctx.config.table_name)
        if "__is_skeleton" not in {f.name for f in target_schema.fields}:
            return
        if not join_keys:
            return
        from kimball.processing.skeleton_generator import SkeletonGenerator

        gen = SkeletonGenerator(ctx.spark)
        surrogate = ctx.config.surrogate_key or "surrogate_key"
        gen.generate_skeletons(
            fact_df=source_df,
            dim_table_name=ctx.config.table_name,
            fact_join_key=join_keys[0],
            dim_join_key=join_keys[0],
            surrogate_key_col=surrogate,
            batch_id=ctx.batch_id,
            durable_key_col=ctx.config.durable_key,
        )
