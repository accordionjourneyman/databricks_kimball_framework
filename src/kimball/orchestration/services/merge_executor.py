from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import count as spark_count

from kimball.common.utils import quote_table_name
from kimball.observability.resilience import _feature_enabled
from kimball.orchestration.services.context import PipelineContext
from kimball.processing import merger as _merger
from kimball.processing.table_creator import TableCreator

try:
    from pyspark.errors import PySparkException as PYSPARK_EXCEPTION_BASE
except ImportError:
    try:
        from pyspark.sql.utils import AnalysisException as PYSPARK_EXCEPTION_BASE
    except ImportError:
        PYSPARK_EXCEPTION_BASE = Exception

logger = logging.getLogger(__name__)

SYSTEM_COLUMNS = {
    "__is_current", "__valid_from", "__valid_to", "__etl_processed_at",
    "__is_deleted", "__etl_batch_id", "__is_skeleton", "hashdiff", "__merge_action",
}
CDF_COLUMNS = {"_change_type", "_commit_version", "_commit_timestamp"}


class MergeExecutor:
    def __init__(self, table_creator: TableCreator | None = None):
        self.table_creator = table_creator or TableCreator()

    def ensure_target_table(self, ctx: PipelineContext, transformed_df: DataFrame) -> bool:
        if ctx.spark.catalog.tableExists(ctx.config.table_name):
            return False
        logger.info(f"Creating table {ctx.config.table_name}...")
        self._create_target_table(ctx, transformed_df)
        if ctx.config.scd_type == 4 and ctx.config.history_table:
            self.table_creator.create_history_table(ctx.config.history_table)
        return True

    def _create_target_table(self, ctx: PipelineContext, transformed_df: DataFrame) -> None:
        schema_df = self.table_creator.add_system_columns(
            transformed_df.limit(0),
            ctx.config.scd_type,
            ctx.config.surrogate_key,
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
            config=ctx.config.model_dump(),
            cluster_by=cluster_cols or [],
            surrogate_key_col=ctx.config.surrogate_key,
        )

    def seed_defaults(self, ctx: PipelineContext, table_created: bool) -> None:
        if not table_created or ctx.config.table_type != "dimension":
            return
        target_schema = ctx.spark.table(ctx.config.table_name).schema
        if ctx.config.scd_type == 2:
            _merger.ensure_scd2_defaults(
                ctx.config.table_name,
                target_schema,
                ctx.config.surrogate_key or "surrogate_key",
                ctx.config.default_rows,
            )
        elif ctx.config.scd_type == 1 and ctx.config.surrogate_key:
            _merger.ensure_scd1_defaults(
                ctx.config.table_name,
                target_schema,
                ctx.config.surrogate_key,
                ctx.config.default_rows,
            )

    def prepare_source_df(self, ctx: PipelineContext, transformed_df: DataFrame) -> DataFrame:
        if getattr(ctx.config, "enable_lineage_truncation", False):
            logger.info("Creating DataFrame checkpoint for merge operation...")
            try:
                checkpoint_dir = ctx.spark.sparkContext.getCheckpointDir()
                if checkpoint_dir:
                    checkpointed_df = transformed_df.checkpoint()
                else:
                    checkpointed_df = transformed_df.localCheckpoint()
            except PYSPARK_EXCEPTION_BASE as e:
                logger.info(f"Checkpoint failed, using local: {e}")
                checkpointed_df = transformed_df.localCheckpoint()
            except Exception as e:
                logger.info(f"Unexpected checkpoint error, using local: {e}")
                checkpointed_df = transformed_df.localCheckpoint()
        else:
            checkpointed_df = transformed_df

        if ctx.spark.catalog.tableExists(ctx.config.table_name):
            target_schema = ctx.spark.table(ctx.config.table_name).schema
            target_columns = {f.name for f in target_schema.fields}
            return self._apply_adaptive_pruning(ctx, checkpointed_df, target_columns)
        return checkpointed_df

    def _apply_adaptive_pruning(
        self, ctx: PipelineContext, df: DataFrame, target_columns: set[str]
    ) -> DataFrame:
        protection_set: set[str] = set()
        if ctx.config.natural_keys:
            protection_set.update(ctx.config.natural_keys)
        if ctx.config.merge_keys:
            protection_set.update(ctx.config.merge_keys)
        if ctx.config.surrogate_key:
            protection_set.add(ctx.config.surrogate_key)
        if ctx.config.track_history_columns:
            protection_set.update(ctx.config.track_history_columns)
        if ctx.config.current_value_columns:
            protection_set.update(ctx.config.current_value_columns)
        if ctx.config.foreign_keys:
            for fk in ctx.config.foreign_keys:
                protection_set.add(fk.column)
        protection_set |= SYSTEM_COLUMNS
        protection_set |= CDF_COLUMNS

        cols_to_keep: list[str] = []
        cols_dropped: list[str] = []
        cols_added_to_target: list[str] = []

        for c in df.columns:
            if c in target_columns or c in protection_set:
                cols_to_keep.append(c)
                if c not in target_columns and c not in SYSTEM_COLUMNS and c not in CDF_COLUMNS:
                    cols_added_to_target.append(c)
            else:
                cols_dropped.append(c)

        if ctx.config.schema_evolution and cols_added_to_target:
            self._evolve_target_schema(ctx, cols_added_to_target)

        if cols_dropped:
            logger.info(
                f"Adaptive pruning: keeping {len(cols_to_keep)}/{len(df.columns)} columns, "
                f"dropping {len(cols_dropped)} unused columns"
            )
            return df.select(*cols_to_keep)
        return df

    def _evolve_target_schema(self, ctx: PipelineContext, new_columns: list[str]) -> None:
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
                    logger.info(f"Added column {col_name} ({src_type.simpleString()}) to target")
                except Exception as e:
                    logger.warning(f"Could not add column {col_name}: {e}")
        except Exception as e:
            logger.warning(f"Schema evolution failed: {e}")

    def validate_grain(self, ctx: PipelineContext, source_df: DataFrame, join_keys: list[str]) -> None:
        if not join_keys:
            return
        grain_mode = getattr(ctx.config, "grain_validation", "error")
        if grain_mode == "skip":
            logger.info(f"Grain validation skipped for {ctx.config.table_name} (grain_validation=skip)")
            return
        grain_violations = (
            source_df.groupBy(*join_keys)
            .agg(spark_count("*").alias("__grain_count"))
            .filter("__grain_count > 1")
        )
        if len(grain_violations.limit(1).head(1)) > 0:
            sample_violations = grain_violations.limit(5).collect()
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

    def execute_merge(self, ctx: PipelineContext, source_df: DataFrame, join_keys: list[str]) -> None:
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
            history_table=ctx.config.history_table,
            current_value_columns=ctx.config.current_value_columns,
        )

        if ctx.config.optimize_after_merge:
            if os.environ.get("KIMBALL_ENABLE_INLINE_OPTIMIZE") == "1":
                _merger.optimize_table(ctx.config.table_name, ctx.config.cluster_by or [])
            else:
                logger.info(
                    "Skipping inline OPTIMIZE. "
                    "Set KIMBALL_ENABLE_INLINE_OPTIMIZE=1 to enable."
                )

    def get_merge_metrics(self, ctx: PipelineContext) -> dict[str, int]:
        metrics = _merger.get_last_merge_metrics(ctx.config.table_name, batch_id=ctx.batch_id)
        return {
            "rows_read": int(metrics.get("numSourceRows", 0)),
            "rows_written": int(metrics.get("numTargetRowsInserted", 0))
            + int(metrics.get("numTargetRowsUpdated", 0)),
        }