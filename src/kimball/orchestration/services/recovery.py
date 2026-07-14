from __future__ import annotations

import logging

from kimball.common.utils import quote_table_name
from kimball.orchestration.services.context import PipelineContext
from kimball.orchestration.transaction import TransactionManager

logger = logging.getLogger(__name__)


class RecoveryService:
    def __init__(self, transaction_manager: TransactionManager | None = None):
        self.transaction_manager = transaction_manager

    def recover_zombies(self, ctx: PipelineContext) -> bool:
        if not getattr(ctx.config, "enable_crash_recovery", True):
            return True
        try:
            ctx.spark.conf.set(
                "spark.databricks.delta.commitInfo.userMetadata", "test"
            )
            ctx.spark.conf.unset("spark.databricks.delta.commitInfo.userMetadata")
        except Exception:
            logger.info(
                "WARNING: Commit tagging unavailable (likely Serverless Compute). "
                "Crash recovery / Zombie detection is disabled."
            )
            return False

        running_batches = ctx.etl_control.get_running_batches(ctx.config.table_name)
        if running_batches:
            logger.info(
                f"Found {len(running_batches)} incomplete batches. Attempting recovery..."
            )
            for batch_info in running_batches:
                bad_batch_id = batch_info["batch_id"]
                source_table = batch_info["source_table"]
                if self.transaction_manager:
                    self.transaction_manager.recover_zombies(
                        ctx.config.table_name, bad_batch_id
                    )
                try:
                    ctx.etl_control.batch_fail(
                        ctx.config.table_name,
                        source_table,
                        "CRASH_RECOVERY: Rolled back",
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to mark batch as failed during crash recovery: {e}"
                    )
        return True

    def run_full_reload(self, ctx: PipelineContext) -> dict:
        logger.info(f"Full reload requested for {ctx.config.table_name}")

        if ctx.spark.catalog.tableExists(ctx.config.table_name):
            logger.info(f"Dropping target table {ctx.config.table_name}")
            ctx.spark.sql(
                f"DROP TABLE IF EXISTS {quote_table_name(ctx.config.table_name)}"
            )
        if ctx.config.scd_type == 4 and ctx.config.history_table:
            if ctx.spark.catalog.tableExists(ctx.config.history_table):
                logger.info(f"Dropping history table {ctx.config.history_table}")
                ctx.spark.sql(
                    f"DROP TABLE IF EXISTS {quote_table_name(ctx.config.history_table)}"
                )

        for source in ctx.config.sources:
            ctx.etl_control.reset_watermark(ctx.config.table_name, source.name)

        return {}