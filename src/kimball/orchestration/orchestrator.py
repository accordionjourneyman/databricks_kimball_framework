"""Orchestrator module for Kimball ETL pipelines.

Thin coordinator that delegates to focused service classes:
  - SourceLoader: loads CDF/full snapshot sources
  - SkeletonManager: early-arriving fact skeletons + identity bridge
  - TransformValidator: transformation SQL, PII, FK/NK validation
  - MergeExecutor: table creation, adaptive pruning, merge dispatch
  - RecoveryService: zombie recovery, full reload
"""

from __future__ import annotations

import logging
import os
import time
import uuid
import warnings
from typing import Any, cast

from pyspark.errors import AnalysisException, PySparkException
from pyspark.sql import SparkSession

from kimball.common.config import ConfigLoader, TableConfig
from kimball.common.constants import (
    SPARK_CONF_AQE_COALESCE,
    SPARK_CONF_AQE_ENABLED,
    SPARK_CONF_AQE_SKEW_JOIN,
    SPARK_CONF_SHUFFLE_PARTITIONS,
    SPARK_CONF_SKEW_FACTOR,
    SPARK_CONF_SKEW_SIZE_THRESHOLD,
)
from kimball.common.errors import NonRetriableError, RetriableError
from kimball.common.runtime import RuntimeOptions
from kimball.common.spark_session import get_spark
from kimball.observability.resilience import (
    PipelineCheckpoint,  # noqa: F401 — kept for test patch compatibility
    QueryMetricsCollector,
    StagingCleanupManager,
    _feature_enabled,
)
from kimball.observability.temporal_state import commit_temporal_state_updates
from kimball.orchestration.services.context import PipelineContext
from kimball.orchestration.services.merge_executor import MergeExecutor
from kimball.orchestration.services.recovery import RecoveryService
from kimball.orchestration.services.skeleton_manager import SkeletonManager
from kimball.orchestration.services.source_loader import SourceLoader
from kimball.orchestration.services.transform_validator import TransformValidator
from kimball.orchestration.services.work_plan import (
    SourceWorkPlan,
    build_source_work_plan,
)
from kimball.orchestration.transaction import TransactionManager
from kimball.orchestration.watermark import (
    ETLControlManager,
    compute_source_schema_fingerprint,
    get_etl_schema,
)
from kimball.processing import (
    merger as _merger,  # noqa: F401 — kept for test patch compatibility
)
from kimball.processing.loader import DataLoader
from kimball.processing.skeleton_generator import SkeletonGenerator
from kimball.processing.table_creator import TableCreator

logger = logging.getLogger(__name__)


class Orchestrator:
    """Coordinates the ETL process by delegating to focused service classes."""

    def __init__(
        self,
        config_path: str | TableConfig,
        spark: SparkSession | None = None,
        etl_schema: str | None = None,
        watermark_database: str | None = None,
        enable_metrics: bool = True,
        checkpoint_table: str | None = None,
        checkpoint_root: str | None = None,
        loader: DataLoader | None = None,
        etl_control: ETLControlManager | None = None,
        table_creator: TableCreator | None = None,
        skeleton_generator: SkeletonGenerator | None = None,
        cleanup_manager: StagingCleanupManager | None = None,
        transaction_manager: TransactionManager | None = None,
    ):
        self.config_loader = ConfigLoader()
        if isinstance(config_path, str):
            self.config = self.config_loader.load_config(config_path)
        else:
            self.config = config_path
        self.spark = spark or get_spark()
        self.runtime_options = RuntimeOptions.from_environment()

        if watermark_database is not None:
            warnings.warn(
                "The 'watermark_database' parameter is deprecated. Use 'etl_schema' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if etl_schema is None:
                etl_schema = watermark_database

        if etl_schema is None:
            etl_schema = get_etl_schema()
        if etl_schema is None:
            if "." in self.config.table_name:
                etl_schema = self.config.table_name.split(".")[0]
            else:
                raise ValueError(
                    "ETL schema must be specified via one of:\n"
                    "  1. Set KIMBALL_ETL_SCHEMA environment variable\n"
                    "  2. Pass etl_schema parameter to Orchestrator\n"
                    "  3. Use fully-qualified table names (db.table) in config"
                )

        if checkpoint_root is None:
            checkpoint_root = os.getenv("KIMBALL_CHECKPOINT_ROOT")
        if checkpoint_root:
            logger.info(f"Setting Spark checkpoint directory to: {checkpoint_root}")
            self.spark.sparkContext.setCheckpointDir(checkpoint_root)

        self.etl_control = etl_control or ETLControlManager(
            etl_schema=etl_schema, spark_session=self.spark
        )
        self.loader = loader or DataLoader(spark_session=self.spark)
        self.transaction_manager = transaction_manager or TransactionManager(self.spark)

        self.metrics_collector = (
            QueryMetricsCollector()
            if enable_metrics and _feature_enabled("metrics")
            else None
        )
        self.checkpoint_manager = (
            _feature_enabled("checkpoints") and checkpoint_table is not None
        )
        self.cleanup_manager = cleanup_manager or (
            StagingCleanupManager() if _feature_enabled("staging_cleanup") else None
        )

        self._source_loader = SourceLoader()
        if skeleton_generator is None:
            from kimball.processing.skeleton_generator import SkeletonGenerator

            skeleton_generator = SkeletonGenerator(spark_session=self.spark)
        self._skeleton_manager = SkeletonManager(skeleton_generator)
        self._transform_validator = TransformValidator()
        self._merge_executor = MergeExecutor(table_creator)
        self._recovery_service = RecoveryService(self.transaction_manager)

    @property
    def _validator(self):
        return getattr(getattr(self, "_transform_validator", None), "_validator", None)

    @_validator.setter
    def _validator(self, value):
        if not hasattr(self, "_transform_validator"):
            self._transform_validator = TransformValidator()
        self._transform_validator._validator = value

    @property
    def skeleton_generator(self):
        return getattr(
            getattr(self, "_skeleton_manager", None), "skeleton_generator", None
        )

    @skeleton_generator.setter
    def skeleton_generator(self, value):
        if not hasattr(self, "_skeleton_manager"):
            self._skeleton_manager = SkeletonManager()
        self._skeleton_manager.skeleton_generator = value

    @property
    def table_creator(self):
        return getattr(getattr(self, "_merge_executor", None), "table_creator", None)

    @table_creator.setter
    def table_creator(self, value):
        if not hasattr(self, "_merge_executor"):
            self._merge_executor = MergeExecutor()
        self._merge_executor.table_creator = value

    def _transform_and_validate(self, active_dfs):
        ctx = self._make_context_safe()
        ctx.active_dfs = active_dfs
        tv = getattr(self, "_transform_validator", None) or TransformValidator()
        return tv.transform_and_validate(ctx, active_dfs)

    def _generate_skeletons(self, active_dfs, batch_id):
        ctx = self._make_context_safe(batch_id)
        ctx.active_dfs = active_dfs
        sm = getattr(self, "_skeleton_manager", None) or SkeletonManager()
        sm.generate_skeletons(ctx, active_dfs)

    def _recover_zombies(self) -> bool:
        ctx = self._make_context_safe()
        tm = getattr(self, "transaction_manager", None)
        rs = getattr(self, "_recovery_service", None) or RecoveryService(tm)
        if tm and not rs.transaction_manager:
            rs.transaction_manager = tm
        return rs.recover_zombies(ctx)

    def _make_context_safe(self, batch_id: str = "") -> PipelineContext:
        return PipelineContext(
            spark=cast(SparkSession, getattr(self, "spark", None)),
            config=cast(TableConfig, getattr(self, "config", None)),
            etl_control=cast(ETLControlManager, getattr(self, "etl_control", None)),
            loader=cast(DataLoader, getattr(self, "loader", None)),
            runtime_options=getattr(
                self, "runtime_options", RuntimeOptions.from_environment()
            ),
            batch_id=batch_id,
        )

    def _load_active_sources(self, batch_id: str = ""):
        ctx = self._make_context_safe(batch_id)
        sl = getattr(self, "_source_loader", None) or SourceLoader()
        plan = self._build_source_work_plan()
        ctx.work_plan = plan
        return sl.load(ctx, plan)

    def _build_source_work_plan(self) -> SourceWorkPlan:
        incremental = [
            source
            for source in self.config.sources
            if source.cdc_strategy != "full"
        ]
        states = self.etl_control.get_states(
            self.config.table_name, [source.name for source in incremental]
        )
        watermarks = {
            source.name: states.get(source.name, {}).get("last_processed_version")
            for source in incremental
        }
        latest_versions = {
            source.name: self.loader.get_latest_version(source.name)
            for source in incremental
        }
        return build_source_work_plan(
            self.config.sources,
            watermarks=watermarks,
            latest_versions=latest_versions,
            preserve_all_changes=(
                self.config.preserve_all_changes and self.config.scd_type == 2
            ),
        )

    def _apply_identity_bridge(self, df):
        ctx = self._make_context_safe()
        sm = getattr(self, "_skeleton_manager", None) or SkeletonManager()
        return sm.apply_identity_bridge(ctx, df)

    def _run_compile_time_sql_check(self) -> None:
        if not self.runtime_options.compile_time_sql_check:
            return
        if not self.config.transformation_sql:
            return
        issues = self.config_loader.validate_transformation_sql(
            self.config, spark=self.spark
        )
        if issues:
            raise ValueError(
                f"Compile-time SQL validation failed for {self.config.table_name}:\n"
                + "\n".join(f"  - {i}" for i in issues)
            )

    def _apply_spark_configs(self) -> dict[str, str | None]:
        previous: dict[str, str | None] = {}
        try:
            spark = self.spark
            runtime_options = getattr(self, "runtime_options", RuntimeOptions())
            settings = {
                SPARK_CONF_AQE_ENABLED: "true",
                SPARK_CONF_AQE_SKEW_JOIN: "true",
                SPARK_CONF_AQE_COALESCE: "true",
                SPARK_CONF_SKEW_SIZE_THRESHOLD: f"{runtime_options.skew_threshold_mb}MB",
                SPARK_CONF_SKEW_FACTOR: str(runtime_options.skew_factor),
            }
            if runtime_options.shuffle_partitions != "auto":
                settings[SPARK_CONF_SHUFFLE_PARTITIONS] = str(
                    runtime_options.shuffle_partitions
                )
            for key, value in settings.items():
                try:
                    previous[key] = spark.conf.get(key)
                except (PySparkException, AnalysisException):
                    previous[key] = None
                spark.conf.set(key, value)
        except (PySparkException, AnalysisException) as e:
            logger.debug(f"Could not set Spark configs: {e}")
        return previous

    def _restore_spark_configs(self, previous: dict[str, str | None]) -> None:
        for key, value in previous.items():
            try:
                if value is None:
                    self.spark.conf.unset(key)
                else:
                    self.spark.conf.set(key, value)
            except (PySparkException, AnalysisException) as exc:
                logger.debug(f"Could not restore Spark config {key}: {exc}")

    def _make_context(self, batch_id: str = "") -> PipelineContext:
        return PipelineContext(
            spark=self.spark,
            config=self.config,
            etl_control=self.etl_control,
            loader=self.loader,
            runtime_options=self.runtime_options,
            batch_id=batch_id,
        )

    def run(self, max_retries: int = 0, full_reload: bool = False) -> dict[str, Any]:
        previous = self._apply_spark_configs()
        try:
            if full_reload:
                return self._run_full_reload()
            if self.config.preserve_all_changes and self.config.scd_type == 2:
                return self._run_with_version_loop()
            if max_retries > 0:
                return self.run_with_retry(max_retries=max_retries)
            return self._run_pipeline_once()
        finally:
            self._restore_spark_configs(previous)

    def _run_full_reload(self) -> dict[str, Any]:
        ctx = self._make_context_safe()
        rs = getattr(self, "_recovery_service", None) or RecoveryService(
            getattr(self, "transaction_manager", None)
        )
        rs.run_full_reload(ctx)
        return self._run_pipeline_once()

    def _run_with_version_loop(self, max_iterations: int = 100) -> dict[str, Any]:
        iteration = 0
        combined_result = {"rows_read": 0, "rows_written": 0}
        while iteration < max_iterations:
            iteration += 1
            result = self._run_pipeline_once()
            if result.get("active_sources") == 0:
                logger.info("Preserve All Changes: All CDF sources caught up")
                return combined_result
            combined_result["rows_read"] += result.get("rows_read", 0)
            combined_result["rows_written"] += result.get("rows_written", 0)
            logger.info(f"Preserve All Changes: Iteration {iteration} processed.")

        return combined_result

    def _run_pipeline_once(self) -> dict[str, Any]:
        logger.info(f"Starting pipeline for {self.config.table_name}")
        batch_id = str(uuid.uuid4())
        pipeline_start = time.time()
        ctx = self._make_context(batch_id)

        if self.metrics_collector:
            self.metrics_collector.start_collection()

        self._recovery_service.recover_zombies(ctx)
        work_plan = self._build_source_work_plan()
        ctx.work_plan = work_plan
        active_names = [item.source_name for item in work_plan.active_items]
        if not active_names:
            if self.metrics_collector:
                self.metrics_collector.stop_collection()
            return {
                "status": "SUCCESS",
                "batch_id": batch_id,
                "target_table": self.config.table_name,
                "rows_read": 0,
                "rows_written": 0,
                "active_sources": 0,
                "metrics": {},
                "validation_metrics": [],
            }
        self.etl_control.batch_start_all(
            self.config.table_name, active_names, run_batch_id=batch_id
        )

        active_dfs: dict[str, Any] = {}
        try:
            with self.transaction_manager.table_transaction(
                self.config.table_name, batch_id
            ):
                source_versions, active_dfs = self._source_loader.load(ctx, work_plan)
                ctx.source_versions = source_versions
                ctx.active_dfs = active_dfs

                self._skeleton_manager.generate_skeletons(ctx, active_dfs)
                transformed_df = self._transform_validator.transform_and_validate(
                    ctx, active_dfs
                )

                table_created = self._merge_executor.ensure_target_table(
                    ctx, transformed_df
                )
                self._merge_executor.seed_defaults(ctx, table_created)
                source_df = self._merge_executor.prepare_source_df(ctx, transformed_df)
                if self.config.table_type == "fact":
                    join_keys = self.config.merge_keys or []
                else:
                    join_keys = self.config.natural_keys or []
                self._merge_executor.validate_grain(ctx, source_df, join_keys)
                self._merge_executor.execute_merge(ctx, source_df, join_keys)
                merge_executed = True

                metrics = self._merge_executor.get_merge_metrics(ctx)
                total_rows_read = metrics["rows_read"]
                total_rows_written = metrics["rows_written"]

                if self.metrics_collector:
                    self.metrics_collector.add_operation_metric(
                        "merge",
                        duration_ms=(time.time() - pipeline_start) * 1000,
                        rows_read=total_rows_read,
                        rows_written=total_rows_written,
                    )

                if merge_executed:
                    config_fingerprint = self.config_loader.compute_fingerprint(
                        self.config
                    )
                    self.etl_control.batch_complete_all(
                        self.config.table_name,
                        [
                            {
                                "source_table": item.source_name,
                                "new_version": source_versions[item.source_name],
                                "rows_read": total_rows_read,
                                "rows_written": total_rows_written,
                                "config_fingerprint": config_fingerprint,
                                "source_schema_fingerprint": (
                                    compute_source_schema_fingerprint(
                                        self.spark, item.source_name
                                    )
                                ),
                            }
                            for item in work_plan.active_items
                        ],
                    )
                    try:
                        commit_temporal_state_updates(ctx)
                    except Exception:
                        observability = self.config.observability
                        if observability and observability.write_failure == "error":
                            raise
                        logger.warning(
                            "Temporal observability state could not be persisted; "
                            "the target load remains successful",
                            exc_info=True,
                        )
                else:
                    logger.info(
                        "Merge skipped — no rows after transformation. "
                        "Watermarks will NOT be advanced."
                    )

            logger.info(
                f"Pipeline completed. Read: {total_rows_read}, Written: {total_rows_written}"
            )

            metrics_summary = {}
            if self.metrics_collector:
                self.metrics_collector.stop_collection()
                metrics_summary = self.metrics_collector.get_summary()

            return {
                "status": "SUCCESS",
                "batch_id": batch_id,
                "target_table": self.config.table_name,
                "rows_read": total_rows_read,
                "rows_written": total_rows_written,
                "active_sources": len(active_names),
                "metrics": metrics_summary,
                "validation_metrics": ctx.validation_metrics,
            }

        except Exception as e:
            if self.metrics_collector:
                self.metrics_collector.stop_collection()
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.info(f"Pipeline failed: {error_msg}")
            try:
                self.etl_control.batch_fail_all(
                    self.config.table_name, active_names, error_msg
                )
            except PySparkException as batch_err:
                logger.debug(f"Could not mark batch as failed: {batch_err}")
            raise e

        finally:
            active_dfs.clear()
            for source in self.config.sources:
                try:
                    self.spark.catalog.dropTempView(source.alias)
                except (PySparkException, OSError, AttributeError):
                    pass  # Cleanup must never mask the original error

    def run_with_retry(
        self, max_retries: int = 3, backoff_seconds: int = 30
    ) -> dict[str, Any]:
        attempt = 0
        last_error = None
        while attempt <= max_retries:
            try:
                return self.run()
            except RetriableError as e:
                attempt += 1
                last_error = e
                if attempt <= max_retries:
                    wait_time = backoff_seconds * (2 ** (attempt - 1))
                    logger.info(
                        f"Retriable error: {e}. Waiting {wait_time}s before retry {attempt}/{max_retries}"
                    )
                    time.sleep(wait_time)
                else:
                    raise
            except NonRetriableError:
                raise
            except Exception:
                raise
        if last_error:
            raise last_error
        raise RuntimeError("Max retries exceeded with unknown error")
