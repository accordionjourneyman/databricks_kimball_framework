"""Orchestrator module for Kimball ETL pipelines.

Thin coordinator that delegates to focused service classes:
  - SourceLoader: loads CDF/full snapshot sources
  - KeyBroker: centralized early-arriving dimension key resolution
  - TransformValidator: transformation SQL, PII, FK/NK validation
  - MergeExecutor: table creation, adaptive pruning, merge dispatch
  - RecoveryService: zombie recovery, full reload
"""

from __future__ import annotations

import logging
import os
import time
import uuid
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
from kimball.observability.temporal_state import commit_temporal_state_updates
from kimball.orchestration.runtime import PipelineRuntime
from kimball.orchestration.services.context import PipelineContext
from kimball.orchestration.services.merge_executor import MergeExecutor
from kimball.orchestration.services.recovery import RecoveryService
from kimball.orchestration.services.source_loader import SourceLoader
from kimball.orchestration.services.transform_validator import TransformValidator
from kimball.orchestration.services.work_plan import (
    SourceWorkPlan,
    build_source_work_plan,
)
from kimball.orchestration.watermark import (
    ETLControlManager,
    compute_source_schema_fingerprint,
)
from kimball.processing.loader import DataLoader

logger = logging.getLogger(__name__)


class Orchestrator:
    """Coordinates the ETL process by delegating to focused service classes."""

    def __init__(self, config: TableConfig, runtime: PipelineRuntime) -> None:
        """Create an orchestrator from a validated config and shared runtime."""
        self.config = config
        self.runtime = runtime
        self.spark = runtime.spark
        self.runtime_options = runtime.options
        self.etl_control = runtime.etl_control
        self.loader = runtime.loader
        self.transaction_manager = runtime.transaction_manager
        self.metrics_collector = runtime.metrics_collector
        self._source_loader = SourceLoader()
        self._transform_validator = TransformValidator()
        self._merge_executor = MergeExecutor(runtime.table_creator)
        self._recovery_service = RecoveryService(self.transaction_manager)

    @classmethod
    def from_config(
        cls,
        config: TableConfig | str,
        *,
        spark: SparkSession | None = None,
        etl_schema: str | None = None,
        checkpoint_root: str | None = None,
        enable_metrics: bool = True,
    ) -> Orchestrator:
        """Load a configuration and build its runtime convenience bundle."""
        table_config = (
            ConfigLoader().load_config(config) if isinstance(config, str) else config
        )
        runtime = PipelineRuntime.for_config(
            table_config,
            spark=spark,
            etl_schema=etl_schema,
            checkpoint_root=checkpoint_root,
            enable_metrics=enable_metrics,
        )
        return cls(table_config, runtime)

    @property
    def _validator(self):
        return getattr(getattr(self, "_transform_validator", None), "_validator", None)

    @_validator.setter
    def _validator(self, value):
        if not hasattr(self, "_transform_validator"):
            self._transform_validator = TransformValidator()
        self._transform_validator._validator = value

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
            source for source in self.config.sources if source.cdc_strategy != "full"
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
        combined_result = {"rows_read": 0, "rows_written": 0}
        for iteration in range(1, max_iterations + 1):
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
        ctx = self._make_context(batch_id)

        if self.metrics_collector:
            self.metrics_collector.start_collection()

        pre_pipeline_start = time.time()

        self._recovery_service.recover_zombies(ctx)
        work_plan = self._build_source_work_plan()
        ctx.work_plan = work_plan
        active_names = [item.source_name for item in work_plan.active_items]

        if self.metrics_collector:
            self.metrics_collector.add_operation_metric(
                "pre_pipeline_overhead",
                duration_ms=(time.time() - pre_pipeline_start) * 1000,
            )
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
        batch_writes_start = time.time()

        # H4 Optimization: Skip synchronous 'RUNNING' writes to reduce Delta operations.
        # The completion write (batch_complete_all) will later upsert the SUCCESS state.
        if os.environ.get("KIMBALL_BATCH_CONTROL_WRITES") != "1":
            self.etl_control.batch_start_all(
                self.config.table_name, active_names, run_batch_id=batch_id
            )
        if self.metrics_collector:
            self.metrics_collector.add_operation_metric(
                "etl_control_batch_start",
                duration_ms=(time.time() - batch_writes_start) * 1000,
            )

        active_dfs: dict[str, Any] = {}
        try:
            merge_start = time.time()
            with self.transaction_manager.table_transaction(
                self.config.table_name, batch_id
            ):
                result = self._execute_merge_phases(
                    ctx, work_plan, batch_id, merge_start
                )
            return self._finalize_success(ctx, batch_id, active_names, result)

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
        while attempt <= max_retries:
            try:
                return self.run()
            except RetriableError as e:
                attempt += 1
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
        raise RuntimeError("run_with_retry exhausted retries without a result")

    def _execute_merge_phases(
        self, ctx, work_plan, batch_id: str, merge_start: float
    ) -> dict[str, Any]:
        """Load, transform, merge, and complete control records for one run.

        Runs inside the table transaction. Watermarks advance only on
        success: ``batch_complete_all`` fires after the merge metrics are
        read, inside the same transaction, guarded by ``merge_executed``.
        """
        source_versions, active_dfs = self._source_loader.load(ctx, work_plan)
        ctx.source_versions = source_versions
        ctx.active_dfs = active_dfs

        transformed_df = self._transform_validator.transform_and_validate(
            ctx, active_dfs
        )

        table_created = self._merge_executor.ensure_target_table(ctx, transformed_df)
        self._merge_executor.seed_defaults(ctx, table_created)
        source_df = self._merge_executor.prepare_source_df(ctx, transformed_df)
        if self.config.table_type == "fact":
            join_keys = self.config.merge_keys or []
        else:
            join_keys = self.config.natural_keys or []
        self._merge_executor.generate_skeletons(ctx, source_df, join_keys)
        self._merge_executor.validate_grain(ctx, source_df, join_keys)
        self._merge_executor.execute_merge(ctx, source_df, join_keys)
        merge_executed = True

        metrics = self._merge_executor.get_merge_metrics(ctx)
        total_rows_read = metrics["rows_read"]
        total_rows_written = metrics["rows_written"]

        if self.metrics_collector:
            self.metrics_collector.add_operation_metric(
                "merge_and_load",
                duration_ms=(time.time() - merge_start) * 1000,
                rows_read=total_rows_read,
                rows_written=total_rows_written,
            )

        if not merge_executed:
            logger.info(
                "Merge skipped: no rows after transformation. "
                "Watermarks will NOT be advanced."
            )
            return {
                "rows_read": total_rows_read,
                "rows_written": total_rows_written,
            }
        self._complete_batch(
            ctx, work_plan, source_versions, total_rows_read, total_rows_written
        )
        return {
            "rows_read": total_rows_read,
            "rows_written": total_rows_written,
        }

    def _complete_batch(
        self, ctx, work_plan, source_versions, total_rows_read, total_rows_written
    ) -> None:
        """Upsert SUCCESS control rows; tolerate observability-state failures."""
        batch_complete_start = time.time()
        config_fingerprint = ConfigLoader().compute_fingerprint(self.config)
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
                        compute_source_schema_fingerprint(self.spark, item.source_name)
                    ),
                }
                for item in work_plan.active_items
            ],
        )
        if self.metrics_collector:
            self.metrics_collector.add_operation_metric(
                "etl_control_batch_complete",
                duration_ms=(time.time() - batch_complete_start) * 1000,
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

    def _finalize_success(
        self, ctx, batch_id: str, active_names: list[str], result: dict[str, Any]
    ) -> dict[str, Any]:
        """Stop metrics collection and shape the SUCCESS summary."""
        logger.info(
            f"Pipeline completed. Read: {result['rows_read']}, "
            f"Written: {result['rows_written']}"
        )
        metrics_summary = {}
        if self.metrics_collector:
            self.metrics_collector.stop_collection()
            metrics_summary = self.metrics_collector.get_summary()
        return {
            "status": "SUCCESS",
            "batch_id": batch_id,
            "target_table": self.config.table_name,
            "rows_read": result["rows_read"],
            "rows_written": result["rows_written"],
            "active_sources": len(active_names),
            "metrics": metrics_summary,
            "validation_metrics": ctx.validation_metrics,
        }
