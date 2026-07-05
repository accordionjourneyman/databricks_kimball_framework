"""Wave-based pipeline executor for Kimball ETL."""

import concurrent.futures
import logging
import time
import warnings
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import SparkSession

from kimball.common.config import ConfigLoader
from kimball.common.errors import NonRetriableError
from kimball.orchestration.orchestrator import Orchestrator
from kimball.orchestration.watermark import get_etl_schema

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Result of a single pipeline execution."""
    config_path: str
    table_name: str
    table_type: str
    status: str  # SUCCESS, FAILED, SKIPPED
    rows_read: int = 0
    rows_written: int = 0
    duration_seconds: float = 0.0
    error_message: str | None = None
    batch_id: str | None = None


@dataclass
class ExecutionSummary:
    """Summary of the entire execution run."""
    total_pipelines: int
    successful: int
    failed: int
    skipped: int
    total_rows_read: int
    total_rows_written: int
    total_duration_seconds: float
    results: list[PipelineResult] = field(default_factory=list)

    def __str__(self) -> str:
        return (
            f"Execution Summary:\n"
            f"  Total Pipelines: {self.total_pipelines}\n"
            f"  Successful: {self.successful}\n"
            f"  Failed: {self.failed}\n"
            f"  Skipped: {self.skipped}\n"
            f"  Total Rows Read: {self.total_rows_read:,}\n"
            f"  Total Rows Written: {self.total_rows_written:,}\n"
            f"  Total Duration: {self.total_duration_seconds:.1f}s"
        )


class PipelineExecutor:
    # Wave-based parallel executor: dimensions first, then facts.
    def __init__(
        self,
        config_paths: list[str],
        spark: SparkSession | None = None,
        etl_schema: str | None = None,
        max_workers: int = 4,
        stop_on_failure: bool = True,
        watermark_database: str | None = None,
    ):
        if watermark_database is not None:
            warnings.warn(
                "The 'watermark_database' parameter is deprecated. Use 'etl_schema' instead, "
                "or set KIMBALL_ETL_SCHEMA environment variable.",
                DeprecationWarning,
                stacklevel=2,
            )
            if etl_schema is None:
                etl_schema = watermark_database

        if etl_schema is None:
            etl_schema = get_etl_schema()

        if etl_schema is None:
            raise ValueError(
                "ETL schema must be specified via one of:\n"
                "  1. Set KIMBALL_ETL_SCHEMA environment variable\n"
                "  2. Pass etl_schema parameter to PipelineExecutor"
            )

        self.config_paths = config_paths
        self.etl_schema = etl_schema
        self.max_workers = max_workers
        self.stop_on_failure = stop_on_failure
        self.config_loader = ConfigLoader()
        self.spark = spark
        self._categorize_pipelines()

    def _create_orchestrator(self, config_path: str) -> Orchestrator:
        return Orchestrator(config_path, spark=self.spark, etl_schema=self.etl_schema)

    def _categorize_pipelines(self) -> None:
        self.dimensions: list[dict[str, Any]] = []
        self.facts: list[dict[str, Any]] = []

        for path in self.config_paths:
            try:
                config = self.config_loader.load_config(path)
                pipeline_info = {
                    "path": path,
                    "table_name": config.table_name,
                    "table_type": config.table_type,
                }

                if config.table_type == "dimension":
                    self.dimensions.append(pipeline_info)
                else:
                    self.facts.append(pipeline_info)
            except Exception as e:
                logger.error(f"FATAL: Could not load config {path}: {e}")
                raise NonRetriableError(f"Invalid config file: {path}") from e

    def _failed_result(
        self, path: str, table_name: str, table_type: str, duration: float, error_msg: str
    ) -> PipelineResult:
        return PipelineResult(
            config_path=path,
            table_name=table_name,
            table_type=table_type,
            status="FAILED",
            duration_seconds=duration,
            error_message=error_msg,
        )

    def _run_single_pipeline(self, pipeline_info: dict[str, Any]) -> PipelineResult:
        path = pipeline_info["path"]
        table_name = pipeline_info["table_name"]
        table_type = pipeline_info["table_type"]
        start_time = time.time()

        try:
            logger.info(f"  Starting: {table_name}")
            orchestrator = self._create_orchestrator(path)
            result = orchestrator.run()
            duration = time.time() - start_time
            logger.info(
                f"  ✓ Completed: {table_name} ({duration:.1f}s, {result.get('rows_written', 0)} rows)"
            )
            return PipelineResult(
                config_path=path,
                table_name=table_name,
                table_type=table_type,
                status="SUCCESS",
                rows_read=result.get("rows_read", 0),
                rows_written=result.get("rows_written", 0),
                duration_seconds=duration,
                batch_id=result.get("batch_id"),
            )
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.info(f"  ✗ Failed: {table_name} - {error_msg}")
            return self._failed_result(path, table_name, table_type, duration, error_msg)

    def _run_wave(
        self, wave_name: str, pipelines: list[dict[str, Any]]
    ) -> list[PipelineResult]:
        if not pipelines:
            return []
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Wave: {wave_name} ({len(pipelines)} pipelines)")
        logger.info(f"{'=' * 60}")
        if self.max_workers == 1:
            return self._run_sequential(pipelines)
        return self._run_parallel(pipelines)

    def _run_sequential(self, pipelines: list[dict[str, Any]]) -> list[PipelineResult]:
        results = []
        for pipeline in pipelines:
            result = self._run_single_pipeline(pipeline)
            results.append(result)
            if result.status == "FAILED" and self.stop_on_failure:
                logger.info(f"\n⚠ Stopping due to failure in {result.table_name}")
                break
        return results

    def _run_parallel(self, pipelines: list[dict[str, Any]]) -> list[PipelineResult]:
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_pipeline = {executor.submit(self._run_single_pipeline, p): p for p in pipelines}
            for future in concurrent.futures.as_completed(future_to_pipeline):
                result = future.result()
                results.append(result)
                if result.status == "FAILED" and self.stop_on_failure:
                    logger.info(f"\n⚠ Failure detected in {result.table_name}. Cancelling remaining...")
                    for f in future_to_pipeline:
                        f.cancel()
                    break
        return results

    def run(self) -> ExecutionSummary:
        overall_start = time.time()
        all_results: list[PipelineResult] = []
        wave_failed = False

        logger.info(f"\n{'#' * 60}")
        logger.info("# Kimball Pipeline Executor")
        logger.info(f"# Dimensions: {len(self.dimensions)}, Facts: {len(self.facts)}")
        logger.info(f"# Max Workers: {self.max_workers}")
        logger.info(f"{'#' * 60}")

        dim_results = self._run_wave("Dimensions", self.dimensions)
        all_results.extend(dim_results)

        dim_failures = [r for r in dim_results if r.status == "FAILED"]
        if dim_failures and self.stop_on_failure:
            wave_failed = True
            logger.info(f"\n⚠ {len(dim_failures)} dimension(s) failed. Skipping facts wave.")

            for fact in self.facts:
                all_results.append(
                    PipelineResult(
                        config_path=fact["path"],
                        table_name=fact["table_name"],
                        table_type=fact["table_type"],
                        status="SKIPPED",
                        error_message="Skipped due to dimension failure",
                    )
                )

        if not wave_failed:
            fact_results = self._run_wave("Facts", self.facts)
            all_results.extend(fact_results)

        overall_duration = time.time() - overall_start

        summary = ExecutionSummary(
            total_pipelines=len(all_results),
            successful=len([r for r in all_results if r.status == "SUCCESS"]),
            failed=len([r for r in all_results if r.status == "FAILED"]),
            skipped=len([r for r in all_results if r.status == "SKIPPED"]),
            total_rows_read=sum(r.rows_read for r in all_results),
            total_rows_written=sum(r.rows_written for r in all_results),
            total_duration_seconds=overall_duration,
            results=all_results,
        )

        logger.info(f"\n{'=' * 60}")
        logger.info(summary)
        logger.info(f"{'=' * 60}\n")

        return summary

    def dry_run(self) -> None:
        logger.info(f"\n{'#' * 60}")
        logger.info("# Kimball Pipeline Executor - DRY RUN")
        logger.info("# (No pipelines will be executed)")
        logger.info(f"{'#' * 60}")

        logger.info(f"\nWave 1: Dimensions ({len(self.dimensions)} pipelines)")
        for i, dim in enumerate(self.dimensions, 1):
            logger.info(f"  {i}. {dim['table_name']} ({dim['path']})")

        logger.info(f"\nWave 2: Facts ({len(self.facts)} pipelines)")
        for i, fact in enumerate(self.facts, 1):
            logger.info(f"  {i}. {fact['table_name']} ({fact['path']})")

        logger.info("\nExecution Order:")
        logger.info(f"  1. All dimensions run in parallel (max {self.max_workers} workers)")
        logger.info("  2. After ALL dimensions complete, facts run in parallel")
        if self.stop_on_failure:
            logger.info("  3. If any dimension fails, facts wave is skipped")
        logger.info()
