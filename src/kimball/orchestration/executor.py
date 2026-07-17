"""Dependency-aware, single-process pipeline executor for Kimball ETL."""

from __future__ import annotations

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
from kimball.planning.compiler import Profile, ProjectCompiler, ProjectValidationError

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
    """Summary of an execution run."""

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
            "Execution Summary:\n"
            f"  Total Pipelines: {self.total_pipelines}\n"
            f"  Successful: {self.successful}\n"
            f"  Failed: {self.failed}\n"
            f"  Skipped: {self.skipped}\n"
            f"  Total Rows Read: {self.total_rows_read:,}\n"
            f"  Total Rows Written: {self.total_rows_written:,}\n"
            f"  Total Duration: {self.total_duration_seconds:.1f}s"
        )


class PipelineExecutor:
    """Execute a compiled DAG without sharing Spark across worker threads.

    Generated Databricks Jobs provide safe cross-pipeline parallelism. This
    in-process executor runs ready nodes serially because a shared Spark session
    is not an isolation boundary for concurrent DDL/DML.
    """

    def __init__(
        self,
        config_paths: list[str],
        spark: SparkSession | None = None,
        etl_schema: str | None = None,
        max_workers: int = 4,
        stop_on_failure: bool = True,
        watermark_database: str | None = None,
        profile: Profile = "dev",
    ):
        if watermark_database is not None:
            warnings.warn(
                "The 'watermark_database' parameter is deprecated. Use 'etl_schema' "
                "instead, or set KIMBALL_ETL_SCHEMA.",
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
        self.profile: Profile = profile
        self.config_loader = ConfigLoader()
        self.spark = spark
        self._categorize_pipelines()

    def _create_orchestrator(self, config_path: str) -> Orchestrator:
        return Orchestrator(config_path, spark=self.spark, etl_schema=self.etl_schema)

    def _categorize_pipelines(self) -> None:
        entries = []
        for path in self.config_paths:
            try:
                entries.append((path, self.config_loader.load_config(path)))
            except Exception as exc:
                logger.error("FATAL: Could not load config %s: %s", path, exc)
                raise NonRetriableError(f"Invalid config file: {path}") from exc

        try:
            self.project = ProjectCompiler(profile=self.profile).compile(entries)
        except ProjectValidationError as exc:
            logger.error("FATAL: Invalid pipeline project: %s", exc)
            raise NonRetriableError(f"Invalid pipeline project: {exc}") from exc

        self.pipeline_by_table: dict[str, dict[str, Any]] = {}
        self.dimensions: list[dict[str, Any]] = []
        self.facts: list[dict[str, Any]] = []
        for table_name, node in self.project.nodes.items():
            info = {
                "path": node.config_path,
                "table_name": table_name,
                "table_type": node.table_type,
                "dependencies": list(node.dependencies),
            }
            self.pipeline_by_table[table_name] = info
            (self.dimensions if node.table_type == "dimension" else self.facts).append(
                info
            )

    @staticmethod
    def _failed_result(
        path: str,
        table_name: str,
        table_type: str,
        duration: float,
        error_msg: str,
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
            logger.info("  Starting: %s", table_name)
            result = self._create_orchestrator(path).run()
            duration = time.time() - start_time
            logger.info("  Completed: %s (%.1fs)", table_name, duration)
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
        except Exception as exc:
            duration = time.time() - start_time
            error_msg = f"{type(exc).__name__}: {exc}"
            logger.info("  Failed: %s - %s", table_name, error_msg)
            return self._failed_result(
                path, table_name, table_type, duration, error_msg
            )

    def _run_sequential(self, pipelines: list[dict[str, Any]]) -> list[PipelineResult]:
        results = []
        for pipeline in pipelines:
            result = self._run_single_pipeline(pipeline)
            results.append(result)
            if result.status == "FAILED" and self.stop_on_failure:
                break
        return results

    def _run_parallel(self, pipelines: list[dict[str, Any]]) -> list[PipelineResult]:
        warnings.warn(
            "In-process parallel pipelines would share a Spark session; executing "
            "serially. Use generated Databricks job tasks for safe parallelism.",
            RuntimeWarning,
            stacklevel=2,
        )
        return self._run_sequential(pipelines)

    def _run_wave(
        self, wave_name: str, pipelines: list[dict[str, Any]]
    ) -> list[PipelineResult]:
        """Backward-compatible serial wave helper; DAG execution uses ``run``."""

        if not pipelines:
            return []
        logger.info("Legacy wave %s (%s pipelines)", wave_name, len(pipelines))
        return self._run_sequential(pipelines)

    def run(self) -> ExecutionSummary:
        overall_start = time.time()
        all_results: list[PipelineResult] = []
        results_by_table: dict[str, PipelineResult] = {}

        logger.info(
            "Kimball DAG executor: %s pipelines in %s levels",
            len(self.pipeline_by_table),
            len(self.project.levels),
        )
        for level_number, level in enumerate(self.project.levels, 1):
            logger.info("Dependency level %s: %s", level_number, ", ".join(level))
            for table_name in level:
                node = self.project.nodes[table_name]
                pipeline = self.pipeline_by_table[table_name]
                failed_dependencies = [
                    dependency
                    for dependency in node.dependencies
                    if results_by_table[dependency].status != "SUCCESS"
                ]
                if failed_dependencies:
                    result = PipelineResult(
                        config_path=pipeline["path"],
                        table_name=table_name,
                        table_type=pipeline["table_type"],
                        status="SKIPPED",
                        error_message=(
                            "Skipped because upstream did not succeed: "
                            + ", ".join(failed_dependencies)
                        ),
                    )
                else:
                    result = self._run_single_pipeline(pipeline)
                results_by_table[table_name] = result
                all_results.append(result)

        summary = ExecutionSummary(
            total_pipelines=len(all_results),
            successful=sum(r.status == "SUCCESS" for r in all_results),
            failed=sum(r.status == "FAILED" for r in all_results),
            skipped=sum(r.status == "SKIPPED" for r in all_results),
            total_rows_read=sum(r.rows_read for r in all_results),
            total_rows_written=sum(r.rows_written for r in all_results),
            total_duration_seconds=time.time() - overall_start,
            results=all_results,
        )
        logger.info(summary)
        return summary

    def dry_run(self) -> None:
        logger.info("Kimball Pipeline Executor - DRY RUN")
        for number, level in enumerate(self.project.levels, 1):
            logger.info("Level %s: %s", number, ", ".join(level))
            for table_name in level:
                node = self.project.nodes[table_name]
                logger.info(
                    "  %s (%s) depends_on=%s",
                    table_name,
                    node.config_path,
                    list(node.dependencies),
                )
        logger.info("In-process execution is serialized for Spark session safety.")
