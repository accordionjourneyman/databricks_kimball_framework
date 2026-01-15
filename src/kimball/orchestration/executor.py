"""PipelineExecutor - Wave-based pipeline execution for notebooks.

This module provides an executor for running multiple Kimball pipelines
with automatic dependency ordering (dimensions before facts).

IMPORTANT LIMITATION (C-06):
    ThreadPoolExecutor does NOT provide real Spark parallelism:
    - Python GIL means only one thread runs Python code at a time
    - All threads share the same SparkSession, so jobs serialize
    - For actual parallel execution, use Databricks Jobs with for_each tasks

This executor is designed for:
    - Demo notebooks (sequential execution is fine)
    - Local development
    - Quick iteration during development
    - Single-threaded deterministic testing

For production workloads with parallelism requirements, use:
    - Databricks Jobs with for_each tasks
    - Separate job clusters per pipeline
    - Delta Live Tables (DLT) pipelines

Configuration:
    Set KIMBALL_ETL_SCHEMA environment variable to configure ETL control table location:

        import os
        os.environ["KIMBALL_ETL_SCHEMA"] = "my_catalog.etl_config"

Example:
    from kimball import PipelineExecutor

    # Option 1: Use environment variable (recommended)
    import os
    os.environ["KIMBALL_ETL_SCHEMA"] = "gold"
    executor = PipelineExecutor([
        "configs/dim_customer.yml",
        "configs/dim_product.yml",
        "configs/fact_sales.yml"
    ])

    # Option 2: Explicit etl_schema
    executor = PipelineExecutor([
        "configs/dim_customer.yml",
        "configs/dim_product.yml",
        "configs/fact_sales.yml"
    ], etl_schema="gold")

    results = executor.run()
"""

import concurrent.futures
import logging
import time
import warnings
from dataclasses import dataclass, field
from typing import Any

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
    """
    Wave-based parallel pipeline executor for Kimball ETL.

    Automatically orders pipelines into waves:
    - Wave 1: Dimensions (can run in parallel)
    - Wave 2: Facts (run after all dimensions complete)

    Within each wave, pipelines run in parallel using ThreadPoolExecutor.

    ⚠️ FINDING-020: PARALLELISM LIMITATION WARNING ⚠️
    Using ThreadPoolExecutor for Spark jobs within the same driver provides LIMITED
    actual parallelism. All threads share the same SparkSession and Spark jobs are
    submitted to the same cluster. Whether they run in parallel depends on:
    - Cluster resources (cores, memory)
    - Spark scheduler configuration
    - Python GIL (though Spark operations release it)

    For TRUE parallelism at scale, use one of:
    - Databricks Jobs with for_each tasks (RECOMMENDED for production)
    - Separate job clusters per pipeline
    - Databricks Workflows with parallel tasks

    Within-session parallelism is best for:
    - I/O-bound operations (many small dimension loads)
    - Demo notebooks and local development
    - Quick iteration during development

    For compute-heavy fact loads, expect serialized execution despite threading.

    Args:
        config_paths: List of paths to pipeline YAML config files
        etl_schema: Schema for ETL control table. If not provided, uses
                    KIMBALL_ETL_SCHEMA environment variable.
        max_workers: Maximum parallel pipelines per wave (default: 4)
        stop_on_failure: If True, stop execution when any pipeline fails (default: True)
        watermark_database: **Deprecated** - Use etl_schema instead.

    Example:
        # Option 1: Use environment variable (recommended)
        import os
        os.environ["KIMBALL_ETL_SCHEMA"] = "gold"
        executor = PipelineExecutor([
            "configs/dim_customer.yml",
            "configs/dim_product.yml",
            "configs/dim_date.yml",
            "configs/fact_sales.yml",
        ])

        # Option 2: Explicit etl_schema
        executor = PipelineExecutor([...], etl_schema="gold")

        summary = executor.run()
        logger.info(summary)
    """

    def __init__(
        self,
        config_paths: list[str],
        etl_schema: str | None = None,
        max_workers: int = 4,
        stop_on_failure: bool = True,
        # Deprecated parameter
        watermark_database: str | None = None,
    ):
        # Handle deprecated 'watermark_database' parameter
        if watermark_database is not None:
            warnings.warn(
                "The 'watermark_database' parameter is deprecated. Use 'etl_schema' instead, "
                "or set KIMBALL_ETL_SCHEMA environment variable.",
                DeprecationWarning,
                stacklevel=2,
            )
            if etl_schema is None:
                etl_schema = watermark_database

        # Resolve ETL schema: explicit param > env var
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

        # Categorize pipelines into waves
        self._categorize_pipelines()

    def _create_orchestrator(self, config_path: str) -> Orchestrator:
        """Factory method to create Orchestrator instance. Can be overridden for testing."""
        return Orchestrator(config_path, etl_schema=self.etl_schema)

    def _categorize_pipelines(self) -> None:
        """Categorize pipelines into dimension and fact waves."""
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
                logger.info(f"Warning: Could not load config {path}: {e}")
                # Add to facts wave (will fail during execution with proper error)
                self.facts.append(
                    {
                        "path": path,
                        "table_name": f"unknown ({path})",
                        "table_type": "unknown",
                    }
                )

    def _run_single_pipeline(self, pipeline_info: dict[str, Any]) -> PipelineResult:
        """Execute a single pipeline and return the result."""
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

        except NonRetriableError as e:
            duration = time.time() - start_time
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.info(f"  ✗ Failed (non-retriable): {table_name} - {error_msg}")

            return PipelineResult(
                config_path=path,
                table_name=table_name,
                table_type=table_type,
                status="FAILED",
                duration_seconds=duration,
                error_message=error_msg,
            )

        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.info(f"  ✗ Failed: {table_name} - {error_msg}")

            return PipelineResult(
                config_path=path,
                table_name=table_name,
                table_type=table_type,
                status="FAILED",
                duration_seconds=duration,
                error_message=error_msg,
            )

    def _run_wave(
        self, wave_name: str, pipelines: list[dict[str, Any]]
    ) -> list[PipelineResult]:
        """Run a wave of pipelines in parallel."""
        if not pipelines:
            return []

        logger.info(f"\n{'=' * 60}")
        logger.info(f"Wave: {wave_name} ({len(pipelines)} pipelines)")
        logger.info(f"{'=' * 60}")

        results = []

        if self.max_workers == 1:
            # Sequential execution
            for pipeline in pipelines:
                result = self._run_single_pipeline(pipeline)
                results.append(result)

                if result.status == "FAILED" and self.stop_on_failure:
                    logger.info(f"\n⚠ Stopping due to failure in {result.table_name}")
                    break
        else:
            # Parallel execution
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_workers
            ) as executor:
                future_to_pipeline = {
                    executor.submit(self._run_single_pipeline, p): p for p in pipelines
                }

                for future in concurrent.futures.as_completed(future_to_pipeline):
                    result = future.result()
                    results.append(result)

                    if result.status == "FAILED" and self.stop_on_failure:
                        logger.info(
                            f"\n⚠ Failure detected in {result.table_name}. Cancelling remaining..."
                        )
                        # Cancel pending futures
                        for f in future_to_pipeline:
                            f.cancel()
                        break

        return results

    def run(self) -> ExecutionSummary:
        """
        Execute all pipelines in wave order (dimensions first, then facts).

        Returns:
            ExecutionSummary with results for all pipelines
        """
        overall_start = time.time()
        all_results: list[PipelineResult] = []
        wave_failed = False

        logger.info(f"\n{'#' * 60}")
        logger.info("# Kimball Pipeline Executor")
        logger.info(f"# Dimensions: {len(self.dimensions)}, Facts: {len(self.facts)}")
        logger.info(f"# Max Workers: {self.max_workers}")
        logger.info(f"{'#' * 60}")

        # Wave 1: Dimensions
        dim_results = self._run_wave("Dimensions", self.dimensions)
        all_results.extend(dim_results)

        # Check for failures
        dim_failures = [r for r in dim_results if r.status == "FAILED"]
        if dim_failures and self.stop_on_failure:
            wave_failed = True
            logger.info(
                f"\n⚠ {len(dim_failures)} dimension(s) failed. Skipping facts wave."
            )

            # Mark facts as skipped
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

        # Wave 2: Facts (only if dimensions succeeded or stop_on_failure is False)
        if not wave_failed:
            fact_results = self._run_wave("Facts", self.facts)
            all_results.extend(fact_results)

        # Build summary
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

        # Print summary
        logger.info(f"\n{'=' * 60}")
        logger.info(summary)
        logger.info(f"{'=' * 60}\n")

        return summary

    def dry_run(self) -> None:
        """
        Print the execution plan without running any pipelines.

        Useful for verifying wave ordering before execution.
        """
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
        logger.info(
            f"  1. All dimensions run in parallel (max {self.max_workers} workers)"
        )
        logger.info("  2. After ALL dimensions complete, facts run in parallel")
        if self.stop_on_failure:
            logger.info("  3. If any dimension fails, facts wave is skipped")
        logger.info()
