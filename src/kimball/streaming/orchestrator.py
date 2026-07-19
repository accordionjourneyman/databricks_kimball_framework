"""
Streaming orchestrator: same public contract as ``Orchestrator``,
implemented on top of Spark structured streaming.

The class is a thin shell over the existing batch code.  Micro-batch
processing (transform, validate, merge) is delegated to
``StreamingMicroBatchProcessor``.  The streaming layer's job is to
feed each micro-batch through the processor via ``foreachBatch``.

For multi-source pipelines, the orchestrator starts one streaming query
per CDF source and joins their output by a per-batch ``foreachBatch``
that fans the rows into the same MERGE.
"""

from __future__ import annotations

import logging
import os
import shutil
import time
import uuid
from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from kimball.streaming.checkpoint import default_checkpoint_path
from kimball.streaming.loader import StreamCdfLoader
from kimball.streaming.services.microbatch import StreamingMicroBatchProcessor

if TYPE_CHECKING:
    from kimball.common.config import TableConfig

logger = logging.getLogger(__name__)


class StreamingOrchestrator:
    """Run a Kimball pipeline as a Spark structured-streaming query."""

    def __init__(
        self,
        config: TableConfig | str,
        spark: SparkSession | None = None,
        etl_schema: str | None = None,
        max_workers: int = 1,
    ) -> None:
        from kimball.common.config import ConfigLoader

        self._config_path: str | None = None
        if isinstance(config, str):
            self._config_path = config
            self.config: TableConfig = ConfigLoader().load_config(config)
        else:
            self.config = config

        if spark is None:
            from databricks.sdk.runtime import spark as _sdk_spark

            self.spark: SparkSession = _sdk_spark
        else:
            self.spark = spark

        if etl_schema is None:
            etl_schema = os.environ.get("KIMBALL_ETL_SCHEMA", "etl_control")
        self.etl_schema = etl_schema

        from kimball.orchestration.watermark import ETLControlManager

        self.etl_control = ETLControlManager(etl_schema=self.etl_schema)
        self.stream_loader = StreamCdfLoader(self.spark)
        self._active_queries: dict[str, StreamingQuery] = {}

    def run(self, full_reload: bool = False) -> dict[str, Any]:
        if full_reload:
            self._run_full_reload()

        if not self._is_streaming():
            logger.info("No source has streaming.enabled=True; falling back to batch.")
            from kimball.orchestration.orchestrator import Orchestrator

            return Orchestrator(self.config, spark=self.spark).run()

        start = time.time()
        summary: dict[str, Any] = {
            "status": "SUCCESS",
            "rows_read": 0,
            "rows_written": 0,
            "duration_seconds": 0.0,
            "queries": {},
            "errors": [],
        }
        try:
            self._start_queries(summary)
            for source_name, query in list(self._active_queries.items()):
                query.awaitTermination()
                summary["queries"][source_name] = query
        except Exception as exc:
            logger.exception("Streaming pipeline failed")
            summary["status"] = "FAILED"
            summary["errors"].append(str(exc))
        finally:
            summary["duration_seconds"] = time.time() - start
        return summary

    def _run_full_reload(self) -> None:
        logger.info(f"Full reload requested for {self.config.table_name}")
        for source in self.config.sources:
            self.etl_control.reset_watermark(self.config.table_name, source.name)
        for source in self.config.sources:
            streaming = source.streaming
            if streaming and streaming.enabled:
                cp = streaming.checkpoint_location or default_checkpoint_path(
                    source.name, self.etl_schema
                )
                if os.path.exists(cp):
                    shutil.rmtree(cp, ignore_errors=True)
        from kimball.orchestration.orchestrator import Orchestrator

        batch = Orchestrator(
            self._config_path or self.config,
            spark=self.spark,
            etl_schema=self.etl_schema,
        )
        result = batch.run(full_reload=True)
        if result.get("status") == "FAILED":
            raise RuntimeError(f"Full reload failed: {result.get('errors', 'unknown')}")

    def stop(self) -> None:
        for source_name, query in list(self._active_queries.items()):
            try:
                query.stop()
            except Exception as exc:
                logger.warning(f"Failed to stop query for {source_name}: {exc}")

    def _is_streaming(self) -> bool:
        return any(getattr(s.streaming, "enabled", False) for s in self.config.sources)

    def _streaming_sources(self) -> list[Any]:
        return [
            s
            for s in self.config.sources
            if getattr(s.streaming, "enabled", False) and s.cdc_strategy == "cdf"
        ]

    def _start_queries(self, summary: dict[str, Any]) -> None:
        for source in self._streaming_sources():
            streaming_cfg = source.streaming
            if (
                streaming_cfg.starting_version is None
                and streaming_cfg.starting_timestamp is None
            ):
                watermark = self.etl_control.get_watermark(
                    self.config.table_name, source.name
                )
                latest_v = self.stream_loader.get_latest_version(source.name)
                if watermark is not None and watermark < latest_v:
                    logger.info(
                        f"Resuming streaming for {source.name} from version {watermark + 1}"
                    )
                    streaming_cfg = streaming_cfg.model_copy(
                        update={"starting_version": watermark + 1}
                    )

            stream_df = self.stream_loader.stream_cdf(
                table_name=source.name, config=streaming_cfg
            )
            checkpoint = (
                source.streaming.checkpoint_location
                or default_checkpoint_path(source.name, self.etl_schema)
            )
            trigger_kwargs = self._build_trigger_kwargs(source.streaming)
            query_name = f"kimball__{self.config.table_name}__{source.alias}__{uuid.uuid4().hex[:8]}"
            writer = (
                stream_df.writeStream.queryName(query_name)
                .foreachBatch(self._make_foreach(source))
                .option("checkpointLocation", checkpoint)
            )
            writer = writer.trigger(**trigger_kwargs)
            query = writer.start()
            self._active_queries[source.name] = query
            summary["queries"][source.name] = query
            logger.info(f"Started streaming query {query_name} for {source.name}")

    def _build_trigger_kwargs(self, cfg: Any) -> dict[str, Any]:
        if cfg.trigger == "available_now":
            return {"availableNow": True}
        return {"processingTime": cfg.trigger_interval or "30 seconds"}

    def _make_foreach(self, source: Any):
        def _foreach(batch_df: DataFrame, batch_id: int) -> None:
            if "_change_type" in batch_df.columns:
                batch_df = batch_df.filter("_change_type != 'update_preimage'")
            if batch_df.isEmpty():
                logger.info("Micro-batch %s for %s is empty", batch_id, source.name)
                return
            persisted = False
            try:
                batch_df = batch_df.persist()
                persisted = True
            except Exception as exc:
                logger.debug("persist() unavailable (%s); continuing without cache", exc)
            batch_spark = batch_df.sparkSession
            batch_df.createOrReplaceTempView(source.alias)
            try:
                if source.streaming and source.streaming.per_version:
                    self._execute_microbatch_per_version(batch_df, source, batch_id)
                else:
                    self._execute_one_microbatch(batch_df, source, batch_id)
            except Exception as exc:
                logger.exception(f"Micro-batch {batch_id} for {source.name} failed")
                self.etl_control.batch_fail(
                    target_table=self.config.table_name,
                    source_table=source.name,
                    error_message=str(exc)[:3500],
                )
                raise
            finally:
                batch_spark.catalog.dropTempView(source.alias)
                if persisted:
                    batch_df.unpersist(blocking=False)

        return _foreach

    def _get_processor(self, spark: Any | None = None) -> StreamingMicroBatchProcessor:
        return StreamingMicroBatchProcessor(
            spark or self.spark, self.config, self.etl_schema, self.etl_control
        )

    def _execute_one_microbatch(
        self, batch_df: DataFrame, source: Any, batch_id: int
    ) -> None:
        batch_spark = batch_df.sparkSession if isinstance(batch_df, DataFrame) else None
        self._get_processor(batch_spark).process_microbatch(batch_df, source, batch_id)

    def _execute_microbatch_per_version(
        self, batch_df: DataFrame, source: Any, batch_id: int
    ) -> None:
        if "_commit_version" not in batch_df.columns:
            self._execute_one_microbatch(batch_df, source, batch_id)
            return

        versions = sorted(
            int(r._commit_version)
            for r in batch_df.select("_commit_version").distinct().collect()
        )
        logger.info(
            f"Processing {len(versions)} version(s) for {source.name} in micro-batch {batch_id}"
        )
        for version in versions:
            version_df = batch_df.filter(f"_commit_version = {version}")
            version_df.createOrReplaceTempView(source.alias)
            self._execute_one_microbatch(version_df, source, batch_id)
            logger.info(f"Processed version {version} for {source.name}")
        self.spark.catalog.dropTempView(source.alias)
