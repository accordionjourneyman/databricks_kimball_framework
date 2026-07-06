"""
Streaming orchestrator: same public contract as ``Orchestrator``,
implemented on top of Spark structured streaming.

The class is intentionally a thin shell over the existing batch code:
``merger.merge_scd1`` / ``merge_scd2`` / ``merge_scd4`` / ``merge_scd6``
do not change. The streaming layer's job is to feed each micro-batch
through the same per-batch workhorse that ``Orchestrator._run_pipeline_once``
runs.

A streaming run is one ``Trigger.AvailableNow`` query per CDF source
(or one ``Trigger.ProcessingTime`` query, if the user asked for it),
piped into ``foreachBatch`` which:

1. Resolves ``_change_type`` and dedups in micro-batch.
2. Registers the source DF as a temp view so the user's
   ``transformation_sql`` SELECTs the right alias.
3. Applies adaptive column pruning (when configured) and SCD1/SCD2
   transformations the same way the batch path does.
4. Issues the MERGE via the existing ``merger.merge(...)`` dispatcher.
5. Advances the etl_control watermark to the highest ``_commit_version``
   seen in this micro-batch.

For multi-source pipelines, the orchestrator starts one streaming query
per CDF source and joins their output by a per-batch ``foreachBatch``
that fans the rows into the same MERGE. Full-snapshot sources are
loaded once at startup (their state is fixed at query start).
"""

from __future__ import annotations

import logging
import os
import shutil
import time
import uuid
from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from kimball.processing import merger as _merger
from kimball.streaming.checkpoint import default_checkpoint_path
from kimball.streaming.loader import StreamCdfLoader

if TYPE_CHECKING:
    from kimball.common.config import TableConfig

logger = logging.getLogger(__name__)


class StreamingOrchestrator:
    """Run a Kimball pipeline as a Spark structured-streaming query.

    The constructor mirrors ``Orchestrator``'s. When
    ``sources[*].streaming.enabled`` is true on the config, the
    orchestrator starts one streaming query per CDF source. Otherwise
    it falls back to the regular batch path (``Orchestrator.run``).

    Example::

        from kimball import StreamingOrchestrator
        from kimball.common.config import ConfigLoader

        cfg = ConfigLoader().load_config("configs/dim_customer.yml")
        orch = StreamingOrchestrator(cfg, spark=spark)
        summary = orch.run()        # blocks until trigger completes
    """

    def __init__(
        self,
        config: TableConfig | str,
        spark: SparkSession | None = None,
        etl_schema: str | None = None,
        max_workers: int = 1,
    ) -> None:
        from kimball.common.config import ConfigLoader

        if isinstance(config, str):
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

    # ---- public API ---------------------------------------------------

    def run(self, full_reload: bool = False) -> dict[str, Any]:
        """Run the streaming pipeline.

        Parameters
        ----------
        full_reload : bool
            If True, reset watermarks, clear streaming checkpoints, run a
            full batch reload (``Orchestrator._run_full_reload``), then
            resume streaming from the new baseline.

        Returns a summary dict shaped like ``Orchestrator.run()``::

            {
                "status": "SUCCESS" | "FAILED",
                "rows_read": int,
                "rows_written": int,
                "duration_seconds": float,
                "queries": {alias: StreamingQuery, ...},
                "errors": [...],
            }
        """
        if full_reload:
            self._run_full_reload()

        if not self._is_streaming():
            logger.info(
                "No source has streaming.enabled=True; "
                "falling back to batch Orchestrator."
            )
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
        except Exception as exc:  # noqa: BLE001
            logger.exception("Streaming pipeline failed")
            summary["status"] = "FAILED"
            summary["errors"].append(str(exc))
        finally:
            summary["duration_seconds"] = time.time() - start
        return summary

    def _run_full_reload(self) -> None:
        """Reset watermarks, clear checkpoints, then run a full batch reload.

        After this method returns the target table has been rebuilt from a
        full snapshot and the watermark is at the latest source version.
        The subsequent streaming run will start from that version.
        """
        logger.info(f"Full reload requested for {self.config.table_name}")

        # 1. Reset watermarks so the batch path sees no watermark → full snapshot.
        for source in self.config.sources:
            self.etl_control.reset_watermark(self.config.table_name, source.name)

        # 2. Clear streaming checkpoint directories.
        for source in self.config.sources:
            if getattr(source.streaming, "enabled", False):
                cp = source.streaming.checkpoint_location or default_checkpoint_path(
                    source.name, self.etl_schema
                )
                if os.path.exists(cp):
                    logger.info(f"Removing checkpoint directory: {cp}")
                    shutil.rmtree(cp, ignore_errors=True)

        # 3. Run the batch Orchestrator with full_reload=True.
        from kimball.orchestration.orchestrator import Orchestrator

        batch = Orchestrator(self.config, spark=self.spark, etl_schema=self.etl_schema)
        result = batch.run(full_reload=True)
        if result.get("status") == "FAILED":
            raise RuntimeError(
                f"Full reload batch run failed: {result.get('errors', 'unknown error')}"
            )
        logger.info(
            "Full reload complete — streaming will resume from current watermark"
        )

    def stop(self) -> None:
        """Stop all active streaming queries (best-effort)."""
        for source_name, query in list(self._active_queries.items()):
            try:
                query.stop()
                logger.info(f"Stopped streaming query for {source_name}")
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    f"Failed to stop streaming query for {source_name}: {exc}"
                )

    # ---- internals ----------------------------------------------------

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
            stream_df = self.stream_loader.stream_cdf(
                table_name=source.name,
                config=source.streaming,
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
            logger.info(
                f"Started streaming query {query_name} for {source.name} "
                f"with checkpoint at {checkpoint} "
                f"(trigger={source.streaming.trigger})"
            )

    def _build_trigger_kwargs(self, cfg: Any) -> dict[str, Any]:
        """Return kwargs for ``DataStreamWriter.trigger`` (PySpark 4.x API).

        In PySpark 4.0 ``Trigger`` is no longer importable from
        ``pyspark.sql.streaming``; the trigger is set via
        ``writer.trigger(processingTime=...)`` or
        ``writer.trigger(availableNow=True)``.
        """
        if cfg.trigger == "available_now":
            return {"availableNow": True}
        return {"processingTime": cfg.trigger_interval or "30 seconds"}

    def _make_foreach(self, source: Any):
        """Build a foreachBatch closure that runs the per-micro-batch work."""

        def _foreach(batch_df: DataFrame, batch_id: int) -> None:
            # 1. Drop update_preimage rows.
            if "_change_type" in batch_df.columns:
                batch_df = batch_df.filter("_change_type != 'update_preimage'")
            # 2. Register as a temp view so the user's transformation_sql
            #    can SELECT against the alias.
            batch_df.createOrReplaceTempView(source.alias)
            try:
                self._execute_one_microbatch(batch_df, source.name, batch_id)
            except Exception as exc:  # noqa: BLE001
                logger.exception(f"Micro-batch {batch_id} for {source.name} failed")
                self.etl_control.batch_fail(
                    target_table=self.config.table_name,
                    source_table=source.name,
                    error_message=str(exc)[:3500],
                )
                raise

        return _foreach

    def _execute_one_microbatch(
        self, batch_df: DataFrame, source_name: str, batch_id: int
    ) -> None:
        """Apply the SCD merge for one micro-batch.

        The temp view for ``source.alias`` is already registered by
        ``_make_foreach``, so ``transformation_sql`` can SELECT against
        it the same way the batch path does.
        """
        rows_written = 0

        if not self.spark.catalog.tableExists(self.config.table_name):
            raise RuntimeError(
                f"Target table {self.config.table_name} does not exist. "
                "Run the batch Orchestrator once to create it, then "
                "start the streaming query."
            )

        # Run transformation_sql against the registered temp view.
        if self.config.transformation_sql:
            source_df = self.spark.sql(self.config.transformation_sql)
        else:
            source_df = batch_df

        rows_read = source_df.count() if source_df.columns else 0

        # Derive join keys the same way the batch orchestrator does.
        if self.config.table_type == "fact":
            join_keys = self.config.merge_keys or []
        else:
            join_keys = self.config.natural_keys or []

        _merger.merge(
            target_table_name=self.config.table_name,
            source_df=source_df,
            join_keys=join_keys,
            delete_strategy=self.config.delete_strategy,
            scd_type=self.config.scd_type,
            track_history_columns=self.config.track_history_columns,
            surrogate_key_col=self.config.surrogate_key,
            surrogate_key_strategy=self.config.surrogate_key_strategy,
            schema_evolution=self.config.schema_evolution,
            effective_at_column=self.config.effective_at,
            history_table=self.config.history_table,
            current_value_columns=self.config.current_value_columns,
        )
        # Count rows written from the merge operation.
        try:
            metrics = _merger.get_last_merge_metrics(self.config.table_name)
            rows_written = int(metrics.get("num_target_rows_inserted", 0)) + int(
                metrics.get("num_target_rows_updated", 0)
            )
        except Exception:
            rows_written = 0

        # Update etl_control with the highest _commit_version seen.
        new_version: int | None = None
        if "_commit_version" in batch_df.columns:
            max_row = batch_df.agg({"_commit_version": "max"}).first()
            if max_row and max_row[0] is not None:
                new_version = int(max_row[0])

        if new_version is not None:
            self.etl_control.batch_complete(
                target_table=self.config.table_name,
                source_table=source_name,
                new_version=new_version,
                rows_read=rows_read,
                rows_written=rows_written,
            )
