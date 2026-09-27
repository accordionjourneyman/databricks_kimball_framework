"""Shared runtime dependency bundle for Kimball orchestrators."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession

from kimball.common.config import TableConfig, TargetConfig
from kimball.common.runtime import RuntimeOptions
from kimball.common.spark_session import get_spark
from kimball.observability.resilience import QueryMetricsCollector
from kimball.orchestration.transaction import TransactionManager
from kimball.orchestration.watermark import ETLControlManager
from kimball.processing.loader import DataLoader
from kimball.processing.table_creator import TableCreator


@dataclass
class PipelineRuntime:
    """Runtime services shared by batch and streaming orchestrators."""

    spark: SparkSession
    etl_schema: str
    options: RuntimeOptions
    etl_control: ETLControlManager
    loader: DataLoader
    transaction_manager: TransactionManager
    table_creator: TableCreator
    metrics_collector: QueryMetricsCollector | None

    @classmethod
    def for_config(
        cls,
        config: TableConfig,
        *,
        spark: SparkSession | None = None,
        etl_schema: str | None = None,
        checkpoint_root: str | None = None,
        enable_metrics: bool | None = None,
        runtime_options: RuntimeOptions | None = None,
        target: TargetConfig | None = None,
    ) -> PipelineRuntime:
        """Build the complete runtime once for a validated table configuration."""
        options = (runtime_options or RuntimeOptions.from_environment()).resolved(
            etl_schema=etl_schema,
            target_etl_schema=target.etl_schema if target else None,
            checkpoint_root=checkpoint_root,
            target_checkpoint_root=target.checkpoint_root if target else None,
        )
        active_spark = spark or get_spark()
        resolved_schema = options.etl_schema
        if resolved_schema is None and "." in config.table_name:
            resolved_schema = config.table_name.split(".")[0]
        if resolved_schema is None:
            raise ValueError(
                "ETL schema must be specified via KIMBALL_ETL_SCHEMA, the runtime, "
                "or a fully-qualified target table name"
            )

        if checkpoint_dir := options.checkpoint_root:
            active_spark.sparkContext.setCheckpointDir(checkpoint_dir)

        return cls(
            spark=active_spark,
            etl_schema=resolved_schema,
            options=options,
            etl_control=ETLControlManager(
                etl_schema=resolved_schema, spark_session=active_spark
            ),
            loader=DataLoader(spark_session=active_spark),
            transaction_manager=TransactionManager(active_spark),
            table_creator=TableCreator(),
            metrics_collector=(
                QueryMetricsCollector()
                if (
                    options.feature_enabled("metrics")
                    if enable_metrics is None
                    else enable_metrics
                )
                else None
            ),
        )
