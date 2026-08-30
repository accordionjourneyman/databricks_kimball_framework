"""Shared runtime dependency bundle for Kimball orchestrators."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession

from kimball.common.config import TableConfig
from kimball.common.runtime import RuntimeOptions
from kimball.common.spark_session import get_spark
from kimball.observability.resilience import QueryMetricsCollector, _feature_enabled
from kimball.orchestration.transaction import TransactionManager
from kimball.orchestration.watermark import ETLControlManager, get_etl_schema
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
        enable_metrics: bool = True,
    ) -> PipelineRuntime:
        """Build the complete runtime once for a validated table configuration."""
        options = RuntimeOptions.from_environment()
        active_spark = spark or get_spark()
        resolved_schema = etl_schema or options.etl_schema or get_etl_schema()
        if resolved_schema is None and "." in config.table_name:
            resolved_schema = config.table_name.split(".")[0]
        if resolved_schema is None:
            raise ValueError(
                "ETL schema must be specified via KIMBALL_ETL_SCHEMA, the runtime, "
                "or a fully-qualified target table name"
            )

        if checkpoint_dir := checkpoint_root or options.checkpoint_root:
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
                if enable_metrics and _feature_enabled("metrics")
                else None
            ),
        )
