from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import SparkSession

from kimball.common.config import TableConfig
from kimball.common.runtime import RuntimeOptions
from kimball.orchestration.watermark import ETLControlManager
from kimball.processing.loader import DataLoader


@dataclass
class PipelineContext:
    spark: SparkSession
    config: TableConfig
    etl_control: ETLControlManager
    loader: DataLoader
    runtime_options: RuntimeOptions
    batch_id: str = ""
    source_versions: dict[str, Any] = field(default_factory=dict)
    active_dfs: dict[str, Any] = field(default_factory=dict)
    table_created: bool = False
    merge_executed: bool = False
    total_rows_read: int = 0
    total_rows_written: int = 0
    pending_temporal_state: list[Any] = field(default_factory=list)
    validation_metrics: list[dict[str, Any]] = field(default_factory=list)
    work_plan: Any | None = None
    validated_grains: set[tuple[str, ...]] = field(default_factory=set)
