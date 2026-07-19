from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar

from pyspark.sql import SparkSession

from kimball.common.config import TableConfig
from kimball.common.runtime import RuntimeOptions
from kimball.orchestration.watermark import ETLControlManager
from kimball.processing.loader import DataLoader

if TYPE_CHECKING:
    from delta.tables import DeltaTable
    from pyspark.sql.types import StructType

_T = TypeVar("_T")


def _get(cache: dict[str, _T], key: str, factory: Callable[[], _T]) -> _T:
    """Return ``cache[key]``, computing it via *factory* on first access."""
    try:
        return cache[key]
    except KeyError:
        value = factory()
        cache[key] = value
        return value


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

    _table_exists_cache: dict[str, bool] = field(default_factory=dict)
    _schema_cache: dict[str, StructType] = field(default_factory=dict)
    _delta_table_cache: dict[str, DeltaTable] = field(default_factory=dict)

    def table_exists(self, table_name: str) -> bool:
        return _get(
            self._table_exists_cache,
            table_name,
            lambda: self.spark.catalog.tableExists(table_name),
        )

    def get_target_schema(self, table_name: str) -> StructType:
        return _get(
            self._schema_cache, table_name, lambda: self.spark.table(table_name).schema
        )

    def get_delta_table(self, table_name: str) -> DeltaTable:
        from delta.tables import DeltaTable

        return _get(
            self._delta_table_cache,
            table_name,
            lambda: DeltaTable.forName(self.spark, table_name),
        )

    def invalidate_table(self, table_name: str) -> None:
        self._table_exists_cache.pop(table_name, None)
        self._schema_cache.pop(table_name, None)
        self._delta_table_cache.pop(table_name, None)
