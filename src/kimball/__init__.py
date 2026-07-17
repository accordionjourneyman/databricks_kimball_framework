"""Public package facade with lazy runtime imports.

Configuration, contract, and planning APIs remain importable without PySpark
or Delta. Runtime symbols are loaded only when accessed.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from kimball.common.errors import (
        ConfigurationError,
        DeltaConcurrentModificationError,
        ETLControlConflictError,
        ETLControlNotFoundError,
        KimballError,
        NonRetriableError,
        RetriableError,
        SchemaMismatchError,
        SourceTableBusyError,
        TransformationSQLError,
        TransientSparkError,
    )
    from kimball.common.runtime import RuntimeOptions
    from kimball.observability.bus_matrix import generate_bus_matrix
    from kimball.observability.resilience import (
        PipelineCheckpoint,
        QueryMetricsCollector,
        StagingCleanupManager,
        StagingTableManager,
    )
    from kimball.orchestration.contracts_monitor import ContractMonitor
    from kimball.orchestration.executor import (
        ExecutionSummary,
        PipelineExecutor,
        PipelineResult,
    )
    from kimball.orchestration.orchestrator import Orchestrator
    from kimball.orchestration.watermark import (
        KIMBALL_ETL_SCHEMA_ENV,
        ETLControlManager,
        get_etl_schema,
    )
    from kimball.streaming import (
        StreamCdfLoader,
        StreamingOrchestrator,
        default_checkpoint_path,
    )

__all__ = [
    "Orchestrator",
    "ContractMonitor",
    "StreamingOrchestrator",
    "PipelineExecutor",
    "PipelineResult",
    "ExecutionSummary",
    "StreamCdfLoader",
    "default_checkpoint_path",
    "generate_bus_matrix",
    "RuntimeOptions",
    "ETLControlManager",
    "get_etl_schema",
    "KIMBALL_ETL_SCHEMA_ENV",
    "PipelineCheckpoint",
    "QueryMetricsCollector",
    "StagingCleanupManager",
    "StagingTableManager",
    "KimballError",
    "RetriableError",
    "NonRetriableError",
    "SourceTableBusyError",
    "DeltaConcurrentModificationError",
    "TransientSparkError",
    "ETLControlConflictError",
    "ConfigurationError",
    "TransformationSQLError",
    "SchemaMismatchError",
    "ETLControlNotFoundError",
]

_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "Orchestrator": ("kimball.orchestration.orchestrator", "Orchestrator"),
    "ContractMonitor": ("kimball.orchestration.contracts_monitor", "ContractMonitor"),
    "StreamingOrchestrator": ("kimball.streaming", "StreamingOrchestrator"),
    "PipelineExecutor": ("kimball.orchestration.executor", "PipelineExecutor"),
    "PipelineResult": ("kimball.orchestration.executor", "PipelineResult"),
    "ExecutionSummary": ("kimball.orchestration.executor", "ExecutionSummary"),
    "StreamCdfLoader": ("kimball.streaming", "StreamCdfLoader"),
    "default_checkpoint_path": ("kimball.streaming", "default_checkpoint_path"),
    "generate_bus_matrix": ("kimball.observability.bus_matrix", "generate_bus_matrix"),
    "RuntimeOptions": ("kimball.common.runtime", "RuntimeOptions"),
    "ETLControlManager": ("kimball.orchestration.watermark", "ETLControlManager"),
    "get_etl_schema": ("kimball.orchestration.watermark", "get_etl_schema"),
    "KIMBALL_ETL_SCHEMA_ENV": (
        "kimball.orchestration.watermark",
        "KIMBALL_ETL_SCHEMA_ENV",
    ),
    "PipelineCheckpoint": ("kimball.observability.resilience", "PipelineCheckpoint"),
    "QueryMetricsCollector": (
        "kimball.observability.resilience",
        "QueryMetricsCollector",
    ),
    "StagingCleanupManager": (
        "kimball.observability.resilience",
        "StagingCleanupManager",
    ),
    "StagingTableManager": (
        "kimball.observability.resilience",
        "StagingTableManager",
    ),
}
for _error_name in __all__[17:]:
    _LAZY_IMPORTS[_error_name] = ("kimball.common.errors", _error_name)


def __getattr__(name: str) -> Any:
    try:
        module_name, attribute = _LAZY_IMPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module 'kimball' has no attribute {name!r}") from exc
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()).union(__all__))
