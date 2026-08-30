"""Provider interfaces and value objects for the operational harness.

The harness logic talks to these Protocols so it is unit-testable with fakes.
Spark-backed implementations live in :mod:`kimball.ops.spark_adapters`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class BatchInfo:
    """One row of ``etl_control`` for a (target, source) pair."""

    batch_id: str
    source_table: str
    status: str  # RUNNING | SUCCESS | FAILED
    last_processed_version: int | None
    previous_success_watermark: int | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_message: str | None = None
    config_fingerprint: str | None = None
    source_schema_fingerprint: str | None = None


@dataclass(frozen=True)
class TargetControlState:
    target_table: str
    control_table_exists: bool
    batches: tuple[BatchInfo, ...] = ()


@dataclass(frozen=True)
class DeltaCommit:
    """A row from Delta ``DESCRIBE HISTORY`` / ``DeltaTable.history``."""

    version: int
    operation: str | None
    batch_id: str | None  # from commitInfo.userMetadata; None when untagged
    timestamp: datetime | None = None


@dataclass(frozen=True)
class TargetDeltaState:
    target_table: str
    table_exists: bool
    current_version: int | None  # None when table missing
    commits: tuple[DeltaCommit, ...] = ()


@dataclass(frozen=True)
class SourceHealthReport:
    source_table: str
    exists: bool
    cdf_enabled: bool | None
    earliest_cdf_version: int | None
    watermark_version: int | None
    recorded_schema_fingerprint: str | None
    current_schema_fingerprint: str | None


@runtime_checkable
class ETLControlStore(Protocol):
    def control_table_exists(self) -> bool: ...

    def get_target_state(self, target_table: str) -> TargetControlState: ...

    def set_batch_failed(
        self, target_table: str, source_table: str, message: str
    ) -> None: ...

    def rewind_watermark(
        self, target_table: str, source_table: str, version: int | None
    ) -> None: ...


@runtime_checkable
class DeltaHistoryProvider(Protocol):
    def get_target_delta_state(
        self, target_table: str, history_limit: int = 200
    ) -> TargetDeltaState: ...

    def restore_to_version(self, target_table: str, version: int) -> None: ...
    def restore_to_timestamp(self, target_table: str, ts: datetime) -> None: ...


@runtime_checkable
class SourceMetadataProvider(Protocol):
    def get_source_health(
        self,
        source_table: str,
        watermark_version: int | None,
        recorded_schema_fingerprint: str | None = None,
    ) -> SourceHealthReport: ...


@dataclass(frozen=True)
class OpsProviders:
    """Bundle of the three provider protocols the tools compose."""

    control: ETLControlStore
    history: DeltaHistoryProvider
    sources: SourceMetadataProvider
