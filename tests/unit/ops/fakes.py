"""Fake providers for ops unit tests (no Spark)."""

from __future__ import annotations

from kimball.ops.providers import (
    BatchInfo,
    DeltaCommit,
    OpsProviders,
    SourceHealthReport,
    TargetControlState,
    TargetDeltaState,
)


class FakeControl:
    def __init__(
        self, exists: bool = True, batches: tuple[BatchInfo, ...] = ()
    ) -> None:
        self.exists = exists
        self.batches = batches
        self.calls: list[tuple] = []

    def control_table_exists(self) -> bool:
        return self.exists

    def get_target_state(self, target_table: str) -> TargetControlState:
        return TargetControlState(target_table, self.exists, self.batches)

    def set_batch_failed(
        self, target_table: str, source_table: str, message: str
    ) -> None:
        self.calls.append(("fail", target_table, source_table, message))

    def rewind_watermark(
        self, target_table: str, source_table: str, version: int | None
    ) -> None:
        self.calls.append(("rewind", target_table, source_table, version))


class FakeHistory:
    def __init__(
        self,
        exists: bool = True,
        current_version: int | None = 5,
        commits: tuple[DeltaCommit, ...] = (),
    ) -> None:
        self.exists = exists
        self.current_version = current_version
        self.commits = commits
        self.restored: list[tuple[str, int]] = []
        self.restored_ts: list[tuple] = []

    def get_target_delta_state(
        self, target_table: str, history_limit: int = 200
    ) -> TargetDeltaState:
        return TargetDeltaState(
            target_table, self.exists, self.current_version, self.commits
        )

    def restore_to_version(self, target_table: str, version: int) -> None:
        self.restored.append((target_table, version))

    def restore_to_timestamp(self, target_table: str, ts) -> None:
        self.restored_ts.append((target_table, ts))


class FakeSources:
    def __init__(self, reports: dict[str, SourceHealthReport] | None = None) -> None:
        self.reports = reports or {}

    def get_source_health(
        self,
        source_table: str,
        watermark_version: int | None,
        recorded_schema_fingerprint: str | None = None,
    ) -> SourceHealthReport:
        if source_table in self.reports:
            r = self.reports[source_table]
            return SourceHealthReport(
                r.source_table,
                r.exists,
                r.cdf_enabled,
                r.earliest_cdf_version,
                watermark_version,
                recorded_schema_fingerprint,
                r.current_schema_fingerprint,
            )
        return SourceHealthReport(
            source_table, True, True, 0, watermark_version, None, None
        )


def providers(
    control: FakeControl, history: FakeHistory, sources: FakeSources
) -> OpsProviders:
    return OpsProviders(control=control, history=history, sources=sources)


def commit(version: int, batch_id: str | None, operation: str = "WRITE") -> DeltaCommit:
    return DeltaCommit(version=version, operation=operation, batch_id=batch_id)


def batch(
    batch_id: str,
    source: str,
    status: str,
    version: int | None = None,
    config_fingerprint: str | None = None,
    source_schema_fingerprint: str | None = None,
    error_message: str | None = None,
) -> BatchInfo:
    return BatchInfo(
        batch_id=batch_id,
        source_table=source,
        status=status,
        last_processed_version=version,
        config_fingerprint=config_fingerprint,
        source_schema_fingerprint=source_schema_fingerprint,
        error_message=error_message,
    )
