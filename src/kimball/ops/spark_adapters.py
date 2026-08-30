"""Spark-backed providers for the operational harness (ROADMAP 1.1-1.5).

These adapt the existing ``ETLControlManager`` / ``TransactionManager`` /
Delta history APIs to the :mod:`kimball.ops.providers` protocols. They are
exercised by integration tests against real Spark+Delta; the harness logic
itself is unit-tested with fakes and never imports Spark.
"""

from __future__ import annotations

from typing import Any, cast

from kimball.common.utils import quote_table_name
from kimball.ops.providers import (
    BatchInfo,
    DeltaCommit,
    OpsProviders,
    SourceHealthReport,
    TargetControlState,
    TargetDeltaState,
)


def _as_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "asDict"):
        return cast("dict[str, Any]", row.asDict())
    return cast("dict[str, Any]", dict(row))


class SparkETLControlStore:
    """ETLControlStore backed by :class:`ETLControlManager`."""

    def __init__(self, etl_control: Any) -> None:
        self._ctl = etl_control
        self._fq = etl_control.fq_table

    @property
    def _spark(self) -> Any:
        return self._ctl.spark

    def control_table_exists(self) -> bool:
        try:
            return bool(self._spark.catalog.tableExists(self._fq))
        except Exception:  # noqa: BLE001
            return False

    def get_target_state(self, target_table: str) -> TargetControlState:
        if not self.control_table_exists():
            return TargetControlState(target_table, False, ())
        from pyspark.sql.functions import col

        rows = (
            self._spark.table(self._fq)
            .filter(col("target_table") == target_table)
            .collect()
        )
        batches = tuple(_row_to_batch(r) for r in rows)
        return TargetControlState(target_table, True, batches)

    def set_batch_failed(
        self, target_table: str, source_table: str, message: str
    ) -> None:
        self._ctl.batch_fail(target_table, source_table, message)

    def rewind_watermark(
        self, target_table: str, source_table: str, version: int | None
    ) -> None:
        if version is None:
            self._ctl.reset_watermark(target_table, source_table)
            return
        self._ctl.rewind_to_version(target_table, source_table, version)


def _row_to_batch(row: Any) -> BatchInfo:
    d = _as_dict(row)
    return BatchInfo(
        batch_id=d.get("batch_id") or "",
        source_table=d["source_table"],
        status=d.get("batch_status") or "",
        last_processed_version=d.get("last_processed_version"),
        started_at=d.get("batch_started_at"),
        previous_success_watermark=d.get("previous_success_watermark"),
        completed_at=d.get("batch_completed_at"),
        error_message=d.get("error_message"),
        config_fingerprint=d.get("config_fingerprint"),
        source_schema_fingerprint=d.get("source_schema_fingerprint"),
    )


class SparkDeltaHistoryProvider:
    """DeltaHistoryProvider backed by ``DeltaTable.history`` + RESTORE."""

    def __init__(self, spark: Any) -> None:
        self._spark = spark

    def get_target_delta_state(
        self, target_table: str, history_limit: int = 200
    ) -> TargetDeltaState:
        try:
            exists = bool(self._spark.catalog.tableExists(target_table))
        except Exception:  # noqa: BLE001
            exists = False
        if not exists:
            return TargetDeltaState(target_table, False, None, ())
        from delta.tables import DeltaTable

        history = (
            DeltaTable.forName(self._spark, target_table)
            .history(history_limit)
            .collect()
        )
        commits = tuple(_row_to_commit(r) for r in history)
        current = int(history[0]["version"]) if history else None
        return TargetDeltaState(target_table, True, current, commits)

    def restore_to_version(self, target_table: str, version: int) -> None:
        self._spark.sql(
            f"RESTORE TABLE {quote_table_name(target_table)} TO VERSION AS OF {version}"
        )

    def restore_to_timestamp(self, target_table: str, ts: Any) -> None:
        self._spark.sql(
            f"RESTORE TABLE {quote_table_name(target_table)} "
            f"TO TIMESTAMP AS OF '{ts.isoformat()}'"
        )


def _row_to_commit(row: Any) -> DeltaCommit:
    d = _as_dict(row)
    return DeltaCommit(
        version=int(d["version"]),
        operation=d.get("operation"),
        batch_id=_extract_batch_id(d.get("userMetadata")),
        timestamp=d.get("timestamp"),
    )


def _extract_batch_id(raw: Any) -> str | None:
    """Mirror TransactionManager's matching: ``userMetadata == batch_id`` OR
    ``endswith("batch_id={batch_id}")`` (transaction.py:109-110). The framework
    sets userMetadata to exactly the batch_id, but be robust to a compound
    ``...; batch_id=<id>`` value."""
    if not raw:
        return None
    text = str(raw)
    if "batch_id=" in text:
        tail = text.rsplit("batch_id=", 1)[-1].strip()
        return tail or None
    return text or None


class SparkSourceMetadataProvider:
    """SourceMetadataProvider backed by catalog/TBLPROPERTIES/CDF probe."""

    def __init__(self, spark: Any) -> None:
        self._spark = spark

    def get_source_health(
        self,
        source_table: str,
        watermark_version: int | None,
        recorded_schema_fingerprint: str | None = None,
    ) -> SourceHealthReport:
        try:
            exists = bool(self._spark.catalog.tableExists(source_table))
        except Exception:  # noqa: BLE001
            exists = False
        if not exists:
            return SourceHealthReport(
                source_table, False, None, None, watermark_version, None, None
            )
        cdf_enabled = _cdf_enabled(self._spark, source_table)
        earliest = (
            _earliest_cdf_version(self._spark, source_table) if cdf_enabled else None
        )
        current_fp = _schema_fingerprint(self._spark, source_table)
        return SourceHealthReport(
            source_table,
            True,
            cdf_enabled,
            earliest,
            watermark_version,
            recorded_schema_fingerprint,
            current_fp,
        )


def _cdf_enabled(spark: Any, table: str) -> bool | None:
    try:
        rows = spark.sql(f"SHOW TBLPROPERTIES {quote_table_name(table)}").collect()
        for row in rows:
            d = _as_dict(row)
            key = d.get("key")
            value = d.get("value")
            if key == "delta.enableChangeDataFeed":
                return str(value).lower() == "true"
        return False
    except Exception:  # noqa: BLE001
        return None


def _earliest_cdf_version(spark: Any, table: str) -> int | None:
    """Return the first retained, queryable CDF version for a catalog table.

    CDF retention can be shorter than Delta log retention, so the oldest entry
    in DESCRIBE HISTORY is only a lower bound. Probe retained table versions
    with the table reader and return the first one whose CDF can be materialized.
    """
    try:
        history_rows = spark.sql(
            f"DESCRIBE HISTORY {quote_table_name(table)}"
        ).collect()
        versions = sorted(
            int(_as_dict(row)["version"])
            for row in history_rows
            if _as_dict(row).get("version") is not None
        )
    except Exception:  # noqa: BLE001
        return None
    for version in versions:
        try:
            (
                spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", version)
                .option("endingVersion", version)
                .table(table)
                .limit(1)
                .collect()
            )
            return version
        except Exception:  # noqa: BLE001
            continue
    return None


def _schema_fingerprint(spark: Any, table: str) -> str | None:
    from kimball.orchestration.watermark import compute_source_schema_fingerprint

    return compute_source_schema_fingerprint(spark, table)


def build_providers(spark: Any, etl_schema: str) -> OpsProviders:
    """Construct an :class:`OpsProviders` bundle for a Spark session."""
    from kimball.orchestration.watermark import ETLControlManager

    etl_control = ETLControlManager(etl_schema=etl_schema, spark_session=spark)
    return OpsProviders(
        control=SparkETLControlStore(etl_control),
        history=SparkDeltaHistoryProvider(spark),
        sources=SparkSourceMetadataProvider(spark),
    )
