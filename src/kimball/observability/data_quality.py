"""Durable data-quality/contract findings and optional alert delivery."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import urllib.request
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession

from kimball.common.utils import quote_table_name

logger = logging.getLogger(__name__)


class DataQualityEventWriter:
    """Append-only event writer; it deliberately never mutates ``etl_control``."""

    def __init__(self, spark: SparkSession, etl_schema: str, table_name: str = "etl_data_quality_events"):
        self.spark = spark
        self.table = table_name if "." in table_name else f"{etl_schema}.{table_name}"
        self._ensure_table()

    def _ensure_table(self) -> None:
        self.spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {quote_table_name(self.table)} (
                event_id STRING NOT NULL, observed_at TIMESTAMP NOT NULL,
                run_id STRING, monitor_run_id STRING, pipeline_table STRING,
                source_table STRING, source_alias STRING, contract_id STRING,
                contract_version STRING, contract_owner STRING, source_version LONG,
                category STRING NOT NULL, check_name STRING NOT NULL,
                severity STRING NOT NULL, status STRING NOT NULL, action STRING NOT NULL,
                observed_value STRING, expected_value STRING, threshold STRING,
                failed_rows LONG, total_rows LONG, details_json STRING,
                sample_failures_json STRING, alerted_at TIMESTAMP,
                alert_delivery_status STRING, alert_error STRING
            ) USING DELTA"""
        )

    @staticmethod
    def event_id(values: dict[str, Any]) -> str:
        stable = {k: values.get(k) for k in ("pipeline_table", "source_table", "contract_id", "contract_version", "source_version", "category", "check_name", "details")}
        return hashlib.sha256(json.dumps(stable, sort_keys=True, default=str).encode()).hexdigest()[:32]

    def write(self, *, pipeline_table: str, source: Any, finding: Any, run_id: str | None = None,
              source_version: int | None = None, action: str = "recorded", monitor_run_id: str | None = None) -> str:
        contract = getattr(source, "contract", None)
        values = {
            "pipeline_table": pipeline_table, "source_table": source.name,
            "contract_id": getattr(contract, "id", None), "contract_version": getattr(contract, "version", None),
            "source_version": source_version, "category": finding.category,
            "check_name": finding.check_name, "details": finding.details,
        }
        event_id = self.event_id(values)
        row = {
            "event_id": event_id, "observed_at": datetime.now(timezone.utc), "run_id": run_id,
            "monitor_run_id": monitor_run_id, "pipeline_table": pipeline_table,
            "source_table": source.name, "source_alias": source.alias,
            "contract_id": values["contract_id"], "contract_version": values["contract_version"],
            "contract_owner": getattr(contract, "owner", None), "source_version": source_version,
            "category": finding.category, "check_name": finding.check_name,
            "severity": str(finding.severity.value if hasattr(finding.severity, "value") else finding.severity),
            "status": "PASSED" if finding.passed else "FAILED", "action": action,
            "observed_value": finding.observed_value, "expected_value": finding.expected_value,
            "threshold": None, "failed_rows": finding.failed_rows, "total_rows": finding.total_rows,
            "details_json": json.dumps({"message": finding.details}),
            "sample_failures_json": json.dumps(finding.samples, default=str),
            "alerted_at": None, "alert_delivery_status": None, "alert_error": None,
        }
        self.spark.createDataFrame([row]).write.format("delta").mode("append").saveAsTable(self.table)
        return event_id


class AlertDispatcher:
    """Best-effort generic webhook dispatcher. Event storage remains authoritative."""

    def __init__(self, webhook_env: str = "KIMBALL_ALERT_WEBHOOK_URL", timeout_seconds: int = 5):
        self.url = os.getenv(webhook_env)
        self.timeout_seconds = timeout_seconds

    def dispatch(self, payload: dict[str, Any]) -> bool:
        if not self.url:
            return False
        try:
            req = urllib.request.Request(
                self.url, data=json.dumps(payload, default=str).encode("utf-8"),
                headers={"Content-Type": "application/json"}, method="POST",
            )
            with urllib.request.urlopen(req, timeout=self.timeout_seconds):
                return True
        except Exception as exc:  # alert transport must never hide an ETL result
            logger.warning("Data-quality webhook delivery failed: %s", type(exc).__name__)
            return False
