from __future__ import annotations

import hashlib
import logging
import os
import time
import uuid
from datetime import datetime
from typing import Any, TypedDict, cast

from delta.tables import DeltaTable
from pyspark.errors import PySparkException
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)
KIMBALL_ETL_SCHEMA_ENV = "KIMBALL_ETL_SCHEMA"


def compute_source_schema_fingerprint(
    spark: SparkSession, source_name: str
) -> str | None:
    """Compute a fingerprint of a source table's schema (columns + types).

    Stored as audit metadata after successful processing.  It must never be
    used to skip data-quality validation because data can change independently
    of its schema.
    """
    try:
        if not spark.catalog.tableExists(source_name):
            return None
        fields = spark.read.format("delta").table(source_name).schema.fields
        schema_repr = ",".join(f"{f.name}:{f.dataType.simpleString()}" for f in fields)
        return hashlib.sha256(schema_repr.encode("utf-8")).hexdigest()[:16]
    except PySparkException:
        return None


def get_etl_schema() -> str | None:
    return os.environ.get(KIMBALL_ETL_SCHEMA_ENV)


class ETLControlRecord(TypedDict, total=False):
    target_table: str
    source_table: str
    last_processed_version: int | None
    batch_id: str | None
    batch_started_at: datetime | None
    batch_completed_at: datetime | None
    batch_status: str | None
    rows_read: int | None
    rows_written: int | None
    error_message: str | None
    updated_at: datetime
    config_fingerprint: str | None
    source_schema_fingerprint: str | None


class ETLControlManager:
    DEFAULT_TABLE_NAME = 'etl_control'

    def __init__(
        self,
        etl_schema: str | None = None,
        table_name: str = DEFAULT_TABLE_NAME,
        spark_session: SparkSession | None = None,
    ):
        self._spark = spark_session
        if etl_schema is None:
            etl_schema = get_etl_schema()
        if "." in table_name:
            self.fq_table = table_name
            self.schema = table_name.split(".")[0]
        else:
            if not etl_schema:
                raise ValueError(
                    "ETLControlManager requires either KIMBALL_ETL_SCHEMA or etl_schema"
                )
            self.schema = etl_schema
            self.fq_table = f"{self.schema}.{table_name}"
        self.database = self.schema
        self._ensure_table_exists()

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            from kimball.common.spark_session import get_spark

            self._spark = get_spark()
        return self._spark

    def _ensure_table_exists(self) -> None:
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.schema}")
        if self.spark.catalog.tableExists(self.fq_table):
            return
        try:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.fq_table} (
                    target_table STRING NOT NULL,
                    source_table STRING NOT NULL,
                    last_processed_version LONG,
                    batch_id STRING,
                    batch_started_at TIMESTAMP,
                    batch_completed_at TIMESTAMP,
                    batch_status STRING,
                    rows_read LONG,
                    rows_written LONG,
                    error_message STRING,
                    updated_at TIMESTAMP NOT NULL,
                    config_fingerprint STRING,
                    source_schema_fingerprint STRING
                ) USING DELTA
            """)
        except PySparkException:
            if not self.spark.catalog.tableExists(self.fq_table):
                raise

    def get_watermark(self, target_table: str, source_table: str) -> int | None:
        row = (
            self.spark.table(self.fq_table)
            .filter(
                (col("target_table") == target_table)
                & (col("source_table") == source_table)
            )
            .select("last_processed_version")
            .first()
        )
        return (
            int(row["last_processed_version"])
            if row and row["last_processed_version"] is not None
            else None
        )

    def get_states(
        self, target_table: str, source_tables: list[str]
    ) -> dict[str, dict[str, Any]]:
        if not source_tables:
            return {}
        rows = (
            self.spark.table(self.fq_table)
            .filter(
                (col("target_table") == target_table)
                & col("source_table").isin(source_tables)
            )
            .collect()
        )
        return {row["source_table"]: row.asDict(recursive=True) for row in rows}

    def update_watermark(
        self, target_table: str, source_table: str, version: int
    ) -> None:
        self._upsert_control_record(
            target_table,
            source_table,
            {
                "last_processed_version": version,
                "batch_status": "SUCCESS",
                "batch_completed_at": datetime.now(),
            },
        )

    def batch_start(self, target_table: str, source_table: str) -> str:
        batch_id = str(uuid.uuid4())
        self._upsert_control_record(
            target_table,
            source_table,
            {
                "batch_id": batch_id,
                "batch_started_at": datetime.now(),
                "batch_completed_at": None,
                "batch_status": "RUNNING",
                "rows_read": None,
                "rows_written": None,
                "error_message": None,
            },
        )
        return batch_id

    def batch_start_all(
        self,
        target_table: str,
        source_tables: list[str],
        run_batch_id: str | None = None,
    ) -> dict[str, str]:
        timestamp = datetime.now()
        records = []
        batch_ids = {}
        for source in source_tables:
            batch_id = run_batch_id or str(uuid.uuid4())
            batch_ids[source] = batch_id
            records.append(
                {
                    "target_table": target_table,
                    "source_table": source,
                    "batch_id": batch_id,
                    "batch_started_at": timestamp,
                    "batch_completed_at": None,
                    "batch_status": "RUNNING",
                    "rows_read": None,
                    "rows_written": None,
                    "error_message": None,
                }
            )
        self._upsert_control_records(cast(list[ETLControlRecord], records))
        return batch_ids

    def batch_complete(
        self,
        target_table: str,
        source_table: str,
        new_version: int,
        rows_read: int | None = None,
        rows_written: int | None = None,
    ) -> None:
        self._upsert_control_record(
            target_table,
            source_table,
            {
                "last_processed_version": new_version,
                "batch_completed_at": datetime.now(),
                "batch_status": "SUCCESS",
                "rows_read": rows_read,
                "rows_written": rows_written,
                "error_message": None,
            },
        )

    def batch_complete_all(
        self, target_table: str, completions: list[dict[str, Any]]
    ) -> None:
        completed_at = datetime.now()
        records: list[ETLControlRecord] = []
        for completion in completions:
            records.append(
                {
                    "target_table": target_table,
                    "source_table": completion["source_table"],
                    "last_processed_version": completion.get("new_version"),
                    "batch_completed_at": completed_at,
                    "batch_status": "SUCCESS",
                    "rows_read": completion.get("rows_read"),
                    "rows_written": completion.get("rows_written"),
                    "error_message": None,
                    "config_fingerprint": completion.get("config_fingerprint"),
                    "source_schema_fingerprint": completion.get(
                        "source_schema_fingerprint"
                    ),
                }
            )
        self._upsert_control_records(records)

    def batch_fail(
        self, target_table: str, source_table: str, error_message: str
    ) -> None:
        if error_message and len(error_message) > 4000:
            error_message = error_message[:4000] + "... (truncated)"
        self._upsert_control_record(
            target_table,
            source_table,
            {
                "batch_completed_at": datetime.now(),
                "batch_status": "FAILED",
                "error_message": error_message,
            },
        )

    def batch_fail_all(
        self, target_table: str, source_tables: list[str], error_message: str
    ) -> None:
        if error_message and len(error_message) > 4000:
            error_message = error_message[:4000] + "... (truncated)"
        completed_at = datetime.now()
        self._upsert_control_records(
            [
                {
                    "target_table": target_table,
                    "source_table": source_table,
                    "batch_completed_at": completed_at,
                    "batch_status": "FAILED",
                    "error_message": error_message,
                }
                for source_table in source_tables
            ]
        )

    def get_batch_status(
        self, target_table: str, source_table: str
    ) -> dict[str, Any] | None:
        row = (
            self.spark.table(self.fq_table)
            .filter(
                (col("target_table") == target_table)
                & (col("source_table") == source_table)
            )
            .first()
        )
        if not row:
            return None
        return {
            "batch_id": row["batch_id"],
            "batch_status": row["batch_status"],
            "batch_started_at": row["batch_started_at"],
            "batch_completed_at": row["batch_completed_at"],
            "last_processed_version": row["last_processed_version"],
            "rows_read": row["rows_read"],
            "rows_written": row["rows_written"],
            "error_message": row["error_message"],
        }

    def get_running_batches(
        self, target_table: str, ttl_minutes: int = 60
    ) -> list[dict[str, str]]:
        try:
            rows = (
                self.spark.table(self.fq_table)
                .filter(
                    (col("target_table") == target_table)
                    & (col("batch_status") == "RUNNING")
                    & (
                        col("batch_started_at")
                        < (
                            current_timestamp()
                            - F.expr(f"INTERVAL {ttl_minutes} MINUTES")
                        )
                    )
                )
                .select("batch_id", "source_table")
                .collect()
            )
            return [
                {"batch_id": r["batch_id"], "source_table": r["source_table"]}
                for r in rows
                if r["batch_id"]
            ]
        except PySparkException:
            return []

    _UPDATE_SCHEMA = StructType(
        [
            StructField("target_table", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("last_processed_version", LongType(), True),
            StructField("batch_id", StringType(), True),
            StructField("batch_started_at", TimestampType(), True),
            StructField("batch_completed_at", TimestampType(), True),
            StructField("batch_status", StringType(), True),
            StructField("rows_read", LongType(), True),
            StructField("rows_written", LongType(), True),
            StructField("error_message", StringType(), True),
            StructField("updated_at", TimestampType(), False),
            StructField("config_fingerprint", StringType(), True),
            StructField("source_schema_fingerprint", StringType(), True),
        ]
    )

    def get_config_fingerprint(
        self, target_table: str, source_table: str
    ) -> str | None:
        row = (
            self.spark.table(self.fq_table)
            .filter(
                (col("target_table") == target_table)
                & (col("source_table") == source_table)
            )
            .select("config_fingerprint")
            .first()
        )
        return row["config_fingerprint"] if row else None

    def get_source_schema_fingerprint(
        self, target_table: str, source_table: str
    ) -> str | None:
        row = (
            self.spark.table(self.fq_table)
            .filter(
                (col("target_table") == target_table)
                & (col("source_table") == source_table)
            )
            .select("source_schema_fingerprint")
            .first()
        )
        return row["source_schema_fingerprint"] if row else None

    def update_fingerprints(
        self,
        target_table: str,
        source_table: str,
        config_fingerprint: str | None = None,
        source_schema_fingerprint: str | None = None,
    ) -> None:
        updates: ETLControlRecord = {}
        if config_fingerprint is not None:
            updates["config_fingerprint"] = config_fingerprint
        if source_schema_fingerprint is not None:
            updates["source_schema_fingerprint"] = source_schema_fingerprint
        if updates:
            self._upsert_control_record(target_table, source_table, updates)

    def reset_watermark(
        self, target_table: str, source_table: str | None = None
    ) -> None:
        """Delete watermark records for a target (and optionally a specific source).

        Used by ``full_reload`` to force the next run to start from scratch.
        """
        from kimball.common.utils import quote_table_name

        safe_target = target_table.replace("'", "''")
        condition = f"target_table = '{safe_target}'"
        if source_table is not None:
            safe_source = source_table.replace("'", "''")
            condition += f" AND source_table = '{safe_source}'"
        self.spark.sql(
            f"DELETE FROM {quote_table_name(self.fq_table)} WHERE {condition}"
        )

    def _upsert_control_record(
        self, target_table: str, source_table: str, updates: ETLControlRecord
    ) -> None:
        record = updates.copy()
        record["target_table"] = target_table
        record["source_table"] = source_table
        record.setdefault("updated_at", datetime.now())
        self._upsert_control_records([record])

    def _upsert_control_records(self, records: list[ETLControlRecord]) -> None:
        if not records:
            return
        delta_table = DeltaTable.forName(self.spark, self.fq_table)
        normalized = []
        for record in records:
            target_table = record.get("target_table")
            source_table = record.get("source_table")
            if target_table is None or source_table is None:
                raise ValueError(
                    "ETL control upserts require target_table and source_table"
                )
            item = {
                field.name: record.get(field.name)
                for field in self._UPDATE_SCHEMA.fields
            }
            item["target_table"] = target_table
            item["source_table"] = source_table
            if not item.get("updated_at"):
                item["updated_at"] = datetime.now()
            normalized.append(item)
        update_df = self.spark.createDataFrame(normalized, schema=self._UPDATE_SCHEMA)
        update_set = {"updated_at": "u.updated_at"}
        schema_field_names = {field.name for field in self._UPDATE_SCHEMA.fields}
        all_update_keys: set[str] = set()
        for record in records:
            all_update_keys.update(record.keys())
        for key in all_update_keys:
            if (
                key not in {"target_table", "source_table", "updated_at"}
                and key in schema_field_names
                and key in update_df.columns
            ):
                update_set[key] = f"u.{key}"
        insert_values = {
            field.name: f"u.{field.name}" for field in self._UPDATE_SCHEMA.fields
        }
        max_retries = 5
        for attempt in range(max_retries):
            try:
                delta_table.alias("w").merge(
                    update_df.alias("u"),
                    "w.target_table = u.target_table AND w.source_table = u.source_table",
                ).whenMatchedUpdate(
                    set=cast(dict[str, str | Column], update_set)
                ).whenNotMatchedInsert(
                    values=cast(dict[str, str | Column], insert_values)
                ).execute()
                return
            except PySparkException as exc:
                exc_str = str(exc)
                is_concurrent = (
                    "ConcurrentAppend" in exc_str
                    or "ProtocolChanged" in exc_str
                    or "DELTA_CONCURRENT" in exc_str
                )
                if is_concurrent and attempt < max_retries - 1:
                    logger.warning(
                        "etl_control MERGE conflict (attempt %d/%d), retrying",
                        attempt + 1,
                        max_retries,
                    )
                    time.sleep(0.5 * (attempt + 1))
                    delta_table = DeltaTable.forName(self.spark, self.fq_table)
                    continue
                raise
