from __future__ import annotations

import hashlib
import logging
import os
import uuid
import warnings
from datetime import datetime
from typing import Any, TYPE_CHECKING, TypedDict, cast

from delta.tables import DeltaTable
from pyspark.sql import Column, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

logger = logging.getLogger(__name__)
KIMBALL_ETL_SCHEMA_ENV = "KIMBALL_ETL_SCHEMA"


def compute_source_schema_fingerprint(spark: SparkSession, source_name: str) -> str | None:
    """Compute a fingerprint of a source table's schema (columns + types).

    Used for state-aware validation skipping: if the source schema hasn't
    changed since the last successful run, data quality tests can be skipped
    (they would produce the same result).
    """
    try:
        if not spark.catalog.tableExists(source_name):
            return None
        fields = spark.read.format("delta").table(source_name).schema.fields
        schema_repr = ",".join(f"{f.name}:{f.dataType.simpleString()}" for f in fields)
        return hashlib.sha256(schema_repr.encode("utf-8")).hexdigest()[:16]
    except Exception:
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
    DEFAULT_TABLE_NAME = "etl_control"

    def __init__(self, etl_schema: str | None = None, table_name: str = DEFAULT_TABLE_NAME, spark_session: SparkSession | None = None):
        self._spark = spark_session
        if etl_schema is None:
            etl_schema = get_etl_schema()
        if "." in table_name:
            self.fq_table = table_name
            self.schema = table_name.split(".")[0]
        else:
            if not etl_schema:
                raise ValueError("ETLControlManager requires either KIMBALL_ETL_SCHEMA or etl_schema")
            self.schema = etl_schema
            self.fq_table = f"{self.schema}.{table_name}"
        self.database = self.schema
        self._ensure_table_exists()

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            from databricks.sdk.runtime import spark
            self._spark = spark
        return self._spark

    def _ensure_table_exists(self) -> None:
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.schema}")
        if self.spark.catalog.tableExists(self.fq_table):
            self._migrate_schema()
            return
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
            ) USING DELTA PARTITIONED BY (target_table, source_table)
        """)

    def _migrate_schema(self) -> None:
        existing = {f.name.lower() for f in self.spark.table(self.fq_table).schema.fields}
        for col_name, col_type in {
            "target_table": "STRING",
            "source_table": "STRING",
            "last_processed_version": "LONG",
            "batch_id": "STRING",
            "batch_started_at": "TIMESTAMP",
            "batch_completed_at": "TIMESTAMP",
            "batch_status": "STRING",
            "rows_read": "LONG",
            "rows_written": "LONG",
            "error_message": "STRING",
            "updated_at": "TIMESTAMP",
            "config_fingerprint": "STRING",
            "source_schema_fingerprint": "STRING",
        }.items():
            if col_name not in existing:
                try:
                    self.spark.sql(f"ALTER TABLE {self.fq_table} ADD COLUMN {col_name} {col_type}")
                except Exception:
                    pass  # Column may already exist from concurrent migration

    def get_watermark(self, target_table: str, source_table: str) -> int | None:
        row = self.spark.table(self.fq_table).filter((col("target_table") == target_table) & (col("source_table") == source_table)).select("last_processed_version").first()
        return int(row["last_processed_version"]) if row and row["last_processed_version"] is not None else None

    def update_watermark(self, target_table: str, source_table: str, version: int) -> None:
        self._upsert_control_record(target_table, source_table, {"last_processed_version": version, "batch_status": "SUCCESS", "batch_completed_at": datetime.now()})

    def batch_start(self, target_table: str, source_table: str) -> str:
        batch_id = str(uuid.uuid4())
        self._upsert_control_record(target_table, source_table, {"batch_id": batch_id, "batch_started_at": datetime.now(), "batch_completed_at": None, "batch_status": "RUNNING", "rows_read": None, "rows_written": None, "error_message": None})
        return batch_id

    def batch_start_all(self, target_table: str, source_tables: list[str]) -> dict[str, str]:
        timestamp = datetime.now()
        records = []
        batch_ids = {}
        for source in source_tables:
            batch_id = str(uuid.uuid4())
            batch_ids[source] = batch_id
            records.append({"target_table": target_table, "source_table": source, "batch_id": batch_id, "batch_started_at": timestamp, "batch_completed_at": None, "batch_status": "RUNNING", "rows_read": None, "rows_written": None, "error_message": None})
        self._upsert_control_records(cast(list[ETLControlRecord], records))
        return batch_ids

    def batch_complete(self, target_table: str, source_table: str, new_version: int, rows_read: int | None = None, rows_written: int | None = None) -> None:
        self._upsert_control_record(target_table, source_table, {"last_processed_version": new_version, "batch_completed_at": datetime.now(), "batch_status": "SUCCESS", "rows_read": rows_read, "rows_written": rows_written, "error_message": None})

    def batch_fail(self, target_table: str, source_table: str, error_message: str) -> None:
        if error_message and len(error_message) > 4000:
            error_message = error_message[:4000] + "... (truncated)"
        self._upsert_control_record(target_table, source_table, {"batch_completed_at": datetime.now(), "batch_status": "FAILED", "error_message": error_message})

    def get_batch_status(self, target_table: str, source_table: str) -> dict[str, Any] | None:
        row = self.spark.table(self.fq_table).filter((col("target_table") == target_table) & (col("source_table") == source_table)).first()
        if not row:
            return None
        return {"batch_id": row["batch_id"], "batch_status": row["batch_status"], "batch_started_at": row["batch_started_at"], "batch_completed_at": row["batch_completed_at"], "last_processed_version": row["last_processed_version"], "rows_read": row["rows_read"], "rows_written": row["rows_written"], "error_message": row["error_message"]}

    def get_running_batches(self, target_table: str) -> list[dict[str, str]]:
        try:
            rows = self.spark.table(self.fq_table).filter((col("target_table") == target_table) & (col("batch_status") == "RUNNING")).select("batch_id", "source_table").collect()
            return [{"batch_id": r["batch_id"], "source_table": r["source_table"]} for r in rows if r["batch_id"]]
        except Exception:
            return []

    _UPDATE_SCHEMA = StructType([
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
    ])

    def get_config_fingerprint(self, target_table: str, source_table: str) -> str | None:
        row = (
            self.spark.table(self.fq_table)
            .filter(
                (col("target_table") == target_table) & (col("source_table") == source_table)
            )
            .select("config_fingerprint")
            .first()
        )
        return row["config_fingerprint"] if row else None

    def get_source_schema_fingerprint(self, target_table: str, source_table: str) -> str | None:
        row = (
            self.spark.table(self.fq_table)
            .filter(
                (col("target_table") == target_table) & (col("source_table") == source_table)
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

    def _upsert_control_record(self, target_table: str, source_table: str, updates: ETLControlRecord) -> None:
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
            item = {field.name: record.get(field.name) for field in self._UPDATE_SCHEMA.fields}
            item["target_table"] = record["target_table"]
            item["source_table"] = record["source_table"]
            item.setdefault("updated_at", datetime.now())
            normalized.append(item)
        update_df = self.spark.createDataFrame(normalized, schema=self._UPDATE_SCHEMA)
        update_set = {"updated_at": "u.updated_at"}
        all_update_keys: set[str] = set()
        for record in records:
            all_update_keys.update(record.keys())
        for key in all_update_keys:
            if key not in {"target_table", "source_table", "updated_at"} and key in update_df.columns:
                update_set[key] = f"u.{key}"
        insert_values = {field.name: f"u.{field.name}" for field in self._UPDATE_SCHEMA.fields}
        delta_table.alias("w").merge(update_df.alias("u"), "w.target_table = u.target_table AND w.source_table = u.source_table").whenMatchedUpdate(set=cast(dict[str, str | Column], update_set)).whenNotMatchedInsert(values=cast(dict[str, str | Column], insert_values)).execute()
