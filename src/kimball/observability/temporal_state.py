"""Durable event-time maxima used to detect disorder across pipeline runs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from kimball.common.utils import quote_table_name


@dataclass
class PendingTemporalState:
    store: TemporalStateStore
    dataframe: DataFrame
    run_id: str


class TemporalStateStore:
    """Monotonic Delta state keyed by pipeline, source, contract, and business key."""

    def __init__(
        self,
        spark: SparkSession,
        etl_schema: str,
        table_name: str = "etl_contract_temporal_state",
    ) -> None:
        self.spark = spark
        self.table = table_name if "." in table_name else f"{etl_schema}.{table_name}"
        self._ensure_table()

    def _ensure_table(self) -> None:
        self.spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {quote_table_name(self.table)} (
                pipeline_table STRING NOT NULL,
                source_table STRING NOT NULL,
                contract_id STRING NOT NULL,
                contract_version STRING NOT NULL,
                business_key_hash STRING NOT NULL,
                business_key_json STRING NOT NULL,
                max_event_time TIMESTAMP NOT NULL,
                max_commit_version LONG,
                last_run_id STRING NOT NULL,
                updated_at TIMESTAMP NOT NULL
            ) USING DELTA"""
        )

    @staticmethod
    def _key_columns(df: DataFrame, keys: list[str]) -> tuple[Any, Any]:
        if keys:
            key_json = F.to_json(F.struct(*[F.col(key).alias(key) for key in keys]))
        else:
            key_json = F.lit('{"__all__":true}')
        return key_json, F.sha2(key_json, 256)

    def existing(
        self, pipeline_table: str, source_table: str, contract_id: str
    ) -> DataFrame:
        return self.spark.table(self.table).filter(
            (F.col("pipeline_table") == pipeline_table)
            & (F.col("source_table") == source_table)
            & (F.col("contract_id") == contract_id)
        )

    def prepare(
        self,
        df: DataFrame,
        *,
        pipeline_table: str,
        source: Any,
    ) -> DataFrame:
        contract = source.contract
        temporal = contract.temporal
        keys = (
            (
                contract.cdc.primary_key
                if contract.cdc and contract.cdc.primary_key
                else None
            )
            or source.primary_keys
            or []
        )
        observations = self.observations(df, temporal.event_time_column, keys)
        return (
            observations.groupBy("business_key_json", "business_key_hash")
            .agg(
                F.max("event_time").alias("max_event_time"),
                F.max("commit_version").alias("max_commit_version"),
            )
            .filter(F.col("max_event_time").isNotNull())
            .select(
                F.lit(pipeline_table).alias("pipeline_table"),
                F.lit(source.name).alias("source_table"),
                F.lit(contract.id).alias("contract_id"),
                F.lit(contract.version).alias("contract_version"),
                "business_key_hash",
                "business_key_json",
                "max_event_time",
                "max_commit_version",
            )
        )

    @classmethod
    def observations(
        cls, df: DataFrame, event_time_column: str, keys: list[str]
    ) -> DataFrame:
        """Normalize raw rows using the exact key encoding stored in Delta."""
        key_json, key_hash = cls._key_columns(df, keys)
        commit_version = (
            F.col("_commit_version").cast("long")
            if "_commit_version" in df.columns
            else F.lit(None).cast("long")
        )
        return df.select(
            key_json.alias("business_key_json"),
            key_hash.alias("business_key_hash"),
            F.col(event_time_column).cast("timestamp").alias("event_time"),
            commit_version.alias("commit_version"),
        )

    def commit(self, updates: DataFrame, run_id: str) -> None:
        safe_run_id = re.sub(r"[^A-Za-z0-9_]", "_", run_id)
        view = f"__kimball_temporal_state_{safe_run_id}"
        updates.createOrReplaceTempView(view)
        table = quote_table_name(self.table)
        self.spark.sql(
            f"""MERGE INTO {table} AS target
            USING {view} AS source
            ON target.pipeline_table = source.pipeline_table
              AND target.source_table = source.source_table
              AND target.contract_id = source.contract_id
              AND target.business_key_hash = source.business_key_hash
            WHEN MATCHED AND (
              source.max_event_time > target.max_event_time OR
              source.max_commit_version > target.max_commit_version
            ) THEN UPDATE SET
              target.contract_version = source.contract_version,
              target.business_key_json = source.business_key_json,
              target.max_event_time = greatest(target.max_event_time, source.max_event_time),
              target.max_commit_version = greatest(
                target.max_commit_version, source.max_commit_version
              ),
              target.last_run_id = '{safe_run_id}',
              target.updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (
              pipeline_table, source_table, contract_id, contract_version,
              business_key_hash, business_key_json, max_event_time,
              max_commit_version, last_run_id, updated_at
            ) VALUES (
              source.pipeline_table, source.source_table, source.contract_id,
              source.contract_version, source.business_key_hash,
              source.business_key_json, source.max_event_time,
              source.max_commit_version, '{safe_run_id}', current_timestamp()
            )"""
        )
        self.spark.catalog.dropTempView(view)


def commit_temporal_state_updates(ctx: Any) -> None:
    """Finalize staged temporal observations after the target transaction succeeds."""
    for pending in ctx.pending_temporal_state:
        pending.store.commit(pending.dataframe, pending.run_id)
    ctx.pending_temporal_state.clear()
