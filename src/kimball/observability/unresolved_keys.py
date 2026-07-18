from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from kimball.common.utils import quote_table_name


class UnresolvedKeyRegistry:
    """Durable evidence for facts explicitly assigned NOT_YET_AVAILABLE."""

    def __init__(
        self,
        spark: SparkSession,
        schema: str,
        table: str = "etl_unresolved_dimension_keys",
    ) -> None:
        self.spark = spark
        self.table_name = f"{schema}.{table}"

    def ensure_table(self) -> None:
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {quote_table_name(self.table_name)} (
                fact_table STRING NOT NULL,
                relationship STRING NOT NULL,
                fact_grain_hash STRING NOT NULL,
                fact_grain_json STRING NOT NULL,
                source_identity_json STRING NOT NULL,
                event_time TIMESTAMP NOT NULL,
                assigned_key BIGINT NOT NULL,
                source_version BIGINT NOT NULL,
                dimension_version BIGINT NOT NULL,
                first_batch_id STRING NOT NULL,
                last_batch_id STRING NOT NULL,
                status STRING NOT NULL,
                first_observed_at TIMESTAMP NOT NULL,
                last_observed_at TIMESTAMP NOT NULL,
                resolved_at TIMESTAMP
            ) USING DELTA
            """
        )

    def record(
        self,
        rows: DataFrame,
        *,
        fact_table: str,
        relationship: str,
        fact_grain: list[str],
        source_identity: list[str],
        event_time: str | None,
        batch_id: str,
        source_version: int,
        dimension_version: int,
        assigned_key: int = -3,
    ) -> None:
        """Idempotently persist unresolved facts without storing raw PII columns."""
        self.ensure_table()
        grain_json = F.to_json(F.struct(*[F.col(name) for name in fact_grain]))
        identity_json = F.to_json(F.struct(*[F.col(name) for name in source_identity]))
        observed_event = (
            F.col(event_time).cast("timestamp")
            if event_time and event_time in rows.columns
            else F.current_timestamp()
        )
        evidence = rows.select(
            F.lit(fact_table).alias("fact_table"),
            F.lit(relationship).alias("relationship"),
            F.sha2(grain_json, 256).alias("fact_grain_hash"),
            grain_json.alias("fact_grain_json"),
            identity_json.alias("source_identity_json"),
            F.coalesce(observed_event, F.current_timestamp()).alias("event_time"),
            F.lit(assigned_key).cast("long").alias("assigned_key"),
            F.lit(source_version).cast("long").alias("source_version"),
            F.lit(dimension_version).cast("long").alias("dimension_version"),
            F.lit(batch_id).alias("first_batch_id"),
            F.lit(batch_id).alias("last_batch_id"),
            F.lit("OPEN").alias("status"),
            F.current_timestamp().alias("first_observed_at"),
            F.current_timestamp().alias("last_observed_at"),
            F.lit(None).cast("timestamp").alias("resolved_at"),
        ).dropDuplicates(["fact_table", "relationship", "fact_grain_hash"])
        condition = (
            "target.fact_table = source.fact_table AND "
            "target.relationship = source.relationship AND "
            "target.fact_grain_hash = source.fact_grain_hash"
        )
        (
            DeltaTable.forName(self.spark, self.table_name)
            .alias("target")
            .merge(evidence.alias("source"), condition)
            .whenMatchedUpdate(
                set={
                    "last_batch_id": "source.last_batch_id",
                    "last_observed_at": "source.last_observed_at",
                    "source_version": "source.source_version",
                    "dimension_version": "source.dimension_version",
                    "status": "'OPEN'",
                    "resolved_at": "CAST(NULL AS TIMESTAMP)",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    def resolve(
        self,
        rows: DataFrame,
        *,
        fact_table: str,
        relationship: str,
        fact_grain: list[str],
        batch_id: str,
        dimension_version: int,
    ) -> None:
        """Close matching OPEN evidence after a later broker lookup succeeds."""
        if not self.spark.catalog.tableExists(self.table_name):
            return
        grain_json = F.to_json(F.struct(*[F.col(name) for name in fact_grain]))
        resolved = rows.select(
            F.lit(fact_table).alias("fact_table"),
            F.lit(relationship).alias("relationship"),
            F.sha2(grain_json, 256).alias("fact_grain_hash"),
            F.lit(batch_id).alias("last_batch_id"),
            F.lit(dimension_version).cast("long").alias("dimension_version"),
            F.current_timestamp().alias("resolved_at"),
        ).dropDuplicates(["fact_table", "relationship", "fact_grain_hash"])
        condition = (
            "target.fact_table = source.fact_table AND "
            "target.relationship = source.relationship AND "
            "target.fact_grain_hash = source.fact_grain_hash AND "
            "target.status = 'OPEN'"
        )
        (
            DeltaTable.forName(self.spark, self.table_name)
            .alias("target")
            .merge(resolved.alias("source"), condition)
            .whenMatchedUpdate(
                set={
                    "last_batch_id": "source.last_batch_id",
                    "last_observed_at": "source.resolved_at",
                    "dimension_version": "source.dimension_version",
                    "status": "'RESOLVED'",
                    "resolved_at": "source.resolved_at",
                }
            )
            .execute()
        )
