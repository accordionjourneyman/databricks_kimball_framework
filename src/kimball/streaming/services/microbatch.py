from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, count as spark_count

from kimball.common.config import ConfigLoader
from kimball.orchestration.validation import DataQualityValidator
from kimball.orchestration.watermark import compute_source_schema_fingerprint
from kimball.processing import merger as _merger
from kimball.processing.pii import apply_pii_masking
from kimball.processing.table_creator import TableCreator

logger = logging.getLogger(__name__)


class StreamingMicroBatchProcessor:
    """Handles a single streaming micro-batch: transform, validate, merge."""

    def __init__(self, spark, config, etl_schema: str, etl_control):
        self.spark = spark
        self.config = config
        self.etl_schema = etl_schema
        self.etl_control = etl_control
        self._table_creator = TableCreator()

    def ensure_target_table(self, sample_df: DataFrame) -> None:
        if self.spark.catalog.tableExists(self.config.table_name):
            return

        if self.config.transformation_sql:
            transformed_sample = self.spark.sql(self.config.transformation_sql)
        else:
            transformed_sample = sample_df

        if self.config.pii and self.config.pii.columns:
            transformed_sample = apply_pii_masking(transformed_sample, self.config.pii)

        schema_df = self._table_creator.add_system_columns(
            transformed_sample.limit(0),
            self.config.scd_type,
            self.config.surrogate_key,
            current_value_columns=self.config.current_value_columns,
        )

        cluster_cols = self.config.cluster_by
        if not cluster_cols and self.config.table_type == "dimension":
            cluster_cols = self.config.natural_keys or []

        self._table_creator.create_table_with_clustering(
            table_name=self.config.table_name,
            schema_df=schema_df,
            config=self.config.model_dump(),
            cluster_by=cluster_cols or [],
            surrogate_key_col=self.config.surrogate_key,
        )

        if self.config.table_type == "dimension":
            target_schema = self.spark.table(self.config.table_name).schema
            if self.config.scd_type == 2:
                _merger.ensure_scd2_defaults(
                    self.config.table_name,
                    target_schema,
                    self.config.surrogate_key or "surrogate_key",
                    self.config.default_rows,
                )
            elif self.config.scd_type == 1 and self.config.surrogate_key:
                _merger.ensure_scd1_defaults(
                    self.config.table_name,
                    target_schema,
                    self.config.surrogate_key,
                    self.config.default_rows,
                )

    def process_microbatch(
        self, batch_df: DataFrame, source: Any, batch_id: int
    ) -> None:
        source_name = source.name
        rows_written = 0

        if not self.spark.catalog.tableExists(self.config.table_name):
            self.ensure_target_table(batch_df)

        if self.config.table_type == "fact":
            join_keys = self.config.merge_keys or []
        else:
            join_keys = self.config.natural_keys or []

        if self.config.transformation_sql:
            source_df = self.spark.sql(self.config.transformation_sql)
        else:
            source_df = batch_df

        if self.config.pii and self.config.pii.columns:
            logger.info(f"Applying PII masking to {len(self.config.pii.columns)} column(s)")
            source_df = apply_pii_masking(source_df, self.config.pii)

        cdf_meta_cols = [
            c for c in ("_commit_version", "_commit_timestamp") if c in batch_df.columns
        ]
        if cdf_meta_cols and self.config.transformation_sql and "_commit_version" not in source_df.columns:
            batch_table_name = (
                f"{self.etl_schema}._kimball_batch_{source.alias}_{batch_id}"
            )
            batch_table = self.spark.table(batch_table_name)
            common_cols = [c for c in source_df.columns if c in batch_table.columns]
            meta_df = batch_table.select(*(common_cols + cdf_meta_cols))
            source_df = source_df.join(broadcast(meta_df), common_cols, "left")

        self._validate_fks(source_df)
        self._validate_grain(source_df, join_keys)

        _merger.merge(
            target_table_name=self.config.table_name,
            source_df=source_df,
            join_keys=join_keys,
            delete_strategy=self.config.delete_strategy,
            scd_type=self.config.scd_type,
            track_history_columns=self.config.track_history_columns,
            surrogate_key_col=self.config.surrogate_key,
            schema_evolution=self.config.schema_evolution,
            effective_at_column=self.config.effective_at,
            history_table=self.config.history_table,
            current_value_columns=self.config.current_value_columns,
        )

        try:
            metrics = _merger.get_last_merge_metrics(self.config.table_name)
            rows_written = int(metrics.get("num_target_rows_inserted", 0)) + int(
                metrics.get("num_target_rows_updated", 0)
            )
        except Exception:
            rows_written = 0

        self._save_fingerprints()

        new_version = self._get_max_version(batch_df)
        if new_version is not None:
            self.etl_control.batch_complete(
                target_table=self.config.table_name,
                source_table=source_name,
                new_version=new_version,
                rows_read=0,
                rows_written=rows_written,
            )

    def _validate_fks(self, source_df: DataFrame) -> None:
        if self.config.table_type != "fact" or not self.config.foreign_keys:
            return
        fk_defs = [
            {
                "column": fk.column,
                "dimension_table": fk.references,
                "dimension_key": fk.dimension_key or fk.column,
            }
            for fk in self.config.foreign_keys
            if hasattr(fk, "references") and fk.references
        ]
        if fk_defs:
            validator = DataQualityValidator()
            fk_report = validator.validate_fact_fk_integrity(source_df, fk_defs)
            for result in fk_report.results:
                logger.info(str(result))
            fk_report.raise_on_failure()

    def _validate_grain(self, source_df: DataFrame, join_keys: list[str]) -> None:
        if not join_keys:
            return
        grain_mode = getattr(self.config, "grain_validation", "error")
        if grain_mode == "skip":
            return
        grain_violations = (
            source_df.groupBy(*join_keys)
            .agg(spark_count("*").alias("__grain_count"))
            .filter("__grain_count > 1")
        )
        if len(grain_violations.limit(1).head(1)) > 0:
            sample = grain_violations.limit(5).collect()
            keys_str = ", ".join({k: row[k] for k in join_keys} for row in sample)
            msg = (
                f"Grain violation in {self.config.table_name}: "
                f"Duplicate keys for grain {join_keys}. Sample: {keys_str}"
            )
            if grain_mode == "warn":
                logger.warning(msg)
            else:
                raise ValueError(msg)

    def _save_fingerprints(self) -> None:
        try:
            config_fp = ConfigLoader().compute_fingerprint(self.config)
            for src in self.config.sources:
                src_fp = compute_source_schema_fingerprint(self.spark, src.name)
                self.etl_control.update_fingerprints(
                    self.config.table_name,
                    src.name,
                    config_fingerprint=config_fp,
                    source_schema_fingerprint=src_fp,
                )
        except Exception:
            pass

    def _get_max_version(self, batch_df: DataFrame) -> int | None:
        if "_commit_version" not in batch_df.columns:
            return None
        max_row = batch_df.agg({"_commit_version": "max"}).first()
        if max_row and max_row[0] is not None:
            return int(max_row[0])
        return None