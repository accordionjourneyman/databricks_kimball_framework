from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from kimball.common.config import SourceContractConfig
from kimball.common.errors import DataQualityError
from kimball.observability.data_quality import (
    DataQualityEventSink,
    DataQualityEventWriter,
)
from kimball.orchestration.services.context import PipelineContext
from kimball.orchestration.services.contracts import ContractValidator
from kimball.orchestration.services.fingerprint import FingerprintService
from kimball.orchestration.validation import DataQualityValidator

logger = logging.getLogger(__name__)


class TransformValidator:
    def __init__(self, validator: DataQualityValidator | None = None):
        self._validator = validator or DataQualityValidator()

    def transform_and_validate(
        self,
        ctx: PipelineContext,
        active_dfs: dict[str, DataFrame],
        fingerprint_service: FingerprintService,
    ) -> DataFrame:
        spark = ctx.spark
        config = ctx.config

        # The validator's spark may otherwise resolve via get_spark() to a
        # different session than the pipeline's (e.g. the mocked
        # databricks.sdk.runtime.spark in local tests). FK integrity reads
        # dimension tables from the catalog, so it MUST use the pipeline's
        # own session to see the tables the pipeline created.
        self._validator._spark = spark

        if config.transformation_sql:
            sql_stripped = config.transformation_sql.strip().upper()
            if not sql_stripped.startswith("SELECT") and not sql_stripped.startswith(
                "WITH"
            ):
                raise ValueError(
                    f"transformation_sql must be a SELECT or WITH statement for safety. "
                    f"Got: {config.transformation_sql[:50]}..."
                )
            logger.info("Executing Transformation SQL...")
            transformed_df = spark.sql(config.transformation_sql)

            for source in config.sources:
                if source.cdc_strategy == "cdf":
                    source_df = active_dfs.get(source.name)
                    if (
                        source_df is not None
                        and "_change_type" in source_df.columns
                        and "_change_type" not in transformed_df.columns
                    ):
                        logger.warning(
                            f"CDF source '{source.name}' has _change_type column but it's not in "
                            f"transformation SQL output. Delete detection will NOT work. "
                            f"Add '_change_type' to your SELECT clause for proper SCD2 delete handling."
                        )
                        break
        else:
            if len(config.sources) == 1:
                source_name = config.sources[0].name
                transformed_df = active_dfs[source_name]
            else:
                raise ValueError(
                    "transformation_sql is required for multi-source pipelines"
                )

        if config.pii and config.pii.columns:
            from kimball.processing.pii import apply_pii_masking

            logger.info(f"Applying PII masking to {len(config.pii.columns)} column(s)")
            transformed_df = apply_pii_masking(transformed_df, config.pii)

        if config.junk_dimensions:
            from kimball.processing.junk_dimensions import materialize_junk_dimensions

            transformed_df = materialize_junk_dimensions(
                spark, transformed_df, config.junk_dimensions
            )

        if config.table_type == "fact":
            from kimball.orchestration.services.model_contracts import (
                validate_fact_output_columns,
            )

            validate_fact_output_columns(config, transformed_df.columns)

        if config.identity_bridge:
            from kimball.orchestration.services.skeleton_manager import SkeletonManager

            sm = getattr(self, "_skeleton_manager", None) or SkeletonManager()
            logger.info(
                f"Applying identity bridge: {config.identity_bridge.table} on "
                f"{config.identity_bridge.join_on} -> "
                f"{config.identity_bridge.target_column}"
            )
            transformed_df = sm.apply_identity_bridge(ctx, transformed_df)

        # Contract data rules are intentionally never fingerprint-skipped: source
        # content can change while both configuration and schema remain stable.
        contracted_sources = [
            source
            for source in config.sources
            if isinstance(source.contract, SourceContractConfig)
        ]
        if contracted_sources:
            schema = getattr(ctx.etl_control, "schema", None) or "default"
            obs = config.observability
            writer = DataQualityEventSink(
                spark,
                schema,
                obs.event_table if obs else "etl_data_quality_events",
                failure_mode=obs.write_failure if obs else "warn",
                writer_type=DataQualityEventWriter,
            )
            validator = ContractValidator(spark)
            contract_findings = []
            for source in contracted_sources:
                findings = validator.validate_data(transformed_df, source)
                if validator.last_metrics:
                    ctx.validation_metrics.append(
                        {
                            **validator.last_metrics,
                            "source_table": source.name,
                            "pipeline_table": config.table_name,
                        }
                    )
                contract_findings.extend(findings)
                for finding in findings:
                    writer.write(
                        pipeline_table=config.table_name,
                        source=source,
                        finding=finding,
                        run_id=ctx.batch_id,
                        source_version=ctx.source_versions.get(source.name),
                        action="blocked"
                        if not finding.passed and finding.severity.value == "error"
                        else "recorded",
                    )
            ContractValidator.raise_for_errors(contract_findings)

        if config.foreign_keys:
            for fk in config.foreign_keys:
                col_name = fk.column
                default_val = fk.default_value
                field = next(
                    (f for f in transformed_df.schema.fields if f.name == col_name),
                    None,
                )
                if field:
                    if isinstance(field.dataType, StringType):
                        fill_val = str(default_val)
                    else:
                        fill_val = default_val
                    logger.info(
                        f"Filling NULL foreign key '{col_name}' with default: {fill_val}"
                    )
                    transformed_df = transformed_df.withColumn(
                        col_name,
                        F.when(F.col(col_name).isNull(), F.lit(fill_val)).otherwise(
                            F.col(col_name)
                        ),
                    )
                else:
                    logger.info(
                        f"Warning: Foreign key column '{col_name}' not found in transformed DataFrame"
                    )

        if getattr(config, "tests", None):
            if (
                fingerprint_service.should_skip_validation(ctx)
                and not contracted_sources
            ):
                logger.info(
                    "Skipping data quality validation: config + source schema "
                    "fingerprints unchanged since last successful run."
                )
            else:
                logger.info("Running data quality validation on transformed data...")
                report = self._validator.run_config_tests(
                    config,
                    df=transformed_df,
                    use_approximate_unique=ctx.runtime_options.use_approximate_unique,
                )
                report.raise_on_failure()

        if config.table_type == "dimension" and config.natural_keys:
            if (
                not fingerprint_service.should_skip_validation(ctx)
                or contracted_sources
            ):
                logger.info("Validating natural key uniqueness (pre-merge gate)...")
                nk_result = self._validator.validate_natural_key_uniqueness(
                    transformed_df,
                    config.natural_keys,
                    table_name=config.table_name,
                )
                logger.info(str(nk_result))
                if not nk_result.passed:
                    raise DataQualityError(
                        f"Natural key uniqueness violation in {config.table_name}: "
                        f"{nk_result.failed_rows} duplicate keys. Details: {nk_result.details}",
                        details={"sample_failures": nk_result.sample_failures},
                    )
            else:
                logger.info("Skipping NK uniqueness: fingerprints unchanged.")

        if config.table_type == "fact" and (
            config.foreign_keys or config.junk_dimensions
        ):
            if (
                not fingerprint_service.should_skip_validation(ctx)
                or contracted_sources
            ) and not getattr(config, "tests", None):
                logger.info(
                    "Validating FK integrity against dimensions (pre-merge gate)..."
                )
                fk_defs = [
                    {
                        "column": fk.column,
                        "dimension_table": fk.references,
                        "dimension_key": fk.dimension_key or fk.column,
                    }
                    for fk in config.foreign_keys or []
                    if hasattr(fk, "references") and fk.references
                ]
                fk_defs.extend(
                    {
                        "column": junk.surrogate_key,
                        "dimension_table": junk.dimension_table,
                        "dimension_key": junk.surrogate_key,
                    }
                    for junk in config.junk_dimensions
                )
                if fk_defs:
                    fk_report = self._validator.validate_fact_fk_integrity(
                        transformed_df, fk_defs
                    )
                    for result in fk_report.results:
                        logger.info(str(result))
                    fk_report.raise_on_failure()
            else:
                logger.info("Skipping FK integrity: fingerprints unchanged.")

        return transformed_df
