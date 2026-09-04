"""Transform-and-validate pipeline for one configured pipeline.

``TransformValidator.transform_and_validate`` is the orchestrator; each
phase of the transform/gate pipeline lives in its own ``_phase_*`` method
(ADR-004 grade-A pass) so the sequence is readable and every phase is
independently testable. Phases run in a fixed order — transformation,
PII masking, junk materialization, FK resolution, contract gates, null
policy, declared tests, grain gate, FK-integrity gate — because some
gates (FK integrity) observe columns that earlier phases add.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame

from kimball.common.config import SourceContractConfig
from kimball.common.errors import DataQualityError
from kimball.observability.data_quality import (
    DataQualityEventSink,
    DataQualityEventWriter,
)
from kimball.orchestration.services.context import PipelineContext
from kimball.orchestration.services.contracts import ContractValidator
from kimball.orchestration.validation import DataQualityValidator

logger = logging.getLogger(__name__)


class TransformValidator:
    def __init__(self, validator: DataQualityValidator | None = None):
        self._validator = validator or DataQualityValidator()

    def transform_and_validate(
        self,
        ctx: PipelineContext,
        active_dfs: dict[str, DataFrame],
    ) -> DataFrame:
        spark = ctx.spark

        # The validator's spark may otherwise resolve via get_spark() to a
        # different session than the pipeline's (e.g. the mocked
        # databricks.sdk.runtime.spark in local tests). FK integrity reads
        # dimension tables from the catalog, so it MUST use the pipeline's
        # own session to see the tables the pipeline created.
        self._validator._spark = spark

        transformed_df = self._phase_execute_transformation(ctx, active_dfs)
        transformed_df = self._phase_apply_column_transforms(ctx, transformed_df)
        transformed_df = self._phase_resolve_foreign_keys(ctx, transformed_df)
        self._phase_fact_output_contract(ctx, transformed_df)
        self._phase_source_contracts(ctx, transformed_df)
        transformed_df = self._phase_dimension_null_policy(ctx, transformed_df)
        self._phase_declared_tests(ctx, transformed_df)
        self._phase_natural_key_gate(ctx, transformed_df)
        self._phase_fk_integrity_gate(ctx, transformed_df)
        return transformed_df

    # ------------------------------------------------------------------
    # Phase 1: build the transformed DataFrame from SQL or a single source.
    # ------------------------------------------------------------------
    def _phase_execute_transformation(
        self,
        ctx: PipelineContext,
        active_dfs: dict[str, DataFrame],
    ) -> DataFrame:
        spark = ctx.spark
        config = ctx.config

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

            self._warn_if_cdf_deletes_lost(ctx, active_dfs, transformed_df)
        elif len(config.sources) == 1:
            source_name = config.sources[0].name
            transformed_df = active_dfs[source_name]
        else:
            raise ValueError(
                "transformation_sql is required for multi-source pipelines"
            )
        return transformed_df

    @staticmethod
    def _warn_if_cdf_deletes_lost(
        ctx: PipelineContext,
        active_dfs: dict[str, DataFrame],
        transformed_df: DataFrame,
    ) -> None:
        """Warn when a CDF source's delete marker was dropped by the SQL.

        Delete detection reads ``_change_type`` from the transformed
        output; a transformation that omits it silently disables SCD2
        delete handling, so this is a loud, actionable warning.
        """
        for source in ctx.config.sources:
            if source.cdc_strategy != "cdf":
                continue
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

    # ------------------------------------------------------------------
    # Phase 2: PII masking and junk-dimension materialization.
    # ------------------------------------------------------------------
    def _phase_apply_column_transforms(
        self, ctx: PipelineContext, transformed_df: DataFrame
    ) -> DataFrame:
        spark = ctx.spark
        config = ctx.config

        if config.pii and config.pii.columns:
            from kimball.processing.pii import apply_pii_masking

            logger.info(f"Applying PII masking to {len(config.pii.columns)} column(s)")
            transformed_df = apply_pii_masking(transformed_df, config.pii)

        if config.junk_dimensions:
            from kimball.processing.junk_dimensions import materialize_junk_dimensions

            transformed_df = materialize_junk_dimensions(
                spark, transformed_df, config.junk_dimensions
            )
        return transformed_df

    # ------------------------------------------------------------------
    # Phase 3: set-based FK resolution for lookup-style foreign keys.
    # ------------------------------------------------------------------
    def _phase_resolve_foreign_keys(
        self, ctx: PipelineContext, transformed_df: DataFrame
    ) -> DataFrame:
        config = ctx.config
        if config.table_type != "fact" or not any(
            fk.lookup is not None for fk in config.foreign_keys or []
        ):
            return transformed_df

        from kimball.observability.unresolved_keys import UnresolvedKeyRegistry
        from kimball.processing.key_broker import KeyBroker

        spark = ctx.spark
        unresolved = any(
            fk.lookup and fk.lookup.early_arriving == "default"
            for fk in config.foreign_keys or []
        )
        schema = getattr(ctx.etl_control, "schema", None) or "default"
        obs = config.observability
        registry = (
            UnresolvedKeyRegistry(
                spark,
                schema,
                obs.unresolved_key_table if obs else "etl_unresolved_dimension_keys",
            )
            if unresolved
            else None
        )
        numeric_versions = [
            int(value) for value in ctx.source_versions.values() if value is not None
        ]
        return KeyBroker(spark, registry).resolve_fact_keys(
            transformed_df,
            config.foreign_keys or [],
            batch_id=ctx.batch_id,
            null_policy=config.null_policy,
            fact_table=config.table_name,
            fact_grain=config.merge_keys or [],
            source_version=max(numeric_versions, default=-1),
        )

    # ------------------------------------------------------------------
    # Phase 4: fact output-column contract (declared measures/degenerates).
    # ------------------------------------------------------------------
    @staticmethod
    def _phase_fact_output_contract(ctx: PipelineContext, df: DataFrame) -> None:
        if ctx.config.table_type != "fact":
            return
        from kimball.orchestration.services.model_contracts import (
            validate_fact_output_columns,
        )

        validate_fact_output_columns(ctx.config, df.columns)

    # ------------------------------------------------------------------
    # Phase 5: source contracts -> findings + DQ event sink.
    # ------------------------------------------------------------------
    def _phase_source_contracts(
        self, ctx: PipelineContext, transformed_df: DataFrame
    ) -> None:
        config = ctx.config
        contracted_sources = [
            source
            for source in config.sources
            if isinstance(source.contract, SourceContractConfig)
        ]
        if not contracted_sources:
            return

        spark = ctx.spark
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
        events = []
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
                events.append(
                    {
                        "pipeline_table": config.table_name,
                        "source": source,
                        "finding": finding,
                        "run_id": ctx.batch_id,
                        "source_version": ctx.source_versions.get(source.name),
                        "action": "blocked"
                        if not finding.passed and finding.severity.value == "error"
                        else "recorded",
                    }
                )
        writer.write_many(events)
        ContractValidator.raise_for_errors(contract_findings)

    # ------------------------------------------------------------------
    # Phase 6: dimension null policy (governed defaults).
    # ------------------------------------------------------------------
    @staticmethod
    def _phase_dimension_null_policy(
        ctx: PipelineContext, transformed_df: DataFrame
    ) -> DataFrame:
        config = ctx.config
        if config.table_type != "dimension":
            return transformed_df
        from kimball.processing.dimension_nulls import apply_dimension_null_policy

        identity_columns = list(config.natural_keys)
        if config.effective_at:
            identity_columns.append(config.effective_at)
        return apply_dimension_null_policy(
            transformed_df,
            config.null_policy,
            identity_columns=identity_columns,
        )

    # ------------------------------------------------------------------
    # Phase 7: user-declared data-quality tests.
    # ------------------------------------------------------------------
    def _phase_declared_tests(self, ctx: PipelineContext, df: DataFrame) -> None:
        if not getattr(ctx.config, "tests", None):
            return
        logger.info("Running data quality validation on transformed data...")
        report = self._validator.run_config_tests(
            ctx.config,
            df=df,
            use_approximate_unique=ctx.runtime_options.use_approximate_unique,
        )
        report.raise_on_failure()

    # ------------------------------------------------------------------
    # Phase 8: natural-key uniqueness pre-merge gate (dimensions).
    # ------------------------------------------------------------------
    def _phase_natural_key_gate(self, ctx: PipelineContext, df: DataFrame) -> None:
        config = ctx.config
        if config.table_type != "dimension" or not config.natural_keys:
            return
        logger.info("Validating natural key uniqueness (pre-merge gate)...")
        nk_result = self._validator.validate_natural_key_uniqueness(
            df,
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
        ctx.validated_grains.add(tuple(config.natural_keys))

    # ------------------------------------------------------------------
    # Phase 9: FK-integrity pre-merge gate (facts with FKs/junk dims).
    # ------------------------------------------------------------------
    def _phase_fk_integrity_gate(self, ctx: PipelineContext, df: DataFrame) -> None:
        config = ctx.config
        if not (
            config.table_type == "fact"
            and (config.foreign_keys or config.junk_dimensions)
            and not getattr(config, "tests", None)
        ):
            return
        logger.info("Validating FK integrity against dimensions (pre-merge gate)...")
        fk_defs = [
            {
                "column": fk.column,
                "dimension_table": fk.references,
                "dimension_key": fk.dimension_key or fk.column,
                "current_only": fk.relationship != "type7",
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
        if not fk_defs:
            return
        fk_report = self._validator.validate_fact_fk_integrity(df, fk_defs)
        for result in fk_report.results:
            logger.info(str(result))
        fk_report.raise_on_failure()
