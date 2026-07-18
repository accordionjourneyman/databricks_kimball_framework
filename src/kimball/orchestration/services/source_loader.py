from __future__ import annotations

import logging
import time
from typing import Any

from pyspark.sql import DataFrame

from kimball.common.config import SourceContractConfig
from kimball.observability.data_quality import (
    AlertDispatcher,
    DataQualityEventSink,
    DataQualityEventWriter,
)
from kimball.observability.temporal_state import (
    PendingTemporalState,
    TemporalStateStore,
)
from kimball.orchestration.services.context import PipelineContext
from kimball.orchestration.services.contracts import ContractValidator
from kimball.orchestration.services.work_plan import SourceWorkPlan

logger = logging.getLogger(__name__)


class SourceLoader:
    def load(
        self, ctx: PipelineContext, plan: SourceWorkPlan
    ) -> tuple[dict[str, Any], dict[str, DataFrame]]:
        source_versions: dict[str, Any] = {}
        active_dfs: dict[str, DataFrame] = {}
        stage_start = time.time()

        for item in plan.active_items:
            source = item.source
            processed_version = item.ending_version if item.ending_version is not None else 0
            source_versions[source.name] = processed_version
            self._validate_source_contract(ctx, source, processed_version)

            if source.cdc_strategy == 'full':
                df = ctx.loader.load_full_snapshot(
                    source.name, format=source.format, options=source.options
                )
            else:
                if item.starting_version is None or item.ending_version is None:
                    raise ValueError(
                        f'Incremental source {source.name} has no planned version range'
                    )
                df = ctx.loader.load_cdf(
                    source.name,
                    starting_version=item.starting_version,
                    deduplicate_keys=None,
                    ending_version=item.ending_version,
                )

            if source.cdc_strategy == 'append':
                metadata = [
                    column
                    for column in (
                        '_change_type',
                        '_commit_version',
                        '_commit_timestamp',
                    )
                    if column in df.columns
                ]
                if metadata:
                    df = df.drop(*metadata)

            if (
                isinstance(source.contract, SourceContractConfig)
                and source.contract.temporal
            ):
                self._validate_temporal(ctx, source, df, processed_version)

            if source.cdc_strategy == 'cdf':
                df = ctx.loader.deduplicate_cdf(df, source.primary_keys)
            df.createOrReplaceTempView(source.alias)
            active_dfs[source.name] = df

        logger.info(
            'Loaded %d source(s) in %.2fs',
            len(active_dfs),
            time.time() - stage_start,
        )
        return source_versions, active_dfs

    @staticmethod
    def _event_sink(ctx: PipelineContext) -> DataQualityEventSink:
        schema = getattr(ctx.etl_control, 'schema', None) or 'default'
        observability = ctx.config.observability
        return DataQualityEventSink(
            ctx.spark,
            schema,
            observability.event_table
            if observability
            else 'etl_data_quality_events',
            failure_mode=observability.write_failure if observability else 'warn',
            writer_type=DataQualityEventWriter,
        )

    def _validate_source_contract(
        self, ctx: PipelineContext, source: Any, source_version: int
    ) -> None:
        if not isinstance(source.contract, SourceContractConfig):
            return
        validator = ContractValidator(ctx.spark)
        findings = validator.validate_source(source)
        sink = self._event_sink(ctx)
        observability = ctx.config.observability
        events = [
            {
                'pipeline_table': ctx.config.table_name,
                'source': source,
                'finding': finding,
                'run_id': ctx.batch_id,
                'source_version': source_version,
                'action': (
                    'blocked'
                    if not finding.passed and finding.severity.value == 'error'
                    else 'recorded'
                ),
            }
            for finding in findings
        ]
        event_ids = sink.write_many(events)
        for finding, event_id in zip(findings, event_ids, strict=True):
            if (
                not finding.passed
                and finding.severity.value == 'error'
                and observability
                and 'error' in observability.alert_on
            ):
                AlertDispatcher(observability.webhook_env).dispatch(
                    {
                        'event_id': event_id,
                        'severity': 'error',
                        'category': finding.category,
                        'source_table': source.name,
                        'pipeline_table': ctx.config.table_name,
                        'summary': finding.details,
                    }
                )
        validator.raise_for_errors(findings)

    def _validate_temporal(
        self,
        ctx: PipelineContext,
        source: Any,
        df: DataFrame,
        source_version: int,
    ) -> None:
        schema = getattr(ctx.etl_control, 'schema', None) or 'default'
        observability = ctx.config.observability
        state_store = TemporalStateStore(
            ctx.spark,
            schema,
            observability.temporal_state_table
            if observability
            else 'etl_contract_temporal_state',
        )
        prior_state = state_store.existing(
            ctx.config.table_name, source.name, source.contract.id
        )
        pending_update = state_store.prepare(
            df, pipeline_table=ctx.config.table_name, source=source
        )
        validator = ContractValidator(ctx.spark)
        findings = validator.validate_temporal(df, source, prior_state=prior_state)
        if validator.last_metrics:
            ctx.validation_metrics.append(
                {
                    **validator.last_metrics,
                    'source_table': source.name,
                    'pipeline_table': ctx.config.table_name,
                }
            )
        sink = self._event_sink(ctx)
        sink.write_many(
            [
                {
                    'pipeline_table': ctx.config.table_name,
                    'source': source,
                    'finding': finding,
                    'run_id': ctx.batch_id,
                    'source_version': source_version,
                    'action': (
                        'blocked'
                        if not finding.passed and finding.severity.value == 'error'
                        else 'accepted_late'
                    ),
                }
                for finding in findings
            ]
        )
        validator.raise_for_errors(findings)
        ctx.pending_temporal_state.append(
            PendingTemporalState(state_store, pending_update, ctx.batch_id)
        )
