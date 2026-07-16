"""Read-only scheduled upstream contract monitor."""
from __future__ import annotations

import uuid
from pathlib import Path

from kimball.common.config import ConfigLoader
from kimball.observability.data_quality import AlertDispatcher, DataQualityEventWriter
from kimball.orchestration.services.contracts import ContractValidator
from kimball.processing.loader import DataLoader


class ContractMonitor:
    def __init__(self, config_paths: list[str], spark, etl_schema: str):
        self.config_paths = config_paths
        self.spark = spark
        self.etl_schema = etl_schema

    @classmethod
    def from_glob(cls, pattern: str, spark, etl_schema: str) -> ContractMonitor:
        return cls([str(p) for p in Path().glob(pattern)], spark, etl_schema)

    def run(self) -> dict[str, int]:
        monitor_run_id = str(uuid.uuid4())
        summary = {"checked": 0, "failed": 0}
        loader = ConfigLoader()
        versions = DataLoader(self.spark)
        for path in self.config_paths:
            config = loader.load_config(path)
            obs = config.observability
            for source in config.sources:
                if not source.contract:
                    continue
                summary["checked"] += 1
                writer = DataQualityEventWriter(
                    self.spark, self.etl_schema, obs.event_table if obs else "etl_data_quality_events"
                )
                findings = ContractValidator(self.spark).validate_source(source)
                version = None
                if self.spark.catalog.tableExists(source.name) and source.format == "delta":
                    version = versions.get_latest_version(source.name)
                for finding in findings:
                    writer.write(
                        pipeline_table=config.table_name, source=source, finding=finding,
                        source_version=version, monitor_run_id=monitor_run_id,
                    )
                    if not finding.passed:
                        summary["failed"] += 1
                        if finding.severity.value == "error" and obs and "error" in obs.alert_on:
                            AlertDispatcher(obs.webhook_env).dispatch({
                                "severity": "error", "category": finding.category,
                                "source_table": source.name, "pipeline_table": config.table_name,
                                "summary": finding.details,
                            })
        return summary
