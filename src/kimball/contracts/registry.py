from __future__ import annotations

import json
from typing import Any

from kimball.common.utils import quote_table_name
from kimball.contracts.odcs import ODCSContract


def _sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def _row_value(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    try:
        return row[key]
    except (KeyError, TypeError):
        return getattr(row, key, None)


class DeltaContractRegistry:
    """Deployment/audit registry; Git contract files remain authoritative."""

    def __init__(self, spark: Any, schema: str):
        self.spark = spark
        self.schema = schema
        self.versions_table = f"{schema}.etl_contract_versions"
        self.consumers_table = f"{schema}.etl_contract_consumers"
        self.manifests_table = f"{schema}.etl_pipeline_manifests"

    def ensure_tables(self) -> None:
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {quote_table_name(self.versions_table)} (
                contract_id STRING NOT NULL,
                contract_version STRING NOT NULL,
                api_version STRING NOT NULL,
                status STRING NOT NULL,
                spec_digest STRING NOT NULL,
                spec_json STRING NOT NULL,
                source_path STRING NOT NULL,
                published_by STRING,
                published_at TIMESTAMP NOT NULL
            ) USING DELTA
            """
        )
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {quote_table_name(self.consumers_table)} (
                pipeline_table STRING NOT NULL,
                source_table STRING NOT NULL,
                contract_id STRING NOT NULL,
                contract_version STRING NOT NULL,
                config_digest STRING NOT NULL,
                registered_at TIMESTAMP NOT NULL
            ) USING DELTA
            """
        )
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {quote_table_name(self.manifests_table)} (
                project_digest STRING NOT NULL,
                manifest_json STRING NOT NULL,
                source_revision STRING,
                environment STRING NOT NULL,
                deployed_by STRING,
                deployed_at TIMESTAMP NOT NULL
            ) USING DELTA
            """
        )

    def publish_contract(
        self,
        contract: ODCSContract,
        *,
        source_path: str,
        published_by: str | None = None,
    ) -> bool:
        self.ensure_tables()
        existing = self.spark.sql(
            f"SELECT spec_digest FROM {quote_table_name(self.versions_table)} "
            f"WHERE contract_id = {_sql_literal(contract.id)} "
            f"AND contract_version = {_sql_literal(contract.version)} LIMIT 1"
        ).collect()
        if existing:
            digest = _row_value(existing[0], "spec_digest")
            if digest != contract.digest:
                raise ValueError(
                    f"Published contract {contract.id} {contract.version} is immutable"
                )
            return False

        spec_json = json.dumps(
            contract.document, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        self.spark.sql(
            f"""
            INSERT INTO {quote_table_name(self.versions_table)}
            (contract_id, contract_version, api_version, status, spec_digest,
             spec_json, source_path, published_by, published_at)
            VALUES ({_sql_literal(contract.id)}, {_sql_literal(contract.version)},
                    {_sql_literal(contract.api_version)}, {_sql_literal(contract.status)},
                    {_sql_literal(contract.digest)}, {_sql_literal(spec_json)},
                    {_sql_literal(source_path)}, {_sql_literal(published_by)},
                    current_timestamp())
            """
        )
        return True

    def register_consumer(
        self,
        *,
        pipeline_table: str,
        source_table: str,
        contract_id: str,
        contract_version: str,
        config_digest: str,
    ) -> None:
        self.ensure_tables()
        self.spark.sql(
            f"""
            MERGE INTO {quote_table_name(self.consumers_table)} AS target
            USING (SELECT {_sql_literal(pipeline_table)} AS pipeline_table,
                          {_sql_literal(source_table)} AS source_table,
                          {_sql_literal(contract_id)} AS contract_id,
                          {_sql_literal(contract_version)} AS contract_version,
                          {_sql_literal(config_digest)} AS config_digest) AS source
            ON target.pipeline_table = source.pipeline_table
               AND target.source_table = source.source_table
            WHEN MATCHED THEN UPDATE SET
              contract_id = source.contract_id,
              contract_version = source.contract_version,
              config_digest = source.config_digest,
              registered_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT
              (pipeline_table, source_table, contract_id, contract_version,
               config_digest, registered_at)
            VALUES
              (source.pipeline_table, source.source_table, source.contract_id,
               source.contract_version, source.config_digest, current_timestamp())
            """
        )

    def publish_manifest(
        self,
        manifest: dict[str, Any],
        *,
        environment: str,
        source_revision: str | None = None,
        deployed_by: str | None = None,
    ) -> None:
        self.ensure_tables()
        rendered = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
        self.spark.sql(
            f"""
            INSERT INTO {quote_table_name(self.manifests_table)}
            (project_digest, manifest_json, source_revision, environment,
             deployed_by, deployed_at)
            VALUES ({_sql_literal(manifest["project_digest"])}, {_sql_literal(rendered)},
                    {_sql_literal(source_revision)}, {_sql_literal(environment)},
                    {_sql_literal(deployed_by)}, current_timestamp())
            """
        )
