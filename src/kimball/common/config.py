import hashlib
import os
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator


class StreamingSourceConfig(BaseModel):
    """Optional streaming configuration for a CDF source.

    When set on a ``SourceConfig``, the framework consumes CDF through a
    Spark structured-streaming query rather than the default batch
    ``readChangeDataFeed`` path. All other source fields (``name``,
    ``alias``, ``primary_keys``, ``cdc_strategy: cdf``) keep their
    existing semantics.

    Example YAML::

        sources:
          - name: silver.customers
            alias: c
            cdc_strategy: cdf
            primary_keys: [customer_id]
            streaming:
              enabled: true
              trigger: available_now       # or processing_time
              trigger_interval: "30 seconds"  # only used by processing_time
              checkpoint_location: /path/to/_checkpoints
    """

    enabled: bool = False
    trigger: Literal["available_now", "processing_time"] = "available_now"
    trigger_interval: str = "30 seconds"
    checkpoint_location: str | None = None
    starting_version: int | None = None
    starting_timestamp: str | None = None
    ignore_deletes: bool = False
    ignore_changes: bool = False
    per_version: bool = True

    @model_validator(mode="after")
    def validate_processing_time(self) -> "StreamingSourceConfig":
        if (
            self.enabled
            and self.trigger == "processing_time"
            and not self.trigger_interval
        ):
            raise ValueError(
                "streaming.trigger_interval is required when trigger='processing_time'"
            )
        return self


class SourceConfig(BaseModel):
    name: str
    alias: str
    format: str = "delta"
    options: dict[str, str] = Field(default_factory=dict)
    join_on: str | None = None
    cdc_strategy: Literal["cdf", "full", "timestamp"] = "cdf"
    primary_keys: list[str] | None = Field(default=None)
    starting_version: int = Field(default=0, ge=0)
    streaming: StreamingSourceConfig | None = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def set_defaults(cls, data: Any) -> Any:
        if isinstance(data, dict) and "alias" not in data:
            data["alias"] = data.get("name", "").split(".")[-1]
        return data


class ForeignKeyConfig(BaseModel):
    column: str
    references: str | None = Field(default=None)
    dimension_key: str | None = Field(default=None)
    default_value: int = Field(default=-1)


class TestDefinition(BaseModel):
    column: str
    tests: list[str | dict[str, Any]] = Field(default_factory=list)
    severity: Literal["error", "warn"] = "error"


class IdentityBridgeConfig(BaseModel):
    table: str
    join_on: str
    target_column: str


class TableConfig(BaseModel):
    table_name: str
    table_type: Literal["dimension", "fact"]
    surrogate_key: str | None = None
    natural_keys: list[str] = Field(default_factory=list)
    sources: list[SourceConfig]
    transformation_sql: str | None = None
    delete_strategy: Literal["hard", "soft"] = "soft"
    enable_audit_columns: bool = Field(alias="audit_columns", default=True)
    scd_type: Literal[1, 2, 4, 6] = 1
    track_history_columns: list[str] | None = None
    history_table: str | None = Field(default=None)
    current_value_columns: list[str] | None = Field(default=None)
    effective_at: str | None = Field(default=None)
    default_rows: dict[str, Any] | None = None
    surrogate_key_strategy: Literal["identity", "hash", "sequence"] = "identity"
    schema_evolution: bool = False
    early_arriving_facts: list[dict[str, str]] | None = None
    cluster_by: list[str] | None = None
    optimize_after_merge: bool = False
    merge_keys: list[str] | None = None
    foreign_keys: list[ForeignKeyConfig] | None = None
    tests: list[TestDefinition] | None = Field(default=None)
    enable_lineage_truncation: bool = False
    preserve_all_changes: bool = Field(default=False)
    identity_bridge: IdentityBridgeConfig | None = None

    @model_validator(mode="before")
    @classmethod
    def flatten_keys(cls, data: Any) -> Any:
        if isinstance(data, dict):
            keys = data.get("keys", {})
            if isinstance(keys, dict):
                for field_name in ("surrogate_key", "natural_keys"):
                    if field_name in keys:
                        data[field_name] = keys[field_name]
        return data

    @model_validator(mode="after")
    def validate_kimball_rules(self) -> "TableConfig":
        if self.table_type == "dimension":
            if not self.surrogate_key:
                raise ValueError("Dimensions require keys.surrogate_key")
            if not self.natural_keys:
                raise ValueError("Dimensions require keys.natural_keys")
            if self.scd_type == 2 and self.surrogate_key_strategy == "hash":
                raise ValueError("SCD Type 2 cannot use 'hash' surrogate key strategy.")
        if self.scd_type == 4 and not self.history_table:
            raise ValueError("SCD Type 4 requires 'history_table' to be specified.")
        if self.scd_type == 6 and not self.current_value_columns:
            raise ValueError(
                "SCD Type 6 requires 'current_value_columns' to be specified."
            )
        return self


class ConfigLoader:
    def __init__(self, env_vars: dict[str, str] | None = None):
        self.env_vars = env_vars or os.environ.copy()

    def load_config(self, file_path: str) -> TableConfig:
        with open(file_path) as f:
            rendered = (
                __import__("jinja2.sandbox", fromlist=["SandboxedEnvironment"])
                .SandboxedEnvironment(
                    undefined=__import__(
                        "jinja2", fromlist=["StrictUndefined"]
                    ).StrictUndefined
                )
                .from_string(f.read())
                .render(self.env_vars)
            )
        try:
            return TableConfig(**yaml.safe_load(rendered))
        except ValidationError as e:
            raise ValueError(
                f"Configuration validation error in {file_path}: {e}"
            ) from e

    def validate_transformation_sql(
        self,
        config: TableConfig,
        spark: Any | None = None,
    ) -> list[str]:
        """
        Compile-time validation of transformation_sql.

        Catches SQL errors (syntax, column references, type mismatches) before
        the full pipeline executes. On Databricks/local with a real SparkSession,
        uses EXPLAIN against an empty source. Without a session, does lightweight
        text checks (alias presence, no DROP/DELETE statements).

        Returns a list of issue strings. Empty list = no issues found.
        """
        issues: list[str] = []
        sql = config.transformation_sql
        if not sql:
            return issues

        if spark is not None:
            try:
                self._explain_dry_run(config, spark)
                return []
            except Exception as e:
                issues.append(f"SQL dry-run failed: {e}")
                return issues

        sql_stripped = sql.strip().upper()
        if not (sql_stripped.startswith("SELECT") or sql_stripped.startswith("WITH")):
            issues.append(
                f"transformation_sql must be a SELECT or WITH statement. "
                f"Got: {sql[:50]}..."
            )
        aliases = {s.alias for s in config.sources}
        sql_upper = sql.upper()
        for alias in aliases:
            if alias.upper() not in sql_upper:
                issues.append(
                    f"transformation_sql does not reference source alias '{alias}'"
                )
        for forbidden in ("DROP ", "DELETE ", "TRUNCATE ", "UPDATE "):
            if forbidden in sql_upper:
                issues.append(
                    f"transformation_sql contains forbidden statement: {forbidden.strip()}"
                )
        return issues

    def _explain_dry_run(self, config: TableConfig, spark: Any) -> None:
        """
        Dry-run transformation_sql via EXPLAIN against empty temp views.
        Raises if the SQL is invalid or references missing columns.
        """
        import uuid as _uuid

        views: list[str] = []
        for source in config.sources:
            view_name = f"_kimball_dryrun_{_uuid.uuid4().hex[:8]}"
            try:
                if spark.catalog.tableExists(source.name):
                    spark.read.format("delta").table(source.name).limit(
                        0
                    ).createOrReplaceTempView(view_name)
                else:
                    spark.createDataFrame([], schema="x int").createOrReplaceTempView(
                        view_name
                    )
                views.append(view_name)
                if source.alias != view_name:
                    spark.sql(
                        f"CREATE OR REPLACE TEMP VIEW {source.alias} AS SELECT * FROM {view_name}"
                    )
                    views.append(source.alias)
            except Exception:
                pass
        try:
            spark.sql(f"EXPLAIN {config.transformation_sql}").collect()
        finally:
            for v in set(views):
                try:
                    spark.catalog.dropTempView(v)
                except Exception:
                    pass

    def compute_fingerprint(
        self, config: TableConfig, sql_text: str | None = None
    ) -> str:
        """
        Compute a deterministic fingerprint of the config + transformation SQL.

        Used for state-aware validation skipping: if the fingerprint matches
        the last successful run for this table, validation can be skipped.
        """
        fingerprint_input = {
            "table_name": config.table_name,
            "table_type": config.table_type,
            "scd_type": config.scd_type,
            "natural_keys": sorted(config.natural_keys),
            "track_history_columns": sorted(config.track_history_columns or []),
            "surrogate_key": config.surrogate_key,
            "surrogate_key_strategy": config.surrogate_key_strategy,
            "transformation_sql": sql_text or config.transformation_sql or "",
            "tests": [
                {"column": t.column, "tests": t.tests, "severity": t.severity}
                for t in (config.tests or [])
            ],
        }
        encoded = yaml.safe_dump(fingerprint_input, sort_keys=True).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()[:16]
