import hashlib
import os
import re
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator
from pyspark.errors import PySparkException


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
    per_version: bool = False

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


class ContractColumnConfig(BaseModel):
    """A supplier column expectation used by a consumer pipeline."""

    type: str
    nullable: bool = True
    required: bool = True


class ContractCDCConfig(BaseModel):
    required: bool = False
    primary_key: list[str] = Field(default_factory=list)


class ContractFreshnessConfig(BaseModel):
    max_age: str

    @model_validator(mode="after")
    def validate_duration(self) -> "ContractFreshnessConfig":
        if not re.match(r"^\d+\s*(s|m|h|d|seconds?|minutes?|hours?|days?)$", self.max_age, re.I):
            raise ValueError("freshness.max_age must be a duration such as '2 hours'")
        return self


class ContractQualityRule(BaseModel):
    name: str | None = None
    rule: Literal["not_null", "unique", "null_rate", "accepted_values", "expression"]
    column: str | None = None
    columns: list[str] | None = None
    max_ratio: float | None = Field(default=None, ge=0, le=1)
    values: list[Any] | None = None
    expression: str | None = None
    severity: Literal["warn", "error"] = "error"


class ContractTemporalConfig(BaseModel):
    event_time_column: str
    allowed_lateness: str = "0 hours"
    late_event_severity: Literal["warn", "error"] = "warn"
    out_of_order_severity: Literal["warn", "error"] = "warn"

    @model_validator(mode="after")
    def validate_duration(self) -> "ContractTemporalConfig":
        if not re.match(r"^\d+\s*(s|m|h|d|seconds?|minutes?|hours?|days?)$", self.allowed_lateness, re.I):
            raise ValueError("temporal.allowed_lateness must be a duration such as '24 hours'")
        return self


class SourceContractConfig(BaseModel):
    """Executable, consumer-side contract for one upstream source."""

    id: str
    version: str
    owner: str | None = None
    compatibility: Literal["nullable_additions", "strict"] = "nullable_additions"
    schema_: dict[str, ContractColumnConfig] = Field(alias="schema")
    cdc: ContractCDCConfig | None = None
    freshness: ContractFreshnessConfig | None = None
    quality: list[ContractQualityRule] = Field(default_factory=list)
    temporal: ContractTemporalConfig | None = None


class SourceConfig(BaseModel):
    name: str
    alias: str
    format: str = "delta"
    options: dict[str, str] = Field(default_factory=dict)
    join_on: str | None = None
    cdc_strategy: Literal["cdf", "full", "timestamp", "append"] = "cdf"
    primary_keys: list[str] | None = Field(default=None)
    starting_version: int = Field(default=0, ge=0)
    streaming: StreamingSourceConfig | None = Field(default=None)
    contract: SourceContractConfig | None = None

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


class PIIColumnConfig(BaseModel):
    """Per-column PII masking declaration.

    See CONFIGURATION.md > PII Masking for full documentation.
    """

    column: str
    strategy: Literal["hash", "mask", "null", "drop"] = "mask"
    reveal_prefix: int = Field(default=0, ge=0)
    mask_char: str = Field(default="*", max_length=1)


class PIIPolicy(BaseModel):
    """Container for PII column policies declared in the ``pii`` YAML block.

    Applied by ``orchestrator._transform_and_validate`` after
    ``transformation_sql`` and before validation/merge.  On Databricks,
    ``TableCreator._apply_pii_masks`` also emits Delta ``MASK`` clauses
    for role-based read-time enforcement.
    """
    columns: list[PIIColumnConfig] = Field(default_factory=list)

    @property
    def column_map(self) -> dict[str, PIIColumnConfig]:
        return {c.column: c for c in self.columns}

    @property
    def drop_columns(self) -> list[str]:
        return [c.column for c in self.columns if c.strategy == "drop"]


class TestDefinition(BaseModel):
    column: str
    tests: list[str | dict[str, Any]] = Field(default_factory=list)
    severity: Literal["error", "warn"] = "error"


class IdentityBridgeConfig(BaseModel):
    table: str
    join_on: str
    target_column: str


class EarlyArrivingDimensionConfig(BaseModel):
    fact_key: str
    dimension_table: str
    dimension_key: str
    surrogate_key: str = "surrogate_key"
    action: Literal["skeleton", "error"] = "skeleton"
    severity: Literal["warn", "error"] = "warn"


class ObservabilityConfig(BaseModel):
    enabled: bool = True
    event_table: str = "etl_data_quality_events"
    state_table: str = "etl_contract_monitor_state"
    webhook_env: str = "KIMBALL_ALERT_WEBHOOK_URL"
    alert_on: list[Literal["warn", "error"]] = Field(default_factory=lambda: ["error"])


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
    schema_evolution: bool = False
    early_arriving_facts: list[dict[str, str]] | None = None
    early_arriving_dimensions: list[EarlyArrivingDimensionConfig] | None = None
    cluster_by: list[str] | None = None
    optimize_after_merge: bool = False
    merge_keys: list[str] | None = None
    foreign_keys: list[ForeignKeyConfig] | None = None
    tests: list[TestDefinition] | None = Field(default=None)
    enable_lineage_truncation: bool = False
    preserve_all_changes: bool = Field(default=False)
    identity_bridge: IdentityBridgeConfig | None = None
    grain_validation: Literal["error", "warn", "skip"] = "error"
    declare_constraints: bool = True
    pii: PIIPolicy | None = None
    append_only: bool = False
    observability: ObservabilityConfig | None = None

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
        if self.table_type == "fact" and not self.merge_keys:
            raise ValueError("fact tables require merge_keys")
        if self.append_only and self.table_type != "fact":
            raise ValueError("append_only is only valid for fact tables")
        if any(s.cdc_strategy == "append" for s in self.sources) and not self.append_only:
            raise ValueError("cdc_strategy='append' requires append_only=true for the target table")
        if self.scd_type == 2 and not self.effective_at:
            raise ValueError(
                "SCD Type 2 requires 'effective_at' for idempotent history tracking. "
                "Specify the business-time column (e.g. 'updated_at') in the YAML config."
            )
        if self.scd_type == 4 and not self.history_table:
            raise ValueError("SCD Type 4 requires 'history_table' to be specified.")
        if self.scd_type == 6 and not self.current_value_columns:
            raise ValueError(
                "SCD Type 6 requires 'current_value_columns' to be specified."
            )
        for source in self.sources:
            if source.contract and source.contract.cdc:
                contract_keys = source.contract.cdc.primary_key
                if contract_keys and source.primary_keys and contract_keys != source.primary_keys:
                    raise ValueError(
                        f"Source '{source.name}' primary_keys must match contract.cdc.primary_key"
                    )
                if source.contract.cdc.required and source.cdc_strategy != "cdf":
                    raise ValueError(
                        f"Source '{source.name}' contract requires CDF but cdc_strategy is '{source.cdc_strategy}'"
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
            except PySparkException as e:
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
            except PySparkException:
                pass
        try:
            spark.sql(f"EXPLAIN {config.transformation_sql}").collect()
        finally:
            for v in set(views):
                try:
                    spark.catalog.dropTempView(v)
                except PySparkException:
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
            "transformation_sql": sql_text or config.transformation_sql or "",
            "tests": [
                {"column": t.column, "tests": t.tests, "severity": t.severity}
                for t in (config.tests or [])
            ],
            "foreign_keys": sorted(
                [
                    {"column": fk.column, "references": fk.references, "dimension_key": fk.dimension_key}
                    for fk in (config.foreign_keys or [])
                ],
                key=lambda x: x["column"],
            ),
            "delete_strategy": config.delete_strategy,
            "schema_evolution": config.schema_evolution,
            "effective_at": config.effective_at,
            "merge_keys": sorted(config.merge_keys or []),
            "current_value_columns": sorted(config.current_value_columns or []),
            "pii": sorted(
                [
                    {"column": p.column, "strategy": p.strategy}
                    for p in (config.pii.columns if config.pii else [])
                ],
                key=lambda x: x["column"],
            ),
        }
        encoded = yaml.safe_dump(fingerprint_input, sort_keys=True).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()[:16]
