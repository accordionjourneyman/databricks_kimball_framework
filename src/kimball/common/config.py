import hashlib
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

import yaml
from jinja2 import StrictUndefined, TemplateError
from jinja2.sandbox import SandboxedEnvironment
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


class StrictConfigModel(BaseModel):
    """Base for configuration objects where typos must fail closed."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class TargetConfig(StrictConfigModel):
    """Non-secret data-plane settings for one deployable environment."""

    name: str
    catalog: str
    silver_schema: str
    gold_schema: str
    etl_schema: str
    checkpoint_root: str | None = None

    def template_context(self) -> dict[str, Any]:
        return {"target": self.model_dump(exclude={"name"}), "target_name": self.name}


class TargetFile(StrictConfigModel):
    version: Literal[1]
    targets: dict[str, dict[str, Any]]


class TargetLoader:
    """Loads the portable, non-secret ``kimball.targets.yml`` descriptor."""

    def __init__(self, path: str | Path = "kimball.targets.yml") -> None:
        self.path = Path(path)

    def load(self, name: str) -> TargetConfig:
        try:
            payload = yaml.safe_load(self.path.read_text(encoding="utf-8"))
            target_file = TargetFile(**(payload or {}))
        except (OSError, ValidationError, yaml.YAMLError) as exc:
            raise ValueError(f"Invalid target descriptor {self.path}: {exc}") from exc
        target_data = target_file.targets.get(name)
        if target_data is None:
            available = ", ".join(sorted(target_file.targets)) or "(none)"
            raise ValueError(
                f"Unknown target '{name}' in {self.path}. Available targets: {available}"
            )
        try:
            return TargetConfig(name=name, **target_data)
        except ValidationError as exc:
            raise ValueError(f"Invalid target '{name}' in {self.path}: {exc}") from exc


class StreamingSourceConfig(StrictConfigModel):
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


class ContractColumnConfig(StrictConfigModel):
    """A supplier column expectation used by a consumer pipeline."""

    type: str
    nullable: bool = True
    required: bool = True


class ContractCDCConfig(StrictConfigModel):
    required: bool = False
    primary_key: list[str] = Field(default_factory=list)


class ContractFreshnessConfig(StrictConfigModel):
    max_age: str

    @model_validator(mode="after")
    def validate_duration(self) -> "ContractFreshnessConfig":
        if not re.match(
            r"^\d+\s*(s|m|h|d|seconds?|minutes?|hours?|days?)$", self.max_age, re.I
        ):
            raise ValueError("freshness.max_age must be a duration such as '2 hours'")
        return self


class ContractQualityRule(StrictConfigModel):
    name: str | None = None
    rule: Literal["not_null", "unique", "null_rate", "accepted_values", "expression"]
    column: str | None = None
    columns: list[str] | None = None
    max_ratio: float | None = Field(default=None, ge=0, le=1)
    values: list[Any] | None = None
    expression: str | None = None
    severity: Literal["warn", "error"] = "error"

    @model_validator(mode="after")
    def validate_rule_shape(self) -> "ContractQualityRule":
        if self.rule in {"not_null", "null_rate", "accepted_values"}:
            if not self.column:
                raise ValueError(f"{self.rule} requires column")
        if self.rule == "null_rate" and self.max_ratio is None:
            raise ValueError("null_rate requires max_ratio")
        if self.rule == "accepted_values" and self.values is None:
            raise ValueError("accepted_values requires values")
        if self.rule == "expression" and not (self.expression or "").strip():
            raise ValueError("expression requires expression")
        if self.rule == "unique":
            if self.column and self.columns:
                raise ValueError("unique accepts either column or columns")
            if not self.column and not self.columns:
                raise ValueError("unique requires column or columns")
        return self


class ContractTemporalConfig(StrictConfigModel):
    event_time_column: str
    allowed_lateness: str = "0 hours"
    late_event_severity: Literal["warn", "error"] = "warn"
    out_of_order_severity: Literal["warn", "error"] = "warn"

    @model_validator(mode="after")
    def validate_duration(self) -> "ContractTemporalConfig":
        if not re.match(
            r"^\d+\s*(s|m|h|d|seconds?|minutes?|hours?|days?)$",
            self.allowed_lateness,
            re.I,
        ):
            raise ValueError(
                "temporal.allowed_lateness must be a duration such as '24 hours'"
            )
        return self


class ContractValidationPolicy(StrictConfigModel):
    """Execution budget for runtime consumer-side contract checks."""

    mode: Literal["full", "sampled", "approximate"] = "full"
    sample_fraction: float | None = Field(default=None, gt=0, le=1)
    sample_seed: int = 17
    max_sample_rows: int | None = Field(default=None, gt=0)
    max_failure_samples: int = Field(default=5, ge=0, le=100)
    max_actions: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_sampling(self) -> "ContractValidationPolicy":
        if self.mode == "sampled" and self.sample_fraction is None:
            raise ValueError("validation.sample_fraction is required in sampled mode")
        return self


class SourceContractConfig(StrictConfigModel):
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
    validation: ContractValidationPolicy = Field(
        default_factory=ContractValidationPolicy
    )


class SourceConfig(StrictConfigModel):
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
    contract_ref: str | None = None

    @model_validator(mode="before")
    @classmethod
    def set_defaults(cls, data: Any) -> Any:
        if isinstance(data, dict) and "alias" not in data:
            data["alias"] = data.get("name", "").split(".")[-1]
        return data

    @model_validator(mode="after")
    def reject_unsupported_cdc_strategy(self) -> "SourceConfig":
        if self.cdc_strategy == "timestamp":
            raise ValueError(
                "cdc_strategy='timestamp' is not implemented; use 'cdf' or 'full'"
            )
        if self.contract and self.contract_ref:
            raise ValueError("contract and contract_ref are mutually exclusive")
        return self


class ForeignKeyConfig(StrictConfigModel):
    column: str
    references: str | None = Field(default=None)
    dimension_key: str | None = Field(default=None)
    default_value: int = Field(default=-1)
    role: str | None = None
    role_playing: bool = False


class PIIColumnConfig(StrictConfigModel):
    """Per-column PII masking declaration.

    See CONFIGURATION.md > PII Masking for full documentation.
    """

    column: str
    strategy: Literal["tokenize", "fast_hash", "hash", "mask", "null", "drop"] = "mask"
    secret_ref: str | None = None
    reveal_prefix: int = Field(default=0, ge=0)
    mask_char: str = Field(default="*", max_length=1)

    @model_validator(mode="after")
    def validate_security_strategy(self) -> "PIIColumnConfig":
        if self.strategy == "tokenize" and not self.secret_ref:
            raise ValueError("tokenize requires secret_ref")
        if self.strategy != "tokenize" and self.secret_ref is not None:
            raise ValueError("secret_ref is only valid for tokenize")
        return self


class PIIPolicy(StrictConfigModel):
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


class TestDefinition(StrictConfigModel):
    column: str
    tests: list[str | dict[str, Any]] = Field(default_factory=list)
    severity: Literal["error", "warn"] = "error"


class FactMeasureConfig(StrictConfigModel):
    name: str
    aggregation: Literal["sum", "avg", "min", "max", "count", "count_distinct"]
    additivity: Literal["additive", "semi_additive", "non_additive"]
    non_additive_dimensions: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_additivity(self) -> "FactMeasureConfig":
        if self.additivity == "semi_additive" and not self.non_additive_dimensions:
            raise ValueError("semi_additive measures require non_additive_dimensions")
        if self.additivity != "semi_additive" and self.non_additive_dimensions:
            raise ValueError(
                "non_additive_dimensions is only valid for semi_additive measures"
            )
        return self


class FactMilestoneConfig(StrictConfigModel):
    name: str
    column: str
    order: int = Field(ge=1)


class JunkDimensionConfig(StrictConfigModel):
    dimension_table: str
    surrogate_key: str
    source_columns: list[str] = Field(min_length=1)


class ConformedDimensionConfig(StrictConfigModel):
    canonical_name: str
    owner: str
    grain: str
    shared_attributes: list[str] = Field(default_factory=list)


class IdentityBridgeConfig(StrictConfigModel):
    table: str
    join_on: str
    target_column: str


class EarlyArrivingDimensionConfig(StrictConfigModel):
    fact_key: str
    dimension_table: str
    dimension_key: str
    surrogate_key: str = "surrogate_key"
    action: Literal["skeleton", "error"] = "skeleton"
    severity: Literal["warn", "error"] = "warn"


def _default_alert_on() -> list[Literal["warn", "error"]]:
    return ["error"]


class ObservabilityConfig(StrictConfigModel):
    enabled: bool = True
    event_table: str = "etl_data_quality_events"
    state_table: str = "etl_contract_monitor_state"
    temporal_state_table: str = "etl_contract_temporal_state"
    write_failure: Literal["warn", "error"] = "warn"
    webhook_env: str = "KIMBALL_ALERT_WEBHOOK_URL"
    alert_on: list[Literal["warn", "error"]] = Field(default_factory=_default_alert_on)


class TableConfig(StrictConfigModel):
    table_name: str
    table_type: Literal["dimension", "fact"]
    depends_on: list[str] = Field(default_factory=list)
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
    grain: str | None = None
    fact_pattern: (
        Literal["transaction", "periodic_snapshot", "accumulating_snapshot"] | None
    ) = None
    snapshot_period: Literal["day", "week", "month", "quarter", "year"] | None = None
    measures: list[FactMeasureConfig] = Field(default_factory=list)
    milestones: list[FactMilestoneConfig] = Field(default_factory=list)
    conformed_dimension: ConformedDimensionConfig | None = None
    degenerate_dimensions: list[str] = Field(default_factory=list)
    junk_dimensions: list[JunkDimensionConfig] = Field(default_factory=list)
    table_description: str | None = None
    column_descriptions: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def flatten_keys(cls, data: Any) -> Any:
        if isinstance(data, dict):
            keys = data.get("keys", {})
            if isinstance(keys, dict):
                for field_name in ("surrogate_key", "natural_keys"):
                    if field_name in keys:
                        data[field_name] = keys[field_name]
                data.pop("keys", None)
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
        if self.table_type == "fact" and self.fact_pattern and not self.grain:
            raise ValueError("facts require a declared grain")
        if self.table_description is not None and not self.table_description.strip():
            raise ValueError("table_description must not be empty")
        if any(
            not name or not description.strip()
            for name, description in self.column_descriptions.items()
        ):
            raise ValueError(
                "column_descriptions requires non-empty column names and descriptions"
            )
        if self.table_type != "fact" and (
            self.fact_pattern
            or self.snapshot_period
            or self.measures
            or self.milestones
            or self.degenerate_dimensions
            or self.junk_dimensions
        ):
            raise ValueError("fact pattern metadata is only valid for fact tables")
        if self.fact_pattern == "periodic_snapshot" and not self.snapshot_period:
            raise ValueError("periodic_snapshot facts require snapshot_period")
        if self.fact_pattern == "accumulating_snapshot":
            if len(self.milestones) < 2:
                raise ValueError(
                    "accumulating_snapshot facts require at least two milestones"
                )
            orders = [m.order for m in self.milestones]
            if len(orders) != len(set(orders)):
                raise ValueError(
                    "accumulating_snapshot milestones must have unique order values"
                )
        for fk in self.foreign_keys or []:
            if fk.role_playing and not fk.role:
                raise ValueError("role_playing foreign keys require role")
            if fk.role_playing and not fk.references:
                raise ValueError(
                    "role_playing foreign keys require references to a physical dimension"
                )
        junk_keys = [junk.surrogate_key for junk in self.junk_dimensions]
        if len(junk_keys) != len(set(junk_keys)):
            raise ValueError("junk dimension surrogate_key values must be unique")
        if set(self.degenerate_dimensions).intersection(junk_keys):
            raise ValueError(
                "a column cannot be both a degenerate and junk dimension key"
            )
        roles = [fk.role for fk in self.foreign_keys or [] if fk.role_playing]
        if len(roles) != len(set(roles)):
            raise ValueError("role_playing foreign key roles must be unique")
        measure_names = [measure.name for measure in self.measures]
        if len(measure_names) != len(set(measure_names)):
            raise ValueError("fact measure names must be unique")
        milestone_names = [milestone.name for milestone in self.milestones]
        milestone_columns = [milestone.column for milestone in self.milestones]
        if len(milestone_names) != len(set(milestone_names)):
            raise ValueError("fact milestone names must be unique")
        if len(milestone_columns) != len(set(milestone_columns)):
            raise ValueError("fact milestone columns must be unique")
        if self.append_only and self.table_type != "fact":
            raise ValueError("append_only is only valid for fact tables")
        if (
            any(s.cdc_strategy == "append" for s in self.sources)
            and not self.append_only
        ):
            raise ValueError(
                "cdc_strategy='append' requires append_only=true for the target table"
            )
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
                if (
                    contract_keys
                    and source.primary_keys
                    and contract_keys != source.primary_keys
                ):
                    raise ValueError(
                        f"Source '{source.name}' primary_keys must match contract.cdc.primary_key"
                    )
                if source.contract.cdc.required and source.cdc_strategy != "cdf":
                    raise ValueError(
                        f"Source '{source.name}' contract requires CDF but cdc_strategy is '{source.cdc_strategy}'"
                    )
        return self


class ConfigLoader:
    def __init__(
        self,
        env_vars: Mapping[str, str] | None = None,
        *,
        template_context: Mapping[str, Any] | None = None,
    ):
        # Windows normalizes environment keys to uppercase when copying them.
        # Keep original keys and lowercase aliases so legacy templates remain
        # portable while new configurations use explicit ``target.*`` values.
        raw = dict(env_vars) if env_vars is not None else dict(os.environ)
        self.env_vars: dict[str, Any] = dict(raw)
        for key, value in raw.items():
            self.env_vars.setdefault(str(key).lower(), value)
        self.template_context = dict(template_context or {})

    def load_config(self, file_path: str) -> TableConfig:
        try:
            with open(file_path, encoding="utf-8") as file_handle:
                rendered = (
                    SandboxedEnvironment(undefined=StrictUndefined)
                    .from_string(file_handle.read())
                    .render({**self.env_vars, **self.template_context})
                )
        except (OSError, TemplateError) as exc:
            raise ValueError(
                f"Configuration template error in {file_path}: {exc}"
            ) from exc
        try:
            config = TableConfig(**yaml.safe_load(rendered))
            return self.resolve_contract_refs(config, file_path)
        except (ValidationError, yaml.YAMLError) as e:
            raise ValueError(
                f"Configuration validation error in {file_path}: {e}"
            ) from e

    def resolve_contract_refs(
        self, config: TableConfig, config_path: str | Path
    ) -> TableConfig:
        """Resolve exact ODCS pins relative to the pipeline configuration."""

        from kimball.contracts.odcs import (
            ODCSContractLoader,
            adapt_odcs_to_source_contract,
        )

        base = Path(config_path).parent
        loader = ODCSContractLoader()
        sources = []
        for source in config.sources:
            if not source.contract_ref:
                sources.append(source)
                continue
            ref_path = Path(source.contract_ref)
            if not ref_path.is_absolute():
                ref_path = base / ref_path
            contract = loader.load_file(ref_path)
            runtime_contract = adapt_odcs_to_source_contract(
                contract, object_name=source.name
            )
            sources.append(source.model_copy(update={"contract": runtime_contract}))
        return config.model_copy(update={"sources": sources})

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
            "transformation_sql": sql_text or config.transformation_sql or "",
            "tests": [
                {"column": t.column, "tests": t.tests, "severity": t.severity}
                for t in (config.tests or [])
            ],
            "foreign_keys": sorted(
                [
                    {
                        "column": fk.column,
                        "references": fk.references,
                        "dimension_key": fk.dimension_key,
                    }
                    for fk in (config.foreign_keys or [])
                ],
                key=lambda x: str(x["column"]),
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
