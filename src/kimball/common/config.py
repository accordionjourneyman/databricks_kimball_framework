import hashlib
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

import yaml
from jinja2 import StrictUndefined, TemplateError
from jinja2.sandbox import SandboxedEnvironment
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)


class StrictConfigModel(BaseModel):
    """Base for configuration objects where typos must fail closed."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


MODEL_INTEGRITY_CODES = (
    "COLUMN_SEMANTICS_CONFLICT",
    "FACT_DIMENSION_ATTRIBUTE",
    "GRAIN_KEY_MISMATCH",
    "MEASURE_ADDITIVITY_MISSING",
    "INCREMENTAL_LOAD_FRAGILE",
    "MISSING_REFERENCE_TARGET",
    "ORPHAN_REFERENCE",
    "MISSING_DESCRIPTION",
)

Fixability = Literal["auto_fixable", "suggest_fix", "decision_required"]


ModelIntegritySeverity = Literal["error", "warning"]


class ModelIntegrityRulePolicy(StrictConfigModel):
    """Per-target policy for one model-integrity rule (ADR-003 §Decision 8)."""

    code: str
    enabled: bool = True
    severity: ModelIntegritySeverity | None = None
    params: dict[str, Any] = Field(default_factory=dict)

    @field_validator("code")
    @classmethod
    def _code_known(cls, value: str) -> str:
        if value not in MODEL_INTEGRITY_CODES:
            known = ", ".join(MODEL_INTEGRITY_CODES)
            raise ValueError(
                f"unknown model_integrity rule code '{value}'; known codes: {known}"
            )
        return value

    @model_validator(mode="after")
    def _params_declared(self) -> "ModelIntegrityRulePolicy":
        from kimball.planning.model_integrity import RULE_SPECS

        spec = RULE_SPECS.get(self.code)
        if spec is None:
            raise ValueError(
                f"model_integrity rule '{self.code}' is registered but has no "
                f"RuleSpec; this is a framework bug"
            )
        unknown = sorted(set(self.params) - spec.params)
        if unknown:
            raise ValueError(
                f"model_integrity rule '{self.code}' does not accept parameter(s) "
                f"{unknown}; declared params: {sorted(spec.params)}"
            )
        return self


class ModelIntegrityPolicy(StrictConfigModel):
    """Per-target tuning of the model-integrity validator (ADR-003 §Decision 8)."""

    rules: list[ModelIntegrityRulePolicy] = Field(default_factory=list)

    def policy_for(self, code: str) -> ModelIntegrityRulePolicy | None:
        for rule in self.rules:
            if rule.code == code:
                return rule
        return None


class TargetConfig(StrictConfigModel):
    """Non-secret data-plane settings for one deployable environment."""

    name: str
    catalog: str
    silver_schema: str
    gold_schema: str
    etl_schema: str
    checkpoint_root: str | None = None
    model_integrity: ModelIntegrityPolicy = Field(default_factory=ModelIntegrityPolicy)

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
        if (
            self.rule in {"not_null", "null_rate", "accepted_values"}
            and not self.column
        ):
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


class ForeignKeyLookupConfig(StrictConfigModel):
    source_columns: list[str] = Field(min_length=1)
    dimension_columns: list[str] | None = None
    event_time: str | None = None
    identity_map: str | None = None
    early_arriving: Literal["skeleton", "default", "error"] = "skeleton"
    not_applicable_when: str | None = None
    invalid_action: Literal["default", "error"] = "error"
    validate_resolution: bool = False
    detect_fanout: bool = True

    @model_validator(mode="after")
    def validate_column_mapping(self) -> "ForeignKeyLookupConfig":
        if self.dimension_columns and len(self.dimension_columns) != len(
            self.source_columns
        ):
            raise ValueError(
                "lookup.dimension_columns must have the same length as source_columns"
            )
        return self


class ForeignKeyConfig(StrictConfigModel):
    column: str
    references: str | None = Field(default=None)
    dimension_key: str | None = Field(default=None)
    role: str | None = None
    role_playing: bool = False
    relationship: Literal["standard", "type7"] = "standard"
    durable_column: str | None = None
    durable_dimension_key: str | None = None
    lookup: ForeignKeyLookupConfig | None = None

    @model_validator(mode="after")
    def validate_type7(self) -> "ForeignKeyConfig":
        if self.lookup is not None and (not self.references or not self.dimension_key):
            raise ValueError(
                "brokered relationships require references and dimension_key"
            )
        if self.lookup and self.lookup.identity_map:
            if len(self.lookup.source_columns) != 1:
                raise ValueError(
                    "identity_map currently requires exactly one lookup.source_columns entry"
                )
            if not self.lookup.event_time:
                raise ValueError(
                    "identity_map requires lookup.event_time for temporal resolution"
                )
        if self.relationship != "type7":
            if self.durable_column or self.durable_dimension_key:
                raise ValueError(
                    "durable columns are only valid for relationship='type7'"
                )
            return self
        if not self.references or not self.dimension_key:
            raise ValueError("type7 relationships require references and dimension_key")
        if not self.durable_column or not self.durable_dimension_key:
            raise ValueError(
                "type7 relationships require durable_column and durable_dimension_key"
            )
        if self.lookup is None or not self.lookup.event_time:
            raise ValueError("type7 relationships require lookup.event_time")
        return self


class NullPolicyConfig(StrictConfigModel):
    attribute_substitutes: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_substitutes(self) -> "NullPolicyConfig":
        if invalid := [
            name
            for name, value in self.attribute_substitutes.items()
            if value is None or (isinstance(value, str) and not value.strip())
        ]:
            raise ValueError(
                "attribute_substitutes values must be non-null and non-blank: "
                + ", ".join(sorted(invalid))
            )
        return self


class PIIColumnConfig(StrictConfigModel):
    """Per-column PII masking declaration.

    See CONFIGURATION.md > PII Masking for full documentation.
    """

    column: str
    strategy: Literal["tokenize", "fast_hash", "mask", "null", "drop"] = "mask"
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


class RowFilterConfig(StrictConfigModel):
    """Unity Catalog row-level security via ``ALTER TABLE SET ROW FILTER``.

    Declares a SQL UDF that returns a boolean per row.  Rows for which the
    function returns ``False`` are hidden from queries.
    """

    function_name: str
    function_body: str
    column: str
    grant_to: list[str] | None = None


class GeneratedColumnConfig(StrictConfigModel):
    """Definition of a Delta generated column.

    Delta requires the generated column's data type to be declared explicitly;
    deriving it from the expression is not reliable and can produce DDL that
    fails only after a deployment has started.
    """

    expression: str
    data_type: str


class ABACPolicyConfig(StrictConfigModel):
    """Attribute-Based Access Control policy applied at catalog/schema/table scope.

    Policies reference governed tags: apply a tag to a column and the policy
    activates automatically for matching users.
    """

    policy_name: str
    policy_type: Literal["row_filter", "column_mask"]
    udf_name: str
    udf_body: str
    target_groups: list[str]
    match_tag: str
    function_argument: str = "matched_value"
    tag_value: str | None = None
    scope: Literal["catalog", "schema", "table"] = "schema"


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


class ModelingExceptionConfig(StrictConfigModel):
    """A recorded, intentional deviation from a model-integrity rule."""

    code: str
    columns: list[str] = Field(min_length=1)
    reason: str
    decision_ref: str | None = None

    @field_validator("code")
    @classmethod
    def _code_known(cls, value: str) -> str:
        if value not in MODEL_INTEGRITY_CODES:
            known = ", ".join(MODEL_INTEGRITY_CODES)
            raise ValueError(
                f"unknown modeling-exception code '{value}'; known codes: {known}"
            )
        return value

    @field_validator("columns")
    @classmethod
    def _columns_non_blank(cls, value: list[str]) -> list[str]:
        if any(not column or not column.strip() for column in value):
            raise ValueError("modeling_exceptions columns must be non-blank")
        return value

    @field_validator("reason")
    @classmethod
    def _reason_non_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("modeling_exceptions reason must not be blank")
        return value


def _default_alert_on() -> list[Literal["warn", "error"]]:
    return ["error"]


class ObservabilityConfig(StrictConfigModel):
    enabled: bool = True
    event_table: str = "etl_data_quality_events"
    state_table: str = "etl_contract_monitor_state"
    temporal_state_table: str = "etl_contract_temporal_state"
    unresolved_key_table: str = "etl_unresolved_dimension_keys"
    write_failure: Literal["warn", "error"] = "warn"
    webhook_env: str = "KIMBALL_ALERT_WEBHOOK_URL"
    alert_on: list[Literal["warn", "error"]] = Field(default_factory=_default_alert_on)


class TableConfig(StrictConfigModel):
    table_name: str
    table_type: Literal["dimension", "fact"]
    depends_on: list[str] = Field(default_factory=list)
    surrogate_key: str | None = None
    durable_key: str | None = None
    natural_keys: list[str] = Field(default_factory=list)
    sources: list[SourceConfig]
    transformation_sql: str | None = None
    delete_strategy: Literal["hard", "soft"] = "soft"
    enable_audit_columns: bool = Field(alias="audit_columns", default=True)
    scd_type: Literal[1, 2, 4, 6, 7] = 1
    track_history_columns: list[str] | None = None
    history_table: str | None = Field(default=None)
    current_value_columns: list[str] | None = Field(default=None)
    effective_at: str | None = Field(default=None)
    default_rows: dict[str, Any] | None = None
    schema_evolution: bool = False
    cluster_by: list[str] | None = None
    generated_columns: dict[str, GeneratedColumnConfig] | None = None
    optimize_after_merge: bool = False
    vacuum_after_merge: bool = False
    vacuum_retention_hours: int = Field(default=168, ge=168)
    merge_keys: list[str] | None = None
    foreign_keys: list[ForeignKeyConfig] | None = None
    tests: list[TestDefinition] | None = Field(default=None)
    enable_lineage_truncation: bool = False
    preserve_all_changes: bool = Field(default=False)
    null_policy: NullPolicyConfig = Field(default_factory=NullPolicyConfig)
    grain_validation: Literal["error", "warn", "skip"] = "error"
    declare_constraints: bool = True
    pii: PIIPolicy | None = None
    row_filter: RowFilterConfig | None = None
    abac_policies: list[ABACPolicyConfig] | None = None
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
    modeling_exceptions: list[ModelingExceptionConfig] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def flatten_keys(cls, data: Any) -> Any:
        if isinstance(data, dict):
            keys = data.get("keys", {})
            if isinstance(keys, dict):
                for field_name in ("surrogate_key", "durable_key", "natural_keys"):
                    if field_name in keys:
                        data[field_name] = keys[field_name]
                data.pop("keys", None)
        return data

    @model_validator(mode="after")
    def validate_kimball_rules(self) -> "TableConfig":
        # Kimball invariants live in config_rules.py as named, pure
        # predicates (ADR-004 step 5): one home per invariant, enumerable
        # rule set, fail-closed ValueError surface unchanged.
        from kimball.common.config_rules import first_config_violation

        if violation := first_config_violation(self):
            raise ValueError(violation)
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
            "durable_key": config.durable_key,
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
                        "relationship": fk.relationship,
                        "durable_column": fk.durable_column,
                        "durable_dimension_key": fk.durable_dimension_key,
                        "lookup": fk.lookup.model_dump() if fk.lookup else None,
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
            "null_policy": config.null_policy.model_dump(),
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
