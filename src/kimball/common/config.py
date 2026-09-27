import os
import re
import warnings
from collections.abc import Mapping, Sequence
from copy import deepcopy
from pathlib import Path
from typing import Annotated, Any, Literal, cast

import yaml
from jinja2 import StrictUndefined, TemplateError, meta
from jinja2.sandbox import SandboxedEnvironment
from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    StringConstraints,
    Tag,
    ValidationError,
    field_validator,
    model_validator,
)


class StrictConfigModel(BaseModel):
    """Base for configuration objects where typos must fail closed."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


NonBlankName = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, pattern=r"\S"),
]

_DURATION_UNITS = (
    r"[sS]|[mM]|[hH]|[dD]|"
    r"[sS][eE][cC][oO][nN][dD][sS]?|"
    r"[mM][iI][nN][uU][tT][eE][sS]?|"
    r"[hH][oO][uU][rR][sS]?|"
    r"[dD][aA][yY][sS]?"
)
DurationString = Annotated[
    str, StringConstraints(pattern=rf"^\d+\s*(?:{_DURATION_UNITS})$")
]


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


class TargetSettings(StrictConfigModel):
    """Validated fields shared by each named target in a target file."""

    catalog: NonBlankName
    silver_schema: NonBlankName
    gold_schema: NonBlankName
    etl_schema: NonBlankName
    checkpoint_root: NonBlankName | None = None
    model_integrity: ModelIntegrityPolicy = Field(default_factory=ModelIntegrityPolicy)


class TargetConfig(TargetSettings):
    """Non-secret data-plane settings for one deployable environment."""

    name: NonBlankName

    def template_context(self) -> dict[str, Any]:
        return {"target": self.model_dump(exclude={"name"}), "target_name": self.name}


def resolve_template_context(
    target: TargetConfig | None,
    explicit_context: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Merge explicit template variables with authoritative target values.

    Callers may supply arbitrary non-secret variables. The target and
    target_name entries always come from the selected target, so an explicit
    context cannot redirect a deployment to another schema or catalog.
    """
    context = dict(explicit_context or {})
    if target is not None:
        context.update(target.template_context())
    return context or None


class TargetFile(StrictConfigModel):
    version: Literal[1]
    targets: dict[str, TargetSettings]

    @field_validator("targets")
    @classmethod
    def validate_target_names(
        cls, value: dict[str, TargetSettings]
    ) -> dict[str, TargetSettings]:
        if any(not name.strip() for name in value):
            raise ValueError("target names must not be blank")
        return value


class TargetLoader:
    """Loads the portable, non-secret ``kimball.targets.yml`` descriptor."""

    def __init__(self, path: str | Path = "kimball.targets.yml") -> None:
        self.path = Path(path)

    def load(self, name: str) -> TargetConfig:
        from kimball.common.errors import (
            ConfigIssue,
            ConfigurationValidationError,
            config_issues_from_validation_error,
        )

        try:
            payload = yaml.safe_load(self.path.read_text(encoding="utf-8"))
            target_file = TargetFile.model_validate(payload or {})
        except OSError as exc:
            raise ConfigurationValidationError(
                [
                    ConfigIssue(
                        str(self.path),
                        None,
                        f"could not read target file ({type(exc).__name__})",
                        "target_io",
                    )
                ]
            ) from exc
        except yaml.YAMLError as exc:
            raise ConfigurationValidationError(
                [ConfigIssue(str(self.path), None, "invalid target YAML", "yaml_error")]
            ) from exc
        except ValidationError as exc:
            raise ConfigurationValidationError(
                config_issues_from_validation_error(str(self.path), exc)
            ) from exc
        target_data = target_file.targets.get(name)
        if target_data is None:
            available = ", ".join(sorted(target_file.targets)) or "(none)"
            raise ValueError(
                f"Unknown target '{name}' in {self.path}. Available targets: {available}"
            )
        return TargetConfig(name=name, **target_data.model_dump())


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
    trigger_interval: DurationString = "30 seconds"
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
    max_age: DurationString


class ContractQualityRuleBase(StrictConfigModel):
    name: str | None = None
    severity: Literal["warn", "error"] = "error"


class NotNullContractQualityRule(ContractQualityRuleBase):
    rule: Literal["not_null"]
    column: NonBlankName


class UniqueContractQualityRule(ContractQualityRuleBase):
    rule: Literal["unique"]
    column: NonBlankName | None = None
    columns: list[NonBlankName] | None = None

    @model_validator(mode="after")
    def validate_unique_columns(self) -> "UniqueContractQualityRule":
        if bool(self.column) == bool(self.columns):
            raise ValueError("unique accepts either column or columns")
        return self


class NullRateContractQualityRule(ContractQualityRuleBase):
    rule: Literal["null_rate"]
    column: NonBlankName
    max_ratio: float = Field(ge=0, le=1)


class AcceptedValuesContractQualityRule(ContractQualityRuleBase):
    rule: Literal["accepted_values"]
    column: NonBlankName
    values: list[Any]


class ExpressionContractQualityRule(ContractQualityRuleBase):
    rule: Literal["expression"]
    expression: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


ContractQualityRule = Annotated[
    NotNullContractQualityRule
    | UniqueContractQualityRule
    | NullRateContractQualityRule
    | AcceptedValuesContractQualityRule
    | ExpressionContractQualityRule,
    Field(discriminator="rule"),
]


class ContractTemporalConfig(StrictConfigModel):
    event_time_column: str
    allowed_lateness: DurationString = "0 hours"
    late_event_severity: Literal["warn", "error"] = "warn"
    out_of_order_severity: Literal["warn", "error"] = "warn"


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
    name: NonBlankName
    alias: NonBlankName = ""
    format: str = "delta"
    options: dict[str, str] = Field(default_factory=dict)
    join_on: str | None = None
    cdc_strategy: Literal["cdf", "full", "append"] = "cdf"
    primary_keys: list[str] | None = Field(default=None)
    starting_version: int = Field(default=0, ge=0)
    streaming: StreamingSourceConfig | None = Field(default=None)
    contract: SourceContractConfig | None = None
    contract_ref: str | None = None

    @model_validator(mode="before")
    @classmethod
    def set_defaults(cls, data: Any) -> Any:
        if isinstance(data, dict):
            data = data.copy()
            if not data.get("alias"):
                data["alias"] = str(data.get("name", "")).split(".")[-1]
        return data

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: Any, handler: Any
    ) -> dict[str, Any]:
        schema = cast(dict[str, Any], handler(core_schema))
        schema["required"] = [
            field for field in schema.get("required", []) if field != "alias"
        ]
        alias_schema = schema.get("properties", {}).get("alias", {})
        alias_schema.pop("default", None)
        alias_schema["description"] = (
            "Optional source alias. When omitted, the final component of name is used."
        )
        return schema

    @model_validator(mode="after")
    def validate_source_contract_selection(self) -> "SourceConfig":
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


class AcceptedValuesTest(StrictConfigModel):
    accepted_values: list[Any]


class RelationshipTestTarget(StrictConfigModel):
    to: NonBlankName
    field: NonBlankName | None = None


class RelationshipsTest(StrictConfigModel):
    relationships: RelationshipTestTarget


class ExpressionTest(StrictConfigModel):
    expression: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


def _structured_test_tag(value: Any) -> str | None:
    if isinstance(value, dict):
        return next(
            (
                key
                for key in ("accepted_values", "relationships", "expression")
                if key in value
            ),
            None,
        )
    if isinstance(value, AcceptedValuesTest):
        return "accepted_values"
    if isinstance(value, RelationshipsTest):
        return "relationships"
    if isinstance(value, ExpressionTest):
        return "expression"
    return None


StructuredTest = Annotated[
    Annotated[AcceptedValuesTest, Tag("accepted_values")]
    | Annotated[RelationshipsTest, Tag("relationships")]
    | Annotated[ExpressionTest, Tag("expression")],
    Discriminator(_structured_test_tag),
]


class TestDefinition(StrictConfigModel):
    column: NonBlankName
    tests: list[Literal["unique", "not_null"] | StructuredTest] = Field(
        default_factory=list
    )
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
    table_name: NonBlankName
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
        if not isinstance(data, dict):
            return data
        data = data.copy()
        keys = data.get("keys", {})
        if keys is None:
            raise ValueError("keys must be a mapping")
        if not isinstance(keys, dict):
            raise ValueError("keys must be a mapping")
        allowed = {"surrogate_key", "durable_key", "natural_keys"}
        unknown = sorted(set(keys) - allowed)
        if unknown:
            raise ValueError(f"keys contains unknown field(s): {unknown}")
        for field_name in allowed:
            if field_name in keys:
                data[field_name] = keys[field_name]
        data.pop("keys", None)
        return data

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: Any, handler: Any
    ) -> dict[str, Any]:
        schema = cast(dict[str, Any], handler(core_schema))
        properties = schema.setdefault("properties", {})
        properties["keys"] = {
            "type": "object",
            "description": "Legacy grouped form of the table key fields.",
            "properties": {
                field_name: deepcopy(properties[field_name])
                for field_name in ("surrogate_key", "durable_key", "natural_keys")
            },
            "additionalProperties": False,
        }
        return schema

    @model_validator(mode="after")
    def validate_kimball_rules(self) -> "TableConfig":
        # Collect independent invariant violations in stable rule order so a
        # single config load can surface every fixable issue.
        from kimball.common.config_rules import (
            ConfigRuleValidationError,
            collect_config_violations,
        )

        if violations := collect_config_violations(self):
            raise ConfigRuleValidationError(violations)
        return self


class ConfigLoader:
    def __init__(
        self,
        env_vars: Mapping[str, str] | None = None,
        *,
        template_context: Mapping[str, Any] | None = None,
        allow_implicit_environment: bool = True,
    ):
        # Explicit env_vars are a portable allowlist. The implicit process
        # environment remains temporarily supported for existing templates.
        raw = dict(env_vars) if env_vars is not None else dict(os.environ)
        self._uses_implicit_environment = env_vars is None
        self.allow_implicit_environment = allow_implicit_environment
        self.env_vars: dict[str, Any] = dict(raw)
        for key, value in raw.items():
            self.env_vars.setdefault(str(key).lower(), value)
        self.template_context = dict(template_context or {})
        self._environment_names = set(self.env_vars)

    def load_config(self, file_path: str) -> TableConfig:
        from kimball.common.errors import (
            ConfigIssue,
            ConfigurationValidationError,
            config_issues_from_validation_error,
        )

        try:
            with open(file_path, encoding="utf-8") as file_handle:
                template_source = file_handle.read()
            environment = SandboxedEnvironment(undefined=StrictUndefined)
            template = environment.from_string(template_source)
            referenced_names = meta.find_undeclared_variables(
                environment.parse(template_source)
            )
            secret_like_names = self._environment_names | set(self.template_context)
            secret_names = sorted(
                name
                for name in referenced_names & secret_like_names
                if re.search(
                    r"(?:secret|password|token|credential|private[_-]?key)",
                    name,
                    re.I,
                )
            )
            if secret_names:
                raise ConfigurationValidationError(
                    [
                        ConfigIssue(
                            file_path,
                            None,
                            "templates cannot interpolate secret environment "
                            "values; use a secret reference instead",
                            "secret_template_value",
                        )
                    ]
                )
            explicit_names = set(self.template_context)
            env_references = sorted(
                (referenced_names & self._environment_names) - explicit_names
            )
            if (
                self._uses_implicit_environment
                and env_references
                and not self.allow_implicit_environment
            ):
                raise ConfigurationValidationError(
                    [
                        ConfigIssue(
                            file_path,
                            None,
                            "implicit process-environment template access is "
                            "disabled; pass explicit template_context/env_vars",
                            "implicit_environment_disabled",
                        )
                    ]
                )
            if self._uses_implicit_environment and env_references:
                warnings.warn(
                    f"{file_path} uses implicit process environment template "
                    f"variables {env_references}; pass them explicitly. This "
                    "compatibility path is deprecated.",
                    UserWarning,
                    stacklevel=2,
                )
            rendered = template.render({**self.env_vars, **self.template_context})
        except ConfigurationValidationError:
            raise
        except OSError as exc:
            raise ConfigurationValidationError(
                [
                    ConfigIssue(
                        file_path,
                        None,
                        f"could not read configuration ({type(exc).__name__})",
                        "config_io",
                    )
                ]
            ) from exc
        except TemplateError as exc:
            raise ConfigurationValidationError(
                [ConfigIssue(file_path, None, str(exc), "template_error")]
            ) from exc

        try:
            payload = yaml.safe_load(rendered)
        except yaml.YAMLError as exc:
            mark = getattr(exc, "problem_mark", None)
            location = (
                f"line {mark.line + 1}, column {mark.column + 1}"
                if mark is not None
                else None
            )
            raise ConfigurationValidationError(
                [ConfigIssue(file_path, location, "invalid YAML", "yaml_error")]
            ) from exc
        if not isinstance(payload, dict):
            raise ConfigurationValidationError(
                [
                    ConfigIssue(
                        file_path,
                        None,
                        "configuration root must be a mapping",
                        "invalid_root",
                    )
                ]
            )
        try:
            config = TableConfig.model_validate(payload)
        except ValidationError as exc:
            raise ConfigurationValidationError(
                config_issues_from_validation_error(file_path, exc)
            ) from exc
        try:
            return self.resolve_contract_refs(config, file_path)
        except Exception as exc:
            raise ConfigurationValidationError(
                [
                    ConfigIssue(
                        file_path,
                        "sources.contract_ref",
                        f"could not resolve referenced contract ({type(exc).__name__})",
                        "contract_reference_error",
                    )
                ]
            ) from exc

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

        sql_code = re.sub(
            r"--[^\r\n]*|/\*.*?\*/|'(?:''|[^'])*'",
            " ",
            sql,
            flags=re.DOTALL,
        )
        sql_stripped = sql_code.strip().upper()
        if not (sql_stripped.startswith("SELECT") or sql_stripped.startswith("WITH")):
            issues.append(
                f"transformation_sql must be a SELECT or WITH statement. "
                f"Got: {sql[:50]}..."
            )
        aliases = {s.alias for s in config.sources}
        sql_upper = sql_code.upper()
        for alias in aliases:
            relation = (
                rf'(?:`{re.escape(alias)}`|"{re.escape(alias)}"|{re.escape(alias)})'
            )
            if not re.search(
                rf"\b(?:FROM|JOIN)\s+{relation}(?![\w$])", sql_code, re.IGNORECASE
            ):
                issues.append(
                    f"transformation_sql does not reference source alias '{alias}'"
                )
        for forbidden in ("DROP", "DELETE", "TRUNCATE", "UPDATE"):
            if re.search(rf"(?:^|;)\s*{forbidden}\b", sql_upper):
                issues.append(
                    f"transformation_sql contains forbidden statement: {forbidden}"
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
        """Compute a full, versioned digest of normalized behavior config."""
        from kimball.common.canonical import (
            CONFIG_FINGERPRINT_VERSION,
            canonical_config_payload,
            canonical_digest,
        )

        payload = canonical_config_payload(config, sql_text=sql_text)
        return CONFIG_FINGERPRINT_VERSION + canonical_digest(payload)


def load_config_entries(
    loader: ConfigLoader, paths: Sequence[str | Path]
) -> list[tuple[str, TableConfig]]:
    """Load every config path and report all independent file errors together."""
    from kimball.common.errors import ConfigIssue, ConfigurationValidationError

    entries: list[tuple[str, TableConfig]] = []
    issues: list[ConfigIssue] = []
    for path_value in paths:
        path = str(path_value)
        try:
            entries.append((path, loader.load_config(path)))
        except ConfigurationValidationError as exc:
            issues.extend(exc.issues)
        except (OSError, ValueError) as exc:
            issues.append(
                ConfigIssue(
                    path,
                    None,
                    f"configuration could not be loaded ({type(exc).__name__})",
                )
            )
    if issues:
        raise ConfigurationValidationError(issues)
    return entries
