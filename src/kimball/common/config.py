import os
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator


class SourceConfig(BaseModel):
    name: str
    alias: str
    format: str = "delta"
    options: dict[str, str] = Field(default_factory=dict)
    join_on: str | None = None
    cdc_strategy: Literal["cdf", "full", "timestamp"] = "cdf"
    primary_keys: list[str] | None = Field(default=None)
    starting_version: int = Field(default=0, ge=0)

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
            raise ValueError("SCD Type 6 requires 'current_value_columns' to be specified.")
        return self


class ConfigLoader:
    def __init__(self, env_vars: dict[str, str] | None = None):
        self.env_vars = env_vars or os.environ.copy()

    def load_config(self, file_path: str) -> TableConfig:
        with open(file_path) as f:
            rendered = (
                __import__("jinja2.sandbox", fromlist=["SandboxedEnvironment"])
                .SandboxedEnvironment(
                    undefined=__import__("jinja2", fromlist=["StrictUndefined"]).StrictUndefined
                )
                .from_string(f.read())
                .render(self.env_vars)
            )
        try:
            return TableConfig(**yaml.safe_load(rendered))
        except ValidationError as e:
            raise ValueError(f"Configuration validation error in {file_path}: {e}") from e
