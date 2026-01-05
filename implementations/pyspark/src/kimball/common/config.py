import os
from typing import Any, Literal

import yaml
from jinja2 import Template
from pydantic import BaseModel, Field, ValidationError, model_validator


class SourceConfig(BaseModel):
    """Configuration for a source table in the ETL pipeline."""

    name: str
    alias: str
    format: str = "delta"
    options: dict[str, str] = Field(default_factory=dict)
    join_on: str | None = None
    cdc_strategy: Literal["cdf", "full", "timestamp"] = "cdf"
    primary_keys: list[str] | None = Field(
        default=None, description="Keys for CDF deduplication"
    )

    @model_validator(mode="before")
    @classmethod
    def set_defaults(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # Default alias logic: if not provided, use last part of name
            if "alias" not in data:
                name = data.get("name", "")
                data["alias"] = name.split(".")[-1]
        return data


class ForeignKeyConfig(BaseModel):
    """
    Kimball-style foreign key declaration for fact tables.
    Explicitly declares which columns are surrogate key references to dimensions.
    """

    column: str  # Column name in the fact table
    references: str | None = Field(
        default=None, description="Dimension table name for documentation"
    )
    default_value: int = Field(
        default=-1, description="Default value for NULL handling"
    )


class TableConfig(BaseModel):
    """
    Configuration for a target Dimension or Fact table.
    Defines keys, sources, and storage settings.
    """

    table_name: str
    table_type: Literal["dimension", "fact"]
    surrogate_key: str | None = None
    natural_keys: list[str] = Field(default_factory=list)
    sources: list[SourceConfig]
    transformation_sql: str | None = None
    delete_strategy: Literal["hard", "soft"] = "hard"
    enable_audit_columns: bool = Field(alias="audit_columns", default=True)
    scd_type: Literal[1, 2] = 1  # Pydantic coerces int to Literal[1, 2] if specific
    track_history_columns: list[str] | None = None
    default_rows: dict[str, Any] | None = None
    surrogate_key_strategy: Literal["identity", "hash", "sequence"] = "identity"
    schema_evolution: bool = False
    early_arriving_facts: list[dict[str, str]] | None = None
    cluster_by: list[str] | None = None
    optimize_after_merge: bool = False
    merge_keys: list[str] | None = None
    foreign_keys: list[ForeignKeyConfig] | None = None
    enable_lineage_truncation: bool = False

    @model_validator(mode="before")
    @classmethod
    def flatten_keys(cls, data: Any) -> Any:
        """Flatten nested 'keys' parameter from YAML into top-level fields."""
        if isinstance(data, dict):
            keys = data.get("keys", {})
            if keys:
                if "surrogate_key" in keys:
                    data["surrogate_key"] = keys["surrogate_key"]
                if "natural_keys" in keys:
                    data["natural_keys"] = keys["natural_keys"]
        return data

    @model_validator(mode="after")
    def validate_kimball_rules(self) -> "TableConfig":
        """Apply Kimball-specific validation rules."""
        if self.table_type == "dimension":
            if not self.surrogate_key:
                raise ValueError("Dimensions require keys.surrogate_key")
            if not self.natural_keys:
                raise ValueError("Dimensions require keys.natural_keys")
        return self


class ConfigLoader:
    """
    Loads and validates YAML configuration files with Jinja2 templating support.
    Uses Pydantic for schema validation and parsing.
    """

    def __init__(self, env_vars: dict[str, str] | None = None):
        self.env_vars = env_vars or os.environ.copy()

    def load_config(self, file_path: str) -> TableConfig:
        """
        Reads a YAML file, renders it with Jinja2 using env_vars,
        and parses it into a TableConfig object using Pydantic.
        """
        with open(file_path) as f:
            raw_content = f.read()

        # Render Jinja2 template
        template = Template(raw_content)
        rendered_content = template.render(self.env_vars)

        # Parse YAML
        config_dict = yaml.safe_load(rendered_content)

        # Parse and Validate with Pydantic
        try:
            return TableConfig(**config_dict)
        except ValidationError as e:
            raise ValueError(
                f"Configuration validation error in {file_path}: {e}"
            ) from e
