import os
from typing import Any, Literal

import yaml

# Jinja2 sandboxed environment is used in ConfigLoader
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
    starting_version: int = Field(
        default=0,
        description="Version to start reading CDF from during initial load. Defaults to 0.",
        ge=0,
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
    # FINDING-026: Add dimension_key to specify the actual SK column in the dimension
    dimension_key: str | None = Field(
        default=None,
        description="Surrogate key column name in the dimension table. "
        "If not specified, assumes same name as 'column'.",
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
    delete_strategy: Literal["hard", "soft"] = (
        "soft"  # Kimball: preserve history for FK integrity
    )
    enable_audit_columns: bool = Field(alias="audit_columns", default=True)
    scd_type: Literal[1, 2, 4, 6] = 1
    track_history_columns: list[str] | None = None
    history_table: str | None = Field(
        default=None,
        description="EAV history table name for SCD4. Required when scd_type=4.",
    )
    current_value_columns: list[str] | None = Field(
        default=None,
        description="Columns to backfill as current_* for SCD6. Required when scd_type=6.",
    )
    effective_at: str | None = Field(
        default=None,
        description="Column name containing business effective date for SCD2. "
        "If not set, uses processing time (__etl_processed_at).",
    )
    default_rows: dict[str, Any] | None = None
    surrogate_key_strategy: Literal["identity", "hash", "sequence"] = "identity"
    schema_evolution: bool = False
    early_arriving_facts: list[dict[str, str]] | None = None
    cluster_by: list[str] | None = None
    optimize_after_merge: bool = False
    merge_keys: list[str] | None = None
    foreign_keys: list[ForeignKeyConfig] | None = None
    enable_lineage_truncation: bool = False
    preserve_all_changes: bool = Field(
        default=False,
        description="For SCD2: When True, processes each CDF version separately to preserve "
        "all intermediate state changes. When False (default), fast-forwards to latest state. "
        "Enable this if capturing every historical state change is critical.",
    )

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
            # FINDING-024: Block hash strategy for SCD2 dimensions
            if self.scd_type == 2 and self.surrogate_key_strategy == "hash":
                raise ValueError(
                    "SCD Type 2 cannot use 'hash' surrogate key strategy. "
                    "Hash-based keys produce identical values for all versions of the same natural key. "
                    "Use 'identity' instead."
                )
        # SCD4 requires history_table
        if self.scd_type == 4 and not self.history_table:
            raise ValueError(
                "SCD Type 4 requires 'history_table' to be specified. "
                "This is the EAV table that stores field-level change history."
            )
        # SCD6 requires current_value_columns
        if self.scd_type == 6 and not self.current_value_columns:
            raise ValueError(
                "SCD Type 6 requires 'current_value_columns' to be specified. "
                "These columns will be backfilled as current_* on all historical rows."
            )
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

        # FINDING-023: Use sandboxed environment to prevent SSTI attacks
        from jinja2 import StrictUndefined
        from jinja2.sandbox import SandboxedEnvironment

        env = SandboxedEnvironment(undefined=StrictUndefined)
        template = env.from_string(raw_content)
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
