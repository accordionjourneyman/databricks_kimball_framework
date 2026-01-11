import os
from dataclasses import dataclass
from typing import Any

import yaml
from jinja2 import Template
from jsonschema import ValidationError, validate


@dataclass
class FreshnessConfig:
    """Source freshness thresholds for alerting."""

    warn_after_hours: int | None = None
    error_after_hours: int | None = None


@dataclass
class SourceConfig:
    name: str
    alias: str
    join_on: str | None = None
    cdc_strategy: str = "cdf"  # cdf, full, timestamp
    primary_keys: list[str] | None = (
        None  # Keys for CDF deduplication (prevents duplicate row errors)
    )
    freshness: FreshnessConfig | None = None  # Source freshness thresholds


@dataclass
class ForeignKeyConfig:
    """
    Kimball-style foreign key declaration for fact tables.
    Explicitly declares which columns are surrogate key references to dimensions.
    """

    column: str  # Column name in the fact table (e.g., 'customer_sk')
    references: str | None = (
        None  # Optional: dimension table name for documentation/Bus Matrix
    )
    default_value: int = (
        -1
    )  # Default value for NULL handling (-1=Unknown, -2=N/A, -3=Error)


@dataclass
class ColumnTest:
    """
    Declarative data quality test definition for a column.
    Follows dbt-style schema.yml test syntax.
    """

    column: str  # Column name to test
    tests: list[
        str | dict[str, Any]
    ]  # e.g., ["unique", "not_null", {"accepted_values": [1,2,3]}]


@dataclass
class TableConfig:
    table_name: str
    table_type: str
    surrogate_key: str
    natural_keys: list[str]
    sources: list[SourceConfig]
    transformation_sql: str | None = None
    delete_strategy: str = "hard"
    enable_audit_columns: bool = True
    scd_type: int = 1
    track_history_columns: list[str] | None = None
    default_rows: dict[str, Any] | None = None
    surrogate_key_strategy: str = (
        "identity"  # identity (GENERATED ALWAYS AS IDENTITY), hash, sequence
    )
    schema_evolution: bool = False
    early_arriving_facts: list[dict[str, str]] | None = (
        None  # List of {dimension_table: ..., join_key: ...}
    )
    cluster_by: list[str] | None = None  # Columns for Liquid Clustering
    optimize_after_merge: bool = False  # Run OPTIMIZE after MERGE
    merge_keys: list[str] | None = (
        None  # For facts: columns used in MERGE condition (degenerate dimensions)
    )
    foreign_keys: list[ForeignKeyConfig] | None = (
        None  # Kimball: explicit FK declarations for fact tables
    )
    enable_lineage_truncation: bool = (
        False  # Enable checkpoint() for large DAG truncation (expensive)
    )
    tests: list[ColumnTest] | None = None  # Data quality tests (dbt-style)


class ConfigLoader:
    """
    Loads and validates YAML configuration files with Jinja2 templating support.
    Validates against JSON Schema for robust configuration validation.
    """

    # JSON Schema for YAML configuration validation
    CONFIG_SCHEMA = {
        "type": "object",
        "required": ["table_name", "table_type", "sources"],
        "properties": {
            "table_name": {"type": "string", "minLength": 1},
            "table_type": {"type": "string", "enum": ["dimension", "fact"]},
            "keys": {
                "type": "object",
                "properties": {
                    "surrogate_key": {"type": "string"},
                    "natural_keys": {"type": "array", "items": {"type": "string"}},
                },
            },
            "sources": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "required": ["name"],
                    "properties": {
                        "name": {"type": "string", "minLength": 1},
                        "alias": {"type": "string"},
                        "join_on": {"type": "string"},
                        "cdc_strategy": {
                            "type": "string",
                            "enum": ["cdf", "full", "timestamp"],
                        },
                        "primary_keys": {"type": "array", "items": {"type": "string"}},
                    },
                },
            },
            "transformation_sql": {"type": "string"},
            "delete_strategy": {"type": "string", "enum": ["hard", "soft"]},
            "audit_columns": {"type": "boolean"},
            "scd_type": {"type": "integer", "enum": [1, 2]},
            "track_history_columns": {"type": "array", "items": {"type": "string"}},
            "default_rows": {"type": "object"},
            "surrogate_key_strategy": {
                "type": "string",
                "enum": ["identity", "hash", "sequence"],
            },
            "schema_evolution": {"type": "boolean"},
            "early_arriving_facts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "dimension_table": {"type": "string"},
                        "fact_join_key": {"type": "string"},
                        "dimension_join_key": {"type": "string"},
                        "surrogate_key_col": {"type": "string"},
                        "surrogate_key_strategy": {"type": "string"},
                    },
                },
            },
            "cluster_by": {"type": "array", "items": {"type": "string"}},
            "optimize_after_merge": {"type": "boolean"},
            "merge_keys": {"type": "array", "items": {"type": "string"}},
            "foreign_keys": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["column"],
                    "properties": {
                        "column": {"type": "string"},
                        "references": {"type": "string"},
                        "default_value": {"type": "integer"},
                    },
                },
            },
            "tests": {
                "type": "array",
                "description": "Data quality tests (dbt-style)",
                "items": {
                    "type": "object",
                    "required": ["column", "tests"],
                    "properties": {
                        "column": {"type": "string"},
                        "tests": {
                            "type": "array",
                            "items": {
                                "oneOf": [
                                    {"type": "string"},
                                    {"type": "object"},
                                ]
                            },
                        },
                    },
                },
            },
        },
        "allOf": [
            {
                "if": {"properties": {"table_type": {"const": "dimension"}}},
                "then": {
                    "required": ["keys"],
                    "properties": {
                        "keys": {"required": ["surrogate_key", "natural_keys"]}
                    },
                },
            }
        ],
    }

    def __init__(
        self, env_vars: dict[str, str] | None = None, use_pydantic: bool = False
    ):
        """Initialize ConfigLoader.

        Args:
            env_vars: Environment variables for Jinja2 rendering.
            use_pydantic: If True, use Pydantic models for validation (stricter).
                          If False, use jsonschema (default, backward compatible).
        """
        self.env_vars = env_vars or os.environ.copy()
        self.use_pydantic = use_pydantic

    def load_config(self, file_path: str) -> TableConfig:
        """
        Reads a YAML file, renders it with Jinja2 using env_vars, validates against JSON Schema,
        and parses it into a TableConfig object.

        If use_pydantic=True was set in __init__, also validates with Pydantic models.
        """
        with open(file_path) as f:
            raw_content = f.read()

        # Render Jinja2 template
        template = Template(raw_content)
        rendered_content = template.render(self.env_vars)

        # Parse YAML
        config_dict = yaml.safe_load(rendered_content)

        # Validate against JSON Schema (fast, catches structural errors)
        try:
            validate(instance=config_dict, schema=self.CONFIG_SCHEMA)
        except ValidationError as e:
            raise ValueError(
                f"Configuration validation error in {file_path}: {e.message}"
            ) from e

        # Optional: Additional Pydantic validation (stricter business rules)
        if self.use_pydantic:
            self._validate_with_pydantic(config_dict, file_path)

        return self._parse_dict(config_dict)

    def _validate_with_pydantic(
        self, config_dict: dict[str, Any], file_path: str
    ) -> None:
        """Validate config using Pydantic models for stricter type checking."""
        from pydantic import ValidationError as PydanticValidationError
        from kimball.models import parse_table_config

        try:
            # This will raise if Pydantic validators fail
            parse_table_config(config_dict.copy())
        except PydanticValidationError as e:
            errors = "; ".join(f"{err['loc']}: {err['msg']}" for err in e.errors())
            raise ValueError(
                f"Pydantic validation error in {file_path}: {errors}"
            ) from e

    def load_config_pydantic(self, file_path: str):
        """
        Load config and return Pydantic model (DimensionConfig or FactConfig).

        Use this method when you want the full Pydantic model with its validators
        instead of the TableConfig dataclass.

        Returns:
            DimensionConfig or FactConfig Pydantic model.
        """
        from kimball.models import parse_table_config

        with open(file_path) as f:
            raw_content = f.read()

        template = Template(raw_content)
        rendered_content = template.render(self.env_vars)
        config_dict = yaml.safe_load(rendered_content)

        return parse_table_config(config_dict)

    def _parse_dict(self, config: dict[str, Any]) -> TableConfig:
        """Converts validated dictionary to TableConfig with Kimball-specific business logic."""
        try:
            sources = [
                SourceConfig(
                    name=s["name"],
                    alias=s.get("alias", s["name"].split(".")[-1]),
                    join_on=s.get("join_on"),
                    cdc_strategy=s.get("cdc_strategy", "cdf"),
                    primary_keys=s.get("primary_keys"),
                    freshness=self._parse_freshness(s.get("freshness")),
                )
                for s in config.get("sources", [])
            ]

            table_type = config.get("table_type", "fact")
            keys_cfg = config.get("keys", {}) or {}

            # Kimball-specific business logic validation (beyond schema validation)
            if table_type == "dimension":
                if not keys_cfg.get("surrogate_key"):
                    raise ValueError("Dimensions require keys.surrogate_key")
                if not keys_cfg.get("natural_keys"):
                    raise ValueError("Dimensions require keys.natural_keys")

            # For facts, use merge_keys (degenerate dimension columns for MERGE condition)
            merge_keys = config.get("merge_keys", [])

            # Parse foreign_keys for fact tables (Kimball-proper FK declarations)
            foreign_keys_raw = config.get("foreign_keys", []) or []
            foreign_keys = [
                ForeignKeyConfig(
                    column=fk["column"],
                    references=fk.get("references"),
                    default_value=fk.get("default_value", -1),
                )
                for fk in foreign_keys_raw
            ]

            return TableConfig(
                table_name=config["table_name"],
                table_type=table_type,
                surrogate_key=keys_cfg.get("surrogate_key"),
                natural_keys=keys_cfg.get("natural_keys", []),
                sources=sources,
                transformation_sql=config.get("transformation_sql"),
                delete_strategy=config.get("delete_strategy", "hard"),
                enable_audit_columns=config.get("audit_columns", True),
                scd_type=config.get("scd_type", 1),
                track_history_columns=config.get("track_history_columns"),
                default_rows=config.get("default_rows"),
                surrogate_key_strategy=config.get("surrogate_key_strategy", "identity"),
                schema_evolution=config.get("schema_evolution", False),
                early_arriving_facts=config.get("early_arriving_facts"),
                cluster_by=config.get("cluster_by"),
                optimize_after_merge=config.get("optimize_after_merge", False),
                merge_keys=merge_keys,
                foreign_keys=foreign_keys if foreign_keys else None,
                tests=self._parse_tests(config.get("tests")),
            )
        except KeyError as e:
            raise ValueError(f"Missing required configuration field: {e}") from e

    def _parse_tests(
        self, tests_raw: list[dict[str, Any]] | None
    ) -> list[ColumnTest] | None:
        """Parse tests block into ColumnTest objects."""
        if not tests_raw:
            return None
        return [
            ColumnTest(
                column=t["column"],
                tests=t.get("tests", []),
            )
            for t in tests_raw
        ]

    def _parse_freshness(
        self, freshness_raw: dict[str, Any] | None
    ) -> FreshnessConfig | None:
        """Parse freshness config from source definition."""
        if not freshness_raw:
            return None
        return FreshnessConfig(
            warn_after_hours=freshness_raw.get("warn_after_hours"),
            error_after_hours=freshness_raw.get("error_after_hours"),
        )
