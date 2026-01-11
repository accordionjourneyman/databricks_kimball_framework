"""
Pydantic Models for Kimball Framework Configuration.

Provides strict validation for table definitions with:
- Type enforcement at config load time
- Field validators for Kimball-specific business rules
- Clear error messages for invalid configurations

Usage:
    from kimball.models import TableConfigModel, DimensionConfig, FactConfig

    # Direct construction
    config = DimensionConfig(
        table_name="gold.dim_customer",
        surrogate_key="customer_sk",
        natural_keys=["customer_id"],
        sources=[SourceConfigModel(name="silver.customers")]
    )

    # From dict (YAML parsed)
    config = TableConfigModel.model_validate(yaml_dict)
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


class CDCStrategy(str, Enum):
    """CDC strategy for source loading."""

    CDF = "cdf"
    FULL = "full"
    TIMESTAMP = "timestamp"


class SCDType(int, Enum):
    """Slowly Changing Dimension type."""

    TYPE_1 = 1
    TYPE_2 = 2


class DeleteStrategy(str, Enum):
    """Delete handling strategy."""

    HARD = "hard"
    SOFT = "soft"


class SurrogateKeyStrategy(str, Enum):
    """Strategy for generating surrogate keys."""

    IDENTITY = "identity"
    HASH = "hash"
    SEQUENCE = "sequence"


class FreshnessConfigModel(BaseModel):
    """Source freshness thresholds for alerting."""

    warn_after_hours: int | None = None
    error_after_hours: int | None = None

    @model_validator(mode="after")
    def validate_thresholds(self) -> "FreshnessConfigModel":
        """Ensure error threshold is greater than warn threshold."""
        if self.warn_after_hours and self.error_after_hours:
            if self.error_after_hours <= self.warn_after_hours:
                raise ValueError(
                    "error_after_hours must be greater than warn_after_hours"
                )
        return self


class SourceConfigModel(BaseModel):
    """Configuration for a source table."""

    name: str = Field(
        ..., min_length=1, description="Fully qualified source table name"
    )
    alias: str | None = Field(default=None, description="Alias for SQL temp view")
    join_on: str | None = Field(
        default=None, description="JOIN condition for multi-source"
    )
    cdc_strategy: CDCStrategy = Field(
        default=CDCStrategy.CDF,
        description="CDC loading strategy: cdf, full, or timestamp",
    )
    primary_keys: list[str] | None = Field(
        default=None, description="Keys for CDF deduplication"
    )
    freshness: FreshnessConfigModel | None = Field(
        default=None, description="Freshness thresholds"
    )

    @field_validator("alias", mode="before")
    @classmethod
    def default_alias_from_name(cls, v: str | None, info) -> str:
        """Default alias to table name suffix if not provided."""
        if v is None and "name" in info.data:
            return info.data["name"].split(".")[-1]
        return v


class ForeignKeyConfigModel(BaseModel):
    """Kimball-style foreign key declaration for fact tables."""

    column: str = Field(..., min_length=1, description="FK column in fact table")
    references: str | None = Field(
        default=None, description="Referenced dimension table"
    )
    default_value: int = Field(
        default=-1,
        description="Default for NULL handling (-1=Unknown, -2=N/A, -3=Error)",
    )


class ColumnTestModel(BaseModel):
    """Declarative data quality test for a column."""

    column: str = Field(..., min_length=1, description="Column to test")
    tests: list[str | dict[str, Any]] = Field(
        ..., min_length=1, description="List of tests to run"
    )

    @field_validator("tests")
    @classmethod
    def validate_test_names(cls, v: list) -> list:
        """Validate that test names are recognized."""
        valid_tests = {
            "unique",
            "not_null",
            "accepted_values",
            "relationships",
            "expression",
        }
        for test in v:
            if isinstance(test, str) and test not in valid_tests:
                raise ValueError(f"Unknown test '{test}'. Valid tests: {valid_tests}")
            elif isinstance(test, dict):
                test_type = list(test.keys())[0]
                if test_type not in valid_tests:
                    raise ValueError(
                        f"Unknown test type '{test_type}'. Valid tests: {valid_tests}"
                    )
        return v


class EarlyArrivingFactConfigModel(BaseModel):
    """Configuration for early arriving fact handling (skeleton generation)."""

    dimension_table: str = Field(..., description="Target dimension table")
    fact_join_key: str = Field(..., description="Join key in fact table")
    dimension_join_key: str = Field(..., description="Join key in dimension table")
    surrogate_key_col: str = Field(
        default="surrogate_key", description="Surrogate key column name"
    )
    surrogate_key_strategy: SurrogateKeyStrategy = Field(
        default=SurrogateKeyStrategy.IDENTITY
    )


class TableConfigModel(BaseModel):
    """
    Base configuration for all Kimball table types.

    Use DimensionConfig or FactConfig for type-specific validation.
    """

    table_name: str = Field(
        ..., min_length=1, description="Fully qualified target table"
    )
    table_type: Literal["dimension", "fact"] = Field(
        ..., description="Kimball table type"
    )
    sources: list[SourceConfigModel] = Field(
        ..., min_length=1, description="Source table configurations"
    )

    # Optional fields
    transformation_sql: str | None = Field(
        default=None, description="SQL transformation query"
    )
    delete_strategy: DeleteStrategy = Field(default=DeleteStrategy.HARD)
    enable_audit_columns: bool = Field(default=True, alias="audit_columns")
    schema_evolution: bool = Field(default=False)
    cluster_by: list[str] | None = Field(
        default=None, description="Columns for Liquid Clustering"
    )
    optimize_after_merge: bool = Field(default=False)
    enable_lineage_truncation: bool = Field(default=False)

    # Data quality tests
    tests: list[ColumnTestModel] | None = None

    class Config:
        populate_by_name = True  # Allow both field name and alias


class DimensionConfig(TableConfigModel):
    """
    Configuration for Dimension tables.

    Enforces Kimball rules:
    - Must have surrogate_key
    - Must have at least one natural_key
    - SCD Type 1 or 2
    """

    table_type: Literal["dimension"] = "dimension"
    surrogate_key: str = Field(
        ..., min_length=1, description="Surrogate key column name"
    )
    natural_keys: list[str] = Field(
        ..., min_length=1, description="Business/natural key columns"
    )
    scd_type: SCDType = Field(default=SCDType.TYPE_1)
    track_history_columns: list[str] | None = Field(
        default=None, description="Columns to track for SCD2 (default: all)"
    )
    default_rows: dict[str, Any] | None = Field(
        default=None, description="Default dimension members (Unknown, N/A)"
    )
    surrogate_key_strategy: SurrogateKeyStrategy = Field(
        default=SurrogateKeyStrategy.IDENTITY
    )

    @field_validator("natural_keys")
    @classmethod
    def natural_keys_not_empty(cls, v: list[str]) -> list[str]:
        """Dimensions must have at least one natural key."""
        if not v:
            raise ValueError("Dimensions must have at least one natural_key")
        return v


class FactConfig(TableConfigModel):
    """
    Configuration for Fact tables.

    Enforces Kimball rules:
    - Must have merge_keys (degenerate dimensions)
    - Foreign keys reference dimensions
    - No surrogate key (facts use composite keys)
    """

    table_type: Literal["fact"] = "fact"
    merge_keys: list[str] = Field(
        ..., min_length=1, description="Degenerate dimension columns for MERGE"
    )
    foreign_keys: list[ForeignKeyConfigModel] | None = Field(
        default=None, description="FK references to dimensions"
    )
    early_arriving_facts: list[EarlyArrivingFactConfigModel] | None = Field(
        default=None, description="Skeleton generation config"
    )

    @field_validator("merge_keys")
    @classmethod
    def merge_keys_not_empty(cls, v: list[str]) -> list[str]:
        """Facts must have at least one merge key."""
        if not v:
            raise ValueError("Facts must have at least one merge_key")
        return v


def parse_table_config(config_dict: dict[str, Any]) -> DimensionConfig | FactConfig:
    """
    Parse a config dictionary into the appropriate Pydantic model.

    Args:
        config_dict: Parsed YAML configuration.

    Returns:
        DimensionConfig or FactConfig based on table_type.

    Raises:
        ValidationError: If config is invalid.
    """
    table_type = config_dict.get("table_type", "fact")

    # Handle nested keys structure for dimensions
    if table_type == "dimension":
        keys = config_dict.pop("keys", {}) or {}
        config_dict["surrogate_key"] = keys.get("surrogate_key")
        config_dict["natural_keys"] = keys.get("natural_keys", [])
        return DimensionConfig.model_validate(config_dict)
    else:
        return FactConfig.model_validate(config_dict)
