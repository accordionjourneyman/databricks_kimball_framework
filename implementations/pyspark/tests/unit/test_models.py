"""
Unit tests for Pydantic models.

Tests validation rules for DimensionConfig, FactConfig, and related models.
"""

import pytest
from pydantic import ValidationError

from kimball.models import (
    CDCStrategy,
    ColumnTestModel,
    DimensionConfig,
    FactConfig,
    ForeignKeyConfigModel,
    FreshnessConfigModel,
    SCDType,
    SourceConfigModel,
    SurrogateKeyStrategy,
    parse_table_config,
)


class TestSourceConfigModel:
    """Tests for SourceConfigModel."""

    def test_valid_source(self):
        """Valid source config should pass."""
        source = SourceConfigModel(name="silver.customers")
        assert source.name == "silver.customers"
        assert source.cdc_strategy == CDCStrategy.CDF

    def test_source_with_all_fields(self):
        """Source with all fields should parse correctly."""
        source = SourceConfigModel(
            name="silver.orders",
            alias="o",
            join_on="c.customer_id = o.customer_id",
            cdc_strategy=CDCStrategy.FULL,
            primary_keys=["order_id"],
            freshness=FreshnessConfigModel(warn_after_hours=12, error_after_hours=24),
        )
        assert source.alias == "o"
        assert source.cdc_strategy == CDCStrategy.FULL
        assert source.freshness.warn_after_hours == 12

    def test_empty_name_fails(self):
        """Empty source name should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            SourceConfigModel(name="")
        assert (
            "too_short" in str(exc_info.value).lower()
            or "at least" in str(exc_info.value).lower()
        )


class TestFreshnessConfigModel:
    """Tests for FreshnessConfigModel."""

    def test_valid_freshness(self):
        """Valid freshness thresholds should pass."""
        freshness = FreshnessConfigModel(warn_after_hours=12, error_after_hours=24)
        assert freshness.warn_after_hours == 12
        assert freshness.error_after_hours == 24

    def test_error_must_be_greater_than_warn(self):
        """Error threshold must be greater than warn threshold."""
        with pytest.raises(ValidationError) as exc_info:
            FreshnessConfigModel(warn_after_hours=24, error_after_hours=12)
        assert "greater than" in str(exc_info.value).lower()


class TestColumnTestModel:
    """Tests for ColumnTestModel."""

    def test_valid_string_tests(self):
        """String-based tests should pass."""
        test = ColumnTestModel(column="id", tests=["unique", "not_null"])
        assert test.column == "id"
        assert "unique" in test.tests

    def test_valid_dict_tests(self):
        """Dict-based tests should pass."""
        test = ColumnTestModel(
            column="status",
            tests=[{"accepted_values": ["active", "inactive"]}],
        )
        assert isinstance(test.tests[0], dict)

    def test_invalid_test_name_fails(self):
        """Invalid test name should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            ColumnTestModel(column="id", tests=["invalid_test_name"])
        assert "Unknown test" in str(exc_info.value)

    def test_empty_tests_fails(self):
        """Empty tests list should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            ColumnTestModel(column="id", tests=[])
        assert (
            "too_short" in str(exc_info.value).lower()
            or "at least" in str(exc_info.value).lower()
        )


class TestDimensionConfig:
    """Tests for DimensionConfig."""

    def test_valid_dimension(self):
        """Valid dimension config should pass."""
        dim = DimensionConfig(
            table_name="gold.dim_customer",
            surrogate_key="customer_sk",
            natural_keys=["customer_id"],
            sources=[SourceConfigModel(name="silver.customers")],
        )
        assert dim.table_name == "gold.dim_customer"
        assert dim.table_type == "dimension"
        assert dim.scd_type == SCDType.TYPE_1

    def test_dimension_with_scd2(self):
        """SCD2 dimension should parse correctly."""
        dim = DimensionConfig(
            table_name="gold.dim_customer",
            surrogate_key="customer_sk",
            natural_keys=["customer_id"],
            scd_type=SCDType.TYPE_2,
            track_history_columns=["name", "email"],
            sources=[SourceConfigModel(name="silver.customers")],
        )
        assert dim.scd_type == SCDType.TYPE_2
        assert dim.track_history_columns == ["name", "email"]

    def test_empty_natural_keys_fails(self):
        """Empty natural_keys should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            DimensionConfig(
                table_name="gold.dim_test",
                surrogate_key="test_sk",
                natural_keys=[],
                sources=[SourceConfigModel(name="silver.test")],
            )
        # Pydantic reports this as "too_short" or "at least 1 item"
        error_str = str(exc_info.value).lower()
        assert "too_short" in error_str or "at least" in error_str

    def test_missing_surrogate_key_fails(self):
        """Missing surrogate_key should fail validation."""
        with pytest.raises(ValidationError):
            DimensionConfig(
                table_name="gold.dim_test",
                natural_keys=["id"],
                sources=[SourceConfigModel(name="silver.test")],
            )


class TestFactConfig:
    """Tests for FactConfig."""

    def test_valid_fact(self):
        """Valid fact config should pass."""
        fact = FactConfig(
            table_name="gold.fact_sales",
            merge_keys=["order_item_id"],
            sources=[SourceConfigModel(name="silver.order_items")],
        )
        assert fact.table_name == "gold.fact_sales"
        assert fact.table_type == "fact"

    def test_fact_with_foreign_keys(self):
        """Fact with foreign keys should parse correctly."""
        fact = FactConfig(
            table_name="gold.fact_sales",
            merge_keys=["order_item_id"],
            foreign_keys=[
                ForeignKeyConfigModel(
                    column="customer_sk",
                    references="gold.dim_customer",
                    default_value=-1,
                ),
            ],
            sources=[SourceConfigModel(name="silver.order_items")],
        )
        assert len(fact.foreign_keys) == 1
        assert fact.foreign_keys[0].default_value == -1

    def test_empty_merge_keys_fails(self):
        """Empty merge_keys should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            FactConfig(
                table_name="gold.fact_test",
                merge_keys=[],
                sources=[SourceConfigModel(name="silver.test")],
            )
        # Pydantic reports this as "too_short" or "at least 1 item"
        error_str = str(exc_info.value).lower()
        assert "too_short" in error_str or "at least" in error_str


class TestParseTableConfig:
    """Tests for parse_table_config function."""

    def test_parse_dimension_dict(self):
        """Should parse dimension dict correctly."""
        config_dict = {
            "table_name": "gold.dim_customer",
            "table_type": "dimension",
            "keys": {
                "surrogate_key": "customer_sk",
                "natural_keys": ["customer_id"],
            },
            "sources": [{"name": "silver.customers"}],
        }
        config = parse_table_config(config_dict)
        assert isinstance(config, DimensionConfig)
        assert config.surrogate_key == "customer_sk"

    def test_parse_fact_dict(self):
        """Should parse fact dict correctly."""
        config_dict = {
            "table_name": "gold.fact_sales",
            "table_type": "fact",
            "merge_keys": ["order_item_id"],
            "sources": [{"name": "silver.order_items"}],
        }
        config = parse_table_config(config_dict)
        assert isinstance(config, FactConfig)
        assert config.merge_keys == ["order_item_id"]


class TestEnums:
    """Tests for enum values."""

    def test_cdc_strategy_values(self):
        """CDC strategy enum should have expected values."""
        assert CDCStrategy.CDF.value == "cdf"
        assert CDCStrategy.FULL.value == "full"
        assert CDCStrategy.TIMESTAMP.value == "timestamp"

    def test_scd_type_values(self):
        """SCD type enum should have expected values."""
        assert SCDType.TYPE_1.value == 1
        assert SCDType.TYPE_2.value == 2

    def test_surrogate_key_strategy_values(self):
        """Surrogate key strategy enum should have expected values."""
        assert SurrogateKeyStrategy.IDENTITY.value == "identity"
        assert SurrogateKeyStrategy.HASH.value == "hash"
        assert SurrogateKeyStrategy.SEQUENCE.value == "sequence"
