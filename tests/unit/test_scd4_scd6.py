"""
Unit tests for SCD4 and SCD6 strategies.

Tests the new SCD Type 4 (EAV History) and SCD Type 6 (Current Columns) implementations.
"""

import pytest

from kimball.processing.merger import (
    _STRATEGY_REGISTRY,
    SCD4Strategy,
    SCD6Strategy,
    create_merge_strategy,
)


class TestStrategyRegistry:
    """Test the strategy registry pattern."""

    def test_scd1_registered(self):
        """SCD1 should be registered in the registry."""
        assert 1 in _STRATEGY_REGISTRY

    def test_scd2_registered(self):
        """SCD2 should be registered in the registry."""
        assert 2 in _STRATEGY_REGISTRY

    def test_scd4_registered(self):
        """SCD4 should be registered in the registry."""
        assert 4 in _STRATEGY_REGISTRY

    def test_scd6_registered(self):
        """SCD6 should be registered in the registry."""
        assert 6 in _STRATEGY_REGISTRY


class TestCreateMergeStrategy:
    """Test the factory function with new SCD types."""

    def test_create_scd4_requires_history_table(self):
        """SCD4 raises error if history_table not provided."""
        with pytest.raises(ValueError, match="requires history_table"):
            create_merge_strategy(
                scd_type=4,
                target_table_name="dim_product",
                join_keys=["product_id"],
            )

    def test_create_scd4_with_history_table(self):
        """SCD4 creates successfully with history_table."""
        strategy = create_merge_strategy(
            scd_type=4,
            target_table_name="dim_product",
            join_keys=["product_id"],
            history_table="dim_product_history",
        )
        assert isinstance(strategy, SCD4Strategy)
        assert strategy.history_table_name == "dim_product_history"

    def test_create_scd6_requires_current_value_columns(self):
        """SCD6 raises error if current_value_columns not provided."""
        with pytest.raises(ValueError, match="requires current_value_columns"):
            create_merge_strategy(
                scd_type=6,
                target_table_name="dim_customer",
                join_keys=["customer_id"],
            )

    def test_create_scd6_with_current_value_columns(self):
        """SCD6 creates successfully with current_value_columns."""
        strategy = create_merge_strategy(
            scd_type=6,
            target_table_name="dim_customer",
            join_keys=["customer_id"],
            current_value_columns=["city", "status"],
        )
        assert isinstance(strategy, SCD6Strategy)
        assert strategy.current_columns == ["city", "status"]


class TestSCD4Strategy:
    """Test SCD4Strategy initialization and configuration."""

    def test_init_composes_scd1(self):
        """SCD4 should internally compose an SCD1 strategy."""
        strategy = SCD4Strategy(
            target_table_name="dim_product",
            history_table_name="dim_product_history",
            join_keys=["product_id"],
            track_history_columns=["*"],
        )
        assert strategy.scd1 is not None
        assert strategy.scd1.target_table_name == "dim_product"

    def test_init_sets_history_table(self):
        """SCD4 should store history table name."""
        strategy = SCD4Strategy(
            target_table_name="dim_product",
            history_table_name="dim_product_history",
            join_keys=["product_id"],
            track_history_columns=["name", "price"],
        )
        assert strategy.history_table_name == "dim_product_history"
        assert strategy.track_columns == ["name", "price"]

    def test_init_defaults_effective_at(self):
        """SCD4 should default effective_at to __etl_processed_at."""
        strategy = SCD4Strategy(
            target_table_name="dim_product",
            history_table_name="dim_product_history",
            join_keys=["product_id"],
            track_history_columns=["*"],
        )
        assert strategy.effective_at_column == "__etl_processed_at"


class TestSCD6Strategy:
    """Test SCD6Strategy initialization and configuration."""

    def test_init_stores_current_columns(self):
        """SCD6 should store current value columns."""
        strategy = SCD6Strategy(
            target_table_name="dim_customer",
            join_keys=["customer_id"],
            track_history_columns=["name", "city", "status"],
            current_value_columns=["city", "status"],
        )
        assert strategy.current_columns == ["city", "status"]
        assert strategy.track_history_columns == ["name", "city", "status"]

    def test_init_defaults_effective_at(self):
        """SCD6 should default effective_at to __etl_processed_at."""
        strategy = SCD6Strategy(
            target_table_name="dim_customer",
            join_keys=["customer_id"],
            track_history_columns=["city"],
            current_value_columns=["city"],
        )
        assert strategy.effective_at_column == "__etl_processed_at"
