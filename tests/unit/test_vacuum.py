"""Tests for VACUUM scheduling support in table_ops and config."""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from kimball.common.config import TableConfig

# --- Config tests ---


class TestVacuumConfig:
    def test_vacuum_defaults(self):
        config = TableConfig(
            table_name="dim_test",
            table_type="dimension",
            surrogate_key="sk",
            natural_keys=["nk"],
            sources=[{"name": "src", "alias": "s"}],
        )
        assert config.vacuum_after_merge is False
        assert config.vacuum_retention_hours == 168

    def test_vacuum_after_merge_enabled(self):
        config = TableConfig(
            table_name="dim_test",
            table_type="dimension",
            surrogate_key="sk",
            natural_keys=["nk"],
            sources=[{"name": "src", "alias": "s"}],
            vacuum_after_merge=True,
        )
        assert config.vacuum_after_merge is True

    def test_vacuum_retention_hours_must_be_at_least_seven_days(self):
        with pytest.raises(ValidationError):
            TableConfig(
                table_name="dim_test",
                table_type="dimension",
                surrogate_key="sk",
                natural_keys=["nk"],
                sources=[{"name": "src", "alias": "s"}],
                vacuum_after_merge=True,
                vacuum_retention_hours=72,
            )


# --- table_ops.vacuum_table tests ---


class TestVacuumTable:
    @patch("kimball.processing.table_ops.get_spark")
    def test_vacuum_calls_sql(self, mock_get_spark):
        from kimball.processing.table_ops import vacuum_table

        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark

        vacuum_table("catalog.schema.my_table")

        mock_spark.sql.assert_called_once_with(
            "VACUUM `catalog`.`schema`.`my_table` RETAIN 168 HOURS"
        )

    def test_vacuum_rejects_unsafe_retention(self):
        from kimball.processing.table_ops import vacuum_table

        with pytest.raises(ValueError, match="at least 168"):
            vacuum_table("my_table", retention_hours=48)

    @patch("kimball.processing.table_ops.get_spark")
    def test_vacuum_simple_table_name(self, mock_get_spark):
        from kimball.processing.table_ops import vacuum_table

        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark

        vacuum_table("dim_customer")

        mock_spark.sql.assert_called_once_with("VACUUM `dim_customer` RETAIN 168 HOURS")


# --- Merger facade test ---


class TestVacuumMergerFacade:
    def test_vacuum_table_exported_from_merger(self):
        from kimball.processing.merger import vacuum_table

        assert callable(vacuum_table)
