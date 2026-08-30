"""Tests for UC-native ROW FILTER and column MASK support."""

from unittest.mock import MagicMock, patch

from kimball.common.config import RowFilterConfig, TableConfig
from kimball.common.runtime_policy import RuntimePolicy


class TestRowFilterConfig:
    def test_row_filter_default_none(self):
        config = TableConfig(
            table_name="dim_test",
            table_type="dimension",
            surrogate_key="sk",
            natural_keys=["nk"],
            sources=[{"name": "src", "alias": "s"}],
        )
        assert config.row_filter is None

    def test_row_filter_populated(self):
        config = TableConfig(
            table_name="dim_test",
            table_type="dimension",
            surrogate_key="sk",
            natural_keys=["nk"],
            sources=[{"name": "src", "alias": "s"}],
            row_filter=RowFilterConfig(
                function_name="region_filter",
                function_body="is_account_group_member('admin') OR region_param = 'US'",
                column="country",
            ),
        )
        assert config.row_filter.function_name == "region_filter"
        assert config.row_filter.column == "country"
        assert config.row_filter.grant_to is None

    def test_row_filter_with_grant_to(self):
        config = TableConfig(
            table_name="dim_test",
            table_type="dimension",
            surrogate_key="sk",
            natural_keys=["nk"],
            sources=[{"name": "src", "alias": "s"}],
            row_filter=RowFilterConfig(
                function_name="region_filter",
                function_body="region_param = 'US'",
                column="country",
                grant_to=["analysts", "data_engineers"],
            ),
        )
        assert config.row_filter.grant_to == ["analysts", "data_engineers"]


class TestRowFilterDDL:
    @patch("kimball.processing.table_creator.get_spark")
    @patch(
        "kimball.processing.table_creator.get_runtime_policy",
        return_value=RuntimePolicy(is_databricks=True),
    )
    def test_apply_row_filter_creates_function_and_sets_filter(
        self, mock_rp, mock_get_spark
    ):
        from kimball.processing.table_creator import TableCreator

        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark

        tc = TableCreator()
        rf_config = {
            "function_name": "region_filter",
            "function_body": "region_param = 'US'",
            "column": "country",
        }
        tc._apply_row_filter("my_table", rf_config)

        calls = [str(c[0][0]) for c in mock_spark.sql.call_args_list]
        # Should create function
        create_calls = [c for c in calls if "CREATE OR REPLACE FUNCTION" in c]
        assert len(create_calls) == 1
        assert "region_filter" in create_calls[0]
        assert "region_param" in create_calls[0]
        assert "region_param = 'US'" in create_calls[0]

        # Should set row filter
        filter_calls = [c for c in calls if "SET ROW FILTER" in c]
        assert len(filter_calls) == 1
        assert "region_filter" in filter_calls[0]
        assert "country" in filter_calls[0]

    @patch("kimball.processing.table_creator.get_spark")
    @patch(
        "kimball.processing.table_creator.get_runtime_policy",
        return_value=RuntimePolicy(is_databricks=True),
    )
    def test_apply_row_filter_with_grant(self, mock_rp, mock_get_spark):
        from kimball.processing.table_creator import TableCreator

        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark

        tc = TableCreator()
        rf_config = {
            "function_name": "region_filter",
            "function_body": "region_param = 'US'",
            "column": "country",
            "grant_to": ["analysts"],
        }
        tc._apply_row_filter("my_table", rf_config)

        calls = [str(c[0][0]) for c in mock_spark.sql.call_args_list]
        grant_calls = [c for c in calls if "GRANT" in c and "FUNCTION" in c]
        assert len(grant_calls) == 1
        assert "analysts" in grant_calls[0]
