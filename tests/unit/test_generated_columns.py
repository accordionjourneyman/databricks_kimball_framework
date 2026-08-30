"""Tests for GENERATED ALWAYS AS column support."""

import pytest

from kimball.common.config import TableConfig


class TestGeneratedColumnsConfig:
    def test_generated_columns_default_none(self):
        config = TableConfig(
            table_name="dim_test",
            table_type="dimension",
            surrogate_key="sk",
            natural_keys=["nk"],
            sources=[{"name": "src", "alias": "s"}],
        )
        assert config.generated_columns is None

    def test_generated_columns_populated(self):
        config = TableConfig(
            table_name="dim_test",
            table_type="dimension",
            surrogate_key="sk",
            natural_keys=["nk"],
            sources=[{"name": "src", "alias": "s"}],
            generated_columns={
                "creation_day": {
                    "expression": "CAST(creation_date AS DATE)",
                    "data_type": "DATE",
                }
            },
        )
        assert (
            config.generated_columns["creation_day"].expression
            == "CAST(creation_date AS DATE)"
        )
        assert config.generated_columns["creation_day"].data_type == "DATE"

    def test_generated_columns_with_partition(self):
        config = TableConfig(
            table_name="dim_test",
            table_type="dimension",
            surrogate_key="sk",
            natural_keys=["nk"],
            sources=[{"name": "src", "alias": "s"}],
            generated_columns={
                "event_month": {
                    "expression": "DATE_TRUNC('month', event_date)",
                    "data_type": "TIMESTAMP",
                }
            },
        )
        assert "event_month" in config.generated_columns


class TestGeneratedColumnsDDL:
    def test_create_table_with_generated_column(self, spark):
        """A table with generated_columns must include GENERATED ALWAYS AS in DDL."""
        spark.sql("CREATE DATABASE IF NOT EXISTS test_gc_db")
        try:
            from kimball.processing.table_creator import TableCreator

            tc = TableCreator()
            schema_df = spark.createDataFrame([], "id BIGINT, creation_date TIMESTAMP")
            try:
                tc.create_table_with_clustering(
                    table_name="test_gc_db.test_gc_table",
                    schema_df=schema_df,
                    config={
                        "generated_columns": {
                            "creation_day": {
                                "expression": "CAST(creation_date AS DATE)",
                                "data_type": "DATE",
                            }
                        }
                    },
                )
            except Exception as exc:  # noqa: BLE001
                if "does not support generated columns" in str(exc):
                    pytest.skip("Delta/Spark build does not support generated columns")
                raise

            # Verify table exists and has the generated column
            desc = spark.sql("DESCRIBE TABLE test_gc_db.test_gc_table").collect()
            col_names = [row.col_name for row in desc]
            assert "creation_day" in col_names
        finally:
            spark.sql("DROP DATABASE IF EXISTS test_gc_db CASCADE")
