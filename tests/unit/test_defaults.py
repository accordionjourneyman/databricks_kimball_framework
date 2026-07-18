from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from kimball.processing.defaults import seed_default_rows


@pytest.fixture
def spark_mock():
    return MagicMock()


class TestSeedDefaultRows:
    def test_skips_when_table_not_exists(self, spark_mock):
        spark_mock.catalog.tableExists.return_value = False
        with patch("kimball.processing.defaults.get_spark", return_value=spark_mock):
            seed_default_rows("test_table", MagicMock(), "sk")
        spark_mock.sql.assert_not_called()

    def test_seeds_four_default_rows(self, spark_mock):
        spark_mock.catalog.tableExists.return_value = True
        with patch("kimball.processing.defaults.get_spark", return_value=spark_mock):
            schema = StructType(
                [
                    StructField("sk", IntegerType(), False),
                    StructField("name", StringType(), True),
                ]
            )
            seed_default_rows("test_table", schema, "sk")
        assert spark_mock.sql.call_count == 4
        insert_calls = [c.args[0] for c in spark_mock.sql.call_args_list]
        for i, sk in enumerate([-1, -2, -3, -4]):
            assert "INSERT INTO test_table" in insert_calls[i]
            assert "WHERE NOT EXISTS" in insert_calls[i]
            assert f"`sk` = {sk}" in insert_calls[i]

    def test_handles_system_columns(self, spark_mock):
        spark_mock.catalog.tableExists.return_value = True
        with patch("kimball.processing.defaults.get_spark", return_value=spark_mock):
            schema = StructType(
                [
                    StructField("sk", IntegerType(), False),
                    StructField("__is_current", BooleanType(), False),
                    StructField("__valid_from", TimestampType(), False),
                    StructField("__valid_to", TimestampType(), True),
                ]
            )
            seed_default_rows("test_table", schema, "sk", include_history_fields=True)
        assert spark_mock.sql.call_count == 4
        first_sql = spark_mock.sql.call_args_list[0].args[0]
        assert "TRUE" in first_sql
        assert "__is_current" in first_sql
        assert "__valid_from" in first_sql
        assert "__valid_to" in first_sql

    def test_handles_non_nullable_system_columns(self, spark_mock):
        spark_mock.catalog.tableExists.return_value = True
        with patch("kimball.processing.defaults.get_spark", return_value=spark_mock):
            schema = StructType(
                [
                    StructField("sk", IntegerType(), False),
                    StructField("__count", IntegerType(), False),
                    StructField("__amount", DecimalType(10, 2), False),
                    StructField("__ratio", DoubleType(), False),
                    StructField("__flag", BooleanType(), False),
                    StructField("__other", StringType(), False),
                ]
            )
            seed_default_rows("test_table", schema, "sk")
        assert spark_mock.sql.call_count == 4
        first_sql = spark_mock.sql.call_args_list[0].args[0]
        assert "`__count`" in first_sql
        assert "`__amount`" in first_sql
        assert "`__ratio`" in first_sql
        assert "FALSE" in first_sql
        assert "'Missing'" in first_sql

    def test_uses_default_values(self, spark_mock):
        spark_mock.catalog.tableExists.return_value = True
        with patch("kimball.processing.defaults.get_spark", return_value=spark_mock):
            schema = StructType(
                [
                    StructField("sk", IntegerType(), False),
                    StructField("name", StringType(), True),
                ]
            )
            seed_default_rows(
                "test_table", schema, "sk", default_values={"name": "Default Name"}
            )
        first_sql = spark_mock.sql.call_args_list[0].args[0]
        assert "'Default Name'" in first_sql

    def test_handles_various_data_types(self, spark_mock):
        spark_mock.catalog.tableExists.return_value = True
        with patch("kimball.processing.defaults.get_spark", return_value=spark_mock):
            schema = StructType(
                [
                    StructField("sk", IntegerType(), False),
                    StructField("ts_col", TimestampType(), True),
                    StructField("date_col", DateType(), True),
                    StructField("float_col", FloatType(), True),
                ]
            )
            seed_default_rows("test_table", schema, "sk")
        first_sql = spark_mock.sql.call_args_list[0].args[0]
        assert "ts_col" in first_sql
        assert "date_col" in first_sql
        assert "float_col" in first_sql
        assert "NULL" not in first_sql

    def test_scd7_defaults_seed_same_reserved_sk_and_dk(self, spark_mock):
        spark_mock.catalog.tableExists.return_value = True
        schema = StructType(
            [
                StructField("customer_sk", IntegerType(), False),
                StructField("customer_dk", IntegerType(), False),
                StructField("__member_status", StringType(), False),
                StructField("__key_origin", StringType(), False),
                StructField("name", StringType(), False),
            ]
        )
        with patch("kimball.processing.defaults.get_spark", return_value=spark_mock):
            seed_default_rows(
                "test_table",
                schema,
                "customer_sk",
                durable_key="customer_dk",
            )

        first_sql = spark_mock.sql.call_args_list[0].args[0]
        assert "-1, -1" in first_sql
        assert "'MISSING'" in first_sql
        assert "'default'" in first_sql
