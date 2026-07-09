"""Unit tests for TableCreator."""

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark_mock():
    mock = MagicMock(spec=SparkSession)
    mock.catalog.tableExists.return_value = False
    return mock


@pytest.fixture
def table_creator(spark_mock):
    with patch("kimball.processing.table_creator.get_spark", return_value=spark_mock):
        from kimball.processing.table_creator import TableCreator

        return TableCreator()


class TestIsValidIdentifier:
    def test_valid_identifiers(self):
        from kimball.processing.table_creator import _is_valid_identifier

        assert _is_valid_identifier("col1") is True
        assert _is_valid_identifier("_col") is True
        assert _is_valid_identifier("col_123") is True

    def test_invalid_identifiers(self):
        from kimball.processing.table_creator import _is_valid_identifier

        assert _is_valid_identifier("123col") is False
        assert _is_valid_identifier("col-name") is False
        assert _is_valid_identifier("") is False


class TestIsSafeSqlExpression:
    def test_safe_expression(self):
        from kimball.processing.table_creator import _is_safe_sql_expression

        assert _is_safe_sql_expression("col1 > 0") is True
        assert _is_safe_sql_expression("col1 = col2") is True

    def test_unsafe_expression_quotes(self):
        from kimball.processing.table_creator import _is_safe_sql_expression

        with pytest.raises(ValueError, match="quotes"):
            _is_safe_sql_expression("col1 = 'value'")

    def test_unsafe_expression_special_chars(self):
        from kimball.processing.table_creator import _is_safe_sql_expression

        assert _is_safe_sql_expression("col1; DROP TABLE t") is False


class TestAddSystemColumns:
    @patch("pyspark.sql.functions.current_timestamp")
    @patch("pyspark.sql.functions.lit")
    def test_scd1_adds_audit_columns(self, _mock_lit, _mock_ts, table_creator):
        df = MagicMock()
        df.columns = ["id", "val"]
        df.schema = MagicMock()
        field = MagicMock()
        field.name = "id"
        field.dataType = MagicMock()
        field.dataType.simpleString.return_value = "bigint"
        field.nullable = True
        df.schema.fields = [field]

        df.withColumn.return_value = df

        result = table_creator.add_system_columns(df, 1, "surrogate_key", "identity")
        assert result is df
        assert df.withColumn.call_count == 4

    @patch("pyspark.sql.functions.current_timestamp")
    @patch("pyspark.sql.functions.lit")
    def test_scd2_adds_history_columns(self, _mock_lit, _mock_ts, table_creator):
        df = MagicMock()
        df.columns = ["id"]
        df.schema = MagicMock()
        field = MagicMock()
        field.name = "id"
        field.dataType = MagicMock()
        field.dataType.simpleString.return_value = "bigint"
        field.nullable = True
        df.schema.fields = [field]

        df.withColumn.return_value = df

        result = table_creator.add_system_columns(df, 2, "surrogate_key", "identity")
        assert result is df
        assert df.withColumn.call_count >= 6


class TestCreateTableWithClustering:
    def test_skips_existing_table(self, spark_mock):
        spark_mock.catalog.tableExists.return_value = True
        schema_df = MagicMock()
        with patch(
            "kimball.processing.table_creator.get_spark", return_value=spark_mock
        ):
            from kimball.processing.table_creator import TableCreator

            table_creator = TableCreator()
            table_creator.create_table_with_clustering("db.tbl", schema_df)
        spark_mock.sql.assert_not_called()

    def test_creates_table_with_cluster_by(self, spark_mock):
        schema_df = MagicMock()
        field = MagicMock()
        field.name = "id"
        field.dataType.simpleString.return_value = "bigint NOT NULL"
        field.nullable = False
        schema_df.schema.fields = [field]

        with (
            patch(
                "kimball.processing.table_creator.get_spark", return_value=spark_mock
            ),
            patch("kimball.processing.table_creator.get_runtime_policy") as mock_policy,
        ):
            mock_policy.return_value.identity_column_def.return_value = (
                "id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY"
            )
            mock_policy.return_value.cluster_clause.return_value = " CLUSTER BY (id)"
            from kimball.processing.table_creator import TableCreator

            table_creator = TableCreator()
            table_creator.create_table_with_clustering(
                "db.tbl", schema_df, cluster_by=["id"]
            )

        assert spark_mock.sql.call_count >= 1
        create_sql = spark_mock.sql.call_args_list[0][0][0]
        assert "CREATE TABLE" in create_sql
        assert "CLUSTER BY" in create_sql
        assert "`db`.`tbl`" in create_sql


class TestEnableDeltaFeatures:
    def test_enables_features(self, spark_mock):
        with patch(
            "kimball.processing.table_creator.get_spark", return_value=spark_mock
        ):
            from kimball.processing.table_creator import TableCreator

            table_creator = TableCreator()
            table_creator.enable_delta_features("db.tbl")
        spark_mock.sql.assert_called_once()
        sql = spark_mock.sql.call_args[0][0]
        assert "TBLPROPERTIES" in sql
        assert "delta.enableDeletionVectors" in sql


class TestDeclareConstraints:
    """PK/FK constraint DDL is emitted on Databricks, skipped elsewhere."""

    def test_skips_on_non_databricks(self, spark_mock):
        with (
            patch(
                "kimball.processing.table_creator.get_spark", return_value=spark_mock
            ),
            patch("kimball.processing.table_creator.get_runtime_policy") as mock_policy,
        ):
            mock_policy.return_value.is_databricks = False
            from kimball.processing.table_creator import TableCreator

            tc = TableCreator()
            tc._declare_pk_fk_constraints(
                "gold.dim_customer",
                {
                    "surrogate_key": "customer_sk",
                    "natural_keys": ["customer_id"],
                    "scd_type": 1,
                    "foreign_keys": [],
                },
            )
        spark_mock.sql.assert_not_called()

    def test_emits_pk_on_databricks(self, spark_mock):
        with (
            patch(
                "kimball.processing.table_creator.get_spark", return_value=spark_mock
            ),
            patch("kimball.processing.table_creator.get_runtime_policy") as mock_policy,
        ):
            mock_policy.return_value.is_databricks = True
            from kimball.processing.table_creator import TableCreator

            tc = TableCreator()
            tc._declare_pk_fk_constraints(
                "gold.dim_customer",
                {
                    "surrogate_key": "customer_sk",
                    "natural_keys": ["customer_id"],
                    "scd_type": 1,
                    "foreign_keys": [],
                },
            )
        sqls = [c[0][0] for c in spark_mock.sql.call_args_list]
        assert any("PRIMARY KEY" in s and "customer_sk" in s for s in sqls)
        assert any("PRIMARY KEY" in s and "customer_id" in s for s in sqls)

    def test_skips_natural_key_pk_for_scd2(self, spark_mock):
        with (
            patch(
                "kimball.processing.table_creator.get_spark", return_value=spark_mock
            ),
            patch("kimball.processing.table_creator.get_runtime_policy") as mock_policy,
        ):
            mock_policy.return_value.is_databricks = True
            from kimball.processing.table_creator import TableCreator

            tc = TableCreator()
            tc._declare_pk_fk_constraints(
                "gold.dim_customer",
                {
                    "surrogate_key": "customer_sk",
                    "natural_keys": ["customer_id"],
                    "scd_type": 2,
                    "foreign_keys": [],
                },
            )
        sqls = [c[0][0] for c in spark_mock.sql.call_args_list]
        assert any("PRIMARY KEY" in s and "customer_sk" in s for s in sqls)
        assert not any("customer_id" in s and "PRIMARY KEY" in s for s in sqls)

    def test_emits_fk_on_databricks(self, spark_mock):
        with (
            patch(
                "kimball.processing.table_creator.get_spark", return_value=spark_mock
            ),
            patch("kimball.processing.table_creator.get_runtime_policy") as mock_policy,
        ):
            mock_policy.return_value.is_databricks = True
            from kimball.processing.table_creator import TableCreator

            tc = TableCreator()
            tc._declare_pk_fk_constraints(
                "gold.fact_orders",
                {
                    "surrogate_key": "order_sk",
                    "natural_keys": [],
                    "scd_type": 1,
                    "foreign_keys": [
                        {
                            "column": "customer_sk",
                            "references": "gold.dim_customer",
                            "dimension_key": "customer_sk",
                        }
                    ],
                },
            )
        sqls = [c[0][0] for c in spark_mock.sql.call_args_list]
        assert any("FOREIGN KEY" in s and "customer_sk" in s for s in sqls)
        assert any("REFERENCES" in s and "dim_customer" in s for s in sqls)

    def test_config_defaults_to_true(self):
        from kimball.common.config import SourceConfig, TableConfig

        cfg = TableConfig(
            table_name="test.dim",
            table_type="dimension",
            scd_type=1,
            keys={"surrogate_key": "sk", "natural_keys": ["id"]},
            sources=[SourceConfig(name="src", alias="s")],
        )
        assert cfg.declare_constraints is True
