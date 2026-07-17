from unittest.mock import MagicMock, patch

import pytest

from kimball.common.config import ConfigLoader, TableConfig


def test_load_config_valid_fact(tmp_path):
    # Kimball: Facts do NOT have surrogate keys - they use merge_keys (degenerate dimensions)
    config_content = """
    table_name: {{ env }}_gold.fact_sales
    table_type: fact
    merge_keys: [transaction_id]
    sources:
      - name: silver.transactions
        alias: t
    """
    config_file = tmp_path / "test_config.yml"
    config_file.write_text(config_content, encoding="utf-8")

    # Load with env vars
    loader = ConfigLoader(env_vars={"env": "prod"})
    config = loader.load_config(str(config_file))

    assert config.table_name == "prod_gold.fact_sales"
    assert config.table_type == "fact"
    assert config.surrogate_key is None  # Facts have no SK
    assert config.merge_keys == ["transaction_id"]
    assert len(config.sources) == 1
    assert config.sources[0].name == "silver.transactions"


def test_load_config_valid_dimension(tmp_path):
    # Kimball: Dimensions MUST have surrogate_key and natural_keys
    config_content = """
    table_name: {{ env }}_gold.dim_customer
    table_type: dimension
    keys:
      surrogate_key: customer_sk
      natural_keys: [customer_id]
    sources:
      - name: silver.customers
        alias: c
    """
    config_file = tmp_path / "test_config.yml"
    config_file.write_text(config_content, encoding="utf-8")

    loader = ConfigLoader(env_vars={"env": "prod"})
    config = loader.load_config(str(config_file))

    assert config.table_name == "prod_gold.dim_customer"
    assert config.table_type == "dimension"
    assert config.surrogate_key == "customer_sk"
    assert config.natural_keys == ["customer_id"]


def test_load_config_dimension_missing_keys(tmp_path):
    # Kimball: Dimensions without keys should fail validation
    config_content = """
    table_name: dim_customer
    table_type: dimension
    sources:
      - name: silver.customers
    """
    config_file = tmp_path / "bad_config.yml"
    config_file.write_text(config_content, encoding="utf-8")

    loader = ConfigLoader()
    with pytest.raises(ValueError) as excinfo:
        loader.load_config(str(config_file))
    assert "surrogate_key" in str(excinfo.value)


class TestTableConfigValidation:
    def test_scd2_requires_effective_at(self):
        with pytest.raises(ValueError, match="SCD Type 2 requires 'effective_at'"):
            TableConfig(
                table_name="dim_test",
                table_type="dimension",
                surrogate_key="sk",
                natural_keys=["nk"],
                scd_type=2,
                sources=[{"name": "src", "alias": "s"}],
            )

    def test_scd2_with_effective_at_ok(self):
        config = TableConfig(
            table_name="dim_test",
            table_type="dimension",
            surrogate_key="sk",
            natural_keys=["nk"],
            scd_type=2,
            effective_at="updated_at",
            sources=[{"name": "src", "alias": "s"}],
        )
        assert config.effective_at == "updated_at"

    def test_scd4_requires_history_table(self):
        with pytest.raises(ValueError, match="SCD Type 4 requires 'history_table'"):
            TableConfig(
                table_name="dim_test",
                table_type="dimension",
                surrogate_key="sk",
                natural_keys=["nk"],
                scd_type=4,
                sources=[{"name": "src", "alias": "s"}],
            )

    def test_scd4_with_history_table_ok(self):
        config = TableConfig(
            table_name="dim_test",
            table_type="dimension",
            surrogate_key="sk",
            natural_keys=["nk"],
            scd_type=4,
            history_table="hist_table",
            sources=[{"name": "src", "alias": "s"}],
        )
        assert config.history_table == "hist_table"

    def test_scd6_requires_current_value_columns(self):
        with pytest.raises(
            ValueError, match="SCD Type 6 requires 'current_value_columns'"
        ):
            TableConfig(
                table_name="dim_test",
                table_type="dimension",
                surrogate_key="sk",
                natural_keys=["nk"],
                scd_type=6,
                sources=[{"name": "src", "alias": "s"}],
            )

    def test_scd6_with_current_value_columns_ok(self):
        config = TableConfig(
            table_name="dim_test",
            table_type="dimension",
            surrogate_key="sk",
            natural_keys=["nk"],
            scd_type=6,
            current_value_columns=["status"],
            sources=[{"name": "src", "alias": "s"}],
        )
        assert config.current_value_columns == ["status"]

    def test_fact_requires_merge_keys(self):
        with pytest.raises(ValueError, match="fact tables require merge_keys"):
            TableConfig(
                table_name="fact_test",
                table_type="fact",
                sources=[{"name": "src", "alias": "s"}],
            )

    def test_fact_with_merge_keys_ok(self):
        config = TableConfig(
            table_name="fact_test",
            table_type="fact",
            merge_keys=["order_item_id"],
            sources=[{"name": "src", "alias": "s"}],
        )
        assert config.merge_keys == ["order_item_id"]

    def test_dimension_ignores_merge_keys(self):
        config = TableConfig(
            table_name="dim_test",
            table_type="dimension",
            surrogate_key="sk",
            natural_keys=["nk"],
            sources=[{"name": "src", "alias": "s"}],
        )
        assert config.merge_keys is None

    def test_append_only_default_false(self):
        config = TableConfig(
            table_name="fact_test",
            table_type="fact",
            merge_keys=["id"],
            sources=[{"name": "src", "alias": "s"}],
        )
        assert config.append_only is False

    def test_append_only_fact_table_ok(self):
        config = TableConfig(
            table_name="fact_test",
            table_type="fact",
            merge_keys=["id"],
            append_only=True,
            sources=[{"name": "src", "alias": "s"}],
        )
        assert config.append_only is True

    def test_append_only_rejected_for_dimension(self):
        with pytest.raises(
            ValueError, match="append_only is only valid for fact tables"
        ):
            TableConfig(
                table_name="dim_test",
                table_type="dimension",
                surrogate_key="sk",
                natural_keys=["nk"],
                append_only=True,
                sources=[{"name": "src", "alias": "s"}],
            )

    def test_cdc_strategy_append_accepts(self):
        config = TableConfig(
            table_name="fact_test",
            table_type="fact",
            merge_keys=["id"],
            append_only=True,
            sources=[{"name": "src", "alias": "s", "cdc_strategy": "append"}],
        )
        assert config.sources[0].cdc_strategy == "append"

    def test_cdc_strategy_append_requires_append_only(self):
        with pytest.raises(
            ValueError, match="cdc_strategy='append' requires append_only=true"
        ):
            TableConfig(
                table_name="fact_test",
                table_type="fact",
                merge_keys=["id"],
                sources=[{"name": "src", "alias": "s", "cdc_strategy": "append"}],
            )

    def test_append_only_with_cdc_full_is_allowed(self):
        # append_only controls merge behavior; full sources may still be used for lookups
        config = TableConfig(
            table_name="fact_test",
            table_type="fact",
            merge_keys=["id"],
            append_only=True,
            sources=[{"name": "src", "alias": "s", "cdc_strategy": "full"}],
        )
        assert config.append_only is True


class TestValidateTransformationSql:
    def test_no_sql_returns_empty(self):
        config = MagicMock(spec=TableConfig)
        config.transformation_sql = None
        config.sources = []
        loader = ConfigLoader()
        assert loader.validate_transformation_sql(config) == []

    def test_empty_sql_returns_empty(self):
        config = MagicMock(spec=TableConfig)
        config.transformation_sql = ""
        config.sources = []
        loader = ConfigLoader()
        assert loader.validate_transformation_sql(config) == []

    def test_not_select_or_with(self):
        config = MagicMock(spec=TableConfig)
        config.transformation_sql = "INSERT INTO foo VALUES (1)"
        config.sources = []
        loader = ConfigLoader()
        issues = loader.validate_transformation_sql(config)
        assert any("SELECT or WITH" in i for i in issues)

    def test_missing_source_alias(self):
        config = MagicMock(spec=TableConfig)
        config.transformation_sql = "SELECT 1 AS x"
        s1 = MagicMock()
        s1.alias = "src_a"
        s2 = MagicMock()
        s2.alias = "src_b"
        config.sources = [s1, s2]
        loader = ConfigLoader()
        issues = loader.validate_transformation_sql(config)
        assert any("src_a" in i for i in issues)
        assert any("src_b" in i for i in issues)

    def test_forbidden_statements(self):
        config = MagicMock(spec=TableConfig)
        config.transformation_sql = "SELECT * FROM t; DROP TABLE t"
        config.sources = []
        loader = ConfigLoader()
        issues = loader.validate_transformation_sql(config)
        assert any("DROP" in i for i in issues)

    def test_forbidden_delete(self):
        config = MagicMock(spec=TableConfig)
        config.transformation_sql = "DELETE FROM t WHERE 1=1"
        config.sources = []
        loader = ConfigLoader()
        issues = loader.validate_transformation_sql(config)
        assert any("DELETE" in i for i in issues)

    def test_forbidden_truncate(self):
        config = MagicMock(spec=TableConfig)
        config.transformation_sql = "TRUNCATE TABLE t"
        config.sources = []
        loader = ConfigLoader()
        issues = loader.validate_transformation_sql(config)
        assert any("TRUNCATE" in i for i in issues)

    def test_forbidden_update(self):
        config = MagicMock(spec=TableConfig)
        config.transformation_sql = "UPDATE t SET x = 1"
        config.sources = []
        loader = ConfigLoader()
        issues = loader.validate_transformation_sql(config)
        assert any("UPDATE" in i for i in issues)

    def test_valid_sql_no_spark(self):
        config = MagicMock(spec=TableConfig)
        config.transformation_sql = "SELECT * FROM src_a"
        s = MagicMock()
        s.alias = "src_a"
        config.sources = [s]
        loader = ConfigLoader()
        assert loader.validate_transformation_sql(config) == []

    def test_with_spark_dry_run_success(self):
        config = MagicMock(spec=TableConfig)
        config.transformation_sql = "SELECT 1 AS x"
        config.sources = []
        spark = MagicMock()
        loader = ConfigLoader()
        with patch.object(loader, "_explain_dry_run") as mock_dry:
            issues = loader.validate_transformation_sql(config, spark)
            mock_dry.assert_called_once_with(config, spark)
            assert issues == []

    def test_with_spark_dry_run_failure(self):
        from pyspark.errors import PySparkException

        config = MagicMock(spec=TableConfig)
        config.transformation_sql = "SELECT bad"
        config.sources = []
        spark = MagicMock()
        loader = ConfigLoader()
        with patch.object(
            loader, "_explain_dry_run", side_effect=PySparkException("bad SQL")
        ):
            issues = loader.validate_transformation_sql(config, spark)
            assert any("bad SQL" in i for i in issues)


class TestExplainDryRun:
    def test_explain_dry_run_table_exists(self):
        config = MagicMock(spec=TableConfig)
        config.transformation_sql = "SELECT 1 AS x"
        s = MagicMock()
        s.name = "existing_table"
        s.alias = "e"
        config.sources = [s]

        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        spark.read.format.return_value.table.return_value.limit.return_value.createOrReplaceTempView.return_value = None

        loader = ConfigLoader()
        loader._explain_dry_run(config, spark)

        spark.catalog.tableExists.assert_called_once_with("existing_table")
        spark.read.format.assert_called_once_with("delta")
        spark.sql.assert_any_call("EXPLAIN SELECT 1 AS x")
        spark.sql.return_value.collect.assert_called_once()

    def test_explain_dry_run_table_not_exists(self):
        config = MagicMock(spec=TableConfig)
        config.transformation_sql = "SELECT 1 AS x"
        s = MagicMock()
        s.name = "missing_table"
        s.alias = "m"
        config.sources = [s]

        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        spark.createDataFrame.return_value.createOrReplaceTempView.return_value = None

        loader = ConfigLoader()
        loader._explain_dry_run(config, spark)

        spark.catalog.tableExists.assert_called_once_with("missing_table")
        spark.createDataFrame.assert_called_once_with([], schema="x int")
        spark.sql.assert_any_call("EXPLAIN SELECT 1 AS x")

    def test_explain_dry_run_alias_differs_from_view(self):
        config = MagicMock(spec=TableConfig)
        config.transformation_sql = "SELECT * FROM my_alias"
        s = MagicMock()
        s.name = "some_table"
        s.alias = "my_alias"
        config.sources = [s]

        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        spark.read.format.return_value.table.return_value.limit.return_value.createOrReplaceTempView.return_value = None

        loader = ConfigLoader()
        loader._explain_dry_run(config, spark)

        alias_sql_calls = [
            c
            for c in spark.sql.call_args_list
            if "CREATE OR REPLACE TEMP VIEW my_alias" in c[0][0]
        ]
        assert len(alias_sql_calls) == 1

    def test_explain_dry_run_cleans_up_views(self):
        config = MagicMock(spec=TableConfig)
        config.transformation_sql = "SELECT 1 AS x"
        s = MagicMock()
        s.name = "tbl"
        s.alias = "t"
        config.sources = [s]

        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        spark.read.format.return_value.table.return_value.limit.return_value.createOrReplaceTempView.return_value = None

        loader = ConfigLoader()
        loader._explain_dry_run(config, spark)

        assert spark.catalog.dropTempView.call_count >= 1

    def test_explain_dry_run_cleanup_on_exception(self):
        from pyspark.errors import PySparkException

        config = MagicMock(spec=TableConfig)
        config.transformation_sql = "SELECT bad"
        s = MagicMock()
        s.name = "tbl"
        s.alias = "t"
        config.sources = [s]

        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        spark.read.format.return_value.table.return_value.limit.return_value.createOrReplaceTempView.return_value = None
        spark.sql.side_effect = PySparkException("EXPLAIN failed")

        loader = ConfigLoader()
        with pytest.raises(PySparkException, match="EXPLAIN failed"):
            loader._explain_dry_run(config, spark)

        spark.catalog.dropTempView.assert_called()

    def test_explain_dry_run_source_exception_does_not_block(self):
        from pyspark.errors import PySparkException

        config = MagicMock(spec=TableConfig)
        config.transformation_sql = "SELECT 1 AS x"
        s1 = MagicMock()
        s1.name = "good_table"
        s1.alias = "g"
        s2 = MagicMock()
        s2.name = "bad_table"
        s2.alias = "b"
        config.sources = [s1, s2]

        spark = MagicMock()
        spark.catalog.tableExists.side_effect = [True, PySparkException("boom")]

        loader = ConfigLoader()
        loader._explain_dry_run(config, spark)

        spark.sql.assert_any_call("EXPLAIN SELECT 1 AS x")
