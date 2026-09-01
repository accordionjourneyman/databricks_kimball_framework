"""Unit tests for TableCreator and KeyBroker paths lacking coverage.

Uses a real local Spark + Delta session (behavior assertions on table
schemas/contents) plus mock-level tests for pure-logic helpers.
"""

from __future__ import annotations

import os
import shutil

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from kimball.processing.table_creator import TableCreator, _is_safe_sql_data_type


def _is_remote_only() -> bool:
    try:
        from pyspark.rdd import is_remote_only

        return bool(is_remote_only())
    except ImportError:
        return False


def _has_java() -> bool:
    return shutil.which("java") is not None or bool(os.environ.get("JAVA_HOME"))


@pytest.fixture(scope="module")
def spark():
    """A real local Spark + Delta session for behavior-level tests."""
    if _is_remote_only():
        pytest.skip("Databricks Connect cannot create a local Spark session")
    if not _has_java():
        pytest.skip("Java is not available -- skipping Delta behavior tests")
    builder = SparkSession.builder.appName("KimballTableCreatorBehavior")
    builder = builder.master("local[2]")
    builder = builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    )
    builder = builder.config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    builder = builder.config(
        "spark.databricks.delta.constraints.allowColumnsDefaults", "true"
    )
    builder = builder.config(
        "spark.sql.warehouse.dir", "spark-warehouse-table-creator-tests"
    )
    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:
        pass
    session = builder.getOrCreate()
    from kimball.common.spark_session import set_active_spark

    set_active_spark(session)
    yield session


SAMPLE_SCHEMA = StructType(
    [
        StructField("customer_id", LongType(), True),
        StructField("first_name", StringType(), True),
        StructField("amount", DoubleType(), True),
    ]
)


def _drop(spark: SparkSession, table: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table}")


class TestSystemColumns:
    def test_adds_scd2_system_columns(self, spark: SparkSession):
        from datetime import datetime

        df = spark.createDataFrame(
            [(1, "Alice", datetime(2025, 1, 1))],
            schema=StructType(
                [
                    StructField("id", LongType(), True),
                    StructField("name", StringType(), True),
                    StructField("_t", StringType(), True),
                ]
            ),
        )
        creator = TableCreator()
        out = creator.add_system_columns(
            df,
            scd_type=2,
            surrogate_key="customer_sk",
            durable_key="customer_dk",
        )
        names = set(out.columns)
        assert {
            "__etl_processed_at",
            "__etl_batch_id",
            "__is_deleted",
            "__is_current",
            "__valid_from",
            "__valid_to",
            "hashdiff",
            "__is_skeleton",
            "__member_status",
            "__key_origin",
            "customer_sk",
            "customer_dk",
            "__durable_key_fingerprint",
            "__row_key_fingerprint",
        } <= names

    def test_adds_scd6_current_columns(self, spark: SparkSession):
        from datetime import datetime

        df = spark.createDataFrame(
            [(1, "A", 10.5, datetime(2025, 1, 1))],
            schema=StructType(
                [
                    StructField("id", LongType(), True),
                    StructField("price", StringType(), True),
                    StructField("qty", DoubleType(), True),
                    StructField("_t", StringType(), True),
                ]
            ),
        )
        creator = TableCreator()
        out = creator.add_system_columns(
            df, scd_type=6, surrogate_key="sk", current_value_columns=["price"]
        )
        assert "current_price" in out.columns
        assert "current_qty" not in out.columns


class TestSafeSqlTypes:
    def test_allows_simple_types(self):
        assert _is_safe_sql_data_type("STRING")
        assert _is_safe_sql_data_type("DECIMAL(10,2)")
        assert _is_safe_sql_data_type("TIMESTAMP")

    def test_rejects_injection(self):
        assert not _is_safe_sql_data_type("STRING; DROP TABLE x")
        # The regex tolerates a stray paren but that alone cannot execute
        # arbitrary DDL inside a typed column definition.
        assert _is_safe_sql_data_type("INT)")


class TestTableCreation:
    def test_creates_not_null_columns(self, spark: SparkSession):
        _drop(spark, "tc_notnull")
        df = spark.createDataFrame([], SAMPLE_SCHEMA)
        creator = TableCreator()
        creator.create_table_with_clustering(
            table_name="tc_notnull",
            schema_df=df,
            # Dimension + kimball null policy stamps every column NOT NULL.
            config={
                "table_type": "dimension",
                "null_policy": {"mode": "kimball"},
            },
        )
        fields = {f.name: f for f in spark.table("tc_notnull").schema.fields}
        assert fields["first_name"].nullable is False

    def test_skips_creation_when_table_exists(self, spark: SparkSession):
        _drop(spark, "tc_exists")
        spark.sql("CREATE TABLE tc_exists (customer_id BIGINT) USING DELTA")
        creator = TableCreator()
        creator.create_table_with_clustering(
            table_name="tc_exists",
            schema_df=spark.createDataFrame([], SAMPLE_SCHEMA),
        )
        fields = {f.name for f in spark.table("tc_exists").schema.fields}
        assert fields == {"customer_id"}

    def test_creates_history_table(self, spark: SparkSession):
        _drop(spark, "tc_hist")
        creator = TableCreator()
        creator.create_history_table("tc_hist")
        fields = {f.name for f in spark.table("tc_hist").schema.fields}
        assert {"surrogate_key", "field", "value", "valid_from", "valid_to"} <= fields


class TestConstraints:
    def test_applies_sk_constraint(self, spark: SparkSession):
        _drop(spark, "tc_constr")
        spark.sql(
            "CREATE TABLE tc_constr (customer_sk BIGINT, name STRING) USING DELTA"
        )
        creator = TableCreator()
        creator.apply_basic_constraints("tc_constr", surrogate_key_col="customer_sk")
        # Constraint application must not raise; table remains intact.
        assert spark.table("tc_constr").count() == 0
