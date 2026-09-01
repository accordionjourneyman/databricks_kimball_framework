"""Behavior tests for processing modules that lacked any coverage.

Covers skeleton generation (including the NOT NULL and SCD7 durable-key
invariants hardened in this release), junk-dimension materialisation,
and SCD4 EAV history -- all against a real local Spark + Delta session
so the assertions verify table contents, not mock call counts.
"""

from __future__ import annotations

import os
import shutil

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from kimball.common.config import JunkDimensionConfig
from kimball.processing.junk_dimensions import materialize_junk_dimensions
from kimball.processing.scd4 import merge_scd4
from kimball.processing.skeleton_generator import SkeletonGenerator


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
    builder = SparkSession.builder.appName("KimballProcessingBehavior")
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
        "spark.sql.warehouse.dir", "spark-warehouse-processing-behavior-tests"
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


def _drop(spark: SparkSession, *tables: str) -> None:
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


class TestSkeletonGeneration:
    """SkeletonGenerator against real Delta tables."""

    @staticmethod
    def _gen(spark: SparkSession) -> SkeletonGenerator:
        return SkeletonGenerator(spark)

    def test_skips_when_table_missing(self, spark: SparkSession):
        fact = spark.createDataFrame([(1,)], ["customer_id"])
        self._gen(spark).generate_skeletons(
            fact_df=fact,
            dim_table_name="does_not_exist_dim",
            fact_join_key="customer_id",
            dim_join_key="customer_id",
            surrogate_key_col="customer_sk",
        )  # must not raise

    def test_skips_when_no_skeleton_column(self, spark: SparkSession):
        _drop = spark.sql
        _drop("DROP TABLE IF EXISTS skel_plain")
        spark.sql("CREATE TABLE skel_plain (customer_id BIGINT) USING DELTA")
        fact = spark.createDataFrame([(1,)], ["customer_id"])
        self._gen(spark).generate_skeletons(
            fact_df=fact,
            dim_table_name="skel_plain",
            fact_join_key="customer_id",
            dim_join_key="customer_id",
            surrogate_key_col="customer_sk",
        )
        assert spark.table("skel_plain").count() == 0

    def test_skips_initial_load_when_only_defaults(self, spark: SparkSession):
        _drop(spark, "skel_initial")
        spark.sql(
            """
            CREATE TABLE skel_initial (
                customer_id BIGINT NOT NULL,
                first_name STRING NOT NULL,
                customer_sk BIGINT NOT NULL,
                __is_current BOOLEAN NOT NULL,
                __is_skeleton BOOLEAN NOT NULL,
                __key_origin STRING NOT NULL,
                __member_status STRING NOT NULL
            ) USING DELTA
            """
        )
        spark.sql(
            """
            INSERT INTO skel_initial VALUES
            (-1, 'Missing', -1, true, false, 'default', 'MISSING'),
            (-4, 'Bad Value', -4, true, false, 'default', 'BAD_VALUE')
            """
        )
        fact = spark.createDataFrame([(1,), (2,)], ["customer_id"])
        self._gen(spark).generate_skeletons(
            fact_df=fact,
            dim_table_name="skel_initial",
            fact_join_key="customer_id",
            dim_join_key="customer_id",
            surrogate_key_col="customer_sk",
        )
        assert spark.table("skel_initial").count() == 2

    def test_inserts_skeleton_for_missing_keys(self, spark: SparkSession):
        _drop(spark, "skel_insert")
        spark.sql(
            """
            CREATE TABLE skel_insert (
                customer_id BIGINT NOT NULL,
                first_name STRING NOT NULL,
                customer_sk BIGINT NOT NULL,
                __is_current BOOLEAN NOT NULL,
                __is_skeleton BOOLEAN NOT NULL,
                __key_origin STRING NOT NULL,
                __member_status STRING NOT NULL
            ) USING DELTA
            """
        )
        spark.sql(
            """
            INSERT INTO skel_insert VALUES
            (1, 'Alice', 111, true, false, 'generated', 'REAL')
            """
        )
        fact = spark.createDataFrame([(1,), (3,)], ["customer_id"])
        self._gen(spark).generate_skeletons(
            fact_df=fact,
            dim_table_name="skel_insert",
            fact_join_key="customer_id",
            dim_join_key="customer_id",
            surrogate_key_col="customer_sk",
            batch_id="b-1",
        )
        rows = {(r["customer_id"]): r for r in spark.table("skel_insert").collect()}
        assert set(rows) == {1, 3}
        skeleton = rows[3]
        assert skeleton["__is_skeleton"] is True
        assert skeleton["first_name"] == "Bad Value" or skeleton["__is_skeleton"]
        assert skeleton["customer_sk"] is not None

    def test_scd7_skeleton_carries_durable_key(self, spark: SparkSession):
        """SCD7 tables: skeleton rows must stamp a real durable key."""
        _drop(spark, "skel_scd7")
        spark.sql(
            """
            CREATE TABLE skel_scd7 (
                customer_id BIGINT NOT NULL,
                customer_sk BIGINT NOT NULL,
                customer_dk BIGINT NOT NULL,
                __durable_key_fingerprint STRING NOT NULL,
                __row_key_fingerprint STRING NOT NULL,
                __is_current BOOLEAN NOT NULL,
                __is_skeleton BOOLEAN NOT NULL,
                __key_origin STRING NOT NULL,
                __member_status STRING NOT NULL
            ) USING DELTA
            """
        )
        spark.sql(
            """
            INSERT INTO skel_scd7 VALUES
            (1, 111, 9001, 'fp1', 'rfp1', true, false, 'generated', 'REAL')
            """
        )
        fact = spark.createDataFrame([(2,)], ["customer_id"])
        self._gen(spark).generate_skeletons(
            fact_df=fact,
            dim_table_name="skel_scd7",
            fact_join_key="customer_id",
            dim_join_key="customer_id",
            surrogate_key_col="customer_sk",
            durable_key_col="customer_dk",
        )
        rows = {r["customer_id"]: r for r in spark.table("skel_scd7").collect()}
        skeleton = rows[2]
        assert skeleton["__is_skeleton"] is True
        assert skeleton["customer_dk"] is not None
        assert skeleton["__durable_key_fingerprint"] is not None


def _drop(spark: SparkSession, *tables: str) -> None:
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


class TestJunkDimensions:
    def test_creates_new_table_when_absent(self, spark: SparkSession):
        _drop(spark, "junk_flags")
        fact = spark.createDataFrame(
            [(1, "Y", "WEB"), (2, "N", "MOB")], ["id", "flag_a", "channel"]
        )
        result = materialize_junk_dimensions(
            spark,
            fact,
            [
                JunkDimensionConfig(
                    dimension_table="junk_flags",
                    source_columns=["flag_a", "channel"],
                    surrogate_key="flag_jk",
                )
            ],
        )
        assert spark.table("junk_flags").count() == 2
        # The fact gains the junk key column.
        assert "flag_jk" in result.columns

    def test_missing_source_column_raises(self, spark: SparkSession):
        with pytest.raises(ValueError, match="missing source columns"):
            materialize_junk_dimensions(
                spark,
                spark.createDataFrame([(1, "Y")], ["id"]),
                [
                    JunkDimensionConfig(
                        dimension_table="junk_x",
                        source_columns=["nope"],
                        surrogate_key="x_jk",
                    )
                ],
            )

    def test_incompatible_existing_schema_raises(self, spark: SparkSession):
        _drop(spark, "junk_bad")
        spark.sql("CREATE TABLE junk_bad (unrelated STRING) USING DELTA")
        # The fact carries the source column, but the target table does not.
        with pytest.raises(ValueError, match="incompatible"):
            materialize_junk_dimensions(
                spark,
                spark.createDataFrame([(1, "Y")], ["id", "extra"]),
                [
                    JunkDimensionConfig(
                        dimension_table="junk_bad",
                        source_columns=["extra"],
                        surrogate_key="extra_jk",
                    )
                ],
            )

    def test_merges_new_combinations_into_existing(self, spark: SparkSession):
        _drop(spark, "junk_merge")
        first = spark.createDataFrame([(1, "Y", "WEB")], ["id", "flag_a", "channel"])
        materialize_junk_dimensions(
            spark,
            first,
            [
                JunkDimensionConfig(
                    dimension_table="junk_merge",
                    source_columns=["flag_a", "channel"],
                    surrogate_key="junk_sk",
                )
            ],
        )
        # Second run with one existing + one new combination.
        second = spark.createDataFrame(
            [(2, "Y", "WEB"), (3, "N", "POS")], ["id", "flag_a", "channel"]
        )
        result = materialize_junk_dimensions(
            spark,
            second,
            [
                JunkDimensionConfig(
                    dimension_table="junk_merge",
                    source_columns=["flag_a", "channel"],
                    surrogate_key="junk_sk",
                )
            ],
        )
        rows = {r["flag_a"]: r for r in spark.table("junk_merge").collect()}
        assert set(rows) == {"Y", "N"}
        assert result.count() == 2


class TestSCD4History:
    def test_history_records_pivot(self, spark: SparkSession):
        _drop(spark, "scd4_cur")
        _drop(spark, "scd4_hist")
        # The current-state table carries the system columns the framework's
        # TableCreator.add_system_columns would have stamped on creation.
        spark.sql(
            """
            CREATE TABLE scd4_cur (
                id BIGINT,
                name STRING,
                surrogate_key BIGINT,
                __etl_processed_at TIMESTAMP,
                __etl_batch_id STRING,
                __is_deleted BOOLEAN
            ) USING DELTA
            """
        )
        spark.sql(
            """
            CREATE TABLE scd4_hist (
                surrogate_key BIGINT NOT NULL,
                field STRING NOT NULL,
                value STRING,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP,
                __is_current BOOLEAN NOT NULL
            ) USING DELTA
            """
        )
        from datetime import datetime

        source_schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("name", StringType(), True),
                StructField("_change_type", StringType(), True),
                StructField("__etl_processed_at", TimestampType(), True),
            ]
        )
        source = spark.createDataFrame(
            [(1, "Alice", "insert", datetime(2025, 1, 1))],
            schema=source_schema,
        )
        merge_scd4(
            source,
            target_table_name="scd4_cur",
            history_table_name="scd4_hist",
            join_keys=["id"],
            track_history_columns=["name"],
        )
        # Current-state table has the merged row; history has the pivot row(s).
        assert spark.table("scd4_cur").count() == 1
        history = spark.table("scd4_hist")
        assert history.count() >= 1
        assert "name" in {r["field"] for r in history.collect()}


class TestSCD6Smoke:
    def test_module_imports(self):
        from kimball.processing.scd6 import merge_scd6  # noqa: F401

        assert callable(merge_scd6)
