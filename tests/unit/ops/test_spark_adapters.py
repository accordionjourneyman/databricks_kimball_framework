"""Tests for Spark-backed operational adapters."""

from unittest.mock import MagicMock

from kimball.ops.spark_adapters import (
    SparkETLControlStore,
    _earliest_cdf_version,
    _extract_batch_id,
)


def test_earliest_cdf_version_uses_catalog_reader_and_skips_expired_versions():
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = [{"version": 8}, {"version": 5}]
    reader = MagicMock()
    reader.option.return_value = reader
    reader.table.return_value.limit.return_value.collect.side_effect = [
        RuntimeError("starting version is no longer available"),
        [],
    ]
    spark.read.format.return_value = reader

    assert _earliest_cdf_version(spark, "catalog.schema.source") == 8

    reader.table.assert_called_with("catalog.schema.source")
    starting_versions = [
        call.args[1]
        for call in reader.option.call_args_list
        if call.args[0] == "startingVersion"
    ]
    assert starting_versions == [5, 8]


def test_extract_batch_id_normalizes_exact_and_compound_metadata():
    assert _extract_batch_id("batch-1") == "batch-1"
    assert _extract_batch_id("workflow=gold; batch_id=batch-1 ") == "batch-1"
    assert _extract_batch_id(None) is None


def test_control_cursor_rewind_is_tagged_and_restores_spark_metadata():
    spark = MagicMock()
    spark.conf.get.return_value = "outer-metadata"
    control = MagicMock()
    control.fq_table = "ops.etl_control"
    control.spark = spark
    store = SparkETLControlStore(control)

    store.rewind_watermark_tagged(
        "gold.fact",
        "silver.orders",
        42,
        "repair_id=r1; operation_token=cursor-1",
    )

    control.rewind_to_version.assert_called_once_with("gold.fact", "silver.orders", 42)
    assert spark.conf.set.call_args_list[0].args == (
        "spark.databricks.delta.commitInfo.userMetadata",
        "repair_id=r1; operation_token=cursor-1",
    )
    assert spark.conf.set.call_args_list[-1].args == (
        "spark.databricks.delta.commitInfo.userMetadata",
        "outer-metadata",
    )
