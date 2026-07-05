from unittest.mock import MagicMock, patch

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from kimball.processing import merger as _merger


@patch("kimball.processing.merger.DeltaTable")
@patch("kimball.common.spark_session.get_spark")
def test_ensure_scd2_defaults_includes_history_fields_and_system_defaults(
    mock_get_spark, mock_dt_cls
):
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    spark.createDataFrame.return_value = MagicMock()
    mock_get_spark.return_value = spark
    mock_dt = MagicMock()
    mock_dt_cls.forName.return_value = mock_dt

    schema = StructType(
        [
            StructField("surrogate_key", StringType(), False),
            StructField("name", StringType(), True),
            StructField("__is_current", StringType(), True),
            StructField("__valid_from", TimestampType(), True),
            StructField("__valid_to", TimestampType(), True),
        ]
    )

    _merger.ensure_scd2_defaults("dim_test", schema, "surrogate_key")

    merge_call = mock_dt.alias.return_value.merge.call_args
    assert merge_call is not None


@patch("kimball.processing.merger.DeltaTable")
@patch("kimball.common.spark_session.get_spark")
def test_ensure_scd1_defaults_preserves_non_history_system_defaults(
    mock_get_spark, mock_dt_cls
):
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    spark.createDataFrame.return_value = MagicMock()
    mock_get_spark.return_value = spark
    mock_dt = MagicMock()
    mock_dt_cls.forName.return_value = mock_dt

    schema = StructType(
        [
            StructField("surrogate_key", StringType(), False),
            StructField("name", StringType(), True),
            StructField("__etl_processed_at", TimestampType(), True),
        ]
    )

    _merger.ensure_scd1_defaults("dim_test", schema, "surrogate_key")

    merge_call = mock_dt.alias.return_value.merge.call_args
    assert merge_call is not None
