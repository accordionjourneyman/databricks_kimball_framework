from unittest.mock import MagicMock, patch

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from kimball.processing.defaults import ensure_scd1_defaults, ensure_scd2_defaults


@patch("kimball.processing.defaults.DeltaTable")
@patch("kimball.processing.defaults.get_spark")
def test_ensure_scd2_defaults_includes_history_fields_and_system_defaults(
    mock_get_spark, mock_dt_cls
):
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    mock_get_spark.return_value = spark
    mock_dt = MagicMock()
    mock_dt_cls.forName.return_value = mock_dt

    schema = StructType([
        StructField("surrogate_key", StringType(), False),
        StructField("name", StringType(), True),
        StructField("__is_current", StringType(), True),
        StructField("__valid_from", TimestampType(), True),
        StructField("__valid_to", TimestampType(), True),
    ])

    ensure_scd2_defaults("dim_test", schema, "surrogate_key")
    assert spark.sql.call_count == 3
    first_sql = spark.sql.call_args_list[0].args[0]
    assert "INSERT INTO dim_test" in first_sql
    assert "__is_current" in first_sql
    assert "__valid_from" in first_sql


@patch("kimball.processing.defaults.DeltaTable")
@patch("kimball.processing.defaults.get_spark")
def test_ensure_scd1_defaults_preserves_non_history_system_defaults(
    mock_get_spark, mock_dt_cls
):
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    mock_get_spark.return_value = spark
    mock_dt = MagicMock()
    mock_dt_cls.forName.return_value = mock_dt

    schema = StructType([
        StructField("surrogate_key", StringType(), False),
        StructField("name", StringType(), True),
        StructField("__etl_processed_at", TimestampType(), True),
    ])

    ensure_scd1_defaults("dim_test", schema, "surrogate_key")
    assert spark.sql.call_count == 3
    first_sql = spark.sql.call_args_list[0].args[0]
    assert "INSERT INTO dim_test" in first_sql
