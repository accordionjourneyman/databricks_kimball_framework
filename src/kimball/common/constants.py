from datetime import date, datetime, timezone

# SCD Type 2 Constants
DEFAULT_VALID_FROM = datetime(1900, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
DEFAULT_VALID_TO = datetime(9999, 12, 31, 0, 0, 0, tzinfo=timezone.utc)
DEFAULT_START_DATE = date(1900, 1, 1)

# Reserved warehouse members. Real hash keys are not allowed to occupy this
# small, human-readable namespace.
DEFAULT_MEMBERS = {
    -1: ("MISSING", "Missing"),
    -2: ("NOT_APPLICABLE", "Not Applicable"),
    -3: ("NOT_YET_AVAILABLE", "Not Yet Available"),
    -4: ("BAD_VALUE", "Bad Value"),
}
RESERVED_DIMENSION_KEYS = frozenset(DEFAULT_MEMBERS)

# SQL Strings for use in Spark SQL expressions
SQL_DEFAULT_VALID_FROM = "cast('1900-01-01 00:00:00' as timestamp)"
SQL_DEFAULT_VALID_TO = "cast('9999-12-31 00:00:00' as timestamp)"

# Spark Configuration Keys
SPARK_CONF_AUTO_MERGE = "spark.databricks.delta.schema.autoMerge.enabled"
SPARK_CONF_AQE_ENABLED = "spark.sql.adaptive.enabled"
SPARK_CONF_AQE_SKEW_JOIN = "spark.sql.adaptive.skewJoin.enabled"
SPARK_CONF_AQE_COALESCE = "spark.sql.adaptive.coalescePartitions.enabled"

# JVM Performance Tuning Keys
SPARK_CONF_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
SPARK_CONF_SKEW_SIZE_THRESHOLD = (
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"
)
SPARK_CONF_SKEW_FACTOR = "spark.sql.adaptive.skewJoin.skewedPartitionFactor"
