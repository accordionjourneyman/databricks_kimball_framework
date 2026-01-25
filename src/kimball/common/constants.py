from datetime import date, datetime

# SCD Type 2 Constants
DEFAULT_VALID_FROM = datetime(1900, 1, 1, 0, 0, 0)
DEFAULT_VALID_TO = datetime(2099, 12, 31, 23, 59, 59)
DEFAULT_START_DATE = date(1900, 1, 1)

# SQL Strings for use in Spark SQL expressions
SQL_DEFAULT_VALID_FROM = "cast('1900-01-01 00:00:00' as timestamp)"
SQL_DEFAULT_VALID_TO = "cast('2099-12-31 23:59:59' as timestamp)"

# Spark Configuration Keys
SPARK_CONF_AUTO_MERGE = "spark.databricks.delta.schema.autoMerge.enabled"
SPARK_CONF_AQE_ENABLED = "spark.sql.adaptive.enabled"
SPARK_CONF_AQE_SKEW_JOIN = "spark.sql.adaptive.skewJoin.enabled"
SPARK_CONF_AQE_COALESCE = "spark.sql.adaptive.coalescePartitions.enabled"

# JVM Performance Tuning Keys
# These have massive impact on shuffle/GC pressure - don't use Spark defaults blindly
SPARK_CONF_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
SPARK_CONF_DEFAULT_PARALLELISM = "spark.default.parallelism"
SPARK_CONF_MEMORY_FRACTION = "spark.memory.fraction"
SPARK_CONF_SKEW_SIZE_THRESHOLD = "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"
SPARK_CONF_SKEW_FACTOR = "spark.sql.adaptive.skewJoin.skewedPartitionFactor"

# Recommended defaults (override via RuntimeOptions or env vars)
# 200 (Spark default) is wrong for almost everyone:
#   - Too many for small data (overhead, small files)
#   - Too few for large data (OOM, disk spill)
# Rule of thumb: 2-4x number of cores, or target 128MB-256MB per partition
DEFAULT_SHUFFLE_PARTITIONS = "auto"  # Let AQE handle it (requires AQE enabled)
DEFAULT_SKEW_SIZE_THRESHOLD = "256MB"  # Partition size that triggers skew handling
DEFAULT_SKEW_FACTOR = "5"  # Partition N times larger than median = skewed
