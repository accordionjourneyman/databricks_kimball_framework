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
