#!/usr/bin/env bash
set -euo pipefail

# Initialize the complete Hive schema serially with Hive's bundled schema tool.
# DataNucleus auto-DDL is intentionally disabled because even one Spark session
# creates multiple ObjectStore connections that can race constraint creation.
export HIVE_HOME=/opt/hive
SCHEMA_TOOL=(
  /opt/java/openjdk/bin/java
  -cp "/opt/spark/conf:/opt/spark/jars/*"
  org.apache.hive.beeline.HiveSchemaTool
  -dbType postgres
  -userName hive
  -passWord hivepass
)
if ! "${SCHEMA_TOOL[@]}" -info; then
  "${SCHEMA_TOOL[@]}" -initSchema
fi

DELTA_PKG="io.delta:delta-spark_4.0_2.13:4.2.0"
export SPARK_JARS_IVY=/opt/spark/ivy

/opt/spark/bin/spark-sql \
  --packages "$DELTA_PKG" \
  --conf spark.jars.ivy=/opt/spark/ivy \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.hive.metastore.sharedPrefixes=org.postgresql,org.apache.spark.sql.delta,io.delta \
  --conf spark.sql.warehouse.dir=/opt/warehouse \
  --conf spark.hadoop.hive.metastore.warehouse.dir=/opt/warehouse \
  --driver-class-path /opt/spark/jars/postgresql-42.7.5.jar \
  -e "CREATE DATABASE IF NOT EXISTS dbt_baseline"
