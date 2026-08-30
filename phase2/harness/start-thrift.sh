#!/usr/bin/env bash
set -euo pipefail

# Delta 4.2 publishes Spark-versioned artifacts.  The unsuffixed _2.13
# coordinate targets Spark 4.1; Spark 4.0.1 requires the _4.0_2.13 build.
# Source: https://github.com/delta-io/delta/releases/tag/v4.2.0
DELTA_PKG="io.delta:delta-spark_4.0_2.13:4.2.0"

# Ivy cache on the shared volume so resolved Maven artifacts persist/replay.
export SPARK_JARS_IVY=/opt/spark/ivy

# HiveServer2 creates isolated per-client sessions. Put Delta's own resolved
# jars on the daemon's base classpath so those sessions can instantiate the
# configured DeltaCatalog (package-only classloading is not inherited there).
for jar in /opt/spark/ivy/jars/io.delta_*.jar /opt/spark/ivy/jars/io.unitycatalog_*.jar; do
  test -f "$jar"
  ln -sf "$jar" "/opt/spark/jars/$(basename "$jar")"
done

exec /opt/spark/sbin/start-thriftserver.sh \
  --packages "$DELTA_PKG" \
  --conf spark.jars.ivy=/opt/spark/ivy \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.hive.metastore.sharedPrefixes=org.postgresql,org.apache.spark.sql.delta,io.delta \
  --conf spark.sql.warehouse.dir=/opt/warehouse \
  --conf spark.hadoop.hive.metastore.warehouse.dir=/opt/warehouse \
  --driver-class-path /opt/spark/jars/postgresql-42.7.5.jar \
  --hiveconf hive.server2.thrift.port=10000 \
  --hiveconf hive.server2.thrift.bind.host=0.0.0.0
